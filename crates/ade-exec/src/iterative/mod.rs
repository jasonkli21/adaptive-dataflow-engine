//! Native finite cyclic operator runtime. Feedback is local and round advancing.
mod progress;
mod source;

use crate::{CancellationToken, Error, Result};
use ade_physical::cycle::{
    Algorithm, BoundCycle, MAX_CREDITS, MAX_EDGES, MAX_PROPOSALS, MAX_ROUNDS, MAX_VERTICES,
};
pub use progress::Credits;
use progress::{Delivery, Feedback};
use serde::{Deserialize, Serialize};
pub use source::{MAX_BATCH_ROWS, MAX_DECODED_GROUP_BYTES, MAX_PARQUET_METADATA, scan_graph_file};
use std::time::Instant;

pub const MAX_STATE_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    #[default]
    Indexed,
    ScanAll,
}

#[derive(Clone, Copy, Debug)]
pub struct Options {
    pub mode: Mode,
    pub credits: usize,
    pub reverse: bool,
}

#[derive(Debug)]
pub struct Graph {
    vertices: Vec<u64>,
    arcs: Vec<(usize, usize)>,
    offsets: Vec<usize>,
    algorithm: Algorithm,
    credit_cap: usize,
    pub directed_edges: usize,
    pub managed_bytes: usize,
}

fn invalid(message: &str) -> Error {
    Error::Invalid(message.into())
}

/// Conservative, platform-specific owned backing storage before graph construction.
/// This covers input vectors, indexed adjacency, state, both frontiers, candidate
/// slots, touched indexes, the active bitmap and the delay queue. Decoder and
/// serialization allocations are independently bounded.
#[must_use]
pub fn managed_bytes(vertices: usize, edges: usize, credits: usize) -> Option<usize> {
    if vertices > MAX_VERTICES || edges > MAX_EDGES || !(1..=MAX_CREDITS).contains(&credits) {
        return None;
    }
    Some(
        vertices * (size_of::<u64>() + 3 * size_of::<Option<u64>>() + 4 * size_of::<usize>() + 1)
            + edges * (size_of::<(u64, u64)>() + 2 * size_of::<(usize, usize)>())
            + size_of::<usize>()
            + credits * size_of::<Delivery>(),
    )
}

impl Graph {
    /// # Errors
    /// Refuses excessive backing storage and non-simple graphs or absent BFS seeds.
    pub fn new(
        mut vertices: Vec<u64>,
        mut edges: Vec<(u64, u64)>,
        cycle: BoundCycle,
        credits: usize,
        byte_cap: usize,
    ) -> Result<Self> {
        let managed_bytes = managed_bytes(vertices.capacity(), edges.capacity(), credits)
            .ok_or_else(|| invalid("graph capacity or feedback credit cap"))?;
        if managed_bytes > byte_cap.min(MAX_STATE_BYTES) {
            return Err(Error::MemoryLimit {
                requested: managed_bytes,
                current: 0,
                limit: byte_cap.min(MAX_STATE_BYTES),
            });
        }
        vertices.sort_unstable();
        edges.sort_unstable();
        if vertices.windows(2).any(|w| w[0] == w[1]) {
            return Err(invalid("duplicate vertex"));
        }
        if edges.windows(2).any(|w| w[0] == w[1]) {
            return Err(invalid("duplicate directed edge"));
        }
        let algorithm = cycle.algorithm();
        if let Algorithm::ReachabilityBfsV1 { seed } = algorithm
            && vertices.binary_search(&seed).is_err()
        {
            return Err(invalid("BFS seed is absent"));
        }
        let weak = matches!(algorithm, Algorithm::WeakComponentsMinLabelV1 {});
        let directed_edges = edges.len();
        let mut arcs = Vec::with_capacity(directed_edges * if weak { 2 } else { 1 });
        for (a, b) in edges {
            let src = vertices
                .binary_search(&a)
                .map_err(|_| invalid("unknown source endpoint"))?;
            let dst = vertices
                .binary_search(&b)
                .map_err(|_| invalid("unknown destination endpoint"))?;
            arcs.push((src, dst));
            if weak {
                arcs.push((dst, src));
            }
        }
        arcs.sort_unstable();
        let mut offsets = vec![0; vertices.len() + 1];
        for &(a, _) in &arcs {
            offsets[a + 1] += 1;
        }
        for i in 0..vertices.len() {
            offsets[i + 1] += offsets[i];
        }
        Ok(Self {
            vertices,
            arcs,
            offsets,
            algorithm,
            credit_cap: credits,
            directed_edges,
            managed_bytes,
        })
    }

    #[must_use]
    pub fn vertices(&self) -> &[u64] {
        &self.vertices
    }
}

/// Runtime state at the start of the indicated round. Checkpoint admission and
/// the externally pinned chain authenticate the cut before calling restore.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cut {
    pub next_round: u32,
    pub values: Vec<Option<u64>>,
    pub frontier: Vec<usize>,
    pub in_flight: usize,
}

#[derive(Debug, Serialize)]
pub struct Round {
    pub round: u32,
    pub feedback_round: u32,
    pub frontier_rows: usize,
    pub next_rows: usize,
    pub edges_examined: usize,
    pub proposals: usize,
    pub suppressed: usize,
    pub credits: Credits,
}

pub struct Executor<'a> {
    graph: &'a Graph,
    cycle: BoundCycle,
    options: Options,
    cut: Cut,
    candidate: Vec<Option<u64>>,
    touched: Vec<usize>,
    active: Vec<bool>,
}

impl<'a> Executor<'a> {
    /// # Errors
    /// Refuses invalid mode bounds or graph/cycle identity mismatch.
    pub fn start(graph: &'a Graph, cycle: BoundCycle, options: Options) -> Result<Self> {
        let mut cut = Cut {
            next_round: 0,
            values: vec![None; graph.vertices.len()],
            frontier: Vec::with_capacity(graph.vertices.len()),
            in_flight: 0,
        };
        match cycle.algorithm() {
            Algorithm::ReachabilityBfsV1 { seed } => {
                let index = graph
                    .vertices
                    .binary_search(&seed)
                    .map_err(|_| invalid("BFS seed is absent"))?;
                cut.values[index] = Some(0);
                cut.frontier.push(index);
            }
            Algorithm::WeakComponentsMinLabelV1 {} => {
                for (index, &id) in graph.vertices.iter().enumerate() {
                    cut.values[index] = Some(id);
                    cut.frontier.push(index);
                }
            }
        }
        Self::restore(graph, cycle, options, cut)
    }

    /// # Errors
    /// Refuses non-quiescent, structurally invalid or mismatched cuts.
    pub fn restore(
        graph: &'a Graph,
        cycle: BoundCycle,
        options: Options,
        mut cut: Cut,
    ) -> Result<Self> {
        let vertices = graph.vertices.len();
        if cycle.algorithm() != graph.algorithm
            || !(1..=graph.credit_cap).contains(&options.credits)
            || cut.in_flight != 0
            || cut.next_round > MAX_ROUNDS
            || cut.values.len() != vertices
            || cut.frontier.len() > vertices
            || cut.frontier.windows(2).any(|w| w[0] >= w[1])
            || cut
                .frontier
                .iter()
                .any(|&index| cut.values.get(index).is_none_or(Option::is_none))
        {
            return Err(invalid("invalid quiescent cyclic cut"));
        }
        match cycle.algorithm() {
            Algorithm::ReachabilityBfsV1 { seed } => {
                let index = graph
                    .vertices
                    .binary_search(&seed)
                    .map_err(|_| invalid("BFS seed is absent"))?;
                if cut.values[index] != Some(0)
                    || cut.values.iter().enumerate().any(|(i, v)| {
                        v.is_some_and(|d| d > u64::from(cut.next_round) || (d == 0 && i != index))
                    })
                    || !cut
                        .values
                        .iter()
                        .enumerate()
                        .filter_map(|(i, v)| (*v == Some(u64::from(cut.next_round))).then_some(i))
                        .eq(cut.frontier.iter().copied())
                {
                    return Err(invalid("BFS cut distances/frontier"));
                }
            }
            Algorithm::WeakComponentsMinLabelV1 {} => {
                if cut.values.iter().zip(&graph.vertices).any(|(label, &id)| {
                    label.is_none_or(|label| {
                        label > id || graph.vertices.binary_search(&label).is_err()
                    })
                }) || (cut.next_round == 0
                    && (cut.frontier.len() != vertices
                        || cut
                            .values
                            .iter()
                            .copied()
                            .ne(graph.vertices.iter().copied().map(Some))))
                {
                    return Err(invalid("component cut labels/frontier"));
                }
            }
        }
        // Do not retain caller-provided spare allocation in the runtime.
        cut.values = cut.values.into_boxed_slice().into_vec();
        let mut frontier = Vec::with_capacity(vertices);
        frontier.extend(cut.frontier);
        cut.frontier = frontier;
        Ok(Self {
            graph,
            cycle,
            options,
            cut,
            candidate: vec![None; vertices],
            touched: Vec::with_capacity(vertices),
            active: vec![false; vertices],
        })
    }

    #[must_use]
    pub fn cut(&self) -> &Cut {
        &self.cut
    }

    /// # Errors
    /// Refuses cancellation, deadline, attempted execution after a terminal cut
    /// or a round/work/credit bound. A failed executor must be discarded.
    pub fn advance(&mut self, cancel: &CancellationToken, deadline: Instant) -> Result<Round> {
        check(cancel, deadline)?;
        let round = self.cut.next_round;
        if self.cut.in_flight != 0
            || round >= MAX_ROUNDS
            || (round > 0 && self.cut.frontier.is_empty())
        {
            return Err(invalid("cyclic round cap or terminal cut"));
        }
        let next_round = self
            .cycle
            .feedback_round(round)
            .map_err(|e| Error::Invalid(e.to_string()))?;
        let frontier_rows = self.cut.frontier.len();
        self.cut.in_flight = 1;
        let (edges_examined, proposals) = self.expand(next_round, cancel, deadline)?;
        let mut feedback = Feedback::new(next_round, self.options.credits);
        self.cut.frontier.clear();
        self.touched.sort_unstable();
        for &index in &self.touched {
            if feedback.full() {
                drain(&mut feedback, &mut self.cut.frontier, cancel, deadline)?;
            }
            let value = self.candidate[index]
                .take()
                .ok_or_else(|| invalid("missing reducer candidate"))?;
            self.cut.values[index] = Some(value);
            feedback.send(index)?;
        }
        self.touched.clear();
        drain(&mut feedback, &mut self.cut.frontier, cancel, deadline)?;
        let credits = feedback.finish()?;
        self.cut.next_round = next_round;
        self.cut.in_flight = 0;
        let next_rows = self.cut.frontier.len();
        Ok(Round {
            round,
            feedback_round: next_round,
            frontier_rows,
            next_rows,
            edges_examined,
            proposals,
            suppressed: proposals
                .checked_sub(next_rows)
                .ok_or(Error::Overflow("proposal reduction"))?,
            credits,
        })
    }

    fn expand(
        &mut self,
        next_round: u32,
        cancel: &CancellationToken,
        deadline: Instant,
    ) -> Result<(usize, usize)> {
        let mut work = Work {
            examined: 0,
            proposals: 0,
            cancel,
            deadline,
        };
        let bfs = matches!(self.cycle.algorithm(), Algorithm::ReachabilityBfsV1 { .. });
        let mut reducer = Reducer {
            values: &self.cut.values,
            candidate: &mut self.candidate,
            touched: &mut self.touched,
            bfs_distance: bfs.then_some(u64::from(next_round)),
        };
        match self.options.mode {
            Mode::Indexed => {
                let frontier = &self.cut.frontier;
                for i in 0..frontier.len() {
                    let src = frontier[if self.options.reverse {
                        frontier.len() - 1 - i
                    } else {
                        i
                    }];
                    for &(_, dst) in
                        &self.graph.arcs[self.graph.offsets[src]..self.graph.offsets[src + 1]]
                    {
                        work.examine(true)?;
                        reducer.offer(src, dst)?;
                    }
                }
            }
            Mode::ScanAll => {
                for &src in &self.cut.frontier {
                    self.active[src] = true;
                }
                for &(src, dst) in &self.graph.arcs {
                    let active = self.active[src];
                    work.examine(active)?;
                    if active {
                        reducer.offer(src, dst)?;
                    }
                }
                self.active.fill(false);
            }
        }
        Ok((work.examined, work.proposals))
    }
}

struct Work<'a> {
    examined: usize,
    proposals: usize,
    cancel: &'a CancellationToken,
    deadline: Instant,
}
impl Work<'_> {
    fn examine(&mut self, active: bool) -> Result<()> {
        // Each CSR arc appears at most once per round for a canonical frontier.
        self.examined += 1;
        self.proposals += usize::from(active);
        if self.examined > MAX_PROPOSALS {
            return Err(invalid("per-round edge proposal cap"));
        }
        if self.examined.is_multiple_of(1024) {
            check(self.cancel, self.deadline)?;
        }
        Ok(())
    }
}

struct Reducer<'a> {
    values: &'a [Option<u64>],
    candidate: &'a mut [Option<u64>],
    touched: &'a mut Vec<usize>,
    bfs_distance: Option<u64>,
}
impl Reducer<'_> {
    fn offer(&mut self, src: usize, dst: usize) -> Result<()> {
        let value = self
            .bfs_distance
            .or(self.values[src])
            .ok_or_else(|| invalid("missing frontier value"))?;
        if self.values[dst].is_none_or(|existing| value < existing) {
            let slot = &mut self.candidate[dst];
            if slot.is_none() {
                self.touched.push(dst);
            }
            *slot = Some(slot.map_or(value, |previous| value.min(previous)));
        }
        Ok(())
    }
}

fn drain(
    queue: &mut Feedback,
    frontier: &mut Vec<usize>,
    cancel: &CancellationToken,
    deadline: Instant,
) -> Result<()> {
    check(cancel, deadline)?;
    while let Some(delivery) = queue.deliver()? {
        frontier.push(delivery.vertex);
        queue.ack(&delivery)?;
    }
    Ok(())
}

pub(super) fn check(cancel: &CancellationToken, deadline: Instant) -> Result<()> {
    cancel.check()?;
    if Instant::now() >= deadline {
        return Err(Error::Deadline("cyclic dataflow"));
    }
    Ok(())
}
