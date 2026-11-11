use crate::{
    Error, Footer, Guard, Pin, Request, Result, fault, output, require,
    source::{Source, no_symlinks},
    storage::{Root, Snapshot},
};
use ade_exec::{
    CancellationToken,
    iterative::{Executor, Mode, Options, Round},
};
use ade_physical::cycle::{BoundCycle, MAX_ROUNDS};
use serde::Serialize;
use std::{io::Write, time::Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Command {
    Explain,
    Run,
    Resume,
    Read,
    Verify,
}

#[derive(Debug, Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum Event<'a> {
    Prepared {
        pin: &'a Pin,
    },
    Selected {
        pin: &'a Pin,
        retained_bytes: u64,
        metadata_entries: usize,
    },
    Round {
        stats: &'a Round,
    },
}

#[derive(Debug, Serialize)]
pub struct Summary {
    pub plan: String,
    pub source: String,
    pub mode: Mode,
    pub source_bytes: u64,
    pub managed_state_bytes: usize,
    pub decode_batch_peak_bytes: usize,
    pub vertices: usize,
    pub directed_edges: usize,
    pub pin: Option<Pin>,
    pub terminal: Option<Footer>,
    pub rounds_executed: u32,
    pub edge_work: u64,
    pub compute_ns: u64,
    pub retained_bytes: u64,
    pub metadata_entries: usize,
}

type Observer<'a> = dyn FnMut(Event<'_>, &Guard) -> Result<()> + 'a;

/// Execute a verified local request. Only read/verify write to `output`.
/// Callback serializers own their own budgets. A caller-supplied `Write`
/// must provide its own interruptible backpressure if it can block.
///
/// # Errors
/// Refuses unsupported plans, invalid or drifting sources, foreign roots,
/// incomplete cuts, changed pins, caps, cancellation, deadline and I/O failure.
pub fn operate(
    command: Command,
    request: &Request,
    cancel: CancellationToken,
    output: &mut impl Write,
    mut observe: impl FnMut(Event<'_>, &Guard) -> Result<()>,
) -> Result<Summary> {
    require(
        request.version == 1 && request.checkpoint_every <= MAX_ROUNDS,
        "unsupported request version or cadence",
    )?;
    request.limits.validate()?;
    let guard = Guard::new(cancel, request.limits.deadline_ms);
    let cycle = request.plan.bind()?;
    let (source_id, plan) = request.inventory.identities(&request.plan)?;
    no_symlinks(&request.root)?;
    if command == Command::Run {
        require(
            request.expected.is_none(),
            "run requires a fresh root and no pin",
        )?;
    } else if command != Command::Explain {
        require(
            request.expected.is_some(),
            "an independently retained exact pin is required",
        )?;
    }
    let source = Source::bind(request, cycle, &guard)?;
    let mut summary = Summary {
        source: source_id,
        plan,
        mode: request.mode,
        source_bytes: source.bytes,
        managed_state_bytes: source.graph.managed_bytes,
        decode_batch_peak_bytes: source.batch_peak,
        vertices: source.graph.vertices().len(),
        directed_edges: source.graph.directed_edges,
        pin: None,
        terminal: None,
        rounds_executed: 0,
        edge_work: 0,
        compute_ns: 0,
        retained_bytes: 0,
        metadata_entries: 0,
    };
    if command == Command::Explain {
        return Ok(summary);
    }
    let limit = request.limits.disk_bytes;
    let mut root = if command == Command::Run {
        let root = Root::create(&request.root, &summary.plan, &summary.source, limit, &guard)?;
        selected(&root, &guard, &mut observe)?;
        root
    } else {
        let pin = request
            .expected
            .as_ref()
            .ok_or(Error::Refused("exact pin required"))?;
        Root::open(
            &request.root,
            &summary.plan,
            &summary.source,
            pin,
            limit,
            command == Command::Resume,
            &guard,
        )?
    };
    let job = Job {
        request,
        cycle,
        source,
        guard,
    };
    job.execute(command, &mut root, &mut summary, output, &mut observe)?;
    summary.pin = Some(root.pin);
    summary.retained_bytes = root.bytes;
    summary.metadata_entries = root.entries;
    Ok(summary)
}

struct Job<'a> {
    request: &'a Request,
    cycle: BoundCycle,
    source: Source<'a>,
    guard: Guard,
}

impl Job<'_> {
    fn terminal<'a>(&'a self, cut: &'a ade_exec::iterative::Cut) -> output::Terminal<'a> {
        output::Terminal {
            graph: &self.source.graph,
            cut,
            algorithm: self.cycle.algorithm(),
        }
    }

    fn execute(
        &self,
        command: Command,
        root: &mut Root,
        summary: &mut Summary,
        output: &mut impl Write,
        observe: &mut Observer<'_>,
    ) -> Result<()> {
        let options = Options {
            mode: self.request.mode,
            credits: self.request.limits.credits,
            reverse: self.request.reverse,
        };
        let executor = match root.selected.take() {
            Some(snapshot) => {
                let executor =
                    Executor::restore(&self.source.graph, self.cycle, options, snapshot.cut)?;
                if let Some(footer) = snapshot.result {
                    return self.read_terminal(command, executor.cut(), &footer, summary, output);
                }
                executor
            }
            None => Executor::start(&self.source.graph, self.cycle, options)?,
        };
        require(
            matches!(command, Command::Run | Command::Resume),
            "selected cut is provisional",
        )?;
        self.run(executor, root, summary, observe)
    }

    fn read_terminal(
        &self,
        command: Command,
        cut: &ade_exec::iterative::Cut,
        footer: &Footer,
        summary: &mut Summary,
        output: &mut impl Write,
    ) -> Result<()> {
        let terminal = self.terminal(cut);
        let (checked, bytes) = terminal.inspect(&self.guard)?;
        require(&checked == footer, "terminal output digest mismatch")?;
        self.source.attest(&self.guard)?;
        if matches!(command, Command::Read | Command::Verify) {
            terminal.write(
                footer,
                bytes,
                command == Command::Verify,
                output,
                self.request.limits.output_bytes,
                &self.guard,
            )?;
        } else if bytes > self.request.limits.output_bytes {
            return Err(Error::Limit("serialized output bytes including footer"));
        }
        summary.terminal = Some(checked);
        Ok(())
    }

    fn run(
        &self,
        mut executor: Executor<'_>,
        root: &mut Root,
        summary: &mut Summary,
        observe: &mut Observer<'_>,
    ) -> Result<()> {
        let mut since_checkpoint = 0;
        loop {
            let round = executor.cut().next_round;
            if round >= self.request.limits.rounds {
                return Err(Error::Limit("rounds before fixed point"));
            }
            fault("before_round", round, &self.guard)?;
            let started = Instant::now();
            let stats = executor.advance(&self.guard.cancel, self.guard.deadline)?;
            let nanos = u64::try_from(started.elapsed().as_nanos())
                .map_err(|_| Error::Limit("compute time counter"))?;
            summary.compute_ns = summary
                .compute_ns
                .checked_add(nanos)
                .ok_or(Error::Limit("compute time counter"))?;
            summary.edge_work += stats.edges_examined as u64;
            summary.rounds_executed += 1;
            if self.request.trace {
                observe(Event::Round { stats: &stats }, &self.guard)?;
            }
            since_checkpoint += 1;
            let cut = executor.cut();
            let terminal = stats.next_rows == 0;
            let cadence = self.request.checkpoint_every;
            if terminal || (cadence != 0 && since_checkpoint >= cadence) {
                let result = if terminal {
                    let (footer, bytes) = self.terminal(cut).inspect(&self.guard)?;
                    if bytes > self.request.limits.output_bytes {
                        return Err(Error::Limit("serialized output bytes including footer"));
                    }
                    Some(footer)
                } else {
                    None
                };
                self.source.attest(&self.guard)?;
                let snapshot = Snapshot {
                    version: 1,
                    plan: summary.plan.clone(),
                    source: summary.source.clone(),
                    cut: cut.clone(),
                    result,
                };
                root.commit(
                    &snapshot,
                    &self.guard,
                    |pin| observe(Event::Prepared { pin }, &self.guard),
                    || self.source.attest(&self.guard),
                )?;
                selected(root, &self.guard, observe)?;
                since_checkpoint = 0;
                if terminal {
                    summary.terminal = snapshot.result;
                    return Ok(());
                }
            }
        }
    }
}

fn selected(root: &Root, guard: &Guard, observe: &mut Observer<'_>) -> Result<()> {
    observe(
        Event::Selected {
            pin: &root.pin,
            retained_bytes: root.bytes,
            metadata_entries: root.entries,
        },
        guard,
    )
}
