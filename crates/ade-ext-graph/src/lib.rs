#![cfg(all(unix, feature = "local-v1"))]
//! Weak component sizes composed from a verified cyclic terminal and native aggregation.

use ade_exec::physical::{Binding, Catalog, arrow_schema};
use ade_ext_sdk::{Digest, digest};
use ade_physical::{Field, Operator, PlanV1, Source, SourceFile, Target, Type};
use arrow::{
    array::{Int64Array, UInt64Array},
    record_batch::RecordBatch,
};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{self, Write},
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

pub use ade_iterate::{
    Algorithm, CancellationToken, Command, CyclicPlanV1, Event, Pin, Request, operate,
};

pub const MAX_STAGE_BYTES: usize = 1024 * 1024;
const MAX_TERMINAL_BYTES: usize = 512 * 1024;
const MAX_VERTICES: usize = ade_physical::cycle::MAX_VERTICES;
const NATIVE_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("component sizes refusal: {0}")]
    Refused(&'static str),
    #[error(transparent)]
    Cyclic(#[from] ade_iterate::Error),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    #[error(transparent)]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error(transparent)]
    Plan(#[from] ade_physical::Error),
    #[error(transparent)]
    Native(#[from] ade_exec::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub stage_bytes: usize,
    pub deadline: Instant,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Component {
    pub component_min_vertex: u64,
    pub size: u64,
}

#[derive(Debug, Serialize)]
pub struct Receipt {
    pub logical: Digest,
    pub plan: String,
    pub source: String,
    pub selected: Pin,
    pub terminal: ade_iterate::Footer,
    pub stage: Digest,
    pub stage_bytes: usize,
    pub stage_rows: usize,
    pub stage_bytes_cap: usize,
    pub aggregate_plan: String,
    pub aggregate_peak_managed_bytes: usize,
    pub aggregate_operators: Vec<(&'static str, usize, usize)>,
    pub result: Digest,
    pub attempt: String,
}

#[derive(Debug, Serialize)]
pub struct Output {
    pub components: Vec<Component>,
    pub receipt: Receipt,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TerminalRow {
    vertex: u64,
    component_min_vertex: u64,
}

fn validate_component_labels(vertices: &[u64], labels: &[u64]) -> Result<()> {
    if vertices.len() != labels.len() {
        return Err(Error::Refused("terminal vertex cardinality"));
    }
    let vertex_set: std::collections::BTreeSet<_> = vertices.iter().copied().collect();
    let mut minima = BTreeMap::new();
    for (&vertex, &leader) in vertices.iter().zip(labels) {
        if !vertex_set.contains(&leader) {
            return Err(Error::Refused("component leader is not a vertex"));
        }
        minima
            .entry(leader)
            .and_modify(|minimum: &mut u64| *minimum = (*minimum).min(vertex))
            .or_insert(vertex);
    }
    if minima.iter().any(|(&leader, &minimum)| leader != minimum) {
        return Err(Error::Refused("component leader is not group minimum"));
    }
    Ok(())
}

struct Guard<'a> {
    cancel: &'a CancellationToken,
    deadline: Instant,
}

impl Guard<'_> {
    fn remaining(&self) -> Result<Duration> {
        self.cancel.check()?;
        let remaining = self.deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(Error::Refused("deadline"));
        }
        Ok(remaining)
    }
}

struct Capped<'a, W> {
    writer: W,
    written: usize,
    cap: usize,
    reason: &'static str,
    guard: &'a Guard<'a>,
    hash: blake3::Hasher,
}

impl<'a, W> Capped<'a, W> {
    fn new(writer: W, cap: usize, reason: &'static str, guard: &'a Guard<'a>) -> Self {
        Self {
            writer,
            written: 0,
            cap,
            reason,
            guard,
            hash: blake3::Hasher::new(),
        }
    }
}

impl<W: Write> Write for Capped<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.guard
            .remaining()
            .map_err(|_| io::Error::other("cancelled or deadline elapsed"))?;
        if bytes.len() > self.cap - self.written {
            return Err(io::Error::other(self.reason));
        }
        let written = self.writer.write(bytes)?;
        self.hash.update(&bytes[..written]);
        self.written += written;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.guard
            .remaining()
            .map_err(|_| io::Error::other("cancelled or deadline elapsed"))?;
        self.writer.flush()
    }
}

fn verify_rows(
    bytes: &[u8],
    summary: &ade_iterate::Summary,
    guard: &Guard<'_>,
) -> Result<Vec<u64>> {
    let terminal = summary
        .terminal
        .as_ref()
        .ok_or(Error::Refused("missing complete terminal"))?;
    if terminal.rows != summary.vertices || terminal.rows > MAX_VERTICES {
        return Err(Error::Refused("terminal row count"));
    }
    let mut lines = bytes.split_inclusive(|&byte| byte == b'\n').peekable();
    let mut hash = blake3::Hasher::new();
    hash.update(b"ade-cycle20-components-result-v1\0");
    let mut previous = None;
    let mut labels = Vec::with_capacity(terminal.rows);
    let mut vertices = Vec::with_capacity(terminal.rows);
    for _ in 0..terminal.rows {
        guard.remaining()?;
        let line = lines
            .next()
            .ok_or(Error::Refused("truncated terminal rows"))?;
        let row: TerminalRow = serde_json::from_slice(line)?;
        if previous.is_some_and(|value| value >= row.vertex)
            || row.vertex < row.component_min_vertex
        {
            return Err(Error::Refused("terminal row identity order"));
        }
        hash.update(line);
        vertices.push(row.vertex);
        labels.push(row.component_min_vertex);
        previous = Some(row.vertex);
    }
    let trailer = lines
        .next()
        .ok_or(Error::Refused("missing terminal footer"))?;
    let footer: ade_iterate::Footer = serde_json::from_slice(trailer)?;
    if !trailer.ends_with(b"\n")
        || lines.peek().is_some()
        || &footer != terminal
        || footer.kind != "complete"
        || footer.result != hash.finalize().to_hex().as_str()
    {
        return Err(Error::Refused("terminal footer or digest mismatch"));
    }
    validate_component_labels(&vertices, &labels)?;
    Ok(labels)
}

#[cfg(feature = "fault-injection")]
fn stage_fault(stage: &Path, guard: &Guard<'_>) -> Result<()> {
    if let Some(path) = std::env::var_os("ADE24_TEST_STAGE_CONTINUE") {
        writeln!(
            io::stderr().lock(),
            "ade24 before aggregate {}",
            stage.display()
        )?;
        while !Path::new(&path).is_file() {
            guard.remaining()?;
            std::thread::sleep(Duration::from_millis(2));
        }
    }
    Ok(())
}

struct Aggregated {
    components: Vec<Component>,
    stage: Digest,
    bytes: usize,
    plan: String,
    managed: usize,
    operators: Vec<(&'static str, usize, usize)>,
}

fn aggregate(
    stage: &Path,
    labels: Vec<u64>,
    limits: Limits,
    guard: &Guard<'_>,
) -> Result<Aggregated> {
    let rows = labels.len();
    let fields = vec![
        Field::new("key", Type::U64, false),
        Field::new("value_i64", Type::I64, false),
    ];
    let schema = arrow_schema(&fields);
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(labels)),
            Arc::new(Int64Array::from(vec![1; rows])),
        ],
    )?;
    let file = File::options()
        .read(true)
        .write(true)
        .create_new(true)
        .open(stage)?;
    let props = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_max_row_group_row_count(Some(256))
        .build();
    let mut writer = ArrowWriter::try_new(
        Capped::new(&file, limits.stage_bytes, "stage byte cap", guard),
        schema,
        Some(props),
    )?;
    writer.write(&batch)?;
    let capped = writer.into_inner()?;
    let stage_bytes = capped.written;
    let stage_digest = Digest::from_bytes(*capped.hash.finalize().as_bytes());
    #[cfg(feature = "fault-injection")]
    stage_fault(stage, guard)?;
    drop(batch);
    let source = Source {
        id: 0,
        table: "input".into(),
        manifest: stage_digest.hex(),
        logical: stage_digest.hex(),
        schema_identity: digest(&serde_json::to_vec(&fields)?).hex(),
        schema: fields,
        files: vec![SourceFile {
            name: "labels.parquet".into(),
            bytes: stage_bytes as u64,
            rows: rows as u64,
            fingerprint: stage_digest.hex(),
        }],
        replayable: true,
    };
    let mut plan = PlanV1::new(Target::Local, vec![source.clone()]);
    let input = plan.push(Operator::Scan { source: 0 })?;
    plan.push(Operator::GroupSumCount {
        input,
        key: 0,
        value: 1,
    })?;
    plan.max_output_rows = rows as u64;
    plan.max_output_bytes = MAX_STAGE_BYTES as u64;
    let aggregate_plan = plan.digest()?;
    let catalog = Catalog {
        bindings: vec![Binding {
            source,
            files: vec![stage.into()],
        }],
    };
    let output = ade_exec::physical::execute(
        &plan,
        &catalog,
        ade_exec::ExecutionConfig {
            memory_limit_bytes: NATIVE_BYTES,
            spill_enabled: false,
            prefetch_batches: 0,
            output_batch_rows: 256,
        },
        256,
        guard.cancel.clone(),
        guard.remaining()?,
    )?;
    let components = collect(&output, rows, guard)?;
    let operators = output
        .metrics
        .operators
        .values()
        .map(|metric| (metric.name, metric.input_rows, metric.output_rows))
        .collect();
    Ok(Aggregated {
        components,
        stage: stage_digest,
        bytes: stage_bytes,
        plan: aggregate_plan,
        managed: output.metrics.managed.peak,
        operators,
    })
}

fn collect(
    output: &ade_exec::ExecutionOutput,
    rows: usize,
    guard: &Guard<'_>,
) -> Result<Vec<Component>> {
    let mut counts = BTreeMap::new();
    let mut total = 0u64;
    for batch in &output.batches {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or(Error::Refused("aggregate key"))?;
        let sums = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or(Error::Refused("aggregate sum"))?;
        let cardinalities = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or(Error::Refused("aggregate count"))?;
        for ((&key, &sum), &count) in keys
            .values()
            .iter()
            .zip(sums.values())
            .zip(cardinalities.values())
        {
            guard.remaining()?;
            if sum != count || count <= 0 {
                return Err(Error::Refused("aggregate sum/count mismatch"));
            }
            let size =
                u64::try_from(count).map_err(|_| Error::Refused("aggregate negative count"))?;
            total = total
                .checked_add(size)
                .ok_or(Error::Refused("aggregate count overflow"))?;
            if counts.insert(key, size).is_some() {
                return Err(Error::Refused("duplicate aggregate key"));
            }
        }
    }
    if total != rows as u64
        || !output
            .metrics
            .operators
            .values()
            .any(|m| m.name == "hash-aggregate")
    {
        return Err(Error::Refused("aggregate route or cardinality"));
    }
    Ok(counts
        .into_iter()
        .map(|(component_min_vertex, size)| Component {
            component_min_vertex,
            size,
        })
        .collect())
}

fn limit_native_deadline(request: &mut Request, original_ms: u64, guard: &Guard<'_>) -> Result<()> {
    let remaining_ms =
        u64::try_from(guard.remaining()?.as_millis()).map_err(|_| Error::Refused("deadline"))?;
    if remaining_ms == 0 {
        return Err(Error::Refused("deadline"));
    }
    request.limits.deadline_ms = original_ms.min(remaining_ms);
    Ok(())
}

fn validate_stage_parent(stage_parent: &Path, request: &Request) -> Result<()> {
    if stage_parent.as_os_str().len() > 4096
        || !stage_parent.is_absolute()
        || stage_parent.canonicalize()? != stage_parent
        || stage_parent.starts_with(&request.root)
        || stage_parent.starts_with(&request.source_dir)
        || !fs::metadata(stage_parent)?.is_dir()
    {
        return Err(Error::Refused("canonical stage parent required"));
    }
    Ok(())
}

/// Reduce the complete terminal selected by the caller's independently retained pin.
/// The caller owns the Phase 20 root and the private stage parent. After process
/// interruption, an operator may inspect/remove orphan `ade24-stage-*` directories.
///
/// # Errors
/// Refuses a missing, provisional, foreign or corrupt terminal, an unsupported
/// graph plan or unsafe stage path, and any resource, cancellation or I/O failure.
pub fn component_sizes_v1(
    request: &Request,
    stage_parent: &Path,
    limits: Limits,
    cancel: &CancellationToken,
) -> Result<Output> {
    let guard = Guard {
        cancel,
        deadline: limits.deadline,
    };
    if limits.stage_bytes == 0
        || limits.stage_bytes > MAX_STAGE_BYTES
        || guard.remaining()? > Duration::from_secs(120)
        || !(1..=120_000).contains(&request.limits.deadline_ms)
        || request.limits.output_bytes == 0
        || request.limits.output_bytes > ade_iterate::MAX_OUTPUT_BYTES
    {
        return Err(Error::Refused("composition limits"));
    }
    if !matches!(
        request.plan.bind()?.algorithm(),
        Algorithm::WeakComponentsMinLabelV1 {}
    ) {
        return Err(Error::Refused("weak components plan required"));
    }
    let expected = request
        .expected
        .as_ref()
        .ok_or(Error::Refused("independently retained pin required"))?;
    validate_stage_parent(stage_parent, request)?;
    // Validate the inventory before cloning it into the effective read request.
    request
        .inventory
        .admit()
        .map_err(ade_iterate::Error::from)?;
    let native_deadline_ms = request.limits.deadline_ms;
    let mut request = request.clone();
    limit_native_deadline(&mut request, native_deadline_ms, &guard)?;
    request.limits.output_bytes = request.limits.output_bytes.min(MAX_TERMINAL_BYTES);
    let mut raw = Capped::new(
        Vec::new(),
        request.limits.output_bytes,
        "terminal byte cap",
        &guard,
    );
    let summary = operate(Command::Read, &request, cancel.clone(), &mut raw, |_, _| {
        Ok(())
    })?;
    if summary.pin.as_ref() != Some(expected) {
        return Err(Error::Refused("changed terminal pin"));
    }
    let labels = verify_rows(&raw.writer, &summary, &guard)?;
    let stage_rows = labels.len();
    drop(raw);
    let directory = tempfile::Builder::new()
        .prefix("ade24-stage-")
        .tempdir_in(stage_parent)?;
    let aggregated = aggregate(
        &directory.path().join("labels.parquet"),
        labels,
        limits,
        &guard,
    )?;
    directory.close()?;
    limit_native_deadline(&mut request, native_deadline_ms, &guard)?;
    let confirmed = operate(
        Command::Verify,
        &request,
        cancel.clone(),
        &mut io::sink(),
        |_, _| Ok(()),
    )?;
    if confirmed.pin.as_ref() != Some(expected) || confirmed.terminal != summary.terminal {
        return Err(Error::Refused("terminal changed during composition"));
    }
    guard.remaining()?;
    let terminal = summary
        .terminal
        .ok_or(Error::Refused("missing complete terminal"))?;
    let logical = digest(&serde_json::to_vec(&(
        "component_sizes_v1",
        &summary.source,
        &summary.plan,
    ))?);
    let result = digest(&serde_json::to_vec(&aggregated.components)?);
    Ok(Output {
        receipt: Receipt {
            logical,
            plan: summary.plan,
            source: summary.source,
            selected: expected.clone(),
            terminal,
            stage: aggregated.stage,
            stage_bytes: aggregated.bytes,
            stage_rows,
            stage_bytes_cap: limits.stage_bytes,
            aggregate_plan: aggregated.plan,
            aggregate_peak_managed_bytes: aggregated.managed,
            aggregate_operators: aggregated.operators,
            result,
            attempt: uuid::Uuid::new_v4().to_string(),
        },
        components: aggregated.components,
    })
}

#[cfg(test)]
mod review_tests {
    use super::validate_component_labels;

    #[test]
    fn rejects_foreign_or_nonminimum_component_leaders() {
        assert!(validate_component_labels(&[10, 20], &[9, 9]).is_err());
        assert!(validate_component_labels(&[10, 20], &[20, 20]).is_err());
        assert!(validate_component_labels(&[10, 20], &[10, 10]).is_ok());
        assert!(validate_component_labels(&[10, 20], &[10]).is_err());
    }
}
