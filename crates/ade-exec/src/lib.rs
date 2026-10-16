//! Provisional single-node pull execution with managed memory and local spill.
mod aggregate;
mod ipc_scan;
pub mod memory;
mod scan;
mod sort;
mod spill;
use arrow::{
    array::{Array, BooleanArray, UInt64Array},
    compute::filter_record_batch,
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use memory::{ManagedMetrics, MemoryManager, Reservation};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
pub use scan::PrefetchMetrics;
use spill::SpillManager;
pub use spill::SpillMetrics;
use std::{
    collections::BTreeMap,
    fs::File,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

/// Structured local execution failures.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("Parquet: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("Arrow: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("invalid local plan/schema: {0}")]
    Invalid(String),
    #[error("arithmetic overflow: {0}")]
    Overflow(&'static str),
    #[error("managed memory limit: requested {requested}, current {current}, limit {limit}")]
    MemoryLimit {
        requested: usize,
        current: usize,
        limit: usize,
    },
    #[error("managed state target reached with spill disabled")]
    SpillDisabled,
    #[error("prefetch producer failed")]
    PrefetchFailed,
    #[error("spill cannot make progress: {0}")]
    SpillNoProgress(&'static str),
    #[error("corrupt internal spill: {0}")]
    CorruptSpill(&'static str),
    #[error("execution cancelled")]
    Cancelled,
}
/// Runtime result.
pub type Result<T> = std::result::Result<T, Error>;

/// Deterministic postorder plan position (zero based).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct OperatorId(pub u32);

/// Cloneable cancellation handle; checked between batches and files.
#[derive(Debug, Clone, Default)]
pub struct CancellationToken(Arc<AtomicBool>);
impl CancellationToken {
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Relaxed);
    }
    /// # Errors
    /// Returns `Cancelled` after cancellation was requested.
    pub fn check(&self) -> Result<()> {
        if self.0.load(Ordering::Relaxed) {
            Err(Error::Cancelled)
        } else {
            Ok(())
        }
    }
}

/// Observational byte accounting, never an allocator or a hard limit.
#[derive(Debug, Clone, Default)]
pub struct MemoryTracker {
    pub current: usize,
    pub peak: usize,
}
impl MemoryTracker {
    /// # Errors
    /// Rejects counter overflow without changing the tracker.
    pub fn reserve(&mut self, bytes: usize) -> Result<()> {
        self.current = self
            .current
            .checked_add(bytes)
            .ok_or(Error::Overflow("tracked memory"))?;
        self.peak = self.peak.max(self.current);
        Ok(())
    }
    /// # Errors
    /// Rejects releases larger than the current tracked amount.
    pub fn release(&mut self, bytes: usize) -> Result<()> {
        self.current = self
            .current
            .checked_sub(bytes)
            .ok_or_else(|| Error::Invalid("memory accounting underflow".into()))?;
        Ok(())
    }
}
/// Exclusive operator work time excludes upstream pulls. Memory is an estimate.
#[derive(Debug, Clone, Default)]
pub struct OperatorMetrics {
    pub name: &'static str,
    pub input_rows: usize,
    pub input_batches: usize,
    pub output_rows: usize,
    pub output_batches: usize,
    pub work_time: Duration,
    pub memory: MemoryTracker,
}
/// Collected output remains tracked when execution returns.
#[derive(Debug, Clone, Default)]
pub struct ExecutionMetrics {
    pub operators: BTreeMap<OperatorId, OperatorMetrics>,
    pub output_rows: usize,
    pub output_batches: usize,
    pub memory: MemoryTracker,
    pub managed: ManagedMetrics,
    pub spill: SpillMetrics,
    pub prefetch: PrefetchMetrics,
}
/// Real local runtime services, scoped to one execution.
#[derive(Debug)]
pub struct ExecContext {
    pub cancellation: CancellationToken,
    pub metrics: ExecutionMetrics,
    config: ExecutionConfig,
    memory: MemoryManager,
    spill: SpillManager,
}
/// Finite managed budget, not an allocator or RSS cap.
#[derive(Debug, Clone)]
pub struct ExecutionConfig {
    pub memory_limit_bytes: usize,
    pub spill_enabled: bool,
    pub prefetch_batches: usize,
    pub output_batch_rows: usize,
}
impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            memory_limit_bytes: 256 * 1024 * 1024,
            spill_enabled: true,
            prefetch_batches: 0,
            output_batch_rows: 1024,
        }
    }
}
impl ExecutionConfig {
    /// # Errors
    /// Rejects zero limits/batch sizes and prefetch depths above 64.
    pub fn validate(&self) -> Result<()> {
        if self.memory_limit_bytes == 0 || self.output_batch_rows == 0 || self.prefetch_batches > 64
        {
            return Err(Error::Invalid(
                "positive memory/output size and prefetch depth <= 64 required".into(),
            ));
        }
        Ok(())
    }
}
impl ExecContext {
    fn new(
        config: ExecutionConfig,
        cancellation: CancellationToken,
        parent: Option<&std::path::Path>,
    ) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            memory: MemoryManager::new(config.memory_limit_bytes)?,
            spill: SpillManager::new(parent)?,
            config,
            cancellation,
            metrics: ExecutionMetrics::default(),
        })
    }

    fn memory(&mut self, id: OperatorId, bytes: usize) -> Result<()> {
        let previous = self.metrics.operators[&id].memory.current;
        self.metrics.memory.release(previous)?;
        self.metrics.memory.reserve(bytes)?;
        let m = &mut self
            .metrics
            .operators
            .get_mut(&id)
            .expect("compiled operator")
            .memory;
        m.current = bytes;
        m.peak = m.peak.max(bytes);
        Ok(())
    }
    fn work(&mut self, id: OperatorId, start: Instant) {
        self.metrics
            .operators
            .get_mut(&id)
            .expect("compiled operator")
            .work_time += start.elapsed();
    }
    fn input(&mut self, id: OperatorId, batch: &RecordBatch) {
        let m = self
            .metrics
            .operators
            .get_mut(&id)
            .expect("compiled operator");
        m.input_batches += 1;
        m.input_rows += batch.num_rows();
    }
}

/// Small local tree, deliberately not a stable general physical IR.
#[derive(Debug, Clone)]
pub enum LocalPlan {
    /// Incremental Arrow IPC streams with identical schemas.
    IpcScan {
        files: Vec<PathBuf>,
    },
    /// Merge non-null key/sum/count partial aggregates using the spillable table.
    MergeAggregate {
        input: Box<Self>,
    },
    ParquetScan {
        files: Vec<PathBuf>,
        batch_size: usize,
    },
    Filter {
        input: Box<Self>,
        column: String,
        threshold: u64,
    },
    Project {
        input: Box<Self>,
        columns: Vec<String>,
    },
    HashAggregate {
        input: Box<Self>,
        key: String,
        value: String,
    },
    /// Narrow non-null `UInt64` key/row-id, `Int64` value sort; emits these columns.
    Sort {
        input: Box<Self>,
        key: String,
        row_id: String,
        value: String,
    },
    Limit {
        input: Box<Self>,
        rows: usize,
    },
}

struct Node {
    id: OperatorId,
    schema: SchemaRef,
    kind: Kind,
    closed: bool,
}
enum Kind {
    Scan(scan::Scan),
    IpcScan(ipc_scan::Scan),
    Filter {
        input: Box<Node>,
        column: usize,
        threshold: u64,
    },
    Project {
        input: Box<Node>,
        columns: Vec<usize>,
    },
    Aggregate {
        input: Box<Node>,
        key: usize,
        value: usize,
        count: Option<usize>,
        state: Option<aggregate::Aggregate>,
    },
    Sort {
        input: Box<Node>,
        key: usize,
        row_id: usize,
        value: usize,
        state: Option<sort::Sort>,
    },
    Limit {
        input: Box<Node>,
        remaining: usize,
    },
}
fn column(schema: &Schema, name: &str, data_type: &DataType) -> Result<usize> {
    let index = schema.index_of(name)?;
    if schema.field(index).data_type() != data_type {
        return Err(Error::Invalid(format!(
            "{name} must have type {data_type:?}"
        )));
    }
    Ok(index)
}
fn aggregate_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::UInt64, false),
        Field::new("sum", DataType::Int64, false),
        Field::new("count", DataType::UInt64, false),
    ]))
}
impl Node {
    #[allow(
        clippy::too_many_lines,
        reason = "Exhaustive local plan compilation keeps IDs and validation together."
    )]
    fn build(plan: LocalPlan, cx: &mut ExecContext) -> Result<Self> {
        cx.cancellation.check()?;
        let (schema, kind, name) = match plan {
            LocalPlan::IpcScan { files } => {
                let scan = ipc_scan::Scan::new(files)?;
                (scan.schema.clone(), Kind::IpcScan(scan), "ipc-scan")
            }
            LocalPlan::MergeAggregate { input } => {
                let input = Box::new(Self::build(*input, cx)?);
                let key = column(&input.schema, "key", &DataType::UInt64)?;
                let value = column(&input.schema, "sum", &DataType::Int64)?;
                let count = column(&input.schema, "count", &DataType::UInt64)?;
                (
                    aggregate_schema(),
                    Kind::Aggregate {
                        input,
                        key,
                        value,
                        count: Some(count),
                        state: None,
                    },
                    "merge-aggregate",
                )
            }
            LocalPlan::ParquetScan { files, batch_size } => {
                if batch_size == 0 || files.is_empty() {
                    return Err(Error::Invalid(
                        "scan needs files and positive batch size".into(),
                    ));
                }
                let schema = ParquetRecordBatchReaderBuilder::try_new(File::open(&files[0])?)?
                    .schema()
                    .clone();
                (
                    schema.clone(),
                    Kind::Scan({
                        let scan = scan::SyncScan::new(files, schema.clone(), batch_size);
                        if cx.config.prefetch_batches == 0 {
                            scan::Scan::Sync(scan)
                        } else {
                            scan::Scan::Prefetch(scan::Prefetch::start(
                                scan,
                                cx.config.prefetch_batches,
                                cx.cancellation.clone(),
                                cx.memory.clone(),
                                OperatorId(
                                    u32::try_from(cx.metrics.operators.len())
                                        .map_err(|_| Error::Overflow("operator ID"))?,
                                ),
                            )?)
                        }
                    }),
                    "parquet-scan",
                )
            }
            LocalPlan::Filter {
                input,
                column: name,
                threshold,
            } => {
                let input = Box::new(Self::build(*input, cx)?);
                let column = column(&input.schema, &name, &DataType::UInt64)?;
                (
                    input.schema.clone(),
                    Kind::Filter {
                        input,
                        column,
                        threshold,
                    },
                    "filter",
                )
            }
            LocalPlan::Project { input, columns } => {
                let input = Box::new(Self::build(*input, cx)?);
                let columns = columns
                    .iter()
                    .map(|n| input.schema.index_of(n))
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                let schema = Arc::new(input.schema.project(&columns)?);
                (schema, Kind::Project { input, columns }, "project")
            }
            LocalPlan::HashAggregate { input, key, value } => {
                let input = Box::new(Self::build(*input, cx)?);
                let key = column(&input.schema, &key, &DataType::UInt64)?;
                let value = column(&input.schema, &value, &DataType::Int64)?;
                (
                    aggregate_schema(),
                    Kind::Aggregate {
                        input,
                        key,
                        value,
                        count: None,
                        state: None,
                    },
                    "hash-aggregate",
                )
            }
            LocalPlan::Sort {
                input,
                key,
                row_id,
                value,
            } => {
                let input = Box::new(Self::build(*input, cx)?);
                let key = column(&input.schema, &key, &DataType::UInt64)?;
                let row_id = column(&input.schema, &row_id, &DataType::UInt64)?;
                let value = column(&input.schema, &value, &DataType::Int64)?;
                let schema = Arc::new(input.schema.project(&[key, row_id, value])?);
                (
                    schema,
                    Kind::Sort {
                        input,
                        key,
                        row_id,
                        value,
                        state: None,
                    },
                    "external-sort",
                )
            }
            LocalPlan::Limit { input, rows } => {
                let input = Box::new(Self::build(*input, cx)?);
                (
                    input.schema.clone(),
                    Kind::Limit {
                        input,
                        remaining: rows,
                    },
                    "limit",
                )
            }
        };
        let id = OperatorId(
            u32::try_from(cx.metrics.operators.len())
                .map_err(|_| Error::Overflow("operator ID"))?,
        );
        cx.metrics.operators.insert(
            id,
            OperatorMetrics {
                name,
                ..Default::default()
            },
        );
        Ok(Self {
            id,
            schema,
            kind,
            closed: false,
        })
    }
    #[allow(
        clippy::too_many_lines,
        reason = "Exhaustive pull dispatch shares metric and cancellation boundaries."
    )]
    fn next_batch(&mut self, cx: &mut ExecContext) -> Result<Option<RecordBatch>> {
        if self.closed {
            return Ok(None);
        }
        cx.cancellation.check()?;
        // Release the previous observed output when downstream asks for another batch.
        cx.memory(self.id, 0)?;
        let output = match &mut self.kind {
            Kind::IpcScan(scan) => scan.next()?,
            Kind::Scan(scan) => {
                let start = Instant::now();
                let result = scan.next(&cx.cancellation)?;
                if let scan::Scan::Prefetch(scan) = scan {
                    cx.metrics.prefetch = scan.metrics();
                }
                cx.work(self.id, start);
                result
            }
            Kind::Filter {
                input,
                column,
                threshold,
            } => {
                let Some(batch) = input.next_batch(cx)? else {
                    input.shutdown(cx)?;
                    return Ok(None);
                };
                cx.input(self.id, &batch);
                let start = Instant::now();
                let values = batch
                    .column(*column)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| Error::Invalid("filter type".into()))?;
                // SQL-like null predicate semantics: nulls are excluded.
                let mask = BooleanArray::from_iter(
                    (0..values.len())
                        .map(|i| Some(!values.is_null(i) && values.value(i) < *threshold)),
                );
                let result = filter_record_batch(&batch, &mask)?;
                cx.work(self.id, start);
                Some(result)
            }
            Kind::Project { input, columns } => {
                let Some(batch) = input.next_batch(cx)? else {
                    input.shutdown(cx)?;
                    return Ok(None);
                };
                cx.input(self.id, &batch);
                let start = Instant::now();
                let result = batch.project(columns)?;
                cx.work(self.id, start);
                Some(result)
            }
            Kind::Limit { input, remaining } => {
                if *remaining == 0 {
                    input.shutdown(cx)?;
                    return Ok(None);
                }
                let Some(batch) = input.next_batch(cx)? else {
                    input.shutdown(cx)?;
                    return Ok(None);
                };
                cx.input(self.id, &batch);
                let start = Instant::now();
                let rows = (*remaining).min(batch.num_rows());
                *remaining -= rows;
                let result = batch.slice(0, rows);
                cx.work(self.id, start);
                if *remaining == 0 {
                    input.shutdown(cx)?;
                }
                Some(result)
            }
            Kind::Aggregate {
                input,
                key,
                value,
                count,
                state,
            } => {
                if state.is_none() {
                    *state = Some(aggregate::Aggregate::build(
                        input, *key, *value, *count, self.id, cx,
                    )?);
                }
                let start = Instant::now();
                let result = state
                    .as_mut()
                    .expect("built")
                    .next(&self.schema, self.id, cx)?;
                cx.work(self.id, start);
                result
            }
            Kind::Sort {
                input,
                key,
                row_id,
                value,
                state,
            } => {
                if state.is_none() {
                    *state = Some(sort::Sort::build(
                        input,
                        [*key, *row_id, *value],
                        &self.schema,
                        self.id,
                        cx,
                    )?);
                }
                let start = Instant::now();
                let result = state
                    .as_mut()
                    .expect("built")
                    .next(&self.schema, self.id, cx)?;
                cx.work(self.id, start);
                result
            }
        };
        if let Some(batch) = &output {
            cx.memory(self.id, batch.get_array_memory_size())?;
            let m = cx
                .metrics
                .operators
                .get_mut(&self.id)
                .expect("compiled operator");
            m.output_rows += batch.num_rows();
            m.output_batches += 1;
        }
        Ok(output)
    }

    /// Retire this subtree once no later pull can use it. Safe to call repeatedly.
    fn shutdown(&mut self, cx: &mut ExecContext) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        match &mut self.kind {
            Kind::Scan(scan) => cx.metrics.prefetch = scan.shutdown(),
            Kind::IpcScan(scan) => scan.shutdown(),
            Kind::Filter { input, .. }
            | Kind::Project { input, .. }
            | Kind::Limit { input, .. } => input.shutdown(cx)?,
            Kind::Aggregate { input, state, .. } => {
                state.take();
                input.shutdown(cx)?;
            }
            Kind::Sort { input, state, .. } => {
                state.take();
                input.shutdown(cx)?;
            }
        }
        cx.memory(self.id, 0)?;
        self.closed = true;
        Ok(())
    }
}
/// Collected bounded output and observational execution metrics.
#[derive(Debug)]
pub struct ExecutionOutput {
    pub batches: Vec<RecordBatch>,
    pub metrics: ExecutionMetrics,
    _retained: Reservation,
}
/// Execute a provisional local plan to completion. Cancellation never returns partial success.
///
/// # Errors
/// Returns invalid-plan/schema, I/O, Arrow/Parquet, overflow, or cancellation errors.
pub fn execute(plan: LocalPlan, cancellation: CancellationToken) -> Result<ExecutionOutput> {
    execute_with_config(plan, ExecutionConfig::default(), cancellation)
}
/// Execute with finite managed resources.
/// # Errors
/// Returns structured plan, resource, I/O, arithmetic and cancellation failures.
pub fn execute_with_config(
    plan: LocalPlan,
    config: ExecutionConfig,
    cancellation: CancellationToken,
) -> Result<ExecutionOutput> {
    execute_in(plan, config, cancellation, None)
}
fn execute_in(
    plan: LocalPlan,
    config: ExecutionConfig,
    cancellation: CancellationToken,
    parent: Option<&std::path::Path>,
) -> Result<ExecutionOutput> {
    let mut cx = ExecContext::new(config, cancellation, parent)?;
    let mut batches = Vec::new();
    let mut retained = cx.memory.reservation(OperatorId(u32::MAX));
    drive(plan, &mut cx, |batch, cx| {
        retained.grow(
            batch
                .get_array_memory_size()
                .checked_add(128)
                .ok_or(Error::Overflow("collection"))?,
        )?;
        batches
            .try_reserve_exact(1)
            .map_err(|_| Error::Invalid("output allocation failed".into()))?;
        cx.metrics.memory.reserve(batch.get_array_memory_size())?;
        batches.push(batch);
        Ok(())
    })?;
    cx.spill.close()?;
    Ok(ExecutionOutput {
        batches,
        metrics: cx.metrics,
        _retained: retained,
    })
}

/// Execute without collecting output. The synchronous sink owns one bounded output batch
/// at a time; returning an error tears down operators and local spill through RAII.
/// # Errors
/// Returns execution, cancellation, or sink errors.
pub fn execute_to_sink(
    plan: LocalPlan,
    config: ExecutionConfig,
    cancellation: CancellationToken,
    mut sink: impl FnMut(RecordBatch) -> Result<()>,
) -> Result<ExecutionMetrics> {
    let mut cx = ExecContext::new(config, cancellation, None)?;
    drive(plan, &mut cx, |batch, _| sink(batch))?;
    cx.spill.close()?;
    Ok(cx.metrics)
}

fn drive(
    plan: LocalPlan,
    cx: &mut ExecContext,
    mut sink: impl FnMut(RecordBatch, &mut ExecContext) -> Result<()>,
) -> Result<()> {
    let mut root = Node::build(plan, cx)?;
    let result = (|| {
        while let Some(batch) = root.next_batch(cx)? {
            cx.metrics.output_rows += batch.num_rows();
            cx.metrics.output_batches += 1;
            sink(batch, cx)?;
        }
        cx.cancellation.check()
    })();
    // Shutdown also runs after sink errors, before the spill root is removed.
    let shutdown = root.shutdown(cx);
    drop(root);
    result?;
    shutdown?;
    cx.metrics.managed = cx.memory.snapshot();
    cx.metrics.spill = cx.spill.snapshot();
    Ok(())
}

#[cfg(test)]
mod tests;
