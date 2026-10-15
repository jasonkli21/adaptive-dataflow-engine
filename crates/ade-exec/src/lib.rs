//! Provisional, synchronous local execution. No SQL, spill, or hard memory limits.
use arrow::{
    array::{Array, ArrayRef, BooleanArray, Int64Array, UInt64Array},
    compute::filter_record_batch,
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use std::{
    collections::{BTreeMap, HashMap},
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
}
/// Real local runtime services, scoped to one execution.
#[derive(Debug, Default)]
pub struct ExecContext {
    pub cancellation: CancellationToken,
    pub metrics: ExecutionMetrics,
}
impl ExecContext {
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
    Limit {
        input: Box<Self>,
        rows: usize,
    },
}

struct Node {
    id: OperatorId,
    schema: SchemaRef,
    kind: Kind,
}
enum Kind {
    Scan {
        files: std::vec::IntoIter<PathBuf>,
        reader: Option<ParquetRecordBatchReader>,
        batch_size: usize,
    },
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
        done: bool,
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
    fn build(plan: LocalPlan, cx: &mut ExecContext) -> Result<Self> {
        cx.cancellation.check()?;
        let (schema, kind, name) = match plan {
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
                    schema,
                    Kind::Scan {
                        files: files.into_iter(),
                        reader: None,
                        batch_size,
                    },
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
                        done: false,
                    },
                    "hash-aggregate",
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
        Ok(Self { id, schema, kind })
    }
    fn next_batch(&mut self, cx: &mut ExecContext) -> Result<Option<RecordBatch>> {
        cx.cancellation.check()?;
        // Release the previous observed output when downstream asks for another batch.
        cx.memory(self.id, 0)?;
        let output = match &mut self.kind {
            Kind::Scan {
                files,
                reader,
                batch_size,
            } => {
                let start = Instant::now();
                let result = loop {
                    cx.cancellation.check()?;
                    if let Some(reader) = reader
                        && let Some(batch) = reader.next()
                    {
                        break Some(batch?);
                    }
                    let Some(file) = files.next() else {
                        break None;
                    };
                    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(file)?)?;
                    if builder.schema().as_ref() != self.schema.as_ref() {
                        return Err(Error::Invalid("scan file schemas differ".into()));
                    }
                    *reader = Some(builder.with_batch_size(*batch_size).build()?);
                };
                cx.work(self.id, start);
                result
            }
            Kind::Filter {
                input,
                column,
                threshold,
            } => {
                let Some(batch) = input.next_batch(cx)? else {
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
                    return Ok(None);
                }
                let Some(batch) = input.next_batch(cx)? else {
                    return Ok(None);
                };
                cx.input(self.id, &batch);
                let start = Instant::now();
                let rows = (*remaining).min(batch.num_rows());
                *remaining -= rows;
                let result = batch.slice(0, rows);
                cx.work(self.id, start);
                Some(result)
            }
            Kind::Aggregate {
                input,
                key,
                value,
                done,
            } => {
                if *done {
                    return Ok(None);
                }
                *done = true;
                Some(aggregate(input, *key, *value, &self.schema, self.id, cx)?)
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
}

fn aggregate(
    input: &mut Node,
    key: usize,
    value: usize,
    schema: &SchemaRef,
    id: OperatorId,
    cx: &mut ExecContext,
) -> Result<RecordBatch> {
    let mut groups: HashMap<u64, (i128, u64)> = HashMap::new();
    while let Some(batch) = input.next_batch(cx)? {
        cx.cancellation.check()?;
        cx.input(id, &batch);
        let start = Instant::now();
        let keys = batch
            .column(key)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| Error::Invalid("aggregate key type".into()))?;
        let values = batch
            .column(value)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| Error::Invalid("aggregate value type".into()))?;
        if keys.null_count() > 0 || values.null_count() > 0 {
            return Err(Error::Invalid("aggregate nulls are unsupported".into()));
        }
        for (key, value) in keys.values().iter().zip(values.values()) {
            let group = groups.entry(*key).or_default();
            group.0 = group
                .0
                .checked_add(i128::from(*value))
                .ok_or(Error::Overflow("aggregate sum"))?;
            group.1 = group
                .1
                .checked_add(1)
                .ok_or(Error::Overflow("aggregate count"))?;
        }
        // Capacity-based estimate includes entry payload and control/padding allowance.
        cx.memory(
            id,
            groups
                .capacity()
                .checked_mul(64)
                .ok_or(Error::Overflow("aggregate memory"))?,
        )?;
        cx.work(id, start);
    }
    let start = Instant::now();
    let mut rows: Vec<_> = groups.into_iter().collect();
    rows.sort_unstable_by_key(|(key, _)| *key);
    let mut keys = Vec::with_capacity(rows.len());
    let mut sums = Vec::with_capacity(rows.len());
    let mut counts = Vec::with_capacity(rows.len());
    for (key, (sum, count)) in rows {
        keys.push(key);
        sums.push(i64::try_from(sum).map_err(|_| Error::Overflow("aggregate Int64 output"))?);
        counts.push(count);
    }
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from(keys)),
        Arc::new(Int64Array::from(sums)),
        Arc::new(UInt64Array::from(counts)),
    ];
    let result = RecordBatch::try_new(schema.clone(), arrays)?;
    cx.work(id, start);
    Ok(result)
}

/// Collected bounded output and observational execution metrics.
#[derive(Debug)]
pub struct ExecutionOutput {
    pub batches: Vec<RecordBatch>,
    pub metrics: ExecutionMetrics,
}
/// Execute a provisional local plan to completion. Cancellation never returns partial success.
///
/// # Errors
/// Returns invalid-plan/schema, I/O, Arrow/Parquet, overflow, or cancellation errors.
pub fn execute(plan: LocalPlan, cancellation: CancellationToken) -> Result<ExecutionOutput> {
    let mut cx = ExecContext {
        cancellation,
        ..Default::default()
    };
    let mut root = Node::build(plan, &mut cx)?;
    let mut batches = Vec::new();
    while let Some(batch) = root.next_batch(&mut cx)? {
        cx.metrics.memory.reserve(batch.get_array_memory_size())?;
        cx.metrics.output_rows += batch.num_rows();
        cx.metrics.output_batches += 1;
        batches.push(batch);
    }
    cx.cancellation.check()?;
    for m in cx.metrics.operators.values_mut() {
        cx.metrics.memory.release(m.memory.current)?;
        m.memory.current = 0;
    }
    Ok(ExecutionOutput {
        batches,
        metrics: cx.metrics,
    })
}

#[cfg(test)]
mod tests;
