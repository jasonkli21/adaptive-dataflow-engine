#![cfg(all(unix, feature = "local-v1"))]
//! Versioned finite local execution for operator-installed Rust row kernels.

mod batch;
mod source;

use ade_exec::physical::{Binding, Catalog};
use ade_physical::{Operator, PlanV1, Selection, Target};
use arrow::array::Array;
use std::{
    path::Path,
    sync::{Arc, atomic::Ordering},
    time::{Duration, Instant},
};

pub use ade_exec::{CancellationToken, ExecutionOutput};
pub use ade_physical::{Field, Type};
pub use batch::{Row, Value};
pub use source::{Digest, SourcePin, digest, executable_digest};

pub const MAX_ROWS: u64 = 32_768;
pub const MAX_SOURCE_BYTES: u64 = 16 * 1024 * 1024;
pub const MAX_RESULT_BYTES: u64 = 4 * 1024 * 1024;
pub const MAX_BATCH_BYTES: usize = 128 * 1024;
pub const MAX_MODEL_BYTES: usize = 4096;
pub const MAX_BATCH_ROWS: usize = 256;
pub const MAX_DEADLINE: Duration = Duration::from_secs(10);
const MAX_COLUMNS: usize = 8;
const NATIVE_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("unsupported extension ABI")]
    Version,
    #[error("unsupported extension capability")]
    Capability,
    #[error("extension descriptor mismatch")]
    Descriptor,
    #[error("source pin or immutable path mismatch")]
    Source,
    #[error("executable pin mismatch")]
    Executable,
    #[error("model pin or model limits")]
    Model,
    #[error("invalid source schema or Parquet layout")]
    Schema,
    #[error("resource limit: {0}")]
    Limit(&'static str),
    #[error("cancelled or deadline elapsed")]
    Interrupted,
    #[error("extension kernel: {0}")]
    Kernel(&'static str),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error(transparent)]
    Plan(#[from] ade_physical::Error),
    #[error(transparent)]
    Native(#[from] ade_exec::Error),
    #[error(transparent)]
    Encode(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum Capability {
    LocalReplayableRowMap,
    StreamingStateful,
    Remote,
    SchemaChanging,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ExtensionDescriptorV1 {
    pub abi: u8,
    pub id: String,
    pub version: u16,
    pub capability: Capability,
    /// Ordered required fields. The first field must be `UInt64` `row_id`.
    pub schema: Vec<Field>,
    /// Indices in strictly increasing order. `UInt64` identity fields are immutable.
    pub mutable: Vec<usize>,
}

/// A statically chosen implementation. Preparation receives exactly the pinned bytes.
pub trait Extension {
    type Kernel: RowKernel;

    fn descriptor(&self) -> ExtensionDescriptorV1;

    /// # Errors
    /// Refuses invalid model or operator parameters before the source is opened.
    fn prepare(&self, model: &[u8]) -> Result<Self::Kernel>;
}

/// A cooperative, deterministic scalar transform with no row-placement authority.
pub trait RowKernel: Send + Sync + 'static {
    /// # Errors
    /// Refuses invalid inputs or a computation whose result is outside the contract.
    fn transform(&self, row: &mut Row) -> Result<()>;
}

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub batch_rows: usize,
    pub batch_bytes: usize,
    pub result_bytes: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            batch_rows: MAX_BATCH_ROWS,
            batch_bytes: MAX_BATCH_BYTES,
            result_bytes: MAX_RESULT_BYTES,
        }
    }
}

#[derive(Debug)]
pub struct Request<'a> {
    pub descriptor: &'a ExtensionDescriptorV1,
    pub executable: Digest,
    pub model_bytes: &'a [u8],
    pub model_pin: Digest,
    pub source: SourcePin,
    pub path: &'a Path,
    /// Ordered projected field indices. Must start with field 0 (row identity).
    pub projection: &'a [usize],
    pub limits: Limits,
    /// Absolute deadline, including executable and source authentication.
    pub deadline: Instant,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct Receipt {
    pub extension: String,
    pub extension_version: u16,
    pub logical: Digest,
    pub executable: Digest,
    pub model: Digest,
    pub source: Digest,
    pub plan: String,
    pub result: Digest,
    pub attempt: String,
    pub peak_map_array_bytes: usize,
    pub kernel_inline_bytes: usize,
    pub batch_rows: usize,
    pub batch_bytes_cap: usize,
    pub result_bytes_cap: u64,
    pub native_bytes_cap: usize,
    pub route: &'static str,
}

pub struct Output {
    pub receipt: Receipt,
    pub native: ExecutionOutput,
}

fn name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value.as_bytes()[0].is_ascii_lowercase()
        && value
            .bytes()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || b"_.-".contains(&c))
}

fn descriptor_ok(descriptor: &ExtensionDescriptorV1) -> bool {
    let fields = &descriptor.schema;
    name(&descriptor.id)
        && descriptor.version != 0
        && fields.first() == Some(&Field::new("row_id", Type::U64, false))
        && fields.len() <= MAX_COLUMNS
        && fields.iter().enumerate().all(|(i, field)| {
            name(&field.name)
                && !field.nullable
                && matches!(field.data_type, Type::U64 | Type::I64 | Type::F64)
                && !fields[..i].iter().any(|prior| prior.name == field.name)
        })
        && descriptor.mutable.windows(2).all(|pair| pair[0] < pair[1])
        && descriptor.mutable.iter().all(|&i| {
            fields
                .get(i)
                .is_some_and(|field| matches!(field.data_type, Type::I64 | Type::F64))
        })
}

/// Negotiate the exact version, capability and installed schema contract.
///
/// # Errors
/// Returns a typed refusal for an unsupported or mismatched descriptor.
pub fn negotiate(
    requested: &ExtensionDescriptorV1,
    installed: &ExtensionDescriptorV1,
) -> Result<()> {
    if requested.abi != 1 {
        return Err(Error::Version);
    }
    if requested.capability != Capability::LocalReplayableRowMap {
        return Err(Error::Capability);
    }
    if requested != installed || !descriptor_ok(requested) {
        return Err(Error::Descriptor);
    }
    Ok(())
}

fn check(cancel: &CancellationToken, deadline: Instant) -> Result<()> {
    if deadline <= Instant::now() {
        return Err(Error::Interrupted);
    }
    cancel.check().map_err(|_| Error::Interrupted)
}

fn plan_for(request: &Request<'_>) -> Result<(PlanV1, Catalog)> {
    let source = source::binding(request)?;
    let fields = &request.descriptor.schema;
    let mut seen = [false; MAX_COLUMNS];
    if request.projection.first() != Some(&0) || request.projection.len() > fields.len() {
        return Err(Error::Schema);
    }
    let mut columns = Vec::with_capacity(request.projection.len());
    for &column in request.projection {
        let field = fields.get(column).ok_or(Error::Schema)?;
        if std::mem::replace(&mut seen[column], true) {
            return Err(Error::Schema);
        }
        columns.push(Selection {
            column,
            alias: field.name.clone(),
        });
    }
    let mut plan = PlanV1::new(Target::Local, vec![source.clone()]);
    let input = plan.push(Operator::Scan { source: 0 })?;
    plan.push(Operator::Project { input, columns })?;
    plan.max_output_rows = request.source.rows;
    plan.validate()?;
    let catalog = Catalog {
        bindings: vec![Binding {
            source,
            files: vec![request.path.into()],
        }],
    };
    Ok((plan, catalog))
}

fn output_digest(
    output: &ExecutionOutput,
    cancel: &CancellationToken,
    deadline: Instant,
) -> Result<Digest> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"ade-extension-result-v1\0");
    for batch in &output.batches {
        let columns = batch::columns(batch)?;
        for index in 0..batch.num_rows() {
            check(cancel, deadline)?;
            for column in &columns {
                hasher.update(&column.get(index).bits().to_le_bytes());
            }
        }
    }
    Ok(Digest::from_hash(hasher.finalize()))
}

/// Execute a pinned local Scan → `MapBatches` → Project with in-memory output.
///
/// # Errors
/// Refuses incompatible contracts, source/model/executable drift, invalid kernels,
/// resource exhaustion and cancellation. Failure returns no output.
pub fn execute<E: Extension>(
    request: &Request<'_>,
    installed: &E,
    cancel: &CancellationToken,
) -> Result<Output> {
    negotiate(request.descriptor, &installed.descriptor())?;
    let limits = request.limits;
    if !(1..=MAX_BATCH_ROWS).contains(&limits.batch_rows)
        || !(1..=MAX_BATCH_BYTES).contains(&limits.batch_bytes)
        || !(1..=MAX_RESULT_BYTES).contains(&limits.result_bytes)
        || request.deadline.saturating_duration_since(Instant::now()) > MAX_DEADLINE
    {
        return Err(Error::Limit("execution limits"));
    }
    check(cancel, request.deadline)?;
    if request.model_bytes.len() > MAX_MODEL_BYTES
        || digest(request.model_bytes) != request.model_pin
    {
        return Err(Error::Model);
    }
    let kernel = installed.prepare(request.model_bytes)?;
    let kernel_inline_bytes = std::mem::size_of_val(&kernel);
    if kernel_inline_bytes > MAX_MODEL_BYTES {
        return Err(Error::Limit("inline kernel state"));
    }
    let (mut plan, catalog) = plan_for(request)?;
    if source::measure_executable(cancel, request.deadline)? != request.executable {
        return Err(Error::Executable);
    }
    source::verify(request, cancel)?;
    let semantic_plan = plan.digest()?;
    let logical = digest(&serde_json::to_vec(&(
        "ade-extension-logical-v1",
        request.descriptor,
        request.model_pin,
        &semantic_plan,
    ))?);
    plan.max_output_bytes = limits.result_bytes;
    let attempt = uuid::Uuid::new_v4().to_string();
    let guarded = Arc::new(batch::Guarded::new(kernel, request));
    let map: Arc<dyn ade_exec::BatchMap> = guarded.clone();
    let native = ade_exec::physical::execute_with_map(
        &plan,
        &catalog,
        ade_exec::ExecutionConfig {
            memory_limit_bytes: NATIVE_BYTES,
            spill_enabled: false,
            prefetch_batches: 0,
            output_batch_rows: limits.batch_rows,
        },
        limits.batch_rows,
        cancel.clone(),
        request.deadline.saturating_duration_since(Instant::now()),
        Some(&map),
    )?;
    if u64::try_from(native.metrics.output_rows).ok() != Some(request.source.rows)
        || native.batches.iter().any(|batch| {
            batch
                .columns()
                .iter()
                .any(|column| column.null_count() != 0)
        })
    {
        return Err(Error::Kernel("output cardinality or nullability"));
    }
    let result = output_digest(&native, cancel, request.deadline)?;
    Ok(Output {
        receipt: Receipt {
            extension: request.descriptor.id.clone(),
            extension_version: request.descriptor.version,
            logical,
            executable: request.executable,
            model: request.model_pin,
            source: request.source.digest,
            plan: semantic_plan,
            result,
            attempt,
            peak_map_array_bytes: guarded.peak.load(Ordering::Relaxed),
            kernel_inline_bytes,
            batch_rows: limits.batch_rows,
            batch_bytes_cap: limits.batch_bytes,
            result_bytes_cap: limits.result_bytes,
            native_bytes_cap: NATIVE_BYTES,
            route: "scan/map-batches/project",
        },
        native,
    })
}
