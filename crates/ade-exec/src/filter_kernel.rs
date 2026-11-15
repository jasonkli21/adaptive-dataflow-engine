//! Explicit, versioned selection for the local `UInt64` filter.
#[cfg(all(target_arch = "aarch64", target_endian = "little"))]
mod neon;
mod policy;
pub use policy::{AutoSelection, Measurements, Prediction, ProfileRefusal, Shape};

use crate::{CancellationToken, Error, MemoryManager, OperatorId, Result, stateless};
use arrow::{
    array::{Array, BooleanArray, UInt64Array},
    compute::{filter_record_batch, kernels::cmp},
    record_batch::RecordBatch,
};
use serde::Serialize;
use std::{collections::BTreeMap, time::Instant};

pub const MAX_ROWS: usize = 262_144;
pub const MAX_INPUT_BYTES: usize = 64 * 1024 * 1024;
#[cfg(all(target_arch = "aarch64", target_endian = "little"))]
const POLL_ROWS: usize = 8192;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Isa {
    Neon,
    Avx2,
}

impl std::fmt::Display for Isa {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Neon => "neon",
            Self::Avx2 => "avx2",
        })
    }
}

#[derive(Debug, Clone)]
pub enum Request {
    Scalar,
    Arrow,
    Force(Isa),
    MeasuredAuto(AutoSelection),
}

impl Request {
    #[must_use]
    pub fn label(&self) -> &'static str {
        match self {
            Self::Scalar => "scalar",
            Self::Arrow => "arrow",
            Self::Force(Isa::Neon) => "force-neon",
            Self::Force(Isa::Avx2) => "force-avx2",
            Self::MeasuredAuto(_) => "measured-auto",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Kernel {
    Scalar,
    Arrow,
    Neon,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Ord, PartialOrd, Serialize)]
#[serde(rename_all = "kebab-case")]
enum Reason {
    ExplicitScalar,
    ArrowControl,
    ForcedNeon,
    MeasuredGain,
    UnsupportedIsa,
    MissingProfile,
    InvalidProfile,
    IncompatibleProfile,
    UnknownShape,
    InsufficientGain,
}

#[derive(Debug, Serialize)]
struct Observation {
    kernel: Kernel,
    batches: u64,
    simd_vectors: u64,
    input_rows: u64,
    output_rows: u64,
    null_rows: u64,
    scratch_peak_bytes: usize,
    mask_ns: u64,
    gather_ns: u64,
}

/// Audit of the kernel actually called. Times are elapsed nanoseconds.
#[derive(Debug, Serialize)]
pub struct Audit {
    requested: &'static str,
    neon_available: bool,
    observations: BTreeMap<Reason, Observation>,
    prediction: Option<Prediction>,
}

#[must_use]
pub fn neon_available() -> bool {
    #[cfg(all(target_arch = "aarch64", target_endian = "little"))]
    {
        std::arch::is_aarch64_feature_detected!("neon")
    }
    #[cfg(not(all(target_arch = "aarch64", target_endian = "little")))]
    {
        false
    }
}

pub(crate) struct Filter {
    request: Request,
    pub(crate) audit: Audit,
}

impl std::fmt::Debug for Filter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Filter")
            .field("requested", &self.request.label())
            .finish_non_exhaustive()
    }
}

impl Filter {
    pub(crate) fn new(request: Request) -> Result<Self> {
        let available = neon_available();
        if let Request::Force(isa) = request
            && (isa != Isa::Neon || !available)
        {
            return Err(Error::UnsupportedFilterIsa(isa));
        }
        let prediction = match &request {
            Request::MeasuredAuto(policy) => policy.prediction(),
            _ => None,
        };
        Ok(Self {
            audit: Audit {
                requested: request.label(),
                neon_available: available,
                observations: BTreeMap::new(),
                prediction,
            },
            request,
        })
    }

    pub(crate) fn apply(
        &mut self,
        batch: &RecordBatch,
        column: usize,
        threshold: u64,
        memory: &MemoryManager,
        owner: OperatorId,
        token: &CancellationToken,
    ) -> Result<RecordBatch> {
        token.check()?;
        let values = stateless::predicate_column(batch, column)?;
        let rows = values.len();
        if rows > MAX_ROWS || batch.get_array_memory_size() > MAX_INPUT_BYTES {
            return Err(Error::FilterInputLimit);
        }
        let nulls = values.null_count();
        let shape = Shape {
            rows,
            nulls,
            threshold,
        };
        let (kernel, reason) = self.select(shape);
        let mask_bytes = rows.div_ceil(8);
        // Scalar's Arrow builder can use both validity and value buffers. Forecast
        // cache-line rounding here. Arrow's private gather allocations are separate.
        let scratch = if kernel == Kernel::Neon {
            mask_bytes
        } else {
            2 * mask_bytes.next_multiple_of(64) + 64
        };
        let mut reservation = memory.reservation(owner);
        reservation.grow(scratch)?;
        let start = Instant::now();
        let mask = match kernel {
            Kernel::Scalar => stateless::scalar_mask(values, threshold),
            Kernel::Arrow => cmp::lt(values, &UInt64Array::new_scalar(threshold))?,
            Kernel::Neon => packed_mask(values, threshold, || token.check())?,
        };
        let mask_ns = ns_since(start);
        token.check()?;
        let start = Instant::now();
        let output = filter_record_batch(batch, &mask)?;
        let gather_ns = ns_since(start);
        token.check()?;
        let observation = self
            .audit
            .observations
            .entry(reason)
            .or_insert(Observation {
                kernel,
                batches: 0,
                simd_vectors: 0,
                input_rows: 0,
                output_rows: 0,
                null_rows: 0,
                scratch_peak_bytes: 0,
                mask_ns: 0,
                gather_ns: 0,
            });
        observation.batches += 1;
        // The NEON loop consumes complete eight-row groups with four vector loads.
        if kernel == Kernel::Neon {
            observation.simd_vectors += ((rows / 8) * 4) as u64;
        }
        observation.input_rows += rows as u64;
        observation.output_rows += output.num_rows() as u64;
        observation.null_rows += nulls as u64;
        observation.scratch_peak_bytes = observation.scratch_peak_bytes.max(scratch);
        observation.mask_ns = observation.mask_ns.saturating_add(mask_ns);
        observation.gather_ns = observation.gather_ns.saturating_add(gather_ns);
        Ok(output)
    }

    fn select(&self, shape: Shape) -> (Kernel, Reason) {
        match &self.request {
            Request::Scalar => (Kernel::Scalar, Reason::ExplicitScalar),
            Request::Arrow => (Kernel::Arrow, Reason::ArrowControl),
            Request::Force(_) => (Kernel::Neon, Reason::ForcedNeon),
            Request::MeasuredAuto(_) if !self.audit.neon_available => {
                (Kernel::Scalar, Reason::UnsupportedIsa)
            }
            Request::MeasuredAuto(policy) => {
                let reason = policy.select(shape);
                let kernel = if reason == Reason::MeasuredGain {
                    Kernel::Neon
                } else {
                    Kernel::Scalar
                };
                (kernel, reason)
            }
        }
    }
}

fn ns_since(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

#[cfg(all(target_arch = "aarch64", target_endian = "little"))]
fn packed_mask(
    values: &UInt64Array,
    threshold: u64,
    mut check: impl FnMut() -> Result<()>,
) -> Result<BooleanArray> {
    use arrow::buffer::BooleanBuffer;
    if !neon_available() {
        return Err(Error::UnsupportedFilterIsa(Isa::Neon));
    }
    let len = values.len().div_ceil(8);
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(len)
        .map_err(|_| Error::Invalid("filter bitmap allocation failed".into()))?;
    bytes.resize(len, 0);
    for (lanes, output) in values
        .values()
        .chunks(POLL_ROWS)
        .zip(bytes.chunks_mut(POLL_ROWS / 8))
    {
        check()?;
        // SAFETY: This CPU supports NEON. Each chunk has ceil(lanes.len()/8)
        // initialized output bytes and starts at a mask-byte boundary.
        // The callee loads only complete eight-row groups.
        #[allow(
            unsafe_code,
            reason = "Native ISA and bounded slices checked by caller."
        )]
        unsafe {
            neon::packed(lanes, threshold, output);
        }
    }
    Ok(BooleanArray::new(
        BooleanBuffer::new(bytes.into(), 0, values.len()),
        values.nulls().cloned(),
    ))
}

#[cfg(not(all(target_arch = "aarch64", target_endian = "little")))]
fn packed_mask(
    _values: &UInt64Array,
    _threshold: u64,
    _check: impl FnMut() -> Result<()>,
) -> Result<BooleanArray> {
    Err(Error::UnsupportedFilterIsa(Isa::Neon))
}

#[cfg(test)]
mod tests;
