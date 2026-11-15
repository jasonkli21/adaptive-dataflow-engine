//! Shared bounded-batch kernels used by pull and live streaming execution.
use crate::{Error, Result};
use arrow::{
    array::{Array, BooleanArray, UInt64Array},
    compute::filter_record_batch,
    record_batch::RecordBatch,
};

/// SQL null semantics for the existing native `UInt64` less-than predicate.
/// # Errors
/// Refuses missing/wrong-typed columns and Arrow errors.
pub fn filter_u64_lt(batch: &RecordBatch, column: usize, threshold: u64) -> Result<RecordBatch> {
    let values = predicate_column(batch, column)?;
    Ok(filter_record_batch(batch, &scalar_mask(values, threshold))?)
}

pub(crate) fn predicate_column(batch: &RecordBatch, column: usize) -> Result<&UInt64Array> {
    batch
        .columns()
        .get(column)
        .and_then(|a| a.as_any().downcast_ref::<UInt64Array>())
        .ok_or_else(|| Error::Invalid("filter type/column".into()))
}

pub(crate) fn scalar_mask(values: &UInt64Array, threshold: u64) -> BooleanArray {
    BooleanArray::from_iter(
        (0..values.len()).map(|i| Some(!values.is_null(i) && values.value(i) < threshold)),
    )
}

/// Native Arrow projection, preserving row order and zero-copy column ownership.
/// # Errors
/// Refuses invalid ordinals.
pub fn project(batch: &RecordBatch, columns: &[usize]) -> Result<RecordBatch> {
    Ok(batch.project(columns)?)
}
