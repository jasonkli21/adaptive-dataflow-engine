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
    let values = batch
        .columns()
        .get(column)
        .and_then(|a| a.as_any().downcast_ref::<UInt64Array>())
        .ok_or_else(|| Error::Invalid("filter type/column".into()))?;
    let mask = BooleanArray::from_iter(
        (0..values.len()).map(|i| Some(!values.is_null(i) && values.value(i) < threshold)),
    );
    Ok(filter_record_batch(batch, &mask)?)
}

/// Native Arrow projection, preserving row order and zero-copy column ownership.
/// # Errors
/// Refuses invalid ordinals.
pub fn project(batch: &RecordBatch, columns: &[usize]) -> Result<RecordBatch> {
    Ok(batch.project(columns)?)
}
