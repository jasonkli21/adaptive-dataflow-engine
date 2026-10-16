//! Version 1 is `SplitMix64`, followed by unsigned modulo partition count.
use crate::{Result, require};
use arrow::{
    array::{Array, UInt32Array, UInt64Array},
    compute::take,
    record_batch::RecordBatch,
};
pub const HASH_VERSION: u32 = 1;
#[must_use]
pub fn hash_v1(key: u64) -> u64 {
    let mut x = key.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}
/// # Errors
/// Rejects a zero partition count or unsupported integer size.
pub fn destination(key: u64, partitions: usize) -> Result<usize> {
    require(partitions > 0, "partition count must be positive")?;
    let partitions = u64::try_from(partitions)
        .map_err(|_| crate::Error::Invalid("partition count overflow".into()))?;
    usize::try_from(hash_v1(key) % partitions)
        .map_err(|_| crate::Error::Invalid("partition index overflow".into()))
}
/// Build only one bounded destination batch at a time.
/// # Errors
/// Rejects invalid partition arguments, schema, row bounds or Arrow kernel errors.
pub fn select(batch: &RecordBatch, partition: usize, partitions: usize) -> Result<RecordBatch> {
    require(partition < partitions, "partition out of range")?;
    require(batch.num_columns() == 3, "partition schema width")?;
    require(
        batch.num_rows() <= crate::exchange::MAX_BATCH_ROWS,
        "partition input batch too large",
    )?;
    let keys = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| crate::Error::Invalid("key type".into()))?;
    require(keys.null_count() == 0, "partition key nulls forbidden")?;
    let mut indices = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        if destination(keys.value(i), partitions)? == partition {
            indices.push(
                u32::try_from(i).map_err(|_| crate::Error::Invalid("row index overflow".into()))?,
            );
        }
    }
    let indices = UInt32Array::from(indices);
    Ok(RecordBatch::try_new(
        batch.schema(),
        batch
            .columns()
            .iter()
            .map(|a| take(a, &indices, None))
            .collect::<std::result::Result<Vec<_>, _>>()?,
    )?)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn stable_vectors() {
        assert_eq!(hash_v1(0), 0xe220_a839_7b1d_cdaf);
        assert_eq!(hash_v1(1), 0x910a_2dec_8902_5cc1);
        assert_eq!(hash_v1(2), 0x9758_35de_1c97_56ce);
        assert_eq!(destination(0, 4).unwrap(), 3);
        assert!(destination(1, 0).is_err());
        let keys: Vec<_> = (0..100).map(|i| i % 13).collect();
        let batch = RecordBatch::try_new(
            crate::exchange::schema(),
            vec![
                std::sync::Arc::new(UInt64Array::from(keys.clone())),
                std::sync::Arc::new(arrow::array::Int64Array::from(vec![1; 100])),
                std::sync::Arc::new(UInt64Array::from(vec![1; 100])),
            ],
        )
        .unwrap();
        let mut routed = Vec::new();
        for p in 0..4 {
            let selected = select(&batch, p, 4).unwrap();
            let selected = selected
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            for key in selected.values() {
                assert_eq!(destination(*key, 4).unwrap(), p);
                routed.push(*key);
            }
        }
        routed.sort_unstable();
        let mut keys = keys;
        keys.sort_unstable();
        assert_eq!(routed, keys);
    }
}
