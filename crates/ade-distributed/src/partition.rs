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
    select_routed(batch, partition, partitions, |key| {
        destination(key, partitions)
    })
}
#[cfg(test)]
fn select_layout(
    batch: &RecordBatch,
    partition: usize,
    layout: &crate::topology::Layout,
) -> Result<RecordBatch> {
    select_routed(
        batch,
        partition,
        layout.workers,
        |key| Ok(layout.route(key)),
    )
}
fn select_routed(
    batch: &RecordBatch,
    partition: usize,
    partitions: usize,
    route: impl Fn(u64) -> Result<usize>,
) -> Result<RecordBatch> {
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
        if route(keys.value(i))? == partition {
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

/// At most one pending full frame per physical destination. No keyed reduction.
pub(crate) struct FramePacker {
    pending: Vec<Pending>,
    pub peak_rows: usize,
    rows: usize,
}
struct Pending {
    keys: Vec<u64>,
    sums: Vec<i64>,
    counts: Vec<u64>,
}
impl Pending {
    fn new() -> Self {
        Self {
            keys: Vec::with_capacity(crate::exchange::MAX_BATCH_ROWS),
            sums: Vec::with_capacity(crate::exchange::MAX_BATCH_ROWS),
            counts: Vec::with_capacity(crate::exchange::MAX_BATCH_ROWS),
        }
    }
    fn take(&mut self) -> Result<Option<RecordBatch>> {
        if self.keys.is_empty() {
            return Ok(None);
        }
        let old = std::mem::replace(self, Self::new());
        Ok(Some(RecordBatch::try_new(
            crate::exchange::schema(),
            vec![
                std::sync::Arc::new(UInt64Array::from(old.keys)),
                std::sync::Arc::new(arrow::array::Int64Array::from(old.sums)),
                std::sync::Arc::new(UInt64Array::from(old.counts)),
            ],
        )?))
    }
}
impl FramePacker {
    pub(crate) fn pending_rows(&self) -> usize {
        self.rows
    }
    pub fn new(destinations: usize) -> Self {
        Self {
            pending: (0..destinations).map(|_| Pending::new()).collect(),
            peak_rows: 0,
            rows: 0,
        }
    }
    #[cfg(test)]
    fn push(&mut self, destination: usize, batch: &RecordBatch) -> Result<Option<RecordBatch>> {
        require(
            batch.schema() == crate::exchange::schema()
                && batch.num_rows() <= crate::exchange::MAX_BATCH_ROWS,
            "packer schema/row bound",
        )?;
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| crate::Error::Invalid("packer key".into()))?;
        let sums = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .ok_or_else(|| crate::Error::Invalid("packer sum".into()))?;
        let counts = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| crate::Error::Invalid("packer count".into()))?;
        require(
            batch.columns().iter().all(|c| c.null_count() == 0),
            "packer nulls",
        )?;
        require(destination < self.pending.len(), "packer destination")?;
        let mut full = None;
        for row in 0..batch.num_rows() {
            let p = &mut self.pending[destination];
            p.keys.push(keys.value(row));
            p.sums.push(sums.value(row));
            p.counts.push(counts.value(row));
            self.rows += 1;
            self.peak_rows = self.peak_rows.max(self.rows);
            if self.pending[destination].keys.len() == crate::exchange::MAX_BATCH_ROWS {
                full = self.pending[destination].take()?;
                self.rows -= crate::exchange::MAX_BATCH_ROWS;
            }
        }
        Ok(full)
    }
    /// Route once and emit at most one transient frame at a time. Each destination
    /// sees exactly the same ordered rows and boundaries as select + push.
    pub fn route(
        &mut self,
        batch: &RecordBatch,
        layout: Option<&crate::topology::Layout>,
        target: Option<usize>,
        mut emit: impl FnMut(usize, RecordBatch) -> Result<()>,
    ) -> Result<()> {
        require(
            batch.schema() == crate::exchange::schema()
                && batch.num_rows() <= crate::exchange::MAX_BATCH_ROWS
                && batch.columns().iter().all(|c| c.null_count() == 0),
            "packer schema/row/null bound",
        )?;
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| crate::Error::Invalid("packer key".into()))?;
        let sums = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .ok_or_else(|| crate::Error::Invalid("packer sum".into()))?;
        let counts = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| crate::Error::Invalid("packer count".into()))?;
        for row in 0..batch.num_rows() {
            let p = layout.map_or_else(
                || destination(keys.value(row), self.pending.len()),
                |l| Ok(l.route(keys.value(row))),
            )?;
            if target.is_some_and(|t| t != p) {
                continue;
            }
            let pending = &mut self.pending[p];
            pending.keys.push(keys.value(row));
            pending.sums.push(sums.value(row));
            pending.counts.push(counts.value(row));
            self.rows += 1;
            self.peak_rows = self.peak_rows.max(self.rows);
            if pending.keys.len() == crate::exchange::MAX_BATCH_ROWS {
                let frame = pending.take()?.expect("full frame");
                self.rows -= crate::exchange::MAX_BATCH_ROWS;
                emit(p, frame)?;
            }
        }
        Ok(())
    }
    pub fn finish(&mut self, destination: usize) -> Result<Option<RecordBatch>> {
        self.rows -= self.pending[destination].keys.len();
        self.pending[destination].take()
    }
}
#[cfg(test)]
mod packer_tests {
    use super::*;
    #[test]
    fn single_pass_matches_reference_frames_including_selective_replay_and_sink_failure() {
        for workers in 2..=4 {
            let config = crate::topology::TopologyConfig {
                policy: crate::topology::PartitionPolicy::SampledV1,
                ..Default::default()
            };
            let observations = (0..workers)
                .map(|p| crate::topology::Observation {
                    producer: p as u64,
                    counts: (0..48)
                        .map(|s| if s % workers == 0 { 100 } else { 0 })
                        .collect(),
                    rows: 4096,
                    files: 1,
                    ..Default::default()
                })
                .collect();
            let sampled = crate::topology::decide(&config, workers, observations)
                .unwrap()
                .layout;
            for layout in [None, Some(&sampled)] {
                for target in [None, Some(0), Some(workers - 1)] {
                    let mut reference = FramePacker::new(workers);
                    let mut routed = FramePacker::new(workers);
                    let mut expected = vec![Vec::new(); workers];
                    let mut actual = expected.clone();
                    for start in (0..8197u64).step_by(511) {
                        let end = (start + 511).min(8197);
                        let batch = RecordBatch::try_new(
                            crate::exchange::schema(),
                            vec![
                                std::sync::Arc::new(UInt64Array::from_iter_values(
                                    (start..end).map(|k| k % 271),
                                )),
                                std::sync::Arc::new(arrow::array::Int64Array::from_iter_values(
                                    (start..end)
                                        .map(|k| if k % 2 == 0 { i64::MIN } else { i64::MAX }),
                                )),
                                std::sync::Arc::new(UInt64Array::from_iter_values(
                                    (start..end).map(|k| k + 1),
                                )),
                            ],
                        )
                        .unwrap();
                        for (p, frames) in expected.iter_mut().enumerate() {
                            if target.is_some_and(|t| t != p) {
                                continue;
                            }
                            let selected = layout
                                .map_or_else(
                                    || select(&batch, p, workers),
                                    |l| select_layout(&batch, p, l),
                                )
                                .unwrap();
                            if let Some(frame) = reference.push(p, &selected).unwrap() {
                                frames.push(frame);
                            }
                        }
                        routed
                            .route(&batch, layout, target, |p, frame| {
                                actual[p].push(frame);
                                Ok(())
                            })
                            .unwrap();
                    }
                    for p in 0..workers {
                        if let Some(frame) = reference.finish(p).unwrap() {
                            expected[p].push(frame);
                        }
                        if let Some(frame) = routed.finish(p).unwrap() {
                            actual[p].push(frame);
                        }
                    }
                    assert_eq!(actual, expected);
                    assert!(routed.peak_rows <= workers * crate::exchange::MAX_BATCH_ROWS);
                    let batch = RecordBatch::try_new(
                        crate::exchange::schema(),
                        vec![
                            std::sync::Arc::new(UInt64Array::from(vec![0; 1024])),
                            std::sync::Arc::new(arrow::array::Int64Array::from(vec![1; 1024])),
                            std::sync::Arc::new(UInt64Array::from(vec![1; 1024])),
                        ],
                    )
                    .unwrap();
                    assert!(
                        FramePacker::new(workers)
                            .route(&batch, layout, None, |_, _| Err(crate::Error::Invalid(
                                "sink failure".into()
                            )))
                            .is_err()
                    );
                }
            }
        }
    }
    #[test]
    fn frame_boundaries_preserve_signed_rows_and_empty_destinations() {
        for rows in [0, 511, 512, 1023, 1024, 1025, 2049] {
            let mut packer = FramePacker::new(4);
            let mut frames = Vec::new();
            for start in (0..rows).step_by(511) {
                let end = (start + 511).min(rows);
                let batch = RecordBatch::try_new(
                    crate::exchange::schema(),
                    vec![
                        std::sync::Arc::new(UInt64Array::from_iter_values(start..end)),
                        std::sync::Arc::new(arrow::array::Int64Array::from_iter_values(
                            (start..end).map(|r| -i64::try_from(r).unwrap()),
                        )),
                        std::sync::Arc::new(UInt64Array::from(vec![
                            1;
                            usize::try_from(end - start)
                                .unwrap()
                        ])),
                    ],
                )
                .unwrap();
                if let Some(frame) = packer.push(2, &batch).unwrap() {
                    frames.push(frame);
                }
            }
            if let Some(frame) = packer.finish(2).unwrap() {
                frames.push(frame);
            }
            assert!(packer.finish(0).unwrap().is_none());
            assert!(packer.peak_rows <= 4 * crate::exchange::MAX_BATCH_ROWS);
            let values: Vec<_> = frames
                .iter()
                .flat_map(|b| {
                    b.column(0)
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .unwrap()
                        .values()
                        .to_vec()
                })
                .collect();
            assert_eq!(values, (0..rows).collect::<Vec<_>>());
            for (index, frame) in frames.iter().enumerate() {
                if index + 1 < frames.len() {
                    assert_eq!(frame.num_rows(), crate::exchange::MAX_BATCH_ROWS);
                }
                let keys = frame
                    .column(0)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap();
                let sums = frame
                    .column(1)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .unwrap();
                for row in 0..frame.num_rows() {
                    assert_eq!(sums.value(row), -i64::try_from(keys.value(row)).unwrap());
                }
            }
        }
    }
}
