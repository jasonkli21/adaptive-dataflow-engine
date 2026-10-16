//! Pure capability-based planner. No files, network, cost model or retry loop.
use crate::{
    Error, Result,
    ids::{AttemptId, JobId, PartitionId, TaskId, WorkerId, transfer},
    lineage::{SourceMorsel, SourceSnapshot, TransferManifest},
    metrics::TaskMetrics,
    require,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SegmentRange {
    pub transfer: crate::ids::LogicalTransferId,
    pub start: u64,
    pub end_exclusive: u64,
}
impl SegmentRange {
    /// # Errors
    /// Rejects inverted or out-of-terminal ranges, including foreign transfers.
    pub fn validate(&self, manifest: &TransferManifest) -> Result<()> {
        require(
            self.transfer == manifest.logical
                && self.start <= self.end_exclusive
                && self.end_exclusive <= manifest.segments,
            "invalid segment range",
        )
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayClosure {
    pub producer: TaskId,
    pub morsels: Vec<SourceMorsel>,
    pub original: TransferManifest,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryPlan {
    pub job: JobId,
    pub partition: PartitionId,
    pub host: WorkerId,
    pub ranges: Vec<SegmentRange>,
    pub closures: Vec<ReplayClosure>,
    pub reused_partitions: Vec<u64>,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryMetrics {
    pub worker_failures_detected: u64,
    pub recovery_attempts: u64,
    pub recovered_partitions: u64,
    pub replayed_producer_tasks: u64,
    pub replayed_morsels: u64,
    pub lost_segments: u64,
    pub lost_payload_bytes: u64,
    pub recomputed_segments: u64,
    pub recomputed_payload_bytes: u64,
    pub recovery_elapsed_ms: u64,
    pub full_job_restart_avoided: bool,
    pub planner_reason: String,
    pub plan: Option<RecoveryPlan>,
    pub replay_manifests: Vec<TransferManifest>,
    pub committed_attempts: BTreeMap<u64, AttemptId>,
}
/// Validate every original terminal against the fixed physical graph/configuration.
/// # Errors
/// Rejects incomplete or inconsistent producer evidence.
pub fn validate_producers(
    job: JobId,
    workers: usize,
    producers: &BTreeMap<u64, TaskMetrics>,
) -> Result<()> {
    require(producers.len() == workers, "original producer incomplete")?;
    for p in 0..workers as u64 {
        let m = producers
            .get(&p)
            .ok_or_else(|| Error::Unrecoverable("original producer missing".into()))?;
        require(
            m.manifests.len() == workers && m.destinations.len() == workers,
            "original manifest coverage mismatch",
        )?;
        let mut total = crate::metrics::NetworkMetrics::default();
        for d in 0..workers as u64 {
            let manifest = m
                .manifests
                .get(&d)
                .ok_or_else(|| Error::Invalid("missing terminal manifest".into()))?;
            let counts = m
                .destinations
                .get(&d)
                .ok_or_else(|| Error::Invalid("missing destination counts".into()))?;
            require(
                manifest.logical == transfer(job, 0, d, p).logical()
                    && manifest.attempt == AttemptId(0)
                    && manifest.segments == counts.batches
                    && manifest.rows == counts.rows
                    && manifest.payload_bytes == counts.payload_bytes
                    && counts.transfers == 1
                    && manifest.fingerprint.len() == 64,
                "invalid original terminal manifest",
            )?;
            total.add(counts);
        }
        require(
            total.transfers == m.sent.transfers
                && total.batches == m.sent.batches
                && total.rows == m.sent.rows
                && total.payload_bytes == m.sent.payload_bytes,
            "producer manifest conservation mismatch",
        )?;
    }
    Ok(())
}
/// The caller proves worker unavailability and successful survivor gather validation.
/// Metadata is bounded by topology and source file count, independent of segment count.
/// # Errors
/// Returns an explicit refusal outside the single final-worker-loss envelope.
pub fn plan(
    job: JobId,
    workers: usize,
    snapshot: Option<&SourceSnapshot>,
    producers: &BTreeMap<u64, TaskMetrics>,
    successful_finals: &[u64],
    lost: &[u64],
) -> Result<RecoveryPlan> {
    let build = || {
        require(
            (2..=4).contains(&workers) && lost.len() == 1 && successful_finals.len() + 1 == workers,
            "requires exactly one lost final worker",
        )?;
        let partition = lost[0];
        require(
            partition < workers as u64
                && (0..workers as u64)
                    .filter(|p| *p != partition)
                    .eq(successful_finals.iter().copied()),
            "ambiguous final partition coverage",
        )?;
        validate_producers(job, workers, producers)?;
        let snapshot = snapshot.ok_or_else(|| Error::Invalid("missing source snapshot".into()))?;
        snapshot.validate(
            &snapshot
                .morsels
                .iter()
                .map(|m| m.path.clone())
                .collect::<Vec<_>>(),
        )?;
        require(snapshot.replayable, "non-replayable lineage")?;
        let mut closures = Vec::new();
        let mut ranges = Vec::new();
        for p in 0..workers as u64 {
            let original = producers[&p].manifests[&partition].clone();
            ranges.push(SegmentRange {
                transfer: original.logical,
                start: 0,
                end_exclusive: original.segments,
            });
            closures.push(ReplayClosure {
                producer: TaskId(p),
                morsels: snapshot.producer_morsels(TaskId(p), workers),
                original,
            });
        }
        Ok(RecoveryPlan {
            job,
            partition: PartitionId(partition),
            host: WorkerId(successful_finals[0]),
            ranges,
            closures,
            reused_partitions: successful_finals.to_vec(),
        })
    };
    build().map_err(|e: Error| Error::Unrecoverable(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ids::MorselId, metrics::NetworkMetrics};
    fn facts() -> (SourceSnapshot, BTreeMap<u64, TaskMetrics>) {
        let snapshot = SourceSnapshot {
            version: 1,
            manifest_fingerprint: "m".into(),
            schema_fingerprint: "s".into(),
            logical_fingerprint: "l".into(),
            replayable: true,
            morsels: (0..4)
                .map(|i| SourceMorsel {
                    id: MorselId(i),
                    path: format!("{i}.parquet").into(),
                    fingerprint: "a".repeat(64),
                    bytes: 1,
                })
                .collect(),
        };
        let mut producers = BTreeMap::new();
        for p in 0..2 {
            let mut m = TaskMetrics::default();
            for d in 0..2 {
                let counts = NetworkMetrics {
                    transfers: 1,
                    batches: p,
                    rows: p,
                    payload_bytes: p * 8,
                    peak_frame_bytes: 8,
                };
                m.sent.add(&counts);
                m.destinations.insert(d, counts);
                m.manifests.insert(
                    d,
                    TransferManifest {
                        logical: transfer(JobId(42), 0, d, p).logical(),
                        attempt: AttemptId(0),
                        segments: p,
                        rows: p,
                        payload_bytes: p * 8,
                        fingerprint: "a".repeat(64),
                    },
                );
            }
            producers.insert(p, m);
        }
        (snapshot, producers)
    }
    #[test]
    fn minimal_file_closure_and_compact_ranges_are_deterministic() {
        let (snapshot, producers) = facts();
        let a = plan(JobId(42), 2, Some(&snapshot), &producers, &[0], &[1]).unwrap();
        let b = plan(JobId(42), 2, Some(&snapshot), &producers, &[0], &[1]).unwrap();
        assert_eq!(
            serde_json::to_vec(&a).unwrap(),
            serde_json::to_vec(&b).unwrap()
        );
        assert_eq!(a.reused_partitions, [0]);
        assert_eq!(
            a.closures[1]
                .morsels
                .iter()
                .map(|m| m.id)
                .collect::<Vec<_>>(),
            [MorselId(1), MorselId(3)]
        );
        assert_eq!(a.ranges.len(), 2);
        assert!(
            a.ranges
                .iter()
                .all(|r| r.transfer.partition == PartitionId(1))
        );
        assert_eq!(a.ranges[0].end_exclusive, 0); // Empty transfer remains explicit.
        for (r, c) in a.ranges.iter().zip(&a.closures) {
            r.validate(&c.original).unwrap();
        }
        let mut bad = a.ranges[1].clone();
        bad.start = 2;
        assert!(bad.validate(&a.closures[1].original).is_err());
    }
    #[test]
    fn refuses_incomplete_multiple_losses_and_invalid_lineage() {
        let (mut snapshot, mut producers) = facts();
        assert!(plan(JobId(42), 2, None, &producers, &[0], &[1]).is_err());
        assert!(plan(JobId(42), 2, Some(&snapshot), &producers, &[], &[0, 1]).is_err());
        snapshot.replayable = false;
        assert!(plan(JobId(42), 2, Some(&snapshot), &producers, &[0], &[1]).is_err());
        snapshot.replayable = true;
        snapshot.version = 2;
        assert!(plan(JobId(42), 2, Some(&snapshot), &producers, &[0], &[1]).is_err());
        snapshot.version = 1;
        producers
            .get_mut(&0)
            .unwrap()
            .manifests
            .get_mut(&1)
            .unwrap()
            .attempt = AttemptId(1);
        assert!(plan(JobId(42), 2, Some(&snapshot), &producers, &[0], &[1]).is_err());
        producers.remove(&0);
        assert!(plan(JobId(42), 2, Some(&snapshot), &producers, &[0], &[1]).is_err());
    }
}
