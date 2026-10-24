//! Diagnostic metrics are deliberately separate from benchmark result schema v1.
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NetworkMetrics {
    pub transfers: u64,
    /// Non-empty data frames; explicit empty transfers have zero batches.
    pub batches: u64,
    pub rows: u64,
    pub payload_bytes: u64,
    pub peak_frame_bytes: usize,
}
impl NetworkMetrics {
    pub fn add(&mut self, other: &Self) {
        self.transfers += other.transfers;
        self.batches += other.batches;
        self.rows += other.rows;
        self.payload_bytes += other.payload_bytes;
        self.peak_frame_bytes = self.peak_frame_bytes.max(other.peak_frame_bytes);
    }
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskMetrics {
    pub partial: Option<ade_exec::partial::Metrics>,
    /// Original partial-input rows ingested before layout commitment.
    pub prefix_rows: usize,
    /// Original prefix preparation wall time (separate from the key probe).
    pub preparation_ns: u64,
    /// Routing/packing wall time, excluding synchronous sender calls.
    pub routing_ns: u64,
    /// Sender open/encode/write wall time during batch delivery (not terminal ACKs).
    pub sending_ns: u64,
    pub routing_buffer_peak_rows: usize,
    pub routing_buffer_capacity_rows: usize,
    pub layout_digest: Option<String>,
    pub executed_final: bool,
    pub operator_work_ns: u64,
    pub coordinator_finished_ns: u64,
    pub source_pull_ns: u64,
    pub source_files: usize,
    pub production: crate::cost::Production,
    pub materializations: BTreeMap<u64, crate::materialization::MaterializationFact>,
    pub input_rows: usize,
    pub output_rows: usize,
    pub elapsed_ms: u64,
    pub managed_peak: usize,
    pub spill_bytes: u64,
    pub sent: NetworkMetrics,
    pub received: NetworkMetrics,
    pub retention: crate::RetentionMetrics,
    pub manifests: BTreeMap<u64, crate::lineage::TransferManifest>,
    pub destinations: BTreeMap<u64, NetworkMetrics>,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DistributedMetrics {
    pub topology: Option<crate::topology::Decision>,
    pub topology_commit_ns: u64,
    pub topology_probe_ns: u64,
    pub inactive: BTreeMap<u64, TaskMetrics>,
    pub recovery: crate::recovery::RecoveryMetrics,
    pub adaptive: crate::adaptive::AdaptiveSummary,
    pub worker_count: usize,
    pub exchange_mode: crate::ExchangeMode,
    pub elapsed_ms: u64,
    pub producers: BTreeMap<u64, TaskMetrics>,
    pub finals: BTreeMap<u64, TaskMetrics>,
    pub received: BTreeMap<u64, NetworkMetrics>,
    pub gather: NetworkMetrics,
}
