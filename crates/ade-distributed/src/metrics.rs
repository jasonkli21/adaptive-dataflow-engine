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
    pub source_files: usize,
    pub input_rows: usize,
    pub output_rows: usize,
    pub elapsed_ms: u64,
    pub managed_peak: usize,
    pub spill_bytes: u64,
    pub sent: NetworkMetrics,
    pub received: NetworkMetrics,
    pub destinations: BTreeMap<u64, NetworkMetrics>,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DistributedMetrics {
    pub worker_count: usize,
    pub elapsed_ms: u64,
    pub producers: BTreeMap<u64, TaskMetrics>,
    pub finals: BTreeMap<u64, TaskMetrics>,
    pub received: BTreeMap<u64, NetworkMetrics>,
    pub gather: NetworkMetrics,
}
