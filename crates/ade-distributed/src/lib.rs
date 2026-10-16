//! Distributed aggregation with static topology and selectable retention and bounded lineage recovery.
pub mod adaptive;
mod retention;
#[cfg(feature = "research-hooks")]
pub use retention::PressureConfig;
pub use retention::{
    ExchangeConfig, ExchangeMode, ObjectStoreBackend, ObjectStoreConfig, ObjectStoreDurability,
    RetentionMetrics,
};
pub mod coordinator;
pub mod exchange;
mod gather;
pub mod ids;
pub mod lineage;
pub mod metrics;
pub mod partition;
pub mod protocol;
pub mod recovery;
pub mod worker;
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, time::Duration};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("Arrow: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("object store: {0}")]
    ObjectStore(#[from] object_store::Error),
    #[error("encoding: {0}")]
    Json(#[from] serde_json::Error),
    #[error("local execution: {0}")]
    Exec(#[from] ade_exec::Error),
    #[error("invalid protocol/configuration: {0}")]
    Invalid(String),
    #[error("job {job:?}, worker {worker:?}, stage {stage}: {message}")]
    Task {
        job: ids::JobId,
        worker: ids::WorkerId,
        stage: &'static str,
        message: String,
    },
    #[error("unrecoverable: {0}")]
    Unrecoverable(String),
    #[error("remote {code}: {message}")]
    Remote { code: String, message: String },
}
pub type Result<T> = std::result::Result<T, Error>;
pub(crate) fn require(ok: bool, message: &str) -> Result<()> {
    if ok {
        Ok(())
    } else {
        Err(Error::Invalid(message.into()))
    }
}
/// Socket inactivity timeout is separate from the whole-stage control deadline.
pub const IO_TIMEOUT: Duration = Duration::from_secs(5);
pub const TASK_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerEndpoint {
    pub id: ids::WorkerId,
    pub control: SocketAddr,
    pub exchange: SocketAddr,
}
/// Effective local execution controls sent explicitly to every task.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskConfig {
    pub batch_size: usize,
    pub memory_limit_bytes: usize,
    pub spill_enabled: bool,
    pub prefetch_batches: usize,
    pub exchange: ExchangeConfig,
    #[cfg(feature = "research-hooks")]
    pub research_exit_final_partition: Option<u64>,
}
impl Default for TaskConfig {
    fn default() -> Self {
        Self {
            batch_size: 8192,
            memory_limit_bytes: 256 * 1024 * 1024,
            spill_enabled: true,
            prefetch_batches: 0,
            exchange: ExchangeConfig::default(),
            #[cfg(feature = "research-hooks")]
            research_exit_final_partition: None,
        }
    }
}
impl TaskConfig {
    #[must_use]
    pub fn execution(&self) -> ade_exec::ExecutionConfig {
        ade_exec::ExecutionConfig {
            memory_limit_bytes: self.memory_limit_bytes,
            spill_enabled: self.spill_enabled,
            prefetch_batches: self.prefetch_batches,
            output_batch_rows: exchange::MAX_BATCH_ROWS,
        }
    }
    /// # Errors
    /// Rejects zero batch/memory sizes or excessive prefetch depth.
    pub fn validate(&self) -> Result<()> {
        require(self.batch_size > 0, "positive batch_size required")?;
        self.execution().validate()?;
        self.exchange.validate()?;
        Ok(())
    }
}
