//! Reproducible benchmark infrastructure and adapters; execution lives in engine crates.
pub mod ade_distributed;
pub mod ade_join;
pub mod ade_local;
pub mod cli;
pub mod compare;
pub mod dataset;
pub mod engine;
pub mod environment;
pub mod join_dataset;
pub mod result;
pub mod runner;
pub mod spec;
pub mod stats;

/// Structured failures at benchmark boundaries.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("ADE distributed execution: {0}")]
    AdeDistributed(#[from] ::ade_distributed::Error),
    #[error("ADE local execution: {0}")]
    AdeLocal(#[from] ade_exec::Error),
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON schema: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid spec: {0}")]
    Toml(#[from] toml::de::Error),
    #[error("Arrow: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("Parquet: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("engine setup/query execution: {0}")]
    Engine(#[from] datafusion::error::DataFusionError),
    #[error("unsupported benchmark engine: {0}")]
    UnsupportedEngine(String),
    #[error("engine setup failed: {0}")]
    EngineSetup(datafusion::error::DataFusionError),
    #[error("query execution failed: {0}")]
    Query(datafusion::error::DataFusionError),
    #[error("invalid configuration/schema: {0}")]
    Invalid(String),
    #[error("correctness validation failed: {0}")]
    Validation(String),
    #[error("child process/protocol failure: {0}")]
    Child(String),
    #[error("incomparable results: {0}")]
    Incompatible(String),
}
/// Benchmark operation result.
pub type Result<T> = std::result::Result<T, Error>;

pub(crate) fn require(ok: bool, message: impl Into<String>) -> Result<()> {
    if ok {
        Ok(())
    } else {
        Err(Error::Invalid(message.into()))
    }
}
