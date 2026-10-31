//! SQL binding and typed Rust queries compiled exclusively to ADE physical v1.
pub mod catalog;
pub mod dataframe;
pub mod execution;
pub mod sql;
pub use ade_physical::{PlanV1, Target};
pub use catalog::Catalog;
pub use dataframe::DataFrame;
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("unsupported/admission: {0}")]
    Unsupported(String),
    #[error(transparent)]
    Physical(#[from] ade_physical::Error),
    #[error(transparent)]
    Native(#[from] ade_exec::Error),
    #[error(transparent)]
    Distributed(#[from] ade_distributed::Error),
    #[error(transparent)]
    Bind(#[from] datafusion::error::DataFusionError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}
pub type Result<T> = std::result::Result<T, Error>;
pub(crate) fn require(ok: bool, message: &str) -> Result<()> {
    if ok {
        Ok(())
    } else {
        Err(Error::Unsupported(message.into()))
    }
}

#[cfg(feature = "unified-planner")]
pub mod unified;
