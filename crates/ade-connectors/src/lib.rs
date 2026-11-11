//! Opt-in connector contracts and bounded staging for ADE.
mod contract;
pub mod local;
pub use contract::{
    Catalog, CatalogEntryV1, ConnectorSpecV1, Context, PinnedSplit, Replay, ReplayCapability,
    ResolvedSnapshotV1, Source, SourceCapabilitiesV1, Stage,
};

#[cfg(feature = "batch")]
pub mod batch;
#[cfg(feature = "delta")]
pub mod delta;
#[cfg(feature = "flight")]
pub mod flight;
#[cfg(feature = "iceberg")]
pub mod iceberg;
#[cfg(feature = "kafka")]
pub mod kafka;
#[cfg(any(feature = "flight", feature = "postgres", feature = "s3"))]
mod network;
#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "s3")]
pub mod s3;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("connector admission: {0}")]
    Admission(&'static str),
    #[error("connector resource limit: {0}")]
    Limit(&'static str),
    #[error("connector snapshot mismatch: {0}")]
    Identity(&'static str),
    #[error("connector cancelled")]
    Cancelled,
    #[error("connector deadline exceeded")]
    Deadline,
    #[error("connector I/O failed: {0:?}")]
    Io(std::io::ErrorKind),
    #[error("connector protocol failed: {0}")]
    Protocol(&'static str),
    #[error("connector publication conflict")]
    Conflict,
    #[error("connector publication receipt ambiguous; reopen the key")]
    Ambiguous,
    #[error("native connector query failed: {0}")]
    Native(&'static str),
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error.kind())
    }
}
impl From<rustix::io::Errno> for Error {
    fn from(error: rustix::io::Errno) -> Self {
        std::io::Error::from(error).into()
    }
}
pub type Result<T> = std::result::Result<T, Error>;
