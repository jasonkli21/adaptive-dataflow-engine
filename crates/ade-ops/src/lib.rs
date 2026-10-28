//! Opt-in durable, fenced local batch operations for bounded native graphs.
//! No operations dependency is introduced into the physical IR or native runtime.
#![forbid(unsafe_code)]
#[cfg(not(unix))]
compile_error!("ade-ops currently requires Unix local filesystem identities/permissions");

pub mod publication;
mod recipe;
pub mod runner;
mod store;
pub use recipe::{Input, Recipe, Worker};
pub use store::{Event, Job, JobState, Limits, Store};

use std::{
    io,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("job {job} ended in {state:?}")]
    JobOutcome { job: uuid::Uuid, state: JobState },
    #[error("native: {0}")]
    Native(#[from] ade_distributed::Error),
    #[error("I/O: {0}")]
    Io(#[from] io::Error),
    #[error("encoding: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid/corrupt operations state: {0}")]
    Invalid(String),
    #[error("quota: {0}")]
    Quota(&'static str),
    #[error("another coordinator owns this state root")]
    Owned,
    #[error("deadline expired or clock moved backwards")]
    Deadline,
    #[error("unsupported: {0}")]
    Unsupported(&'static str),
}
impl Error {
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::JobOutcome { .. } => "JOB_OUTCOME",
            Self::Native(_) => "NATIVE",
            Self::Io(_) => "IO",
            Self::Json(_) => "ENCODING",
            Self::Invalid(_) => "INVALID_STATE_OR_INPUT",
            Self::Quota(_) => "QUOTA",
            Self::Owned => "OWNER_BUSY",
            Self::Deadline => "DEADLINE_OR_CLOCK",
            Self::Unsupported(_) => "UNSUPPORTED",
        }
    }
}
pub type Result<T> = std::result::Result<T, Error>;
pub(crate) fn require(ok: bool, message: &str) -> Result<()> {
    if ok {
        Ok(())
    } else {
        Err(Error::Invalid(message.into()))
    }
}
pub(crate) fn now_ms() -> Result<u64> {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Error::Deadline)?
            .as_millis(),
    )
    .map_err(|_| Error::Deadline)
}
pub(crate) fn deadline(expiry: u64) -> Result<Instant> {
    let remaining = expiry
        .checked_sub(now_ms()?)
        .filter(|n| *n > 0)
        .ok_or(Error::Deadline)?;
    Ok(Instant::now() + Duration::from_millis(remaining.min(120_000)))
}
pub(crate) fn check_time(until: Instant) -> Result<()> {
    if Instant::now() >= until {
        Err(Error::Deadline)
    } else {
        Ok(())
    }
}

// Available only in explicitly feature-enabled test binaries. Witness precedes
// the pause, so a parent can prove which durable boundary was reached before kill.
pub(crate) fn fault(stage: &str) {
    #[cfg(feature = "fault-injection")]
    if std::env::var("ADE_OPS_FAULT").as_deref() == Ok(stage) {
        use std::io::Write;
        eprintln!("ADE_OPS_REACHED {stage}");
        let _ = std::io::stderr().flush();
        loop {
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    let _ = stage;
}

pub(crate) fn io_point(stage: &str) -> Result<()> {
    #[cfg(feature = "fault-injection")]
    if stage == "link" && std::env::var("ADE_OPS_FAIL_IO").as_deref() == Ok("exdev") {
        eprintln!("ADE_OPS_IO_WITNESS exdev");
        return Err(io::Error::from(io::ErrorKind::CrossesDevices).into());
    }
    #[cfg(feature = "fault-injection")]
    if std::env::var("ADE_OPS_FAIL_IO").as_deref() == Ok(stage) {
        eprintln!("ADE_OPS_IO_WITNESS {stage}");
        return Err(io::Error::other(format!("injected {stage} failure")).into());
    }
    let _ = stage;
    Ok(())
}
