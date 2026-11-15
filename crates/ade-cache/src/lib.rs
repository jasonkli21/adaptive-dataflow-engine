//! Bounded, opt-in cross-job materialization for local native plans.
#![cfg(all(feature = "shared-v1", any(target_os = "linux", target_os = "macos")))]

mod fs;
mod plan;
pub mod policy;
mod store;

pub use plan::Snapshot;
pub use store::{
    Audit, Inventory, Job, JobOutput, Limits, collect_expired, initialize, inspect, run,
};

use ade_exec::CancellationToken;
use std::{
    io,
    time::{Duration, Instant},
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("access refused")]
    Access,
    #[error("invalid request: {0}")]
    Request(&'static str),
    #[error("original snapshot or binding changed")]
    Snapshot,
    #[error("cache integrity: {0}")]
    Integrity(&'static str),
    #[error("cache capacity exceeded")]
    Capacity,
    #[error("cache lock wait exhausted")]
    Busy,
    #[error("job deadline exceeded")]
    Deadline,
    #[error("job cancelled")]
    Cancelled,
    #[error("local I/O error ({0:?})")]
    Io(Option<i32>),
    #[error("invalid or oversized JSON")]
    Json,
    #[error("native execution refused: {0}")]
    Native(&'static str),
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Self::Io(e.raw_os_error())
    }
}

impl From<rustix::io::Errno> for Error {
    fn from(e: rustix::io::Errno) -> Self {
        io::Error::from(e).into()
    }
}

impl From<serde_json::Error> for Error {
    fn from(_: serde_json::Error) -> Self {
        Self::Json
    }
}

impl From<ade_exec::Error> for Error {
    fn from(e: ade_exec::Error) -> Self {
        match e {
            ade_exec::Error::Io(e) => e.into(),
            ade_exec::Error::Cancelled => Self::Cancelled,
            ade_exec::Error::Deadline(_) => Self::Deadline,
            ade_exec::Error::Overflow(_) => Self::Native("arithmetic overflow"),
            ade_exec::Error::Invalid(_) => Self::Native("invalid source, plan or output cap"),
            _ => Self::Native("operator, Parquet or resource failure"),
        }
    }
}

type Result<T> = std::result::Result<T, Error>;

/// One cooperative deadline and cancellation token for the whole request.
#[derive(Debug, Clone)]
pub struct Control {
    token: CancellationToken,
    deadline: Instant,
}

impl Control {
    /// # Errors
    /// Refuses a zero timeout or a timeout above 120 seconds.
    pub fn new(token: CancellationToken, timeout: Duration) -> Result<Self> {
        if timeout.is_zero() || timeout > Duration::from_secs(120) {
            return Err(Error::Request("timeout must be in (0, 120s]"));
        }
        Ok(Self {
            token,
            deadline: Instant::now() + timeout,
        })
    }

    fn remaining(&self) -> Result<Duration> {
        self.token.check()?;
        self.deadline
            .checked_duration_since(Instant::now())
            .filter(|time| !time.is_zero())
            .ok_or(Error::Deadline)
    }

    /// # Errors
    /// Returns an error when cancelled or the deadline has elapsed.
    pub fn check(&self) -> Result<()> {
        self.remaining().map(|_| ())
    }

    fn fault(&self, point: &str) -> Result<()> {
        #[cfg(not(feature = "fault-injection"))]
        let _ = point;
        #[cfg(feature = "fault-injection")]
        if std::env::var("ADE_CACHE_PAUSE").as_deref() == Ok(point) {
            eprintln!("phase26_pause {point}");
            let resume = std::env::var_os("ADE_CACHE_RESUME").map(std::path::PathBuf::from);
            while self.check().is_ok() && !resume.as_ref().is_some_and(|path| path.is_file()) {
                std::thread::sleep(Duration::from_millis(5));
            }
        }
        self.check()
    }
}

fn is_digest(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|c| matches!(c, b'0'..=b'9' | b'a'..=b'f'))
}

fn wall_ms() -> Result<u64> {
    use std::time::{SystemTime, UNIX_EPOCH};
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Error::Request("invalid operator clock"))?
            .as_millis(),
    )
    .map_err(|_| Error::Request("clock range"))
}

fn elapsed_us(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_micros()).unwrap_or(u64::MAX)
}
