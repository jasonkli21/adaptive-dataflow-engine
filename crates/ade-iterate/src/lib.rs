#![cfg(all(unix, feature = "cyclic-v1"))]
//! Opt-in immutable graph admission, checkpoint operations and terminal reads.

mod engine;
mod output;
mod source;
mod storage;

pub use ade_exec::CancellationToken;
pub use ade_physical::cycle::{Algorithm, CyclicPlanV1};
pub use engine::{Command, Event, Summary, operate};
pub use output::Footer;
pub use storage::Pin;

use ade_exec::iterative::{MAX_BATCH_ROWS, MAX_STATE_BYTES, Mode};
use ade_logical::iterative::{GraphInventoryV1, MAX_SOURCE_BYTES};
use ade_physical::cycle::{MAX_CREDITS, MAX_ROUNDS};
use serde::{Deserialize, Serialize};
use std::{
    io,
    path::PathBuf,
    time::{Duration, Instant},
};

pub const REQUEST_BYTES: usize = 64 * 1024;
pub const MAX_DISK_BYTES: u64 = 256 * 1024 * 1024;
pub const MAX_OUTPUT_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_DEADLINE_MS: u64 = 120_000;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("refused: {0}")]
    Refused(&'static str),
    #[error("INCOMPLETE_LIMIT: {0}")]
    Limit(&'static str),
    #[error("INCOMPLETE_CANCELLED")]
    Cancelled,
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Logical(#[from] ade_logical::Error),
    #[error(transparent)]
    Physical(#[from] ade_physical::Error),
    #[error(transparent)]
    Native(ade_exec::Error),
}
impl From<ade_exec::Error> for Error {
    fn from(value: ade_exec::Error) -> Self {
        match value {
            ade_exec::Error::Cancelled => Self::Cancelled,
            ade_exec::Error::Deadline(_) => Self::Limit("deadline"),
            ade_exec::Error::MemoryLimit { .. } => Self::Limit("managed state bytes"),
            e => Self::Native(e),
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

fn require(ok: bool, why: &'static str) -> Result<()> {
    if ok { Ok(()) } else { Err(Error::Refused(why)) }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Limits {
    pub source_bytes: u64,
    pub state_bytes: usize,
    pub disk_bytes: u64,
    pub output_bytes: usize,
    pub deadline_ms: u64,
    pub rounds: u32,
    pub credits: usize,
    pub batch_rows: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            source_bytes: MAX_SOURCE_BYTES,
            state_bytes: MAX_STATE_BYTES,
            disk_bytes: MAX_DISK_BYTES,
            output_bytes: MAX_OUTPUT_BYTES,
            deadline_ms: MAX_DEADLINE_MS,
            rounds: MAX_ROUNDS,
            credits: MAX_CREDITS,
            batch_rows: MAX_BATCH_ROWS,
        }
    }
}
impl Limits {
    fn validate(&self) -> Result<()> {
        let max = Self::default();
        for (value, cap) in [
            (self.source_bytes, max.source_bytes),
            (self.state_bytes as u64, max.state_bytes as u64),
            (self.disk_bytes, max.disk_bytes),
            (self.output_bytes as u64, max.output_bytes as u64),
            (self.deadline_ms, max.deadline_ms),
            (u64::from(self.rounds), u64::from(max.rounds)),
            (self.credits as u64, max.credits as u64),
            (self.batch_rows as u64, max.batch_rows as u64),
        ] {
            require(
                value > 0 && value <= cap,
                "limits outside cyclic v1 envelope",
            )?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub version: u8,
    pub plan: CyclicPlanV1,
    pub inventory: GraphInventoryV1,
    pub source_dir: PathBuf,
    pub root: PathBuf,
    #[serde(default)]
    pub expected: Option<Pin>,
    #[serde(default)]
    pub limits: Limits,
    #[serde(default)]
    pub checkpoint_every: u32,
    #[serde(default)]
    pub mode: Mode,
    #[serde(default)]
    pub reverse: bool,
    #[serde(default)]
    pub trace: bool,
}

impl Request {
    /// # Errors
    /// Refuses oversized or malformed request JSON.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        require(bytes.len() <= REQUEST_BYTES, "request byte cap")?;
        Ok(serde_json::from_slice(bytes)?)
    }
}

pub struct Guard {
    cancel: CancellationToken,
    deadline: Instant,
}
impl Guard {
    fn new(cancel: CancellationToken, ms: u64) -> Self {
        Self {
            cancel,
            deadline: Instant::now() + Duration::from_millis(ms),
        }
    }
    /// # Errors
    /// Reports operation cancellation or deadline.
    pub fn check(&self) -> Result<()> {
        self.cancel.check()?;
        if Instant::now() >= self.deadline {
            return Err(Error::Limit("deadline"));
        }
        Ok(())
    }
}

#[cfg(feature = "fault-injection")]
fn fault(point: &str, round: u32, guard: &Guard) -> Result<()> {
    if std::env::var("ADE_CYCLE20_TEST_POINT").as_deref() == Ok(point)
        && std::env::var("ADE_CYCLE20_TEST_ROUND")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0)
            == round
    {
        use std::io::Write;
        writeln!(io::stderr().lock(), "cycle20-fault {point} {round}")?;
        if point == "short_write" {
            return Err(io::Error::new(io::ErrorKind::WriteZero, "injected short write").into());
        }
        if matches!(
            point,
            "enospc_continue" | "source_decoded" | "before_round" | "before_select"
        ) {
            // Used only by an explicitly armed fixture. Continue after the
            // operator creates this sentinel outside the publication root.
            if let Some(path) = std::env::var_os("ADE_CYCLE20_TEST_CONTINUE") {
                while !std::path::Path::new(&path).is_file() {
                    guard.check()?;
                    std::thread::sleep(Duration::from_millis(2));
                }
                return Ok(());
            }
        }
        loop {
            guard.check()?;
            std::thread::sleep(Duration::from_millis(2));
        }
    }
    Ok(())
}

#[cfg(not(feature = "fault-injection"))]
fn fault(_point: &str, _round: u32, _guard: &Guard) -> Result<()> {
    Ok(())
}
