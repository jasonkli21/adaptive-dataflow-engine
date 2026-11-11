//! Opt-in native bounded local maintained dataflows.
#![cfg(feature = "maintained-v1")]

mod arrangement;
mod engine;
mod source;
mod storage;

pub use ade_exec::CancellationToken;
use ade_logical::maintained::{
    Decision, MAX_EPOCH, MaintainedPlanV1, PLAN_BYTES, Policy, Properties, is_digest,
};
pub use engine::{read, read_view, run};
use serde::{Deserialize, Serialize};
use std::{
    io,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

const MAX_STATE_BYTES: u64 = 64 * 1024 * 1024;
const REQUEST_BYTES: usize = 32 * 1024;
const MAX_HOT: usize = 262_144;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("maintained v1 refusal: {0}")]
    Invalid(&'static str),
    #[error("maintained v1 quota: {0}")]
    Quota(&'static str),
    #[error("maintained v1 deadline exceeded")]
    Deadline,
    #[error("maintained v1 writer or reader lease busy")]
    Busy,
    #[error("maintained v1 I/O: {0}")]
    Io(#[from] io::Error),
    #[error("maintained v1 JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Logical(#[from] ade_logical::Error),
    #[error(transparent)]
    Physical(#[from] ade_physical::Error),
    #[error(transparent)]
    Native(#[from] ade_exec::Error),
}
pub type Result<T> = std::result::Result<T, Error>;

fn require(ok: bool, reason: &'static str) -> Result<()> {
    if ok {
        Ok(())
    } else {
        Err(Error::Invalid(reason))
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    pub hot_entries: usize,
    pub state_bytes: u64,
    pub disk_bytes: u64,
    pub output_bytes: u64,
    pub timeout_ms: u64,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            hot_entries: 131_072,
            state_bytes: MAX_STATE_BYTES,
            disk_bytes: 256 * 1024 * 1024,
            output_bytes: 16 * 1024 * 1024,
            timeout_ms: 120_000,
        }
    }
}
impl Limits {
    fn validate(self) -> Result<()> {
        require(
            (1..=MAX_HOT).contains(&self.hot_entries)
                && (1..=MAX_STATE_BYTES).contains(&self.state_bytes)
                && (1..=512 * 1024 * 1024).contains(&self.disk_bytes)
                && (1..=MAX_STATE_BYTES).contains(&self.output_bytes)
                && (1..=120_000).contains(&self.timeout_ms),
            "limits outside supported envelope",
        )
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub version: u32,
    pub plan: PathBuf,
    pub root: PathBuf,
    pub baseline: Option<PathBuf>,
    pub deltas: Option<PathBuf>,
    pub delta_tail: Option<PathBuf>,
    pub epoch: u32,
    pub chain: String,
    #[serde(default)]
    pub limits: Limits,
    #[serde(default)]
    pub policy: Policy,
}
impl Request {
    /// # Errors
    /// Refuses oversized or malformed requests and unsafe input paths.
    pub fn from_path(path: &Path) -> Result<Self> {
        storage::no_symlinks(path)?;
        let guard = Guard::new(CancellationToken::default(), 5000);
        let request: Self =
            serde_json::from_slice(&storage::read_cap(path, REQUEST_BYTES, &guard)?)?;
        request.validate()?;
        Ok(request)
    }

    fn validate(&self) -> Result<()> {
        require(
            self.version == 1 && self.epoch <= MAX_EPOCH && is_digest(&self.chain),
            "request version, epoch or digest",
        )?;
        require(
            self.delta_tail.is_none() || (self.deltas.is_none() && self.epoch > 0),
            "choose one delta binding",
        )?;
        for p in [&self.plan, &self.root]
            .into_iter()
            .chain(self.baseline.iter())
            .chain(self.deltas.iter())
            .chain(self.delta_tail.iter())
        {
            require(
                !p.as_os_str().is_empty() && p.as_os_str().len() <= 4096,
                "bounded nonempty path",
            )?;
        }
        self.limits.validate()
    }

    fn load_plan(&self, guard: &Guard) -> Result<MaintainedPlanV1> {
        storage::no_symlinks(&self.plan)?;
        Ok(MaintainedPlanV1::from_json(&storage::read_cap(
            &self.plan, PLAN_BYTES, guard,
        )?)?)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action {
    Explain,
    Apply,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct Metrics {
    pub baseline_rows: u64,
    pub baseline_scan_bytes: u64,
    pub baseline_verified_bytes: u64,
    pub decoded_batch_peak: usize,
    pub source_delta_bytes: u64,
    pub delta_rows_applied: u64,
    pub keys_updated: u64,
    pub view_updates: u64,
    pub shards_decoded: u64,
    pub shards_written: u64,
    pub shard_bytes_written: u64,
    pub checkpoint_verified_bytes: u64,
    pub state_bytes: u64,
    pub root_bytes: u64,
    pub hot_entries_peak: usize,
    pub arrangement_instances: u8,
    pub defined_views: u8,
    pub wall_us: u128,
}

#[derive(Debug, Serialize)]
pub struct Outcome {
    pub plan: String,
    pub properties: Properties,
    pub epoch: u32,
    pub chain: String,
    pub decision: Decision,
    pub result: Option<String>,
    pub recovered_publication: bool,
    pub metrics: Metrics,
}

#[derive(Debug, Serialize)]
pub struct ViewRow {
    pub view: &'static str,
    pub key: u64,
    pub count: u64,
    pub sum: i64,
}

#[derive(Debug, Serialize)]
pub struct ReadOutcome {
    pub epoch: u32,
    pub chain: String,
    pub result: String,
    pub view_rows: u64,
    pub views_read: u8,
    pub hot_entries_peak: usize,
}

struct Guard {
    cancel: CancellationToken,
    until: Instant,
}
impl Guard {
    fn new(cancel: CancellationToken, timeout_ms: u64) -> Self {
        Self {
            cancel,
            until: Instant::now() + Duration::from_millis(timeout_ms),
        }
    }

    fn check(&self) -> Result<()> {
        self.cancel.check()?;
        if Instant::now() >= self.until {
            return Err(Error::Deadline);
        }
        Ok(())
    }

    fn remaining_ms(&self) -> u64 {
        u64::try_from(
            self.until
                .saturating_duration_since(Instant::now())
                .as_millis(),
        )
        .unwrap_or_default()
    }
}

fn fault(point: &str) -> Result<()> {
    if cfg!(feature = "fault-injection") {
        if std::env::var("ADE19_SHORT_WRITE").as_deref() == Ok(point) {
            return Err(io::Error::from(io::ErrorKind::WriteZero).into());
        }
        if std::env::var("ADE19_STOP_AT").as_deref() == Ok(point) {
            eprintln!("fault reached {point}");
            let resume = std::env::var_os("ADE19_CONTINUE_FILE").map(PathBuf::from);
            while resume.as_ref().is_none_or(|path| !path.exists()) {
                std::thread::sleep(Duration::from_millis(5));
            }
        }
    }
    Ok(())
}
