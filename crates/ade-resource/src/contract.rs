use crate::{Error, Result, decode, encode, hash, require, store::read_bounded};
use ade_ops::{Input, Recipe, Worker};
use ade_physical::{PlanV1, Target};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
};
use uuid::Uuid;

pub const MAX_TENANTS: usize = 8;
pub const MAX_JOBS: usize = 64;
pub const MAX_NATIVE_JOBS: usize = 32;
pub const MAX_LAUNCHES: usize = 2;
pub const FRAME: usize = 256 * 1024;
pub const MAX_SPEC: usize = 64 * 1024;
pub const MAX_TIME_MS: u64 = 120_000;
pub const MIB: u64 = 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    Fifo,
    Adaptive,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    pub queued: usize,
    pub running: usize,
    pub slots: u64,
    pub managed_bytes: u64,
    pub staging_bytes: u64,
    pub retained_bytes: u64,
    pub cumulative_write_bytes: u64,
    pub cost_microunits: Option<u64>,
}

impl Limits {
    pub fn validate(&self) -> Result<()> {
        require(
            (1..=32).contains(&self.queued)
                && (1..=2).contains(&self.running)
                && (1..=8).contains(&self.slots)
                && (1..=64 * 1024 * MIB).contains(&self.managed_bytes)
                && (1..=128 * MIB).contains(&self.staging_bytes)
                && (1..=MAX_JOBS as u64 * 64 * MIB).contains(&self.retained_bytes)
                && (1..=2 * MAX_NATIVE_JOBS as u64 * 6 * 64 * MIB)
                    .contains(&self.cumulative_write_bytes)
                && self
                    .cost_microunits
                    .is_none_or(|cap| (1..=1_000_000_000_000_000).contains(&cap)),
            "INVALID_LIMITS",
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Price {
    pub version: u32,
    pub currency: String,
    pub microunits_per_slot_ms: u64,
}
impl Price {
    pub fn validate(&self) -> Result<()> {
        require(
            self.version == 1
                && self.currency.len() == 3
                && self.currency.bytes().all(|c| c.is_ascii_uppercase())
                && (1..=1_000_000).contains(&self.microunits_per_slot_ms),
            "INVALID_PRICE",
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cohort {
    pub root: PathBuf,
    pub store: Uuid,
    pub workers: Vec<Worker>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub version: u32,
    pub mode: Mode,
    pub aging_ms: u64,
    pub standby_idle_ms: u64,
    pub preempt_after_ms: u64,
    pub limits: Limits,
    pub price: Option<Price>,
    pub cohorts: [Cohort; 2],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Directory {
    pub path: PathBuf,
    device: u64,
    inode: u64,
}

pub fn owned(path: &Path, directory: bool) -> Result<fs::Metadata> {
    let meta = fs::symlink_metadata(path).map_err(|_| Error("UNSAFE_PATH"))?;
    require(
        meta.uid() == rustix::process::geteuid().as_raw()
            && meta.mode() & 0o022 == 0
            && !meta.file_type().is_symlink()
            && if directory {
                meta.is_dir()
            } else {
                meta.is_file() && meta.nlink() == 1
            },
        "UNSAFE_PATH",
    )?;
    Ok(meta)
}

impl Directory {
    pub fn capture(path: PathBuf) -> Result<Self> {
        require(
            path.is_absolute()
                && path.as_os_str().len() <= 4096
                && path.canonicalize().is_ok_and(|canonical| canonical == path),
            "UNSAFE_PATH",
        )?;
        let meta = owned(&path, true)?;
        Ok(Self {
            path,
            device: meta.dev(),
            inode: meta.ino(),
        })
    }

    pub fn check(&self) -> Result<()> {
        require(
            Self::capture(self.path.clone())? == *self,
            "SOURCE_ROOT_DRIFT",
        )
    }

    pub fn allow(&self, path: &Path, directory: bool) -> Result<()> {
        use std::path::Component::Normal;
        self.check()?;
        let relative = path
            .strip_prefix(&self.path)
            .map_err(|_| Error("SOURCE_SCOPE"))?;
        require(
            path.as_os_str().len() <= 4096
                && relative.components().all(|part| matches!(part, Normal(_))),
            "SOURCE_SCOPE",
        )?;
        let mut current = self.path.clone();
        let mut parts = relative.components().peekable();
        if parts.peek().is_none() {
            return require(directory, "SOURCE_SCOPE");
        }
        while let Some(part) = parts.next() {
            current.push(part);
            owned(&current, directory || parts.peek().is_some())?;
        }
        require(
            path.canonicalize().is_ok_and(|canonical| canonical == path),
            "SOURCE_SCOPE",
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TenantConfig {
    pub source_root: PathBuf,
    pub weight: u64,
    pub limits: Limits,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Tenant {
    pub source: Directory,
    pub weight: u64,
    pub limits: Limits,
    pub fingerprint: Option<String>,
    pub granted_work: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Spec {
    pub version: u32,
    pub plan: PlanV1,
    pub inputs: Vec<Input>,
    pub deadline_ms: u64,
    pub priority: u8,
    pub work: u64,
    pub preemptible: bool,
    pub runtime_bytes: u64,
    pub staging_bytes: u64,
}

impl Spec {
    pub fn validate(&self, workers: &[Worker]) -> Result<()> {
        require(
            self.version == 1
                && self.priority <= 2
                && (1..=64).contains(&self.work)
                && (1..=MAX_TIME_MS).contains(&self.deadline_ms)
                && encode(self)?.len() <= MAX_SPEC
                && matches!(
                    self.plan.target,
                    Target::DistributedAggregate | Target::DistributedJoin
                ),
            "INVALID_SPEC",
        )?;
        self.recipe(workers)
            .validate()
            .map_err(|_| Error("INVALID_RECIPE"))
    }

    pub fn recipe(&self, workers: &[Worker]) -> Recipe {
        Recipe {
            version: 1,
            plan: self.plan.clone(),
            inputs: self.inputs.clone(),
            workers: workers.to_vec(),
            timeout_ms: self.deadline_ms,
            max_attempts: 1,
            runtime_bytes: self.runtime_bytes,
            staging_bytes: self.staging_bytes,
        }
    }

    /// Capture filesystem identity and authorize every manifest file that the
    /// native frontend can open for the selected table.
    pub fn source_pins(&self, directory: &Directory) -> Result<Vec<FilePin>> {
        let mut pins = BTreeMap::new();
        for (input, source) in self.inputs.iter().zip(&self.plan.sources) {
            directory.allow(&input.root, true)?;
            directory.allow(&input.manifest, false)?;
            let manifest =
                std::fs::File::open(&input.manifest).map_err(|_| Error("SOURCE_DRIFT"))?;
            let manifest_pin =
                FilePin::from_metadata(&manifest.metadata().map_err(|_| Error("SOURCE_DRIFT"))?);
            let bytes = read_bounded(manifest, 1024 * 1024)?;
            require(hash(&bytes) == source.manifest, "SOURCE_DRIFT")?;
            require(
                FilePin::capture(&input.manifest)? == manifest_pin,
                "SOURCE_DRIFT",
            )?;
            pins.insert(input.manifest.clone(), manifest_pin);
            let manifest: ManifestFiles = decode(&bytes).map_err(|_| Error("SOURCE_DRIFT"))?;
            let prefix = match input.table.as_str() {
                "ade_left" => Some("left/"),
                "ade_right" => Some("right/"),
                _ => None,
            };
            let actual = manifest
                .files
                .iter()
                .filter(|entry| prefix.is_none_or(|prefix| entry.relative_path.starts_with(prefix)))
                .map(|entry| entry.relative_path.as_str());
            require(
                actual.eq(source.files.iter().map(|file| file.name.as_str())),
                "SOURCE_SCOPE",
            )?;
            for file in &source.files {
                let path = input.root.join(&file.name);
                directory.allow(&path, false)?;
                let pin = FilePin::capture(&path)?;
                require(pin.bytes == file.bytes, "SOURCE_DRIFT")?;
                pins.insert(path, pin);
            }
        }
        Ok(pins.into_values().collect())
    }

    pub fn digest(&self) -> Result<String> {
        Ok(hash(&encode(self)?))
    }
}

#[derive(Deserialize)]
struct ManifestFiles {
    files: Vec<ManifestEntry>,
}
#[derive(Deserialize)]
struct ManifestEntry {
    relative_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FilePin {
    device: u64,
    inode: u64,
    bytes: u64,
    modified_sec: i64,
    modified_nsec: i64,
    changed_sec: i64,
    changed_nsec: i64,
}
impl FilePin {
    fn capture(path: &Path) -> Result<Self> {
        Ok(Self::from_metadata(&owned(path, false)?))
    }

    fn from_metadata(metadata: &fs::Metadata) -> Self {
        Self {
            device: metadata.dev(),
            inode: metadata.ino(),
            bytes: metadata.len(),
            modified_sec: metadata.mtime(),
            modified_nsec: metadata.mtime_nsec(),
            changed_sec: metadata.ctime(),
            changed_nsec: metadata.ctime_nsec(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum State {
    Queued,
    Reserved,
    Starting,
    Running,
    Succeeded,
    Failed,
    Cancelled,
    Expired,
    Quarantined,
}
impl State {
    pub fn holds_cohort(self) -> bool {
        matches!(
            self,
            Self::Reserved | Self::Starting | Self::Running | Self::Quarantined
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Intent {
    Preempt,
    Cancel,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Preempted,
    Succeeded,
    Failed,
    Cancelled,
    Refused,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Grant {
    pub slots: u64,
    pub managed_bytes: u64,
    pub staging_bytes: u64,
    pub write_bytes: u64,
    pub cost: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Usage {
    pub end_ms: u64,
    pub outcome: Outcome,
    pub slot_ms: u64,
    pub charge: Option<u64>,
    pub unpriced_slot_ms: u64,
    pub written_bytes: Option<u64>,
    pub committed_bytes: u64,
    pub ipc_bytes: Option<u64>,
    pub ipc_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Attempt {
    pub cohort: usize,
    pub native_key: String,
    pub native_id: Option<Uuid>,
    pub instance: Option<Uuid>,
    pub engine_job: Option<u64>,
    pub owner: Option<u64>,
    pub admitted_ms: u64,
    pub running_ms: Option<u64>,
    pub grant: Grant,
    pub price: Option<Price>,
    pub price_hash: Option<String>,
    pub priority: u8,
    pub tenant_work_before: u64,
    pub usage: Option<Usage>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Job {
    pub tenant: Uuid,
    pub key: String,
    pub digest: String,
    pub spec: Spec,
    pub source_pins: Vec<FilePin>,
    pub sequence: u64,
    pub queued_ms: u64,
    pub expires_ms: u64,
    pub state: State,
    pub intent: Option<Intent>,
    pub reason: String,
    pub attempts: Vec<Attempt>,
}

impl Job {
    pub fn verify_source_scope(&self, directory: &Directory) -> Result<()> {
        require(
            self.spec.source_pins(directory)? == self.source_pins,
            "SOURCE_DRIFT",
        )
    }

    pub fn queue_obligation(&self) -> bool {
        self.state == State::Queued
            || (self.state.holds_cohort()
                && self.spec.preemptible
                && self.attempts.len() < MAX_LAUNCHES)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Ledger {
    pub generation: u64,
    pub clock_ms: u64,
    pub next_sequence: u64,
    pub admin: String,
    pub config: Config,
    pub tenants: BTreeMap<Uuid, Tenant>,
    pub jobs: BTreeMap<Uuid, Job>,
    pub standby_on: bool,
    pub standby_changed_ms: u64,
    pub standby_idle_since_ms: Option<u64>,
    pub standby_transitions: Vec<(u64, bool)>,
}

pub fn valid_key(key: &str) -> bool {
    (1..=128).contains(&key.len())
        && key
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"-_.".contains(&c))
}

pub fn native_key(job: Uuid, ordinal: usize) -> String {
    format!("phase23-{job}-{ordinal}")
}
