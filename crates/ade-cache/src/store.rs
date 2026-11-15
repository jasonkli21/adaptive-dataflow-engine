use crate::{
    Control, Error, Result, elapsed_us, fs, is_digest,
    plan::{Cut, Original, PAYLOAD, SEMANTICS, schema_identity},
    policy::{self, Calibration, Decision, Mode},
    wall_ms,
};
use ade_exec::{ExecutionConfig, ExecutionMetrics, ExecutionOutput, physical};
use ade_physical::{MAX_SOURCE_BYTES, MAX_SOURCE_ROWS, PlanV1};
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    errors::ParquetError,
    file::properties::WriterProperties,
};
use rustix::fs::OFlags;
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    io::{self, Read, Seek, SeekFrom, Write},
    os::unix::fs::MetadataExt,
    path::Path,
    time::Instant,
};

const LOCK: &str = "LOCK";
const CONFIG: &str = "root.json";
const CONFIG_STAGE: &str = ".root";
const STAGE: &str = ".stage";
const TRASH: &str = ".trash";
const MANIFEST: &str = "entry.json";
const META_CAP: usize = 4096;
const ROOT_CAP: u64 = 256 * 1024 * 1024;
const ENTRY_CAP: usize = 32;
const CONFIG_CAP: usize = 256;
const JANITOR_CAP: usize = ENTRY_CAP + 12;
const BATCH: usize = 1024;
const MAX_TTL_MS: u64 = 7 * 24 * 60 * 60 * 1000;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    pub quota_bytes: u64,
    pub max_entries: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            quota_bytes: ROOT_CAP,
            max_entries: ENTRY_CAP,
        }
    }
}

impl Limits {
    fn check(self) -> Result<()> {
        if !(8192..=ROOT_CAP).contains(&self.quota_bytes)
            || !(1..=ENTRY_CAP).contains(&self.max_entries)
        {
            return Err(Error::Request("root quota or entry cap"));
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RootConfig {
    version: u32,
    limits: Limits,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    version: u32,
    semantics: String,
    scope: String,
    key: String,
    snapshot: String,
    schema: String,
    digest: String,
    bytes: u64,
    rows: u64,
    created_ms: u64,
    expires_ms: u64,
    prefix_us: u64,
    calibration: Option<Calibration>,
}

impl Entry {
    fn reusable_at(&self, now: u64) -> Result<bool> {
        if now < self.created_ms {
            return Err(Error::Request(
                "operator clock precedes cache entry creation",
            ));
        }
        Ok(now < self.expires_ms)
    }

    fn validate(&self, root: &Root<'_>, key: &str) -> Result<()> {
        if self.version != 2
            || self.semantics != SEMANTICS
            || self.scope != root.scope
            || self.key != key
            || !is_digest(&self.snapshot)
            || !is_digest(&self.schema)
            || !is_digest(&self.digest)
            || !(12..=MAX_SOURCE_BYTES).contains(&self.bytes)
            || self.rows > MAX_SOURCE_ROWS
            || !matches!(
                self.expires_ms.checked_sub(self.created_ms),
                Some(1..=MAX_TTL_MS)
            )
        {
            return Err(Error::Integrity("manifest identity or bounds"));
        }
        if let Some(estimate) = self.calibration {
            estimate
                .value(true)
                .map_err(|_| Error::Integrity("manifest forecast"))?;
        }
        Ok(())
    }
}

struct Root<'a> {
    path: &'a Path,
    dir: File,
    lease: File,
    scope: String,
}

impl<'a> Root<'a> {
    fn open(path: &'a Path, create: bool) -> Result<Self> {
        let dir = fs::directory(path, true).map_err(|_| Error::Access)?;
        let lease = fs::control_file(&dir, LOCK, create)?;
        let m = dir.metadata()?;
        let scope = ade_physical::fingerprint(
            format!("ade.cache.local.v1/{}/{}/{}", fs::uid(), m.dev(), m.ino()).as_bytes(),
        );
        Ok(Self {
            path,
            dir,
            lease,
            scope,
        })
    }

    fn attest(&self) -> Result<()> {
        let current = fs::directory(self.path, true).map_err(|_| Error::Access)?;
        let (old, new) = (self.dir.metadata()?, current.metadata()?);
        if (old.dev(), old.ino()) != (new.dev(), new.ino()) {
            return Err(Error::Access);
        }
        let old = self.lease.metadata()?;
        let new = fs::control_file(&current, LOCK, false)?.metadata()?;
        if (old.dev(), old.ino(), old.nlink()) != (new.dev(), new.ino(), 1) {
            return Err(Error::Access);
        }
        Ok(())
    }

    fn limits(&self, control: &Control) -> Result<Limits> {
        let cfg = fs::data(&self.dir, CONFIG, OFlags::RDONLY, true)?;
        let value: RootConfig = strict_json(cfg, CONFIG_CAP, control)?;
        if value.version != 1 {
            return Err(Error::Integrity("root version"));
        }
        value.limits.check()?;
        Ok(value.limits)
    }

    fn entry(&self, key: &str, control: &Control) -> Result<Option<(Entry, u64)>> {
        let dir = match fs::private_dir(&self.dir, key) {
            Ok(dir) => dir,
            Err(Error::Io(Some(n))) if n == rustix::io::Errno::NOENT.raw_os_error() => {
                return Ok(None);
            }
            Err(e) => return Err(e),
        };
        if fs::names(&dir, 2, control)? != [MANIFEST, PAYLOAD] {
            return Err(Error::Integrity("incomplete entry"));
        }
        let metadata = fs::data(&dir, MANIFEST, OFlags::RDONLY, true)?;
        let manifest_bytes = metadata.metadata()?.len();
        let entry: Entry = strict_json(metadata, META_CAP, control)?;
        entry.validate(self, key)?;
        let payload = fs::data(&dir, PAYLOAD, OFlags::RDONLY, true)?;
        if payload.metadata()?.len() != entry.bytes {
            return Err(Error::Integrity("payload length"));
        }
        let bytes = entry
            .bytes
            .checked_add(manifest_bytes)
            .ok_or(Error::Capacity)?;
        Ok(Some((entry, bytes)))
    }

    fn inventory(
        &self,
        control: &Control,
        now: u64,
        limits: Limits,
    ) -> Result<Vec<policy::Alternative>> {
        let mut entries = Vec::new();
        let mut total = 0u64;
        for key in fs::names(&self.dir, JANITOR_CAP, control)? {
            if matches!(key.as_str(), LOCK | CONFIG | STAGE | TRASH)
                || fs::slots().any(|n| n == key)
            {
                continue;
            }
            if !is_digest(&key) || entries.len() == limits.max_entries {
                return Err(Error::Integrity("root inventory"));
            }
            let (entry, bytes) = self
                .entry(&key, control)?
                .ok_or(Error::Integrity("entry disappeared"))?;
            total = total.checked_add(bytes).ok_or(Error::Capacity)?;
            if total
                .checked_add(self.config_bytes()?)
                .ok_or(Error::Capacity)?
                > limits.quota_bytes
            {
                return Err(Error::Integrity("recorded capacity"));
            }
            entries.push(policy::Alternative {
                key,
                bytes,
                remaining_us: entry
                    .calibration
                    .map(|c| policy::remaining_value(c, now, entry.created_ms, entry.expires_ms))
                    .transpose()?,
                expired: now >= entry.expires_ms,
                created_ms: entry.created_ms,
                expires_ms: entry.expires_ms,
                observed_ms: now,
            });
        }
        Ok(entries)
    }

    fn config_bytes(&self) -> Result<u64> {
        Ok(fs::data(&self.dir, CONFIG, OFlags::RDONLY, true)?
            .metadata()?
            .len())
    }

    fn clean(&self, name: &str, control: &Control) -> Result<()> {
        let dir = match fs::private_dir(&self.dir, name) {
            Ok(dir) => dir,
            Err(Error::Io(Some(n))) if n == rustix::io::Errno::NOENT.raw_os_error() => {
                return Ok(());
            }
            Err(e) => return Err(e),
        };
        for file in fs::names(&dir, 2, control)? {
            if !matches!(file.as_str(), MANIFEST | PAYLOAD) {
                return Err(Error::Integrity("scratch inventory"));
            }
            fs::data(&dir, &file, OFlags::RDONLY, true)?;
            fs::unlink(&dir, &file, false)?;
        }
        fs::unlink(&self.dir, name, true)?;
        self.dir.sync_all()?;
        control.check()
    }

    fn evict(&self, key: &str, control: &Control) -> Result<()> {
        control.check()?;
        self.attest()?;
        fs::move_dir(&self.dir, key, TRASH)?;
        self.dir.sync_all()?;
        control.fault("eviction_selected")?;
        self.clean(TRASH, control)
    }
}

fn strict_json<T: for<'de> Deserialize<'de> + Serialize>(
    file: File,
    cap: usize,
    control: &Control,
) -> Result<T> {
    let bytes = fs::read_bounded(file, cap, control)?;
    let value: T = serde_json::from_slice(&bytes).map_err(|_| Error::Integrity("manifest JSON"))?;
    if serde_json::to_vec(&value)? != bytes {
        return Err(Error::Integrity("manifest encoding"));
    }
    Ok(value)
}

/// Initialize an existing, empty 0700 directory. Interrupted initialization can be retried.
/// # Errors
/// Refuses unsafe roots, unexpected files, changed limits and I/O or lock failure.
pub fn initialize(path: &Path, limits: Limits, control: &Control) -> Result<()> {
    control.check()?;
    limits.check()?;
    let root = Root::open(path, true)?;
    fs::lock(&root.lease, false, control)?;
    root.attest()?;
    let names = fs::names(&root.dir, JANITOR_CAP, control)?;
    if names.iter().any(|n| n == CONFIG) {
        let existing = root.limits(control)?;
        if (existing.quota_bytes, existing.max_entries) != (limits.quota_bytes, limits.max_entries)
        {
            return Err(Error::Request("root configuration is immutable"));
        }
        root.dir.sync_all()?;
        return control.check();
    }
    for name in names {
        if name == CONFIG_STAGE {
            fs::data(&root.dir, &name, OFlags::RDONLY, true)?;
            fs::unlink(&root.dir, &name, false)?;
        } else if name != LOCK && !fs::slots().any(|n| n == name) {
            return Err(Error::Integrity("initialization inventory"));
        }
    }
    for name in fs::slots() {
        fs::control_file(&root.dir, &name, true)?.sync_all()?;
    }
    root.lease.sync_all()?;
    let mut f = fs::new_data(&root.dir, CONFIG_STAGE)?;
    f.write_all(&serde_json::to_vec(&RootConfig { version: 1, limits })?)?;
    f.sync_all()?;
    control.check()?;
    fs::move_dir(&root.dir, CONFIG_STAGE, CONFIG)?;
    root.dir.sync_all()?;
    control.check()
}

#[derive(Debug, Serialize)]
pub struct Inventory {
    pub version: u32,
    pub limits: Limits,
    pub committed_bytes: u64,
    pub config_bytes: u64,
    pub stage_bytes: u64,
    pub trash_bytes: u64,
    entries: Vec<policy::Alternative>,
}

/// List bounded counts, charges and retention facts under a shared pin.
/// # Errors
/// Refuses unsafe ownership, inconsistent inventory and lock/deadline exhaustion.
pub fn inspect(path: &Path, control: &Control) -> Result<Inventory> {
    control.check()?;
    let root = Root::open(path, false)?;
    let _slot = fs::reader_slot(&root.dir, control)?;
    fs::lock(&root.lease, true, control)?;
    root.attest()?;
    let limits = root.limits(control)?;
    let entries = root.inventory(control, wall_ms()?, limits)?;
    let scratch_bytes = |name| -> Result<u64> {
        let dir = match fs::private_dir(&root.dir, name) {
            Ok(dir) => dir,
            Err(Error::Io(Some(n))) if n == rustix::io::Errno::NOENT.raw_os_error() => {
                return Ok(0);
            }
            Err(e) => return Err(e),
        };
        fs::names(&dir, 2, control)?
            .into_iter()
            .try_fold(0u64, |total, file| {
                if !matches!(file.as_str(), MANIFEST | PAYLOAD) {
                    return Err(Error::Integrity("scratch inventory"));
                }
                total
                    .checked_add(
                        fs::data(&dir, &file, OFlags::RDONLY, true)?
                            .metadata()?
                            .len(),
                    )
                    .ok_or(Error::Capacity)
            })
    };
    Ok(Inventory {
        version: 1,
        limits,
        committed_bytes: entries.iter().map(|e| e.bytes).sum(),
        config_bytes: root.config_bytes()?,
        stage_bytes: scratch_bytes(STAGE)?,
        trash_bytes: scratch_bytes(TRASH)?,
        entries,
    })
}

/// Remove abandoned stages and expired entries under the exclusive root pin.
/// # Errors
/// Refuses unsafe roots, corrupt metadata and lock/deadline exhaustion.
pub fn collect_expired(path: &Path, control: &Control) -> Result<Vec<String>> {
    control.check()?;
    let root = Root::open(path, false)?;
    fs::lock(&root.lease, false, control)?;
    root.attest()?;
    let limits = root.limits(control)?;
    root.clean(STAGE, control)?;
    root.clean(TRASH, control)?;
    let mut removed = Vec::new();
    for entry in root.inventory(control, wall_ms()?, limits)? {
        if entry.expired {
            root.evict(&entry.key, control)?;
            removed.push(entry.key);
        }
    }
    control.check()?;
    Ok(removed)
}

/// Remove exactly one operator-selected committed entry without interpreting its manifest.
/// A malformed directory, symlink, unexpected child or active reader is refused.
///
/// # Errors
/// Returns an error for invalid keys, unsafe roots/entries, active readers, or I/O failure.
pub fn purge_entry(path: &Path, key: &str, control: &Control) -> Result<()> {
    control.check()?;
    if !is_digest(key) {
        return Err(Error::Request(
            "purge requires an exact lowercase cache key",
        ));
    }
    let root = Root::open(path, false)?;
    fs::lock(&root.lease, false, control)?;
    root.attest()?;
    root.limits(control)?;
    root.clean(TRASH, control)?;
    let target = fs::private_dir(&root.dir, key).map_err(|_| Error::Access)?;
    if fs::names(&target, 2, control)? != [MANIFEST, PAYLOAD] {
        return Err(Error::Integrity("purge entry inventory"));
    }
    fs::data(&target, MANIFEST, OFlags::RDONLY, true)?;
    fs::data(&target, PAYLOAD, OFlags::RDONLY, true)?;
    root.attest()?;
    control.check()?;
    fs::move_dir(&root.dir, key, TRASH)?;
    root.dir.sync_all()?;
    root.clean(TRASH, control)
}

/// One finite native job. The external catalog pin and calibration are operator inputs.
pub struct Job<'a> {
    pub plan: &'a PlanV1,
    pub source_dir: &'a Path,
    pub catalog_pin: &'a str,
    pub cache_dir: Option<&'a Path>,
    pub cut: u32,
    pub mode: Mode,
    pub calibration: Option<Calibration>,
    pub stage_bytes: u64,
    pub ttl_ms: u64,
}

#[derive(Debug, Serialize)]
struct OperatorAudit {
    name: &'static str,
    input_rows: usize,
    output_rows: usize,
    output_batches: usize,
    work_us: u64,
}

fn operators(metrics: &ExecutionMetrics) -> Vec<OperatorAudit> {
    metrics
        .operators
        .values()
        .map(|m| OperatorAudit {
            name: m.name,
            input_rows: m.input_rows,
            output_rows: m.output_rows,
            output_batches: m.output_batches,
            work_us: u64::try_from(m.work_time.as_micros()).unwrap_or(u64::MAX),
        })
        .collect()
}

#[derive(Debug, Default, Serialize)]
struct Ledger {
    source_catalog_us: u64,
    source_verify_us: u64,
    source_fingerprint_bytes: u64,
    cache_payload_hash_bytes: u64,
    cache_lookup_us: u64,
    native_prefix_and_stream_write_us: u64,
    payload_finalize_and_attest_us: u64,
    eviction_us: u64,
    native_result_us: u64,
    publish_us: u64,
    managed_peak_bytes: usize,
}

#[derive(Debug, Serialize)]
pub struct Audit {
    pub version: u32,
    pub pid: u32,
    pub mode: Mode,
    pub actions: Vec<&'static str>,
    pub original_plan: String,
    pub snapshot: String,
    pub prefix_key: Option<String>,
    pub scope: Option<String>,
    pub rewritten_plan: Option<String>,
    pub payload_digest: Option<String>,
    pub payload_bytes: u64,
    pub payload_rows: u64,
    pub evicted_bytes: u64,
    calibration: Option<Calibration>,
    decision: Option<Decision>,
    prefix: Vec<OperatorAudit>,
    result: Vec<OperatorAudit>,
    ledger: Ledger,
}

pub struct JobOutput {
    pub result: ExecutionOutput,
    pub audit: Audit,
}

/// Execute a bounded local job, releasing output only after source and cache checks.
/// # Errors
/// Refuses invalid plans, cache/snapshot drift, authorization and resource failures.
#[allow(
    clippy::too_many_lines,
    reason = "Source authorization, adaptive admission and native result release share one job boundary."
)]
pub fn run(job: &Job<'_>, control: &Control) -> Result<JobOutput> {
    control.check()?;
    job.plan.validate().map_err(|_| Error::Request("PlanV1"))?;
    if job.plan.sources.len() != 1 || job.plan.sources[0].table != "input" {
        return Err(Error::Request("local input plan required"));
    }
    if job.mode == Mode::ExpectedValueV1
        && let Some(calibration) = job.calibration
    {
        calibration.value(false)?;
    }
    let possible_hit = policy::hit_is_worthwhile(job.calibration);
    let declined = job.mode == Mode::ExpectedValueV1 && !possible_hit;
    if job.mode.uses_root()
        && !declined
        && (!(META_CAP as u64 + 12..=MAX_SOURCE_BYTES + META_CAP as u64).contains(&job.stage_bytes)
            || !(1..=MAX_TTL_MS).contains(&job.ttl_ms))
    {
        return Err(Error::Request("stage reservation or retention horizon"));
    }
    let cut = if job.mode.uses_root() && !declined {
        Some(Cut::new(job.plan, job.cut)?)
    } else {
        None
    };
    let mut audit = Audit {
        version: 1,
        pid: std::process::id(),
        mode: job.mode,
        actions: vec![],
        original_plan: job
            .plan
            .digest()
            .map_err(|_| Error::Request("PlanV1 identity"))?,
        snapshot: job.catalog_pin.into(),
        prefix_key: None,
        scope: None,
        rewritten_plan: None,
        payload_digest: None,
        payload_bytes: 0,
        payload_rows: 0,
        evicted_bytes: 0,
        calibration: job.calibration,
        decision: None,
        prefix: vec![],
        result: vec![],
        ledger: Ledger::default(),
    };
    let started = Instant::now();
    let original = Original::open(job.source_dir, job.catalog_pin, job.plan, control)?;
    audit.ledger.source_catalog_us = elapsed_us(started);
    let source_preverified = job.mode == Mode::ExpectedValueV1 && !declined;
    if source_preverified {
        let started = Instant::now();
        original.verify(job.plan, control, true)?;
        audit.ledger.source_verify_us = elapsed_us(started);
        audit.ledger.source_fingerprint_bytes = source_bytes(job.plan);
    }
    let root = if job.mode.uses_root() && !declined {
        Some(Root::open(
            job.cache_dir.ok_or(Error::Request("cache root required"))?,
            false,
        )?)
    } else {
        None
    };
    let result = if let Some(root) = root {
        let cut = cut.ok_or(Error::Request("cache cut unavailable"))?;
        let key = cut.key(&root.scope)?;
        audit.prefix_key = Some(key.clone());
        audit.scope = Some(root.scope.clone());
        reuse_or_populate(
            job,
            &original,
            &root,
            &cut,
            &key,
            source_preverified,
            &mut audit,
            control,
        )?
    } else {
        if declined {
            audit.actions.push("skip");
            audit.decision = Some(policy::select(
                job.mode,
                job.calibration,
                job.stage_bytes,
                0,
                0,
                vec![],
            )?);
        } else {
            audit.actions.push("bypass");
        }
        execute_original(job, &original, &mut audit, control)?
    };
    control.check()?;
    audit.result = operators(&result.metrics);
    audit.ledger.managed_peak_bytes = audit
        .ledger
        .managed_peak_bytes
        .max(result.metrics.managed.peak);
    Ok(JobOutput { result, audit })
}

fn execute_original(
    job: &Job<'_>,
    original: &Original<'_>,
    audit: &mut Audit,
    control: &Control,
) -> Result<ExecutionOutput> {
    original.attest(control)?;
    let started = Instant::now();
    let result = physical::execute(
        job.plan,
        &original.catalog,
        ExecutionConfig::default(),
        BATCH,
        control.token.clone(),
        control.remaining()?,
    )?;
    audit.ledger.native_result_us = elapsed_us(started);
    audit.ledger.source_fingerprint_bytes += source_bytes(job.plan) * 2;
    original.attest(control)?;
    Ok(result)
}

fn source_bytes(plan: &PlanV1) -> u64 {
    // PlanV1 validates the ordered inventory and caps its checked sum at 64 MiB.
    plan.sources[0].files.iter().map(|f| f.bytes).sum()
}

#[allow(
    clippy::too_many_arguments,
    reason = "Source preflight state is explicit at the cache authorization boundary."
)]
fn reuse_or_populate(
    job: &Job<'_>,
    original: &Original<'_>,
    root: &Root<'_>,
    cut: &Cut,
    key: &str,
    source_preverified: bool,
    audit: &mut Audit,
    control: &Control,
) -> Result<ExecutionOutput> {
    let lookup = Instant::now();
    {
        let _slot = fs::reader_slot(&root.dir, control)?;
        fs::lock(&root.lease, true, control)?;
        root.attest()?;
        root.limits(control)?;
        if let Some((entry, _)) = root.entry(key, control)?
            && entry.reusable_at(wall_ms()?)?
        {
            audit.ledger.cache_lookup_us = elapsed_us(lookup);
            if job.mode == Mode::ExpectedValueV1 && !policy::hit_is_worthwhile(job.calibration) {
                audit.actions.push("skip");
                audit.decision = Some(policy::select(
                    job.mode,
                    job.calibration,
                    job.stage_bytes,
                    0,
                    0,
                    vec![],
                )?);
                root.lease.unlock()?;
                return execute_original(job, original, audit, control);
            }
            return hit(
                job,
                original,
                root,
                cut,
                &entry,
                source_preverified,
                audit,
                control,
            );
        }
        root.lease.unlock()?;
    }
    audit.ledger.cache_lookup_us = elapsed_us(lookup);

    let lookup = Instant::now();
    fs::lock(&root.lease, false, control)?;
    root.attest()?;
    let limits = root.limits(control)?;
    let now = wall_ms()?;
    let entry = root.entry(key, control)?;
    let expired = if let Some((entry, bytes)) = entry {
        if entry.reusable_at(now)? {
            audit.ledger.cache_lookup_us += elapsed_us(lookup);
            if job.mode == Mode::ExpectedValueV1 && !policy::hit_is_worthwhile(job.calibration) {
                root.lease.unlock()?;
                audit.actions.push("skip");
                audit.decision = Some(policy::select(
                    job.mode,
                    job.calibration,
                    job.stage_bytes,
                    0,
                    0,
                    vec![],
                )?);
                return execute_original(job, original, audit, control);
            }
            return hit(
                job,
                original,
                root,
                cut,
                &entry,
                source_preverified,
                audit,
                control,
            );
        }
        Some(bytes)
    } else {
        None
    };
    audit.ledger.cache_lookup_us += elapsed_us(lookup);
    audit.actions.push("miss");
    populate(
        job, original, root, cut, key, limits, expired, audit, control,
    )
}

#[allow(
    clippy::too_many_arguments,
    reason = "The pinned source-preflight state controls one cache-hit verification path."
)]
fn hit(
    job: &Job<'_>,
    original: &Original<'_>,
    root: &Root<'_>,
    cut: &Cut,
    entry: &Entry,
    source_preverified: bool,
    audit: &mut Audit,
    control: &Control,
) -> Result<ExecutionOutput> {
    if !source_preverified {
        let started = Instant::now();
        original.verify(job.plan, control, true)?;
        audit.ledger.source_verify_us += elapsed_us(started);
    }
    // A visible rename may precede its publisher's directory sync if that process died.
    root.dir.sync_all()?;
    control.fault("reader_pinned")?;
    let output = execute_cached(job, root, cut, &entry.key, entry, audit, control)?;
    let started = Instant::now();
    original.verify(job.plan, control, false)?;
    audit.ledger.source_verify_us += elapsed_us(started);
    audit.ledger.source_fingerprint_bytes = source_bytes(job.plan) * 2;
    root.attest()?;
    audit.actions.push("hit");
    Ok(output)
}

#[allow(
    clippy::too_many_arguments,
    reason = "All operation inputs are explicit under the sole writer pin."
)]
fn populate(
    job: &Job<'_>,
    original: &Original<'_>,
    root: &Root<'_>,
    cut: &Cut,
    key: &str,
    limits: Limits,
    expired: Option<u64>,
    audit: &mut Audit,
    control: &Control,
) -> Result<ExecutionOutput> {
    let started = Instant::now();
    root.clean(STAGE, control)?;
    root.clean(TRASH, control)?;
    let now = wall_ms()?;
    let inventory = root.inventory(control, now, limits)?;
    // An expired prior generation at this key must be unlinked before no-clobber publication.
    let alternatives = inventory
        .into_iter()
        .filter(|e| expired.is_none() || e.key != key)
        .collect();
    let available = limits
        .quota_bytes
        .checked_sub(root.config_bytes()?)
        .ok_or(Error::Capacity)?;
    let decision = policy::select(
        job.mode,
        job.calibration,
        job.stage_bytes,
        available,
        limits.max_entries,
        alternatives,
    )?;
    if !decision.admit() {
        audit.actions.push("skip");
        audit.decision = Some(decision);
        return execute_original(job, original, audit, control);
    }

    let verify_time = Instant::now();
    // Before removing retained entries, prove the candidate still refers to an admitted snapshot.
    if expired.is_some() || !decision.victims.is_empty() {
        original.verify(job.plan, control, false)?;
        audit.ledger.source_fingerprint_bytes += source_bytes(job.plan);
    }
    audit.ledger.source_verify_us += elapsed_us(verify_time);
    if let Some(bytes) = expired {
        root.evict(key, control)?;
        audit.evicted_bytes += bytes;
        audit.actions.push("evict_expired_key");
    }
    for victim in &decision.victims {
        root.evict(victim, control)?;
        let bytes = decision
            .alternatives
            .iter()
            .find(|e| &e.key == victim)
            .ok_or(Error::Integrity("missing planned eviction"))?
            .bytes;
        audit.evicted_bytes = audit
            .evicted_bytes
            .checked_add(bytes)
            .ok_or(Error::Capacity)?;
        audit.actions.push("evict");
    }
    audit.decision = Some(decision);
    audit.ledger.eviction_us = elapsed_us(started);
    let dir = fs::create_dir(&root.dir, STAGE)?;
    let result = fill_and_commit(job, original, root, cut, key, &dir, audit, control);
    if result.is_err() {
        // Cleanup also works after cooperative cancellation. A blocked/failed cleanup
        // leaves a single bounded, unreadable stage for the next exclusive opener.
        let cleanup = Control::new(
            ade_exec::CancellationToken::default(),
            std::time::Duration::from_secs(2),
        )?;
        let _ = root.clean(STAGE, &cleanup);
    }
    result
}

#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "Staging, native suffix and publication share one writer pin."
)]
fn fill_and_commit(
    job: &Job<'_>,
    original: &Original<'_>,
    root: &Root<'_>,
    cut: &Cut,
    key: &str,
    dir: &File,
    audit: &mut Audit,
    control: &Control,
) -> Result<ExecutionOutput> {
    let started = Instant::now();
    let mut sink = PayloadSink {
        file: fs::new_data(dir, PAYLOAD)?,
        remaining: MAX_SOURCE_BYTES.min(job.stage_bytes - META_CAP as u64),
        control,
    };
    control.fault("before_data_write")?;
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(4096))
        .set_dictionary_enabled(false)
        .set_created_by(payload_writer(key))
        .build();
    let mut writer = ArrowWriter::try_new(
        &mut sink,
        physical::arrow_schema(&cut.schema),
        Some(properties),
    )
    .map_err(parquet_error)?;
    let mut write_failure = None;
    let metrics = physical::execute_to_staging_sink(
        &cut.prefix,
        &original.catalog,
        ExecutionConfig::default(),
        BATCH,
        &control.token,
        control.deadline,
        |batch| {
            if let Err(error) = writer.write(&batch) {
                write_failure = Some(parquet_error(error));
                return Err(ade_exec::Error::Invalid("private staging writer".into()));
            }
            Ok(())
        },
    );
    let metrics = if let Some(error) = write_failure {
        return Err(error);
    } else {
        metrics?
    };
    audit.ledger.native_prefix_and_stream_write_us = elapsed_us(started);
    audit.ledger.managed_peak_bytes = metrics.managed.peak;
    audit.prefix = operators(&metrics);
    let started = Instant::now();
    writer.close().map_err(parquet_error)?;
    control.fault("data_written")?;
    sink.file.sync_all()?;
    control.fault("data_synced")?;
    let file = fs::data(dir, PAYLOAD, OFlags::RDONLY, true)?;
    let bytes = file.metadata()?.len();
    let digest = fs::digest(&file, bytes, control)?;
    audit.ledger.cache_payload_hash_bytes = bytes;
    let now = wall_ms()?;
    let entry = Entry {
        version: 2,
        semantics: SEMANTICS.into(),
        scope: root.scope.clone(),
        key: key.into(),
        snapshot: job.catalog_pin.into(),
        schema: schema_identity(&cut.schema)?,
        digest,
        bytes,
        rows: metrics.output_rows as u64,
        created_ms: now,
        expires_ms: now
            .checked_add(job.ttl_ms)
            .ok_or(Error::Request("expiry range"))?,
        prefix_us: audit.ledger.native_prefix_and_stream_write_us,
        calibration: job.calibration,
    };
    entry.validate(root, key)?;
    let manifest = serde_json::to_vec(&entry)?;
    if manifest.len() > META_CAP {
        return Err(Error::Capacity);
    }
    let mut file = fs::new_data(dir, MANIFEST)?;
    file.write_all(&manifest)?;
    file.sync_all()?;
    dir.sync_all()?;
    control.fault("manifest_synced")?;
    if fs::read_bounded(
        fs::data(dir, MANIFEST, OFlags::RDONLY, true)?,
        META_CAP,
        control,
    )? != manifest
    {
        return Err(Error::Integrity("staged manifest drift"));
    }
    audit.ledger.payload_finalize_and_attest_us = elapsed_us(started);
    let result = execute_cached(job, root, cut, STAGE, &entry, audit, control)?;
    let started = Instant::now();
    original.verify(job.plan, control, false)?;
    audit.ledger.source_verify_us += elapsed_us(started);
    audit.ledger.source_fingerprint_bytes += source_bytes(job.plan) * 3;

    let started = Instant::now();
    root.attest()?;
    control.check()?;
    fs::move_dir(&root.dir, STAGE, key)?;
    control.fault("entry_selected")?;
    root.dir.sync_all()?;
    control.fault("entry_synced")?;
    original.attest(control)?;
    audit.ledger.publish_us = elapsed_us(started);
    audit.actions.push("publish");
    Ok(result)
}

fn execute_cached(
    job: &Job<'_>,
    root: &Root<'_>,
    cut: &Cut,
    name: &str,
    entry: &Entry,
    audit: &mut Audit,
    control: &Control,
) -> Result<ExecutionOutput> {
    let started = Instant::now();
    if entry.snapshot != job.catalog_pin || entry.schema != schema_identity(&cut.schema)? {
        return Err(Error::Integrity("source or schema attestation"));
    }
    let dir = fs::private_dir(&root.dir, name)?;
    let mut file = fs::data(&dir, PAYLOAD, OFlags::RDONLY, true)?;
    let stamp = fs::Stamp::from(file.metadata()?);
    file.seek(SeekFrom::End(-8))?;
    let mut footer = [0; 8];
    file.read_exact(&mut footer)?;
    if &footer[4..] != b"PAR1"
        || u32::from_le_bytes(footer[..4].try_into().expect("four bytes")) > 1024 * 1024
    {
        return Err(Error::Integrity("payload footer"));
    }
    file.rewind()?;
    let header = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|_| Error::Integrity("payload Parquet"))?;
    if header.metadata().file_metadata().created_by() != Some(payload_writer(&entry.key).as_str()) {
        return Err(Error::Integrity("payload lineage"));
    }
    drop(header);
    let (plan, catalog) = cut.bind(
        job.plan,
        &root.path.join(name),
        &entry.key,
        entry.bytes,
        entry.rows,
        &entry.digest,
    )?;
    audit.rewritten_plan = Some(
        plan.digest()
            .map_err(|_| Error::Integrity("rewritten plan"))?,
    );
    audit.payload_bytes = entry.bytes;
    audit.payload_rows = entry.rows;
    audit.payload_digest = Some(entry.digest.clone());
    audit.ledger.cache_lookup_us += elapsed_us(started);
    control.fault("before_suffix")?;
    let started = Instant::now();
    let result = physical::execute(
        &plan,
        &catalog,
        ExecutionConfig::default(),
        BATCH,
        control.token.clone(),
        control.remaining()?,
    )?;
    audit.ledger.native_result_us = elapsed_us(started);
    audit.ledger.cache_payload_hash_bytes += entry.bytes * 2;
    root.attest()?;
    let current = fs::private_dir(&root.dir, name)?;
    if fs::Stamp::from(fs::data(&current, PAYLOAD, OFlags::RDONLY, true)?.metadata()?) != stamp {
        return Err(Error::Integrity("payload replaced during suffix"));
    }
    Ok(result)
}

fn payload_writer(key: &str) -> String {
    format!("ADE shared-prefix v1 {key}")
}

struct PayloadSink<'a> {
    file: File,
    remaining: u64,
    control: &'a Control,
}

impl Write for PayloadSink<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.control.check().map_err(io::Error::other)?;
        if bytes.len() as u64 > self.remaining {
            return Err(io::Error::other(Error::Capacity));
        }
        let n = self.file.write(bytes)?;
        self.remaining -= n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.control.check().map_err(io::Error::other)?;
        self.file.flush()
    }
}

fn parquet_error(error: ParquetError) -> Error {
    if let ParquetError::External(e) = error
        && let Ok(io_error) = e.downcast::<io::Error>()
    {
        if let Some(errno) = io_error.raw_os_error() {
            return Error::Io(Some(errno));
        }
        if let Some(inner) = io_error.into_inner()
            && let Ok(error) = inner.downcast::<Error>()
        {
            return *error;
        }
    }
    Error::Native("materialization Parquet writer")
}
