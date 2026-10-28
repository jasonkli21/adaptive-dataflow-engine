use crate::{
    Error, Recipe, Result, deadline, fault, now_ms,
    recipe::{FrozenFile, nonsymlink},
    require,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    thread,
    time::{Duration, Instant},
};
use uuid::Uuid;

pub const MAX_STORE_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_RECIPE_BYTES: usize = 512 * 1024;
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum JobState {
    Pending,
    Claimed,
    Running,
    Staged,
    Committing,
    Succeeded,
    RetryWait,
    Cancelling,
    Cancelled,
    Failed,
    NeedsOperator,
}
impl JobState {
    #[must_use]
    pub fn terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Cancelled | Self::Failed)
    }
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    pub jobs: usize,
    pub queued: usize,
    pub events_per_job: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            jobs: 32,
            queued: 8,
            events_per_job: 32,
        }
    }
}
impl Limits {
    fn validate(self) -> Result<()> {
        require(
            (1..=32).contains(&self.jobs)
                && (1..=8).contains(&self.queued)
                && self.queued <= self.jobs
                && (4..=32).contains(&self.events_per_job),
            "invalid store limits",
        )
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EventCode {
    Registered,
    CancelRequested,
    Cancelled,
    Expired,
    ExecutionRefused,
    Claimed,
    AmbiguousAttempt,
    Running,
    Staged,
    Committing,
    Succeeded,
    RetryScheduled,
    AttemptFailed,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Event {
    pub sequence: u64,
    pub at_ms: u64,
    pub owner: u64,
    pub state: JobState,
    pub code: EventCode,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Attempt {
    pub ordinal: u32,
    pub owner: u64,
    pub engine_job: u64,
    pub instance: Uuid,
    pub state: JobState,
    pub started_ms: u64,
    pub ended_ms: Option<u64>,
    pub error: Option<String>,
    pub worker_disk_bytes: Vec<u64>,
    pub coordinator_disk_bytes: Option<u64>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Job {
    pub id: Uuid,
    pub request_key: String,
    pub recipe_hash: String,
    pub recipe: Recipe,
    pub created_ms: u64,
    pub expires_ms: u64,
    pub state: JobState,
    pub owner: u64,
    pub attempt: u32,
    /// Whole-job execution namespace; engine mapping is reserved before worker effects.
    pub instance: Option<Uuid>,
    pub cancel_requested: bool,
    pub sequence: u64,
    pub events: Vec<Event>,
    pub(crate) files: Vec<FrozenFile>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub engine_job: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attempts: Vec<Attempt>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_at_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<crate::publication::Manifest>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_config: Option<String>,
    /// Versioned operations/frontend compatibility identity. Missing in older v2 records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub coordinator_compat: Option<String>,
}
impl Job {
    pub(crate) fn event(
        &mut self,
        code: EventCode,
        owner: u64,
        at_ms: u64,
        cap: usize,
    ) -> Result<()> {
        self.sequence = self
            .sequence
            .checked_add(1)
            .ok_or(Error::Quota("event sequence overflow"))?;
        if self.events.len() == cap {
            self.events.remove(0);
        }
        self.events.push(Event {
            sequence: self.sequence,
            at_ms,
            owner,
            state: self.state,
            code,
        });
        Ok(())
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Database {
    pub(crate) id: Uuid,
    pub(crate) generation: u64,
    pub(crate) last_ms: u64,
    pub(crate) limits: Limits,
    pub(crate) jobs: BTreeMap<Uuid, Job>,
    #[serde(default, skip_serializing_if = "zero")]
    pub(crate) next_engine_id: u64,
}
#[allow(clippy::trivially_copy_pass_by_ref)] // serde skip_serializing_if signature.
fn zero(n: &u64) -> bool {
    *n == 0
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    version: u32,
    checksum: String,
    data: Database,
}
impl Database {
    #[allow(clippy::too_many_lines)] // Validate the complete bounded snapshot before accepting any state.
    fn validate(&self) -> Result<()> {
        self.limits.validate()?;
        require(
            !self.id.is_nil() && self.jobs.len() <= self.limits.jobs,
            "store ID/history cap",
        )?;
        let mut keys = std::collections::BTreeSet::new();
        let mut engine_ids = std::collections::BTreeSet::new();
        let mut namespaces = std::collections::BTreeSet::new();
        let mut active = 0;
        let mut queued = 0;
        for (id, job) in &self.jobs {
            require(
                id == &job.id && !id.is_nil() && keys.insert(&job.request_key),
                "job/idempotency uniqueness",
            )?;
            valid_key(&job.request_key)?;
            job.recipe.validate()?;
            require(
                job.recipe_hash == job.recipe.digest()?
                    && serde_json::to_vec(&job.recipe)?.len() <= MAX_RECIPE_BYTES,
                "recipe hash/size",
            )?;
            require(
                job.expires_ms
                    == job
                        .created_ms
                        .checked_add(job.recipe.timeout_ms)
                        .ok_or(Error::Deadline)?
                    && job.owner <= self.generation
                    && job.created_ms <= self.last_ms,
                "job deadline/owner",
            )?;
            require(
                job.attempt <= job.recipe.max_attempts
                    && (job.attempt == 0) == job.instance.is_none()
                    && job.instance.is_none_or(|id| !id.is_nil()),
                "attempt/namespace",
            )?;
            if job.state == JobState::Succeeded {
                require(
                    job.output.is_some()
                        && job
                            .attempts
                            .last()
                            .is_some_and(|a| a.state == JobState::Succeeded),
                    "success requires durable native completion",
                )?;
            }
            require(
                job.attempts.len() <= 3
                    && job
                        .engine_job
                        .is_none_or(|id| id > 0 && id <= self.next_engine_id),
                "engine identity/attempt cap",
            )?;
            for (i, a) in job.attempts.iter().enumerate() {
                require(
                    engine_ids.insert(a.engine_job) && namespaces.insert(a.instance),
                    "engine execution identity reused",
                )?;
                require(
                    usize::try_from(a.ordinal).is_ok_and(|ordinal| ordinal == i + 1)
                        && a.owner <= self.generation
                        && a.engine_job > 0
                        && a.engine_job <= self.next_engine_id
                        && !a.instance.is_nil()
                        && a.error.as_ref().is_none_or(|e| e.len() <= 1024)
                        && a.worker_disk_bytes.len() <= 4
                        && a.worker_disk_bytes
                            .iter()
                            .all(|b| *b <= job.recipe.runtime_bytes)
                        && a.coordinator_disk_bytes
                            .is_none_or(|bytes| bytes <= job.recipe.runtime_bytes),
                    "attempt bounds/identity",
                )?;
            }
            if let Some(engine) = job.engine_job {
                require(
                    job.attempts.last().is_some_and(|a| {
                        a.engine_job == engine
                            && a.ordinal == job.attempt
                            && a.owner == job.owner
                            && Some(a.instance) == job.instance
                    }),
                    "current attempt mapping",
                )?;
            }
            require(
                job.native_config.as_ref().is_none_or(|hash| {
                    hash.len() == 64
                        && hash
                            .bytes()
                            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
                }),
                "native config identity",
            )?;
            require(
                job.coordinator_compat.as_ref().is_none_or(|hash| {
                    hash.len() == 64
                        && hash
                            .bytes()
                            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
                }),
                "coordinator compatibility identity",
            )?;
            if let Some(output) = &job.output {
                crate::publication::validate_record(output)?;
                require(
                    output.schema == job.recipe.plan.nodes[job.recipe.plan.output as usize].schema
                        && output.rows <= job.recipe.plan.max_output_rows
                        && output.bytes <= job.recipe.plan.max_output_bytes
                        && Some(&output.identity.config_hash) == job.native_config.as_ref(),
                    "output schema/configuration/bounds",
                )?;
                require(
                    output.identity.store == self.id
                        && output.identity.job == job.id
                        && Some(output.identity.instance) == job.instance
                        && Some(output.identity.engine_job) == job.engine_job
                        && output.identity.owner == job.owner
                        && output.identity.plan_hash
                            == job
                                .recipe
                                .plan
                                .digest()
                                .map_err(|e| Error::Invalid(e.to_string()))?,
                    "output/job identity",
                )?;
            }
            if job.state == JobState::Pending {
                queued += 1;
            } else if !job.state.terminal() && job.state != JobState::NeedsOperator {
                active += 1;
            }
            require(
                job.events.len() <= self.limits.events_per_job && !job.events.is_empty(),
                "history event cap",
            )?;
            require(
                job.events
                    .last()
                    .is_some_and(|e| e.sequence == job.sequence && e.state == job.state),
                "event head",
            )?;
            let mut prior = 0;
            for event in &job.events {
                require(
                    event.sequence > prior
                        && event.at_ms <= self.last_ms
                        && event.owner <= self.generation,
                    "event ordering/owner",
                )?;
                prior = event.sequence;
            }
            // Stored filesystem inventory must cover the exact IR names and hashes.
            let mut expected = BTreeMap::new();
            for (input, source) in job.recipe.inputs.iter().zip(&job.recipe.plan.sources) {
                expected.insert(input.manifest.clone(), source.manifest.clone());
                for f in &source.files {
                    expected.insert(input.root.join(&f.name), f.fingerprint.clone());
                }
            }
            require(
                job.files.len() == expected.len()
                    && job
                        .files
                        .iter()
                        .all(|f| expected.remove(&f.path).is_some_and(|hash| hash == f.hash))
                    && expected.is_empty(),
                "frozen inventory coverage",
            )?;
        }
        require(
            active <= 1 && queued <= self.limits.queued,
            "active/queue cap",
        )
    }
}
/// One OS-enforced owner. All APIs keep the lock alive; lock files must never be
/// unlinked/replaced. No network or worker operations occur while holding it.
pub struct Store {
    pub(crate) root: PathBuf,
    lock: File,
    pub(crate) data: Database,
    poisoned: bool,
}
impl Store {
    /// Create an explicitly new private local state root; never resets existing state.
    /// # Errors
    /// Refuses existing roots, invalid limits, filesystem and durability errors.
    pub fn init(root: &Path, limits: Limits) -> Result<Self> {
        limits.validate()?;
        let parent = root
            .parent()
            .ok_or_else(|| Error::Invalid("state root parent".into()))?
            .canonicalize()?;
        let name = root
            .file_name()
            .ok_or_else(|| Error::Invalid("state root name".into()))?;
        let root = parent.join(name);
        fs::DirBuilder::new().mode(0o700).create(&root)?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(root.join("owner.lock"))?;
        lock.try_lock().map_err(|_| Error::Owned)?;
        lock.sync_all()?;
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(root.join("runner.lock"))?
            .sync_all()?;
        File::open(&parent)?.sync_all()?;
        let data = Database {
            id: Uuid::new_v4(),
            generation: 1,
            last_ms: now_ms()?,
            limits,
            jobs: BTreeMap::new(),
            next_engine_id: 0,
        };
        let mut store = Self {
            root,
            lock,
            data,
            poisoned: false,
        };
        store.persist(store.data.clone())?;
        Ok(store)
    }
    /// Acquire the short metadata lock and validate state; readers do not change owner epochs.
    /// Does not guess what happened to interrupted jobs; call `recover` explicitly.
    /// # Errors
    /// Refuses busy, missing, symlinked, nonprivate, corrupt or incompatible stores.
    pub fn open(root: &Path) -> Result<Self> {
        require(
            !fs::symlink_metadata(root)?.file_type().is_symlink(),
            "symlink state root refused",
        )?;
        let root = root.canonicalize()?;
        nonsymlink(&root)?;
        let meta = fs::metadata(&root)?;
        require(
            meta.is_dir() && meta.permissions().mode() & 0o777 == 0o700,
            "private state directory required",
        )?;
        let lock = open_private(&root.join("owner.lock"))?;
        lock.try_lock().map_err(|_| Error::Owned)?;
        let bytes = read_private(&root.join("state.json"), MAX_STORE_BYTES)?;
        let envelope: Envelope = serde_json::from_slice(&bytes)?;
        require(
            matches!(envelope.version, 1 | 2),
            "incompatible state version",
        )?;
        let old_version = envelope.version == 1;
        require(
            envelope.checksum == ade_physical::fingerprint(&serde_json::to_vec(&envelope.data)?),
            "state checksum",
        )?;
        envelope.data.validate()?;
        if now_ms()? < envelope.data.last_ms {
            return Err(Error::Deadline);
        }
        let mut store = Self {
            root,
            lock,
            data: envelope.data,
            poisoned: false,
        };
        // A leftover temp is never promoted: only state.json is committed state.
        store.cleanup()?;
        if old_version {
            match OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(store.root.join("runner.lock"))
            {
                Ok(file) => file.sync_all()?,
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(e) => return Err(e.into()),
            }
            store.persist(store.data.clone())?;
        }
        let _ = open_private(&store.root.join("runner.lock"))?;
        if store.data.generation == u64::MAX {
            return Err(Error::Quota("owner generation overflow"));
        }
        Ok(store)
    }
    #[must_use]
    pub fn id(&self) -> Uuid {
        self.data.id
    }
    #[must_use]
    pub fn generation(&self) -> u64 {
        self.data.generation
    }
    fn healthy(&self) -> Result<()> {
        require(!self.poisoned, "ambiguous write: close and reopen store")
    }
    /// Release the metadata lock while doing potentially long source I/O, then reopen
    /// and reload the committed snapshot before allowing this handle to be reused.
    /// Any reopen failure poisons this in-memory handle so stale state cannot be written.
    fn with_released_lock<T>(&mut self, operation: impl FnOnce() -> Result<T>) -> Result<T> {
        self.healthy()?;
        self.lock.unlock()?;
        let result = operation();
        let root = self.root.clone();
        let until = Instant::now() + Duration::from_secs(5);
        loop {
            match Self::open(&root) {
                Ok(reopened) => {
                    *self = reopened;
                    return result;
                }
                Err(Error::Owned) if Instant::now() < until => {
                    thread::sleep(Duration::from_millis(2));
                }
                Err(error) => {
                    self.poisoned = true;
                    return Err(error);
                }
            }
        }
    }
    pub(crate) fn persist(&mut self, mut data: Database) -> Result<()> {
        self.healthy()?;
        let now = now_ms()?;
        if now < self.data.last_ms {
            return Err(Error::Deadline);
        }
        data.last_ms = now;
        data.validate()?;
        if data.jobs.iter().any(|(id, j)| {
            j.state == JobState::Succeeded
                && self
                    .data
                    .jobs
                    .get(id)
                    .is_none_or(|old| old.state != JobState::Succeeded)
        }) {
            crate::io_point("success_write")?;
        }
        let envelope = Envelope {
            version: 2,
            checksum: ade_physical::fingerprint(&serde_json::to_vec(&data)?),
            data,
        };
        let bytes = serde_json::to_vec(&envelope)?;
        if bytes.len() > MAX_STORE_BYTES {
            return Err(Error::Quota("snapshot 8MiB"));
        }
        // Once I/O starts, any failure is ambiguous. Refuse more transactions on
        // this handle, including cancellation, until on-disk state is revalidated.
        self.poisoned = true;
        let next = self.root.join("state.next");
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&next)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        fault("snapshot_synced");
        fs::rename(&next, self.root.join("state.json"))?;
        fault("snapshot_renamed");
        File::open(&self.root)?.sync_all()?;
        self.data = envelope.data;
        self.poisoned = false;
        Ok(())
    }
    /// Register a validated immutable recipe, with a stable idempotency key.
    /// Repeating the exact request returns its original job without extending expiry.
    /// # Errors
    /// Refuses source drift, incompatible reuse, bounds, history and queue exhaustion.
    pub fn submit(&mut self, key: &str, recipe: Recipe) -> Result<Job> {
        self.healthy()?;
        valid_key(key)?;
        recipe.validate()?;
        if serde_json::to_vec(&recipe)?.len() > MAX_RECIPE_BYTES {
            return Err(Error::Quota("recipe 512KiB"));
        }
        let hash = recipe.digest()?;
        if let Some(old) = self.data.jobs.values().find(|j| j.request_key == key) {
            require(
                old.recipe_hash == hash && old.recipe == recipe,
                "idempotency key recipe conflict",
            )?;
            return Ok(old.clone());
        }
        if self.data.jobs.len() == self.data.limits.jobs {
            return Err(Error::Quota("retained jobs"));
        }
        if self
            .data
            .jobs
            .values()
            .filter(|j| j.state == JobState::Pending)
            .count()
            == self.data.limits.queued
        {
            return Err(Error::Quota("queued jobs"));
        }
        let created_ms = now_ms()?;
        let expires_ms = created_ms
            .checked_add(recipe.timeout_ms)
            .ok_or(Error::Deadline)?;
        let files = self.with_released_lock(|| recipe.freeze(deadline(expires_ms)?))?;
        // Another submit may have won the same idempotency key while files were
        // being hashed. Re-read under the lock and honor its original record.
        if let Some(old) = self.data.jobs.values().find(|j| j.request_key == key) {
            require(
                old.recipe_hash == hash && old.recipe == recipe,
                "idempotency key recipe conflict",
            )?;
            return Ok(old.clone());
        }
        if now_ms()? >= expires_ms {
            return Err(Error::Deadline);
        }
        if self.data.jobs.len() >= self.data.limits.jobs {
            return Err(Error::Quota("retained jobs"));
        }
        if self
            .data
            .jobs
            .values()
            .filter(|j| j.state == JobState::Pending)
            .count()
            >= self.data.limits.queued
        {
            return Err(Error::Quota("queued jobs"));
        }
        let native_config = Some(crate::runner::config_hash(&recipe)?);
        let coordinator_compat = Some(crate::runner::coordinator_compat_hash());
        let mut job = Job {
            id: Uuid::new_v4(),
            request_key: key.into(),
            recipe_hash: hash,
            recipe,
            created_ms,
            expires_ms,
            state: JobState::Pending,
            owner: self.data.generation,
            attempt: 0,
            instance: None,
            cancel_requested: false,
            sequence: 0,
            events: vec![],
            files,
            engine_job: None,
            attempts: vec![],
            retry_at_ms: None,
            output: None,
            native_config,
            coordinator_compat,
        };
        job.event(
            EventCode::Registered,
            self.data.generation,
            now_ms()?,
            self.data.limits.events_per_job,
        )?;
        if now_ms()? >= expires_ms {
            return Err(Error::Deadline);
        }
        let mut data = self.data.clone();
        require(!data.jobs.contains_key(&job.id), "UUID collision")?;
        data.jobs.insert(job.id, job.clone());
        self.persist(data)?;
        Ok(job)
    }
    /// # Errors
    /// Refuses an unknown job or an ambiguous open handle.
    pub fn status(&self, id: Uuid) -> Result<Job> {
        self.healthy()?;
        self.data
            .jobs
            .get(&id)
            .cloned()
            .ok_or_else(|| Error::Invalid("unknown job".into()))
    }
    /// Return at most 32 jobs in UUID order, with exclusive pagination cursor.
    /// # Errors
    /// Refuses zero/oversized pages or an ambiguous open handle.
    pub fn list(&self, after: Option<Uuid>, limit: usize) -> Result<Vec<Job>> {
        self.healthy()?;
        require((1..=32).contains(&limit), "page limit 1..32")?;
        Ok(self
            .data
            .jobs
            .values()
            .filter(|j| after.is_none_or(|id| j.id > id))
            .take(limit)
            .cloned()
            .collect())
    }
    fn cas(data: &mut Database, expected: &Job, state: JobState, code: EventCode) -> Result<()> {
        let owner = data.generation;
        let current = data
            .jobs
            .get_mut(&expected.id)
            .ok_or_else(|| Error::Invalid("unknown job".into()))?;
        require(
            current.owner == expected.owner
                && current.attempt == expected.attempt
                && current.state == expected.state
                && current.sequence == expected.sequence
                && !current.state.terminal(),
            "stale/terminal transition",
        )?;
        let allowed = match state {
            JobState::Cancelled | JobState::Failed | JobState::Claimed => {
                expected.state == JobState::Pending
            }
            JobState::NeedsOperator => expected.state != JobState::Pending,
            JobState::Cancelling => {
                expected.state != JobState::Pending && expected.state != JobState::NeedsOperator
            }
            _ => false,
        };
        require(allowed, "invalid lifecycle transition")?;
        current.state = state;
        current.event(code, owner, now_ms()?, data.limits.events_per_job)
    }
    /// Persist cancellation intent. Only queued work can be confirmed cancelled.
    /// An ambiguous attempt remains suspended until the operator proves quiescence.
    /// # Errors
    /// Refuses unknown jobs and store failures; repeated cancellation is idempotent.
    pub fn cancel(&mut self, id: Uuid) -> Result<Job> {
        let job = self.status(id)?;
        if job.state.terminal() || job.cancel_requested {
            return Ok(job);
        }
        let mut data = self.data.clone();
        if job.state == JobState::Pending {
            Self::cas(&mut data, &job, JobState::Cancelled, EventCode::Cancelled)?;
        } else if job.state != JobState::NeedsOperator {
            Self::cas(
                &mut data,
                &job,
                JobState::Cancelling,
                EventCode::CancelRequested,
            )?;
        } else {
            data.jobs
                .get_mut(&id)
                .ok_or_else(|| Error::Invalid("unknown job".into()))?
                .event(
                    EventCode::CancelRequested,
                    data.generation,
                    now_ms()?,
                    data.limits.events_per_job,
                )?;
        }
        data.jobs
            .get_mut(&id)
            .ok_or_else(|| Error::Invalid("unknown job".into()))?
            .cancel_requested = true;
        self.persist(data)?;
        self.status(id)
    }
    /// Reconcile metadata conservatively. No worker is contacted or replayed.
    /// # Errors
    /// Refuses clock rollback, invalid state or durability errors.
    pub fn recover(&mut self) -> Result<Vec<Job>> {
        self.healthy()?;
        require(
            self.data
                .jobs
                .values()
                .all(|j| j.engine_job.is_none() || j.state.terminal()),
            "native recovery requires runner::recover with coordinator ownership",
        )?;
        let now = now_ms()?;
        if now < self.data.last_ms {
            return Err(Error::Deadline);
        }
        let mut data = self.data.clone();
        let mut ids = vec![];
        for job in self.data.jobs.values() {
            if job.state.terminal() || job.state == JobState::NeedsOperator {
                continue;
            }
            if job.state == JobState::Pending {
                if now >= job.expires_ms {
                    Self::cas(&mut data, job, JobState::Failed, EventCode::Expired)?;
                    ids.push(job.id);
                }
            } else {
                Self::cas(
                    &mut data,
                    job,
                    JobState::NeedsOperator,
                    EventCode::AmbiguousAttempt,
                )?;
                ids.push(job.id);
            }
        }
        if !ids.is_empty() {
            self.persist(data)?;
        }
        ids.into_iter().map(|id| self.status(id)).collect()
    }
    /// Recheck pinned manifest/source file contents and filesystem identities.
    /// # Errors
    /// Refuses expired jobs, source mutation, replacement or path escape.
    pub fn verify_sources(&mut self, id: Uuid) -> Result<()> {
        let job = self.status(id)?;
        let until = deadline(job.expires_ms)?;
        self.with_released_lock(|| {
            for f in job.files {
                f.verify_stage(until, "verify_sources")?;
            }
            Ok(())
        })
    }
    /// Metadata-handle compatibility API: refuses execution while this lock is held.
    /// Use `crate::runner::run` for durable native execution.
    /// # Errors
    /// Always unsupported here (or an earlier state/deadline error).
    pub fn run(&mut self, id: Uuid) -> Result<()> {
        let job = self.status(id)?;
        require(
            job.state == JobState::Pending && !job.cancel_requested,
            "job not pending",
        )?;
        let _ = deadline(job.expires_ms)?;
        let mut data = self.data.clone();
        data.jobs
            .get_mut(&id)
            .ok_or_else(|| Error::Invalid("unknown job".into()))?
            .event(
                EventCode::ExecutionRefused,
                data.generation,
                now_ms()?,
                data.limits.events_per_job,
            )?;
        self.persist(data)?;
        Err(Error::Unsupported(
            "metadata handle retains its lock; use runner::run or ade-batch run for native execution; no worker contacted",
        ))
    }
    /// Remove only an abandoned transaction temp after validating committed state.
    /// Never removes history, output, worker scratch, unknown files or lock files.
    /// # Errors
    /// Refuses unsafe temporary paths, ambiguous handles or filesystem errors.
    pub fn cleanup(&mut self) -> Result<bool> {
        self.healthy()?;
        let path = self.root.join("state.next");
        match fs::symlink_metadata(&path) {
            Ok(_) => {
                let _ = open_private(&path)?;
                fs::remove_file(path)?;
                File::open(&self.root)?.sync_all()?;
                Ok(true)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e.into()),
        }
    }
    #[cfg(feature = "fault-injection")]
    /// # Errors
    /// Overflow and persistence failures refuse the test transaction.
    pub fn exercise_owner(&mut self) -> Result<()> {
        let mut data = self.data.clone();
        data.generation = data
            .generation
            .checked_add(1)
            .ok_or(Error::Quota("owner generation overflow"))?;
        self.persist(data)
    }
    /// Metadata-only crash fixture. Never allocates an engine ID or contacts workers.
    /// # Errors
    /// Refuses stale, expired or nonpending jobs and more than one active claim.
    #[cfg(feature = "fault-injection")]
    pub fn exercise_claim(&mut self, id: Uuid) -> Result<()> {
        let job = self.status(id)?;
        let _ = deadline(job.expires_ms)?;
        let mut data = self.data.clone();
        Self::cas(&mut data, &job, JobState::Claimed, EventCode::Claimed)?;
        let current = data
            .jobs
            .get_mut(&id)
            .ok_or_else(|| Error::Invalid("unknown job".into()))?;
        current.attempt = 1;
        current.instance = Some(Uuid::new_v4());
        self.persist(data)?;
        fault("claimed");
        Ok(())
    }
}
fn valid_key(key: &str) -> Result<()> {
    require(
        !key.is_empty()
            && key.len() <= 128
            && key
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b)),
        "idempotency key 1..128 ASCII letters/digits/-_.",
    )
}
fn open_private(path: &Path) -> Result<File> {
    nonsymlink(path)?;
    let meta = fs::metadata(path)?;
    require(
        meta.is_file() && meta.nlink() == 1 && meta.permissions().mode() & 0o777 == 0o600,
        "private regular unlinked file required",
    )?;
    Ok(OpenOptions::new().read(true).write(true).open(path)?)
}
fn read_private(path: &Path, cap: usize) -> Result<Vec<u8>> {
    let file = open_private(path)?;
    if file.metadata()?.len() > cap as u64 {
        return Err(Error::Quota("metadata bytes"));
    }
    let mut bytes = vec![];
    file.take(cap as u64 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > cap {
        return Err(Error::Quota("metadata bytes"));
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ade_physical::{Field, Operator, PlanV1, Source, SourceFile, Target, Type};
    #[allow(clippy::too_many_lines)] // Canonical immutable snapshot fixture.
    fn database() -> (Database, Job) {
        let source = Source {
            id: 0,
            table: "input".into(),
            manifest: "a".repeat(64),
            logical: "b".repeat(64),
            schema_identity: "c".repeat(64),
            schema: vec![
                Field::new("key", Type::U64, false),
                Field::new("value_i64", Type::I64, false),
            ],
            files: vec![SourceFile {
                name: "a.parquet".into(),
                bytes: 16,
                rows: 0,
                fingerprint: "d".repeat(64),
            }],
            replayable: true,
        };
        let mut plan = PlanV1::new(Target::DistributedAggregate, vec![source]);
        plan.push(Operator::Scan { source: 0 }).unwrap();
        plan.push(Operator::GroupSumCount {
            input: 0,
            key: 0,
            value: 1,
        })
        .unwrap();
        let recipe = Recipe {
            version: 1,
            plan,
            inputs: vec![crate::Input {
                table: "input".into(),
                root: "/input".into(),
                manifest: "/input/manifest.json".into(),
            }],
            workers: (0..2)
                .map(|id| crate::Worker {
                    id,
                    control: format!("127.0.0.1:{}", 10000 + id * 2).parse().unwrap(),
                    exchange: format!("127.0.0.1:{}", 10001 + id * 2).parse().unwrap(),
                    instance: Uuid::new_v4(),
                    scratch_root: format!("/worker/{id}").into(),
                })
                .collect(),
            timeout_ms: 120_000,
            max_attempts: 1,
            runtime_bytes: ade_distributed::managed::DISK_BYTES,
            staging_bytes: 4096,
        };
        let now = now_ms().unwrap();
        let id = Uuid::new_v4();
        let mut job = Job {
            id,
            request_key: "unit".into(),
            recipe_hash: recipe.digest().unwrap(),
            recipe,
            created_ms: now,
            expires_ms: now + 120_000,
            state: JobState::Pending,
            owner: 1,
            attempt: 0,
            instance: None,
            cancel_requested: false,
            sequence: 0,
            events: vec![],
            engine_job: None,
            attempts: vec![],
            retry_at_ms: None,
            output: None,
            native_config: None,
            coordinator_compat: None,
            files: vec![
                FrozenFile {
                    path: "/input/manifest.json".into(),
                    device: 0,
                    inode: 0,
                    modified_sec: 0,
                    modified_nsec: 0,
                    bytes: 16,
                    hash: "a".repeat(64),
                },
                FrozenFile {
                    path: "/input/a.parquet".into(),
                    device: 0,
                    inode: 0,
                    modified_sec: 0,
                    modified_nsec: 0,
                    bytes: 16,
                    hash: "d".repeat(64),
                },
            ],
        };
        job.event(EventCode::Registered, 1, now, 32).unwrap();
        (
            Database {
                id: Uuid::new_v4(),
                generation: 1,
                last_ms: now,
                limits: Limits::default(),
                jobs: [(id, job.clone())].into(),
                next_engine_id: 0,
            },
            job,
        )
    }
    #[test]
    fn every_state_pair_is_checked_and_terminal_or_stale_cas_never_commits() {
        let states = [
            JobState::Pending,
            JobState::Claimed,
            JobState::Running,
            JobState::Staged,
            JobState::Committing,
            JobState::Succeeded,
            JobState::RetryWait,
            JobState::Cancelling,
            JobState::Cancelled,
            JobState::Failed,
            JobState::NeedsOperator,
        ];
        for from in states {
            for to in states {
                let (mut data, mut expected) = database();
                expected.state = from;
                data.jobs.insert(expected.id, expected.clone());
                let ok = Store::cas(&mut data, &expected, to, EventCode::AmbiguousAttempt).is_ok();
                let allowed = !from.terminal()
                    && match to {
                        JobState::Cancelled | JobState::Failed | JobState::Claimed => {
                            from == JobState::Pending
                        }
                        JobState::NeedsOperator => from != JobState::Pending,
                        JobState::Cancelling => {
                            from != JobState::Pending && from != JobState::NeedsOperator
                        }
                        _ => false,
                    };
                assert_eq!(ok, allowed, "{from:?} -> {to:?}");
            }
        }
        for stale in ["owner", "attempt", "sequence", "state"] {
            let (mut data, mut expected) = database();
            match stale {
                "owner" => expected.owner += 1,
                "attempt" => expected.attempt += 1,
                "sequence" => expected.sequence += 1,
                _ => expected.state = JobState::Claimed,
            }
            assert!(
                Store::cas(
                    &mut data,
                    &expected,
                    JobState::Cancelled,
                    EventCode::Cancelled
                )
                .is_err()
            );
        }
    }
    #[test]
    fn restart_reconciliation_of_every_inflight_state_refuses_replay() {
        for state in [
            JobState::Claimed,
            JobState::Running,
            JobState::Staged,
            JobState::Committing,
            JobState::RetryWait,
            JobState::Cancelling,
        ] {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path().join("state");
            let mut store = Store::init(&root, Limits::default()).unwrap();
            let (mut data, mut job) = database();
            job.state = state;
            job.attempt = 1;
            job.instance = Some(Uuid::new_v4());
            job.events.last_mut().unwrap().state = state;
            data.jobs.insert(job.id, job.clone());
            store.persist(data).unwrap();
            drop(store);
            let mut store = Store::open(&root).unwrap();
            store.recover().unwrap();
            assert_eq!(store.status(job.id).unwrap().state, JobState::NeedsOperator);
            assert!(store.run(job.id).is_err());
            let cancelled = store.cancel(job.id).unwrap();
            assert!(cancelled.cancel_requested);
            assert_eq!(cancelled.state, JobState::NeedsOperator);
            assert_eq!(cancelled.sequence, store.cancel(job.id).unwrap().sequence);
            assert!(store.recover().unwrap().is_empty());
        }
    }
    #[test]
    fn sequence_generation_clock_and_state_size_overflow_fail_closed() {
        let (mut data, mut job) = database();
        job.sequence = u64::MAX;
        assert!(
            job.event(EventCode::Expired, 1, now_ms().unwrap(), 32)
                .is_err()
        );
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("state");
        let mut store = Store::init(&root, Limits::default()).unwrap();
        data.jobs.clear();
        data.generation = u64::MAX;
        store.persist(data).unwrap();
        drop(store);
        assert!(matches!(
            Store::open(&root),
            Err(Error::Quota("owner generation overflow"))
        ));
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("state");
        let mut store = Store::init(&root, Limits::default()).unwrap();
        store.data.last_ms = now_ms().unwrap() + 120_000;
        assert!(matches!(store.recover(), Err(Error::Deadline)));
        assert!(matches!(
            store.persist(store.data.clone()),
            Err(Error::Deadline)
        ));
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("state");
        drop(Store::init(&root, Limits::default()).unwrap());
        let state = root.join("state.json");
        let mut padded = fs::read(&state).unwrap();
        padded.resize(MAX_STORE_BYTES, b' ');
        fs::write(&state, &padded).unwrap();
        drop(Store::open(&root).unwrap());
        padded.push(b' ');
        fs::write(&state, &padded).unwrap();
        assert!(matches!(
            Store::open(&root),
            Err(Error::Quota("metadata bytes"))
        ));
    }
    #[test]
    fn failed_transaction_poisons_handle_and_never_invents_an_update() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("state");
        let mut store = Store::init(&root, Limits::default()).unwrap();
        let original = fs::read(root.join("state.json")).unwrap();
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(root.join("state.next"))
            .unwrap();
        drop(file);
        assert!(store.persist(store.data.clone()).is_err());
        assert!(store.list(None, 32).is_err());
        assert!(store.cleanup().is_err());
        assert_eq!(original, fs::read(root.join("state.json")).unwrap());
        drop(store);
        assert!(Store::open(&root).is_ok());
    }
    #[test]
    fn original_job_deadline_expires_and_is_never_extended_by_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("state");
        let mut store = Store::init(&root, Limits::default()).unwrap();
        let (mut data, mut job) = database();
        job.created_ms = now_ms().unwrap() - 120_001;
        job.expires_ms = job.created_ms + job.recipe.timeout_ms;
        job.events[0].at_ms = job.created_ms;
        data.jobs.insert(job.id, job.clone());
        store.persist(data).unwrap();
        drop(store);
        let mut store = Store::open(&root).unwrap();
        assert!(matches!(store.verify_sources(job.id), Err(Error::Deadline)));
        assert!(matches!(store.run(job.id), Err(Error::Deadline)));
        store.recover().unwrap();
        let expired = store.status(job.id).unwrap();
        assert_eq!(expired.state, JobState::Failed);
        assert_eq!(expired.expires_ms, job.expires_ms);
        assert!(store.recover().unwrap().is_empty());
    }
    #[test]
    fn native_configuration_pin_and_engine_counter_overflow_refuse_before_effects() {
        for overflow in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path().join("state");
            let mut store = Store::init(&root, Limits::default()).unwrap();
            let (mut data, mut job) = database();
            job.coordinator_compat = Some(crate::runner::coordinator_compat_hash());
            if overflow {
                data.next_engine_id = u64::MAX;
            } else {
                job.native_config = Some("f".repeat(64));
            }
            data.jobs.insert(job.id, job.clone());
            store.persist(data).unwrap();
            let root = store.root.clone();
            drop(store);
            let result = crate::runner::run(&root, job.id);
            assert!(
                matches!(
                    result,
                    Err(Error::Quota("engine JobId overflow") | Error::Unsupported(_))
                ),
                "overflow={overflow}: {result:?}"
            );
            let current = Store::open(&root).unwrap().status(job.id).unwrap();
            assert_eq!(current.attempt, 0);
            assert!(current.engine_job.is_none());
            assert!(!root.join("jobs").exists());
        }
    }
    #[test]
    fn older_v2_job_without_coordinator_pin_is_not_replayed() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap().join("state");
        let mut store = Store::init(&root, Limits::default()).unwrap();
        let (mut data, mut job) = database();
        job.coordinator_compat = None;
        data.jobs.insert(job.id, job.clone());
        store.persist(data).unwrap();
        drop(store);

        let current = crate::runner::run(&root, job.id).unwrap();
        assert_eq!(current.state, JobState::NeedsOperator);
        assert_eq!(current.attempt, 0);
        assert!(current.engine_job.is_none());
        assert!(
            current
                .attempts
                .last()
                .is_none_or(|attempt| attempt.engine_job != 1)
        );
        assert!(!root.join("jobs").exists());
    }
    #[test]
    fn checksum_validated_v1_snapshot_migrates_without_reissuing_identity() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("state");
        let store = Store::init(&root, Limits::default()).unwrap();
        let id = store.id();
        let generation = store.generation();
        let envelope = Envelope {
            version: 1,
            checksum: ade_physical::fingerprint(&serde_json::to_vec(&store.data).unwrap()),
            data: store.data.clone(),
        };
        drop(store);
        fs::write(
            root.join("state.json"),
            serde_json::to_vec(&envelope).unwrap(),
        )
        .unwrap();
        fs::remove_file(root.join("runner.lock")).unwrap();
        let store = Store::open(&root).unwrap();
        assert_eq!(store.id(), id);
        assert_eq!(store.generation(), generation);
        let migrated: Envelope =
            serde_json::from_slice(&fs::read(root.join("state.json")).unwrap()).unwrap();
        assert_eq!(migrated.version, 2);
        assert!(root.join("runner.lock").exists());
    }
}
