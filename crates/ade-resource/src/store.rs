use crate::{
    Error, Result,
    contract::{
        Config, Directory, Job, Ledger, MAX_JOBS, MAX_LAUNCHES, MAX_TENANTS, Outcome, State,
        native_key, owned, valid_key,
    },
    decode, encode, hash, now, policy, require,
};
use rustix::fs::{CWD, Mode, OFlags, openat};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt},
    path::Path,
};

const CAP: usize = 8 * 1024 * 1024;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Snapshot {
    version: u32,
    checksum: String,
    state: Ledger,
}

pub struct Store {
    root: Directory,
    _lease: File,
    pub state: Ledger,
    last_seen: u64,
}

impl Store {
    pub fn init(root: &Path, config: Config, admin: String) -> Result<()> {
        let parent = root.parent().ok_or(Error("UNSAFE_PATH"))?;
        require(
            parent.canonicalize().is_ok_and(|p| p == parent),
            "UNSAFE_PATH",
        )?;
        fs::DirBuilder::new()
            .mode(0o700)
            .create(root)
            .map_err(|_| Error("INIT_ROOT"))?;
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(root.join("manager.lock"))
            .and_then(|file| file.sync_all())
            .map_err(|_| Error("INIT_LOCK"))?;
        let at = now()?;
        let state = Ledger {
            generation: 0,
            clock_ms: at,
            next_sequence: 0,
            admin,
            config,
            tenants: BTreeMap::new(),
            jobs: BTreeMap::new(),
            standby_on: false,
            standby_changed_ms: at,
            standby_idle_since_ms: None,
            standby_transitions: vec![],
        };
        let mut store = Self::lock(root, state)?;
        store.change(at, |_| Ok(()))?;
        File::open(parent)
            .and_then(|dir| dir.sync_all())
            .map_err(|_| Error("INIT_SYNC"))
    }

    pub fn open(root: &Path) -> Result<Self> {
        let (root, lease) = lock(root)?;
        let data = private(&root.path.join("state.json"), false)?;
        let raw = read_bounded(data, CAP)?;
        let snapshot: Snapshot = decode(&raw).map_err(|_| Error("CORRUPT_LEDGER"))?;
        require(
            snapshot.version == 1 && snapshot.checksum == hash(&encode(&snapshot.state)?),
            "CORRUPT_LEDGER",
        )?;
        validate(&snapshot.state)?;
        let last_seen = now()?;
        require(last_seen >= snapshot.state.clock_ms, "CLOCK_ROLLBACK")?;
        let store = Self {
            root,
            _lease: lease,
            state: snapshot.state,
            last_seen,
        };
        store.clear_next()?;
        Ok(store)
    }

    fn lock(path: &Path, state: Ledger) -> Result<Self> {
        let (root, lease) = lock(path)?;
        Ok(Self {
            root,
            _lease: lease,
            last_seen: state.clock_ms,
            state,
        })
    }

    pub fn path(&self) -> &Path {
        &self.root.path
    }

    pub fn time(&mut self) -> Result<u64> {
        let current = now()?;
        require(current >= self.last_seen, "CLOCK_ROLLBACK")?;
        self.last_seen = current;
        Ok(current)
    }

    pub fn change<T>(&mut self, at: u64, edit: impl FnOnce(&mut Ledger) -> Result<T>) -> Result<T> {
        self.root.check()?;
        require(at >= self.state.clock_ms, "CLOCK_ROLLBACK")?;
        let mut state = self.state.clone();
        let result = edit(&mut state)?;
        state.generation = state
            .generation
            .checked_add(1)
            .ok_or(Error("GENERATION_FULL"))?;
        state.clock_ms = at;
        write(&self.root.path, &state)?;
        self.state = state;
        Ok(result)
    }

    fn clear_next(&self) -> Result<()> {
        let path = self.root.path.join("state.next");
        match fs::symlink_metadata(&path) {
            Ok(_) => {
                private(&path, false)?;
                fs::remove_file(path).map_err(|_| Error("LEDGER_IO"))?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(Error("LEDGER_IO")),
        }
        Ok(())
    }
}

fn lock(path: &Path) -> Result<(Directory, File)> {
    let root = Directory::capture(path.to_path_buf())?;
    require(owned(path, true)?.mode() & 0o777 == 0o700, "UNSAFE_PATH")?;
    let file = private(&root.path.join("manager.lock"), true)?;
    file.try_lock().map_err(|_| Error("MANAGER_BUSY"))?;
    Ok((root, file))
}

pub fn private(path: &Path, write: bool) -> Result<File> {
    let flags = if write { OFlags::RDWR } else { OFlags::RDONLY };
    let file = File::from(
        openat(
            CWD,
            path,
            flags | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
            Mode::empty(),
        )
        .map_err(|_| Error("PRIVATE_FILE"))?,
    );
    let meta = file.metadata().map_err(|_| Error("PRIVATE_FILE"))?;
    require(
        meta.is_file()
            && meta.nlink() == 1
            && meta.mode() & 0o777 == 0o600
            && meta.uid() == rustix::process::geteuid().as_raw(),
        "PRIVATE_FILE",
    )?;
    Ok(file)
}

pub fn read_bounded(file: impl Read, cap: usize) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    file.take(cap as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| Error("READ_IO"))?;
    require(bytes.len() <= cap, "FRAME_TOO_LARGE")?;
    Ok(bytes)
}

fn write(root: &Path, state: &Ledger) -> Result<()> {
    #[derive(Serialize)]
    struct Envelope<'a> {
        version: u32,
        checksum: String,
        state: &'a Ledger,
    }
    let bytes = encode(&Envelope {
        version: 1,
        checksum: hash(&encode(state)?),
        state,
    })?;
    require(bytes.len() <= CAP, "LEDGER_FULL")?;
    let path = root.join("state.next");
    (|| {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(&path)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        crate::fault("snapshot_before_rename");
        fs::rename(path, root.join("state.json"))?;
        File::open(root)?.sync_all()
    })()
    .map_err(|error| {
        #[cfg(feature = "fault-injection")]
        eprintln!("ADE_RESOURCE_LEDGER_ERRNO {:?}", error.raw_os_error());
        let _ = error;
        Error("LEDGER_IO")
    })
}

fn verify_attempts(ledger: &Ledger, id: uuid::Uuid, job: &Job) -> Result<()> {
    for (index, attempt) in job.attempts.iter().enumerate() {
        require(
            attempt.cohort < 2
                && attempt.native_key == native_key(id, index + 1)
                && attempt.admitted_ms >= job.queued_ms
                && attempt.admitted_ms < job.expires_ms
                && attempt.admitted_ms <= ledger.clock_ms
                && attempt
                    .running_ms
                    .is_none_or(|ms| ms >= attempt.admitted_ms && ms <= ledger.clock_ms)
                && attempt.native_id.is_none_or(|id| !id.is_nil()),
            "CORRUPT_LEDGER",
        )?;
        require(
            attempt.grant == policy::grant(ledger, job, attempt.cohort, attempt.admitted_ms)?
                && attempt.priority == policy::rank(ledger, job, attempt.admitted_ms)
                && encode(&attempt.price)? == encode(&ledger.config.price)?
                && attempt.price_hash
                    == attempt
                        .price
                        .as_ref()
                        .map(|p| encode(p).map(|b| hash(&b)))
                        .transpose()?
                && (attempt.native_id.is_some() || attempt.instance.is_none())
                && (attempt.instance.is_some() == attempt.owner.is_some())
                && (attempt.instance.is_some() == attempt.engine_job.is_some()),
            "CORRUPT_LEDGER",
        )?;
        if let Some(usage) = &attempt.usage {
            let expected = policy::settle(
                attempt,
                job.expires_ms,
                usage.end_ms,
                usage.outcome,
                usage.written_bytes,
                usage
                    .ipc_bytes
                    .zip(usage.ipc_hash.clone())
                    .map(|(n, h)| (usage.committed_bytes, n, h)),
            )?;
            require(
                usage.end_ms <= ledger.clock_ms
                    && usage.committed_bytes <= attempt.grant.staging_bytes
                    && encode(usage)? == encode(&expected)?
                    && (usage.outcome == Outcome::Succeeded) == usage.ipc_hash.is_some()
                    && (index + 1 == job.attempts.len() || usage.outcome == Outcome::Preempted),
                "CORRUPT_LEDGER",
            )?;
        } else {
            require(
                index + 1 == job.attempts.len() && job.state.holds_cohort(),
                "CORRUPT_LEDGER",
            )?;
        }
    }
    Ok(())
}

fn validate(state: &Ledger) -> Result<()> {
    require(
        state.generation > 0
            && state.generation < u64::MAX
            && state.tenants.len() <= MAX_TENANTS
            && state.jobs.len() <= MAX_JOBS
            && state.standby_transitions.len() <= 2 * MAX_JOBS * MAX_LAUNCHES
            && state.standby_changed_ms <= state.clock_ms
            && state
                .standby_idle_since_ms
                .is_none_or(|ms| state.standby_on && ms <= state.clock_ms)
            && state.standby_on == state.standby_transitions.last().is_some_and(|(_, on)| *on)
            && state.admin.len() == 64,
        "CORRUPT_LEDGER",
    )?;
    crate::native::validate_config(&state.config, false)?;
    let mut keys = BTreeSet::new();
    let mut sequences = BTreeSet::new();
    let mut native = BTreeSet::new();
    let mut assigned = BTreeSet::new();
    for (&id, job) in &state.jobs {
        require(
            !id.is_nil()
                && state.tenants.contains_key(&job.tenant)
                && valid_key(&job.key)
                && keys.insert((job.tenant, &job.key))
                && sequences.insert(job.sequence)
                && job.sequence <= state.next_sequence
                && (1..=130).contains(&job.source_pins.len())
                && job.attempts.len() <= MAX_LAUNCHES
                && job.reason.len() <= 96
                && job.digest == job.spec.digest()?
                && job.queued_ms <= state.clock_ms
                && job.queued_ms.checked_add(job.spec.deadline_ms) == Some(job.expires_ms),
            "CORRUPT_LEDGER",
        )?;
        job.spec.validate(&state.config.cohorts[0].workers)?;
        verify_attempts(state, id, job)?;
        let last = job.attempts.last();
        require(
            job.state.holds_cohort() == last.is_some_and(|a| a.usage.is_none())
                && (job.state != State::Succeeded
                    || last
                        .and_then(|a| a.usage.as_ref())
                        .is_some_and(|u| u.outcome == Outcome::Succeeded))
                && (job.state != State::Running || last.is_some_and(|a| a.running_ms.is_some())),
            "CORRUPT_LEDGER",
        )?;
        if job.state.holds_cohort() {
            let a = last.ok_or(Error("CORRUPT_LEDGER"))?;
            require(
                assigned.insert(a.cohort) && (a.cohort == 0 || state.standby_on),
                "CORRUPT_LEDGER",
            )?;
        }
        for a in &job.attempts {
            if let Some(id) = a.native_id {
                require(native.insert((a.cohort, id)), "CORRUPT_LEDGER")?;
            }
        }
    }
    for (&id, tenant) in &state.tenants {
        tenant.limits.validate()?;
        require(
            !id.is_nil()
                && (1..=16).contains(&tenant.weight)
                && tenant
                    .fingerprint
                    .as_ref()
                    .is_none_or(|hash| hash.len() == 64)
                && policy::totals(state, Some(id), None)?.fits(&tenant.limits),
            "CORRUPT_LEDGER",
        )?;
        let work = state
            .jobs
            .values()
            .filter(|j| j.tenant == id)
            .try_fold(0u64, |total, j| {
                total
                    .checked_add(j.spec.work * j.attempts.len() as u64)
                    .ok_or(Error("CORRUPT_LEDGER"))
            })?;
        require(tenant.granted_work == work, "CORRUPT_LEDGER")?;
    }
    require(
        policy::totals(state, None, None)?.fits(&state.config.limits),
        "CORRUPT_LEDGER",
    )
}
