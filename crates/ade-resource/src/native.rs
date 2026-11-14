use crate::{
    Error, Result,
    contract::{Cohort, Config, Directory, Job, MAX_NATIVE_JOBS, MAX_TIME_MS, owned},
    now, require,
};
use ade_distributed::{
    WorkerEndpoint,
    ids::WorkerId,
    managed::{self, Fence, Operation, Response},
};
use ade_ops::{Job as NativeJob, JobState, Store, runner};
use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::Path,
    thread,
    time::{Duration, Instant},
};
use uuid::Uuid;

pub fn validate_config(config: &Config, live: bool) -> Result<()> {
    config.limits.validate()?;
    if let Some(price) = &config.price {
        price.validate()?;
    }
    require(
        config.version == 1
            && (1..=MAX_TIME_MS).contains(&config.aging_ms)
            && (20..=MAX_TIME_MS).contains(&config.standby_idle_ms)
            && (1..=MAX_TIME_MS).contains(&config.preempt_after_ms)
            && (config.limits.cost_microunits.is_none() || config.price.is_some()),
        "INVALID_CONFIG",
    )?;
    let mut endpoints = BTreeSet::new();
    let mut instances = BTreeSet::new();
    let mut directories = Vec::new();
    let mut stores = BTreeSet::new();
    for cohort in &config.cohorts {
        require(
            !cohort.store.is_nil()
                && stores.insert(cohort.store)
                && (2..=4).contains(&cohort.workers.len()),
            "INVALID_COHORT",
        )?;
        directories.push(&cohort.root);
        for (index, worker) in cohort.workers.iter().enumerate() {
            require(
                worker.id == index as u64
                    && !worker.instance.is_nil()
                    && instances.insert(worker.instance),
                "COHORT_ALIAS",
            )?;
            directories.push(&worker.scratch_root);
            for endpoint in [worker.control, worker.exchange] {
                require(
                    endpoint.ip().is_loopback()
                        && endpoint.port() != 0
                        && endpoints.insert(endpoint.port()),
                    "COHORT_ALIAS",
                )?;
            }
        }
        if live {
            require(
                open(cohort)?
                    .list(None, MAX_NATIVE_JOBS)
                    .map_err(|_| Error("NATIVE_STORE"))?
                    .is_empty(),
                "COHORT_NOT_EMPTY",
            )?;
            probe(cohort)?;
        }
    }
    for (i, path) in directories.iter().enumerate() {
        require(
            path.is_absolute()
                && path.as_os_str().len() <= 4096
                && directories[..i]
                    .iter()
                    .all(|other| !path.starts_with(other) && !other.starts_with(path)),
            "COHORT_ALIAS",
        )?;
        if live {
            private_directory(path)?;
        }
    }
    Ok(())
}

// A second manager cannot concurrently lease the same pair or either sibling.
pub fn lease(cohort: &Cohort) -> Result<File> {
    drop(open(cohort)?);
    let path = cohort.root.join("resource.lock");
    match OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&path)
    {
        Ok(file) => {
            file.sync_all()
                .and_then(|()| File::open(&cohort.root)?.sync_all())
                .map_err(|_| Error("COHORT_LEASE"))?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(_) => return Err(Error("COHORT_LEASE")),
    }
    let file = crate::store::private(&path, true)?;
    file.try_lock().map_err(|_| Error("COHORT_LEASE_BUSY"))?;
    Ok(file)
}

fn private_directory(path: &Path) -> Result<()> {
    Directory::capture(path.to_path_buf())?;
    require(owned(path, true)?.mode() & 0o777 == 0o700, "COHORT_PATH")
}

fn probe(cohort: &Cohort) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(2);
    for worker in &cohort.workers {
        private_directory(&worker.scratch_root)?;
        let endpoint = WorkerEndpoint {
            managed: Some(Fence {
                version: 1,
                boot: worker.instance.to_string(),
                store: cohort.store.to_string(),
                owner: 1,
                instance: worker.instance.to_string(),
                job: 1,
                disk_bytes: 1,
                scratch: worker
                    .scratch_root
                    .to_str()
                    .ok_or(Error("COHORT_PATH"))?
                    .into(),
            }),
            id: WorkerId(worker.id),
            control: worker.control,
            exchange: worker.exchange,
        };
        let Response::Hello(ready) = managed::rpc(&endpoint, Operation::Hello, deadline)
            .map_err(|_| Error("WORKER_IDENTITY"))?
        else {
            return Err(Error("WORKER_IDENTITY"));
        };
        require(
            ready.boot.version == 1
                && ready.boot.store == cohort.store.to_string()
                && ready.boot.instance == worker.instance.to_string()
                && ready.boot.runtime == managed::RUNTIME_ID
                && ready.boot.research_hooks == cfg!(feature = "fault-injection")
                && ready.endpoint.id == endpoint.id
                && ready.endpoint.control == endpoint.control
                && ready.endpoint.exchange == endpoint.exchange,
            "WORKER_IDENTITY",
        )?;
    }
    Ok(())
}

fn open_once(cohort: &Cohort) -> Result<Option<Store>> {
    private_directory(&cohort.root)?;
    match Store::open(&cohort.root) {
        Ok(store) if store.id() == cohort.store => Ok(Some(store)),
        Err(ade_ops::Error::Owned) => Ok(None),
        _ => Err(Error("NATIVE_STORE")),
    }
}

fn open(cohort: &Cohort) -> Result<Store> {
    let until = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(store) = open_once(cohort)? {
            return Ok(store);
        }
        require(Instant::now() < until, "NATIVE_STORE_BUSY")?;
        thread::sleep(Duration::from_millis(5));
    }
}

pub fn observe(cohort: &Cohort, id: Uuid) -> Result<Option<NativeJob>> {
    open_once(cohort)?
        .map(|store| store.status(id).map_err(|_| Error("NATIVE_MAPPING")))
        .transpose()
}

pub enum Registration {
    Mapped(Uuid),
    Refused(&'static str),
}

fn audit(
    cohort: &Cohort,
    store: &Store,
    allowed: &[(String, Option<Uuid>)],
) -> Result<Vec<NativeJob>> {
    let jobs = store
        .list(None, MAX_NATIVE_JOBS)
        .map_err(|_| Error("NATIVE_STORE"))?;
    require(
        jobs.iter().all(|j| {
            j.recipe.workers == cohort.workers
                && allowed
                    .iter()
                    .any(|(key, id)| key == &j.request_key && id.is_none_or(|id| id == j.id))
        }),
        "FOREIGN_NATIVE_JOB",
    )?;
    require(
        allowed.iter().all(|(key, id)| {
            id.is_none_or(|id| jobs.iter().any(|j| j.id == id && &j.request_key == key))
        }),
        "NATIVE_MAPPING",
    )?;
    Ok(jobs)
}

pub fn register(
    cohort: &Cohort,
    job: &Job,
    source: &Directory,
    known: &[(String, Option<Uuid>)],
) -> Result<Registration> {
    let attempt = job.attempts.last().ok_or(Error("NATIVE_MAPPING"))?;
    let recipe = job.spec.recipe(&cohort.workers);
    let store = open(cohort)?;
    let jobs = audit(cohort, &store, known)?;
    if let Some(existing) = jobs.iter().find(|j| j.request_key == attempt.native_key) {
        require(
            existing.recipe == recipe && existing.expires_ms == job.expires_ms,
            "NATIVE_MAPPING",
        )?;
        return Ok(Registration::Mapped(existing.id));
    }
    require(jobs.iter().all(|j| j.state.terminal()), "NATIVE_UNRESOLVED")?;
    if jobs.len() >= MAX_NATIVE_JOBS {
        return Ok(Registration::Refused("NATIVE_HISTORY_FULL"));
    }
    drop(store);
    if job.intent.is_some() || now()? >= job.expires_ms || job.verify_source_scope(source).is_err()
    {
        return Ok(Registration::Refused(if job.intent.is_some() {
            "CANCELLED_BEFORE_NATIVE"
        } else if now()? >= job.expires_ms {
            "DEADLINE_EXPIRED"
        } else {
            "SOURCE_DRIFT"
        }));
    }
    probe(cohort)?;
    // Probe can take time. Source authorization precedes the opening of any
    // input by the native layer.
    if job.verify_source_scope(source).is_err() {
        return Ok(Registration::Refused("SOURCE_DRIFT"));
    }
    let mut store = open(cohort)?;
    match store.submit_before(&attempt.native_key, recipe, job.expires_ms) {
        Ok(registered) => {
            crate::fault("native_registered_before_mapping");
            Ok(Registration::Mapped(registered.id))
        }
        Err(error) => {
            drop(store);
            let store = open(cohort)?;
            let jobs = audit(cohort, &store, known)?;
            if jobs.iter().any(|j| j.request_key == attempt.native_key) {
                // A durable publication with an error may have been visible
                // without its directory sync. Stop and require a restart.
                return Err(Error("NATIVE_REGISTER_AMBIGUOUS"));
            }
            match error {
                ade_ops::Error::Invalid(_)
                | ade_ops::Error::Unsupported(_)
                | ade_ops::Error::Deadline => Ok(Registration::Refused("NATIVE_ADMISSION_REFUSED")),
                _ => Err(Error("NATIVE_REGISTER_AMBIGUOUS")),
            }
        }
    }
}

pub struct Resolution {
    pub job: NativeJob,
    pub bytes: Option<u64>,
    pub output: Option<(u64, u64, String)>,
}

pub fn run(cohort: &Cohort, job: &Job, source: &Directory) -> Result<Resolution> {
    let attempt = job.attempts.last().ok_or(Error("NATIVE_MAPPING"))?;
    let id = attempt.native_id.ok_or(Error("NATIVE_MAPPING"))?;
    let original = open(cohort)?
        .status(id)
        .map_err(|_| Error("NATIVE_MAPPING"))?;
    require(
        original.request_key == attempt.native_key
            && original.recipe == job.spec.recipe(&cohort.workers)
            && original.expires_ms == job.expires_ms,
        "NATIVE_MAPPING",
    )?;
    let drift = job.verify_source_scope(source).is_err();
    if job.intent.is_some() || drift {
        cancel(cohort, id)?;
    }
    let job = runner::run(&cohort.root, id).map_err(|error| {
        #[cfg(feature = "fault-injection")]
        if let ade_ops::Error::Io(io) = &error {
            eprintln!("ADE_RESOURCE_NATIVE_ERRNO {:?}", io.raw_os_error());
        }
        let _ = error;
        Error("NATIVE_RUN_UNCERTAIN")
    })?;
    require(job.state.terminal(), "NATIVE_DRAIN_UNPROVED")?;
    let output = if job.state == JobState::Succeeded {
        let manifest =
            runner::verify_output(&cohort.root, id).map_err(|_| Error("OUTPUT_VERIFICATION"))?;
        let marker = output_root(&cohort.root, id)
            .join("COMMITTED.json")
            .metadata()
            .map_err(|_| Error("OUTPUT_VERIFICATION"))?
            .len();
        Some((
            manifest
                .bytes
                .checked_add(marker)
                .ok_or(Error("ACCOUNTING_OVERFLOW"))?,
            manifest.bytes,
            manifest.blake3,
        ))
    } else {
        None
    };
    let bytes = if let Some(a) = job.attempts.last() {
        a.coordinator_disk_bytes
            .filter(|_| a.worker_disk_bytes.len() == cohort.workers.len())
            .and_then(|n| {
                a.worker_disk_bytes
                    .iter()
                    .try_fold(n, |total, b| total.checked_add(*b))
            })
    } else {
        Some(0)
    };
    Ok(Resolution { job, bytes, output })
}

pub fn cancel(cohort: &Cohort, id: Uuid) -> Result<()> {
    drop(open(cohort)?);
    runner::cancel(&cohort.root, id)
        .map(|_| ())
        .map_err(|_| Error("NATIVE_CANCEL_UNCERTAIN"))
}

pub fn output_root(root: &Path, id: Uuid) -> std::path::PathBuf {
    root.join("jobs").join(id.to_string())
}

pub fn read_output(
    cohort: &Cohort,
    id: Uuid,
    digest: &str,
    offset: u64,
) -> Result<serde_json::Value> {
    use std::io::{Read, Seek, SeekFrom, Write as _};
    drop(open(cohort)?);
    let marker =
        runner::verify_output(&cohort.root, id).map_err(|_| Error("OUTPUT_VERIFICATION"))?;
    require(
        marker.blake3 == digest && offset <= marker.bytes,
        "OUTPUT_VERIFICATION",
    )?;
    let path = output_root(&cohort.root, id).join(&marker.file);
    let mut file = fs::File::open(path).map_err(|_| Error("OUTPUT_VERIFICATION"))?;
    file.seek(SeekFrom::Start(offset))
        .map_err(|_| Error("OUTPUT_VERIFICATION"))?;
    let len = usize::try_from((marker.bytes - offset).min(12 * 1024))
        .map_err(|_| Error("OUTPUT_VERIFICATION"))?;
    let mut chunk = vec![0; len];
    file.read_exact(&mut chunk)
        .map_err(|_| Error("OUTPUT_VERIFICATION"))?;
    let mut hex = Vec::with_capacity(len * 2);
    for byte in chunk {
        write!(hex, "{byte:02x}").map_err(|_| Error("OUTPUT_VERIFICATION"))?;
    }
    let encoded = String::from_utf8(hex).map_err(|_| Error("OUTPUT_VERIFICATION"))?;
    Ok(serde_json::json!({
        "offset": offset, "hex": encoded, "total": marker.bytes, "blake3": marker.blake3,
        "rows": marker.rows, "schema": marker.schema,
    }))
}
