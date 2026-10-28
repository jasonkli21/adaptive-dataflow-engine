//! Foreground single-coordinator driver. Metadata locks never span native work.
use crate::{
    Error, Job, JobState, Result, Store, deadline, fault, now_ms,
    publication::{self, Identity, Manifest, Publisher},
    require,
    store::{Attempt, EventCode},
};
use ade_distributed::{
    WorkerEndpoint,
    ids::{JobId, WorkerId},
    managed::{self, Fence},
    physical,
};
use ade_exec::CancellationToken;
use std::{
    fs::{self, File, OpenOptions},
    os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use uuid::Uuid;
fn with_store<T>(root: &Path, f: impl FnOnce(&mut Store) -> Result<T>) -> Result<T> {
    let until = Instant::now() + Duration::from_secs(5);
    let mut store = loop {
        match Store::open(root) {
            Ok(store) => break store,
            Err(Error::Owned) if Instant::now() < until => {
                std::thread::sleep(Duration::from_millis(2));
            }
            Err(error) => return Err(error),
        }
    };
    f(&mut store)
}
fn owner(root: &Path) -> Result<File> {
    with_store(root, |store| {
        let path = store.root.join("runner.lock");
        crate::recipe::nonsymlink(&path)?;
        let metadata = fs::metadata(&path)?;
        require(
            metadata.is_file()
                && metadata.nlink() == 1
                && metadata.permissions().mode() & 0o777 == 0o600,
            "private coordinator lock",
        )?;
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        file.try_lock().map_err(|_| Error::Owned)?;
        Ok(file)
    })
}
fn load(root: &Path, id: Uuid) -> Result<Job> {
    with_store(root, |s| s.status(id))
}
fn directory(path: &Path) -> Result<()> {
    match fs::DirBuilder::new().mode(0o700).create(path) {
        Ok(()) => File::open(
            path.parent()
                .ok_or_else(|| Error::Invalid("directory parent".into()))?,
        )?
        .sync_all()?,
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e.into()),
    }
    crate::recipe::nonsymlink(path)?;
    require(
        fs::metadata(path)?.is_dir() && fs::metadata(path)?.permissions().mode() & 0o777 == 0o700,
        "private runtime directory",
    )
}
fn output_root(root: &Path, id: Uuid) -> PathBuf {
    root.join("jobs").join(id.to_string())
}
fn scratch_root(root: &Path, job: &Job) -> Result<PathBuf> {
    Ok(root.join("scratch").join(
        job.instance
            .ok_or_else(|| Error::Invalid("missing execution namespace".into()))?
            .to_string(),
    ))
}
fn same_attempt(current: &Job, expected: &Job) -> Result<()> {
    require(
        current.owner == expected.owner
            && current.attempt == expected.attempt
            && current.engine_job == expected.engine_job
            && current.instance == expected.instance,
        "stale durable attempt fence",
    )
}
fn change(
    root: &Path,
    expected: &Job,
    state: JobState,
    code: EventCode,
    error: Option<&str>,
    extra: impl FnOnce(&mut Job),
) -> Result<Job> {
    with_store(root, |store| {
        let mut data = store.data.clone();
        let job = data
            .jobs
            .get_mut(&expected.id)
            .ok_or_else(|| Error::Invalid("unknown job".into()))?;
        same_attempt(job, expected)?;
        if job.state == state && job.state.terminal() {
            return Ok(job.clone());
        }
        require(!job.state.terminal(), "terminal transition refused")?;
        if job.cancel_requested
            && matches!(
                state,
                JobState::Running | JobState::Staged | JobState::Committing
            )
        {
            return Err(Error::Invalid("cancellation precedes transition".into()));
        }
        job.state = state;
        extra(job);
        if let Some(attempt) = job.attempts.last_mut() {
            attempt.state = state;
            if let Some(error) = error {
                attempt.error = Some(error.chars().take(256).collect());
            }
            if state.terminal() || matches!(state, JobState::RetryWait | JobState::NeedsOperator) {
                attempt.ended_ms = Some(now_ms()?);
            }
        }
        job.event(code, job.owner, now_ms()?, data.limits.events_per_job)?;
        let result = job.clone();
        store.persist(data)?;
        Ok(result)
    })
}
fn endpoints(store: Uuid, job: &Job) -> Result<Vec<WorkerEndpoint>> {
    let instance = job
        .instance
        .ok_or_else(|| Error::Invalid("missing execution instance".into()))?
        .to_string();
    let engine = job
        .engine_job
        .ok_or_else(|| Error::Invalid("missing engine JobId".into()))?;
    Ok(job
        .recipe
        .workers
        .iter()
        .map(|w| WorkerEndpoint {
            managed: Some(Fence {
                version: 1,
                boot: w.instance.to_string(),
                store: store.to_string(),
                owner: job.owner,
                instance: instance.clone(),
                job: engine,
                disk_bytes: job.recipe.runtime_bytes,
                scratch: w.scratch_root.to_string_lossy().into_owned(),
            }),
            id: WorkerId(w.id),
            control: w.control,
            exchange: w.exchange,
        })
        .collect())
}
fn native_options(recipe: &crate::Recipe) -> physical::Options {
    let mut options = physical::Options::default();
    options.aggregate.spill_enabled = false;
    options.aggregate.memory_limit_bytes = 32 * 1024 * 1024;
    options.aggregate.exchange.mode = ade_distributed::ExchangeMode::Buffered;
    options.join.local.spill_enabled = false;
    options.join.local.algorithm = ade_exec::join::Algorithm::HashOnly;
    options.join.local.max_output_rows = recipe.plan.max_output_rows;
    options
}
pub(crate) fn config_hash(recipe: &crate::Recipe) -> Result<String> {
    let options = native_options(recipe);
    Ok(ade_physical::fingerprint(&serde_json::to_vec(&(
        1u32,
        managed::RUNTIME_ID,
        options.aggregate,
        options.join,
        recipe.runtime_bytes,
    ))?))
}
fn identity(store: Uuid, job: &Job) -> Result<Identity> {
    Ok(Identity {
        store,
        job: job.id,
        instance: job
            .instance
            .ok_or_else(|| Error::Invalid("missing instance".into()))?,
        owner: job.owner,
        engine_job: job
            .engine_job
            .ok_or_else(|| Error::Invalid("missing engine id".into()))?,
        plan_hash: job
            .recipe
            .plan
            .digest()
            .map_err(|e| Error::Invalid(e.to_string()))?,
        source_hash: ade_physical::fingerprint(&serde_json::to_vec(&job.recipe.plan.sources)?),
        config_hash: job
            .native_config
            .clone()
            .ok_or_else(|| Error::Invalid("native configuration not pinned".into()))?,
        workers_hash: ade_physical::fingerprint(&serde_json::to_vec(&job.recipe.workers)?),
    })
}
fn limits(job: &Job) -> publication::Limits {
    publication::Limits {
        rows: job.recipe.plan.max_output_rows,
        bytes: job
            .recipe
            .staging_bytes
            .min(job.recipe.plan.max_output_bytes),
    }
}
fn source_check(
    job: &Job,
    token: &CancellationToken,
    until: Instant,
    stage: &str,
) -> Result<ade_exec::physical::Catalog> {
    for file in &job.files {
        token.check().map_err(|e| Error::Invalid(e.to_string()))?;
        file.verify_token(until, token, stage)?;
    }
    let catalog = ade_exec::physical::Catalog {
        bindings: job
            .recipe
            .inputs
            .iter()
            .zip(&job.recipe.plan.sources)
            .map(|(input, source)| ade_exec::physical::Binding {
                source: source.clone(),
                files: source
                    .files
                    .iter()
                    .map(|f| input.root.join(&f.name))
                    .collect(),
            })
            .collect(),
    };
    catalog
        .verify(&job.recipe.plan, token, until, true)
        .map_err(|e| Error::Invalid(e.to_string()))?;
    Ok(catalog)
}
fn claim(root: &Path, id: Uuid) -> Result<(Uuid, Job)> {
    with_store(root, |store| {
        let mut data = store.data.clone();
        let original = data
            .jobs
            .get(&id)
            .ok_or_else(|| Error::Invalid("unknown job".into()))?;
        require(
            matches!(original.state, JobState::Pending | JobState::RetryWait)
                && !original.cancel_requested,
            "job not claimable",
        )?;
        let _ = deadline(original.expires_ms)?;
        if original
            .native_config
            .as_ref()
            .is_some_and(|hash| config_hash(&original.recipe).is_ok_and(|current| &current != hash))
        {
            return Err(Error::Unsupported(
                "native source/configuration differs from durable registration; use matching binaries",
            ));
        }
        require(
            data.jobs
                .values()
                .all(|j| j.id == id || j.engine_job.is_none() || j.state.terminal()),
            "another unresolved native job requires recovery/operator",
        )?;
        if original
            .retry_at_ms
            .is_some_and(|at| now_ms().is_ok_and(|now| now < at))
        {
            return Err(Error::Invalid("retry backoff not elapsed".into()));
        }
        data.generation = data
            .generation
            .checked_add(1)
            .ok_or(Error::Quota("owner generation overflow"))?;
        data.next_engine_id = data
            .next_engine_id
            .checked_add(1)
            .ok_or(Error::Quota("engine JobId overflow"))?;
        let job = data
            .jobs
            .get_mut(&id)
            .ok_or_else(|| Error::Invalid("unknown job".into()))?;
        require(job.attempt < job.recipe.max_attempts, "attempt limit")?;
        if job.native_config.is_none() {
            job.native_config = Some(config_hash(&job.recipe)?);
        }
        job.attempt = job
            .attempt
            .checked_add(1)
            .ok_or(Error::Quota("attempt overflow"))?;
        job.owner = data.generation;
        job.instance = Some(Uuid::new_v4());
        job.engine_job = Some(data.next_engine_id);
        job.state = JobState::Claimed;
        job.retry_at_ms = None;
        job.output = None;
        job.attempts.push(Attempt {
            ordinal: job.attempt,
            owner: job.owner,
            engine_job: data.next_engine_id,
            instance: job
                .instance
                .ok_or_else(|| Error::Invalid("instance".into()))?,
            state: JobState::Claimed,
            started_ms: now_ms()?,
            ended_ms: None,
            error: None,
            worker_disk_bytes: vec![],
            coordinator_disk_bytes: None,
        });
        job.event(
            EventCode::Claimed,
            job.owner,
            now_ms()?,
            data.limits.events_per_job,
        )?;
        let result = job.clone();
        let store_id = data.id;
        store.persist(data)?;
        Ok((store_id, result))
    })
}
struct Monitor {
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}
impl Monitor {
    fn start(
        root: &Path,
        job: &Job,
        workers: Vec<WorkerEndpoint>,
        token: CancellationToken,
    ) -> Self {
        let root = root.to_path_buf();
        let id = job.id;
        let expiry = job.expires_ms;
        let stop = Arc::new(AtomicBool::new(false));
        let stopping = stop.clone();
        let until = deadline(expiry).unwrap_or_else(|_| Instant::now());
        let thread = std::thread::spawn(move || {
            while !stopping.load(Ordering::Acquire) {
                let cancel = load(&root, id).map_or(true, |job| {
                    job.cancel_requested || job.state == JobState::Cancelling
                });
                if cancel || Instant::now() >= until || now_ms().is_ok_and(|now| now >= expiry) {
                    token.cancel();
                    let _ = managed::drain(&workers, Instant::now() + Duration::from_secs(4));
                    break;
                }
                std::thread::sleep(Duration::from_millis(20));
            }
        });
        Self {
            stop,
            thread: Some(thread),
        }
    }
}
impl Drop for Monitor {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}
fn no_marker_cleanup(root: &Path, job: &Job) -> Result<()> {
    let output = output_root(root, job.id);
    if !output.try_exists()? {
        return Ok(());
    }
    crate::recipe::nonsymlink(&output)?;
    require(
        !output.join("COMMITTED.json").try_exists()?,
        "committed output protected from cleanup",
    )?;
    for attempt in &job.attempts {
        for name in [
            format!("attempt-{}.arrow", attempt.instance),
            format!("manifest-{}.json", attempt.instance),
        ] {
            let path = output.join(name);
            match fs::symlink_metadata(&path) {
                Ok(m) => {
                    require(m.is_file() && m.nlink() == 1, "unsafe attempt garbage")?;
                    fs::remove_file(path)?;
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
    }
    File::open(output)?.sync_all()?;
    Ok(())
}
/// Verify only the canonical marker, all referenced bytes, original identity/schema,
/// and the exact native completion manifest persisted before COMMITTING.
/// # Errors
/// Refuses absent, foreign/corrupt output and unproved native completion.
pub fn verify_output(root: &Path, id: Uuid) -> Result<Manifest> {
    let (store, job) = with_store(root, |s| Ok((s.id(), s.status(id)?)))?;
    let expected = job
        .output
        .as_ref()
        .ok_or_else(|| Error::Invalid("no durable native output proof".into()))?;
    let schema = &job.recipe.plan.nodes[job.recipe.plan.output as usize].schema;
    let manifest = publication::verify(
        &output_root(root, id),
        &identity(store, &job)?,
        schema,
        limits(&job),
        &CancellationToken::default(),
        Instant::now() + Duration::from_secs(120),
    )?;
    require(
        serde_json::to_vec(&manifest)? == serde_json::to_vec(expected)?,
        "marker differs from durable native completion",
    )?;
    Ok(manifest)
}
fn reconcile_marker(root: &Path, job: &Job) -> Result<Option<Job>> {
    if !output_root(root, job.id)
        .join("COMMITTED.json")
        .try_exists()?
    {
        return Ok(None);
    }
    match verify_output(root, job.id).and_then(|manifest| {
        crate::io_point("marker_sync")?;
        File::open(output_root(root, job.id))?.sync_all()?;
        Ok(manifest)
    }) {
        Ok(_) => {
            if job.state == JobState::Succeeded {
                return Ok(Some(job.clone()));
            }
            require(
                !job.state.terminal(),
                "publication conflicts with terminal state",
            )?;
            Ok(Some(change(
                root,
                job,
                JobState::Succeeded,
                EventCode::Succeeded,
                None,
                |_| {},
            )?))
        }
        Err(error) => {
            if !job.state.terminal() {
                let _ = change(
                    root,
                    job,
                    JobState::NeedsOperator,
                    EventCode::AmbiguousAttempt,
                    Some(&error.to_string()),
                    |_| {},
                );
            }
            Err(error)
        }
    }
}
fn fail(root: &Path, job: &Job, error: &str, operator: bool) -> Result<Job> {
    let current = load(root, job.id)?;
    same_attempt(&current, job)?;
    if !operator && current.cancel_requested {
        return cancelled(root, &current);
    }
    change(
        root,
        job,
        if operator {
            JobState::NeedsOperator
        } else {
            JobState::Failed
        },
        if operator {
            EventCode::AmbiguousAttempt
        } else {
            EventCode::AttemptFailed
        },
        Some(error),
        |_| {},
    )
}
fn cancelled(root: &Path, job: &Job) -> Result<Job> {
    no_marker_cleanup(root, job)?;
    change(
        root,
        job,
        JobState::Cancelled,
        EventCode::Cancelled,
        None,
        |j| j.cancel_requested = true,
    )
}
fn retry(root: &Path, job: &Job, error: &str) -> Result<Job> {
    let now = now_ms()?;
    let backoff = 100u64
        .checked_mul(1u64 << job.attempt.min(2))
        .ok_or(Error::Quota("backoff overflow"))?;
    let at = now.checked_add(backoff).ok_or(Error::Deadline)?;
    if job.attempt >= job.recipe.max_attempts || at >= job.expires_ms {
        return fail(root, job, error, false);
    }
    no_marker_cleanup(root, job)?;
    change(
        root,
        job,
        JobState::RetryWait,
        EventCode::RetryScheduled,
        Some(error),
        |j| j.retry_at_ms = Some(at),
    )
}
fn recover_one(root: &Path, id: Uuid) -> Result<Job> {
    let job = load(root, id)?;
    if let Some(committed) = reconcile_marker(root, &job)? {
        return Ok(committed);
    }
    if job.state == JobState::Succeeded {
        return Err(Error::Invalid("succeeded job lost its marker".into()));
    }
    if job.state == JobState::Pending && now_ms()? >= job.expires_ms {
        return fail(root, &job, "global deadline expired", false);
    }
    if job.state.terminal() || matches!(job.state, JobState::Pending | JobState::NeedsOperator) {
        return Ok(job);
    }
    let Some(_) = job.engine_job else {
        return fail(
            root,
            &job,
            "legacy metadata claim has no native mapping; needs operator",
            true,
        );
    };
    let store = with_store(root, |s| Ok(s.id()))?;
    let workers = endpoints(store, &job)?;
    if let Err(error) = managed::drain(&workers, Instant::now() + Duration::from_secs(4)) {
        return fail(root, &job, &error.to_string(), true);
    }
    if job.cancel_requested {
        return cancelled(root, &job);
    }
    if now_ms()? >= job.expires_ms {
        return fail(root, &job, "global deadline expired", false);
    }
    if job.state == JobState::RetryWait {
        return Ok(job);
    }
    if let Some(manifest) = &job.output {
        let token = CancellationToken::default();
        let _monitor = Monitor::start(root, &job, workers.clone(), token.clone());
        let until = deadline(job.expires_ms)?;
        if let Err(error) = source_check(&job, &token, until, "recovery") {
            return fail(root, &job, &error.to_string(), false);
        }
        let prepared =
            match publication::resume(&output_root(root, id), manifest, limits(&job), token, until)
            {
                Ok(prepared) => prepared,
                Err(error) => return fail(root, &job, &error.to_string(), true),
            };
        return match commit(root, &job, prepared) {
            Ok(result) => Ok(result),
            Err(error) => {
                let current = load(root, id)?;
                if let Some(committed) = reconcile_marker(root, &current)? {
                    Ok(committed)
                } else {
                    fail(root, &current, &error.to_string(), false)
                }
            }
        };
    }
    retry(
        root,
        &job,
        "coordinator restart after proved old-session drain",
    )
}
fn commit(root: &Path, expected: &Job, prepared: publication::Prepared) -> Result<Job> {
    source_check(
        expected,
        prepared.token(),
        deadline(expected.expires_ms)?,
        "prepublish",
    )?;
    with_store(root, |store| {
        let current = store.status(expected.id)?;
        same_attempt(&current, expected)?;
        require(!current.state.terminal(), "terminal publish refused")?;
        if current.cancel_requested || now_ms()? >= current.expires_ms {
            return Err(Error::Invalid("cancelled/expired before commit".into()));
        }
        let mut data = store.data.clone();
        let job = data
            .jobs
            .get_mut(&current.id)
            .ok_or_else(|| Error::Invalid("job".into()))?;
        job.state = JobState::Committing;
        job.event(
            EventCode::Committing,
            job.owner,
            now_ms()?,
            data.limits.events_per_job,
        )?;
        store.persist(data)?;
        fault("prepublish");
        let manifest = prepared.commit()?;
        fault("published_before_success");
        let mut data = store.data.clone();
        let job = data
            .jobs
            .get_mut(&current.id)
            .ok_or_else(|| Error::Invalid("job".into()))?;
        job.state = JobState::Succeeded;
        job.output = Some(manifest);
        if let Some(a) = job.attempts.last_mut() {
            a.state = JobState::Succeeded;
            a.ended_ms = Some(now_ms()?);
        }
        job.event(
            EventCode::Succeeded,
            job.owner,
            now_ms()?,
            data.limits.events_per_job,
        )?;
        let result = job.clone();
        store.persist(data)?;
        Ok(result)
    })
}
#[allow(clippy::too_many_lines)] // Keep ordered native effects and their single failure reconciliation together.
fn attempt(root: &Path, store: Uuid, job: &Job) -> Result<Job> {
    let workers = endpoints(store, job)?;
    let token = CancellationToken::default();
    let _monitor = Monitor::start(root, job, workers.clone(), token.clone());
    let result = (|| {
        let until = deadline(job.expires_ms)?;
        let catalog = source_check(job, &token, until, "preflight")?;
        directory(&root.join("jobs"))?;
        directory(&output_root(root, job.id))?;
        directory(&root.join("scratch"))?;
        let scratch = scratch_root(root, job)?;
        directory(&scratch)?;
        managed::claim(&workers, until).map_err(Error::Native)?;

        let running = change(
            root,
            job,
            JobState::Running,
            EventCode::Running,
            None,
            |_| {},
        )?;
        let output_identity = identity(store, &running)?;
        let schema = job.recipe.plan.nodes[job.recipe.plan.output as usize]
            .schema
            .clone();
        let completion = publication::Completion {
            workers: u32::try_from(workers.len()).map_err(|_| Error::Quota("worker count"))?,
            terminals: workers
                .iter()
                .map(|w| {
                    Ok(publication::Terminal {
                        partition: u32::try_from(w.id.0).map_err(|_| Error::Quota("worker ID"))?,
                        attempt: 0,
                        rows: 0,
                        fingerprint: "0".repeat(64),
                    })
                })
                .collect::<Result<Vec<_>>>()?,
        };
        let mut publisher = Publisher::stage(
            &output_root(root, job.id),
            output_identity,
            schema,
            completion,
            limits(job),
            token.clone(),
            until,
        )?;
        let mut options = native_options(&job.recipe);
        options.timeout = until.saturating_duration_since(Instant::now());
        let proof = managed::execute_to_sink_observed(
            physical::Request {
                plan: &job.recipe.plan,
                catalog: &catalog,
                workers: workers.clone(),
                job: JobId(
                    job.engine_job
                        .ok_or_else(|| Error::Invalid("engine id".into()))?,
                ),
                options,
            },
            &scratch,
            &token,
            |batch| {
                publisher
                    .push(&batch)
                    .map_err(|e| ade_distributed::Error::Invalid(e.to_string()))?;
                fault("gather");
                Ok(())
            },
            |_| fault("prepared"),
        )
        .map_err(Error::Native)?;
        token.check().map_err(|e| Error::Invalid(e.to_string()))?;
        let worker_disk = managed::drain(&workers, Instant::now() + Duration::from_secs(4))
            .map_err(Error::Native)?;
        change(
            root,
            job,
            JobState::Running,
            EventCode::Running,
            None,
            |j| {
                if let Some(a) = j.attempts.last_mut() {
                    a.worker_disk_bytes.clone_from(&worker_disk);
                    a.coordinator_disk_bytes = Some(proof.disk_bytes());
                }
            },
        )?;
        publisher.native_completion(&proof, job.recipe.staging_bytes)?;
        let prepared = publisher.prepare()?;
        source_check(job, &token, until, "postflight")?;
        let staged = change(root, job, JobState::Staged, EventCode::Staged, None, |j| {
            j.output = Some(prepared.manifest().clone());
            if let Some(a) = j.attempts.last_mut() {
                a.worker_disk_bytes = worker_disk;
                a.coordinator_disk_bytes = Some(proof.disk_bytes());
            }
        })?;
        fault("staged");
        commit(root, &staged, prepared)
    })();
    // The monitor must be gone before any retry or new claim. It can only target
    // the original fenced instance, never a successor.
    if result.is_ok() {
        return result;
    }
    let error = result
        .err()
        .ok_or_else(|| Error::Invalid("attempt outcome".into()))?;
    let current = load(root, job.id)?;
    if let Some(committed) = reconcile_marker(root, &current)? {
        return Ok(committed);
    }
    if current.state.terminal() {
        return Ok(current);
    }
    if let Err(drain) = managed::drain(&workers, Instant::now() + Duration::from_secs(4)) {
        return fail(root, &current, &drain.to_string(), true);
    }
    if current.cancel_requested {
        return cancelled(root, &current);
    }
    if now_ms()? >= current.expires_ms {
        return fail(root, &current, "global deadline expired", false);
    }
    let transient = matches!(&error, Error::Native(ade_distributed::Error::Io(_)))
        || matches!(&error,Error::Native(ade_distributed::Error::Remote{code,..}) if code=="transient");
    if transient {
        retry(root, &current, &error.to_string())
    } else {
        fail(root, &current, &error.to_string(), false)
    }
}
/// Run or reconcile one job under an exclusive process coordinator fence.
/// # Errors
/// Unknown/unsafe state, competing coordinators and filesystem failures refuse.
pub fn run(root: &Path, id: Uuid) -> Result<Job> {
    crate::recipe::nonsymlink(root)?;
    let root = root.canonicalize()?;
    let _owner = owner(&root)?;
    let mut current = recover_one(&root, id)?;
    loop {
        if current.state.terminal() || current.state == JobState::NeedsOperator {
            return Ok(current);
        }
        if current.cancel_requested {
            return cancelled(&root, &current);
        }
        if now_ms()? >= current.expires_ms {
            return fail(&root, &current, "global deadline expired", false);
        }
        if let Some(at) = current.retry_at_ms
            && now_ms()? < at
        {
            fault("retry_wait");
            std::thread::sleep(Duration::from_millis(20));
            current = load(&root, id)?;
            continue;
        }
        let (store, job) = claim(&root, id)?;
        fault("claimed_native");
        current = attempt(&root, store, &job)?;
    }
}
/// Reconcile all existing native attempts, without starting fresh worker tasks.
/// # Errors
/// A competing coordinator or corrupt/ambiguous output refuses.
pub fn recover(root: &Path) -> Result<Vec<Job>> {
    crate::recipe::nonsymlink(root)?;
    let root = root.canonicalize()?;
    let _owner = owner(&root)?;
    let jobs = with_store(&root, |s| s.list(None, 32))?;
    let mut result = vec![];
    for job in jobs {
        if !job.state.terminal() || job.state == JobState::Succeeded {
            result.push(recover_one(&root, job.id)?);
        }
    }
    Ok(result)
}
/// Persist idempotent cancellation while a coordinator is running. A valid
/// already-published marker wins and is reconciled to success.
/// # Errors
/// Corrupt output or metadata refuses instead of pretending cancellation/success.
pub fn cancel(root: &Path, id: Uuid) -> Result<Job> {
    let job = load(root, id)?;
    if let Some(committed) = reconcile_marker(root, &job)? {
        return Ok(committed);
    }
    with_store(root, |s| s.cancel(id))
}

// Walk only a durable attempt's private coordinator namespace after a terminal
// state has proved native quiescence. Never follow links or scan arbitrary roots.
fn scratch_garbage(
    path: &Path,
    depth: usize,
    entries: &mut Vec<PathBuf>,
    bytes: &mut u64,
) -> Result<()> {
    require(
        depth <= 4 && entries.len() < 512,
        "scratch cleanup metadata cap",
    )?;
    let metadata = fs::symlink_metadata(path)?;
    require(
        !metadata.file_type().is_symlink(),
        "scratch cleanup symlink refused",
    )?;
    if metadata.is_dir() {
        for entry in fs::read_dir(path)? {
            scratch_garbage(&entry?.path(), depth + 1, entries, bytes)?;
        }
    } else {
        require(
            metadata.is_file() && metadata.nlink() == 1,
            "scratch cleanup regular private file",
        )?;
        *bytes = bytes
            .checked_add(metadata.len())
            .ok_or(Error::Quota("scratch cleanup bytes"))?;
    }
    require(entries.len() < 512, "scratch cleanup metadata cap")?;
    entries.push(path.to_path_buf());
    Ok(())
}
#[derive(Debug, serde::Serialize)]
pub struct Garbage {
    pub job: Uuid,
    pub files: Vec<String>,
    pub bytes: u64,
    pub applied: bool,
}
/// Dry-run by default. Removes only recorded, uncommitted attempt files of a
/// terminal job, after native ownership has quiesced. Committed refs always win.
/// # Errors
/// Active/ambiguous jobs, unsafe paths or corrupt committed output refuse cleanup.
pub fn gc(root: &Path, id: Uuid, apply: bool) -> Result<Garbage> {
    crate::recipe::nonsymlink(root)?;
    let root = root.canonicalize()?;
    let _owner = owner(&root)?;
    let job = load(&root, id)?;
    require(job.state.terminal(), "GC requires a terminal quiesced job")?;
    let output = output_root(&root, id);
    let mut protected = std::collections::BTreeSet::new();
    if job.state == JobState::Succeeded || output.join("COMMITTED.json").try_exists()? {
        let committed = verify_output(&root, id)?;
        protected.insert(committed.file);
        protected.insert(format!("manifest-{}.json", committed.identity.instance));
    }
    let mut result = Garbage {
        job: id,
        files: vec![],
        bytes: 0,
        applied: apply,
    };
    for attempt in &job.attempts {
        let scratch = root.join("scratch").join(attempt.instance.to_string());
        if !scratch.try_exists()? {
            continue;
        }
        crate::recipe::nonsymlink(&scratch)?;
        let mut entries = vec![];
        let mut bytes = 0;
        scratch_garbage(&scratch, 0, &mut entries, &mut bytes)?;
        require(
            bytes <= job.recipe.runtime_bytes,
            "scratch cleanup quota mismatch",
        )?;
        result.bytes = result
            .bytes
            .checked_add(bytes)
            .ok_or(Error::Quota("GC bytes"))?;
        for path in entries {
            if fs::symlink_metadata(&path)?.is_dir() {
                if apply {
                    fs::remove_dir(path)?;
                }
            } else {
                result.files.push(
                    path.strip_prefix(&root)
                        .map_err(|_| Error::Invalid("GC namespace".into()))?
                        .to_string_lossy()
                        .into_owned(),
                );
                if apply {
                    fs::remove_file(path)?;
                }
            }
        }
        if apply {
            File::open(root.join("scratch"))?.sync_all()?;
        }
    }
    if !output.try_exists()? {
        return Ok(result);
    }
    crate::recipe::nonsymlink(&output)?;
    for attempt in &job.attempts {
        for name in [
            format!("attempt-{}.arrow", attempt.instance),
            format!("manifest-{}.json", attempt.instance),
        ] {
            if protected.contains(&name) {
                continue;
            }
            let path = output.join(&name);
            match fs::symlink_metadata(&path) {
                Ok(m) => {
                    require(
                        m.is_file() && m.nlink() == 1,
                        "GC refuses unsafe/hard-linked artifact",
                    )?;
                    result.bytes = result
                        .bytes
                        .checked_add(m.len())
                        .ok_or(Error::Quota("GC byte overflow"))?;
                    result.files.push(name);
                    if apply {
                        fs::remove_file(path)?;
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
    }
    if apply {
        File::open(output)?.sync_all()?;
    }
    Ok(result)
}
