//! One active job per worker. Control remains responsive while the task runs.
use crate::{
    Error, Result, WorkerEndpoint,
    exchange::{Receiver, Sender, Store},
    ids::{AttemptId, JobId, TaskId, transfer},
    metrics::TaskMetrics,
    partition,
    protocol::{self, Envelope, JobSpec, Reply, Request},
    require,
};
use ade_exec::{CancellationToken, LocalPlan};
use std::{
    io::Write,
    net::{SocketAddr, TcpListener},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
struct Job {
    spec: JobSpec,
    parent: PathBuf,
    copy: Option<crate::retention::Objects>,
    copy_poison: Arc<AtomicBool>,
    store: Arc<Store>,
    producer_done: AtomicBool,
    producer_started: AtomicBool,
    final_started: AtomicBool,
    recovery_started: AtomicBool,
    #[cfg(feature = "research-hooks")]
    release_final: AtomicBool,
}
struct State {
    endpoint: WorkerEndpoint,
    job: Mutex<Option<Arc<Job>>>,
    // Retained after cancellation so delayed traffic can never target a reused ID.
    last_job_id: Mutex<Option<JobId>>,
    producer_busy: AtomicBool,
    final_busy: AtomicBool,
    poisoned: AtomicBool,
    stop: AtomicBool,
    parent: PathBuf,
    object_backend: Option<crate::ObjectStoreBackend>,
}
struct TaskGuard<'a>(&'a AtomicBool);
impl Drop for TaskGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}
fn job(state: &State, id: JobId) -> Result<Arc<Job>> {
    let current = state.job.lock().expect("job lock");
    let job = current
        .as_ref()
        .ok_or_else(|| Error::Invalid("no prepared job".into()))?;
    require(job.spec.job == id, "job identity mismatch")?;
    job.store.cancellation.check()?;
    Ok(job.clone())
}
#[allow(
    clippy::needless_pass_by_value,
    reason = "Used as an owned map_err adapter across crate errors."
)]
fn local_sink_error(error: Error) -> ade_exec::Error {
    ade_exec::Error::Invalid(format!("distributed sink: {error}"))
}
fn task_metrics(
    start: Instant,
    output: &ade_exec::ExecutionMetrics,
    source_files: usize,
) -> TaskMetrics {
    TaskMetrics {
        source_files,
        input_rows: output
            .operators
            .values()
            .next()
            .map_or(0, |m| m.output_rows),
        output_rows: output.output_rows,
        elapsed_ms: u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX),
        managed_peak: output.managed.peak,
        spill_bytes: output.spill.write_bytes,
        ..Default::default()
    }
}
#[allow(
    clippy::too_many_lines,
    reason = "Producer observations and terminal copy completion share one task lifetime."
)]
fn producer(
    job: &Job,
    files: &[PathBuf],
    task: TaskId,
    attempt: AttemptId,
    target: Option<(usize, SocketAddr)>,
) -> Result<TaskMetrics> {
    let start = Instant::now();
    let token = job.store.cancellation.clone();
    // Do not hold idle receiver connections while a blocking aggregate builds.
    let open = |p: usize| {
        let sender = Sender::new(
            target.map_or(job.spec.workers[p].exchange, |(_, address)| address),
            transfer(job.spec.job, 0, p as u64, task.0).with_attempt(attempt),
            token.clone(),
        )?;
        if target.is_none() && job.copy.is_some() {
            sender.stage(&job.parent, job.copy_poison.clone())
        } else {
            Ok(sender)
        }
    };
    let mut senders: Vec<Option<Sender>> = (0..job.spec.workers.len()).map(|_| None).collect();
    let mut metrics = if files.is_empty() {
        TaskMetrics::default()
    } else {
        let plan = LocalPlan::HashAggregate {
            input: Box::new(LocalPlan::Project {
                input: Box::new(LocalPlan::ParquetScan {
                    files: files.to_vec(),
                    batch_size: job.spec.config.batch_size,
                }),
                columns: vec!["key".into(), "value_i64".into()],
            }),
            key: "key".into(),
            value: "value_i64".into(),
        };
        let count = senders.len();
        let output =
            ade_exec::execute_to_sink(plan, job.spec.config.execution(), token.clone(), |batch| {
                for (p, sender) in senders.iter_mut().enumerate() {
                    if target.is_some_and(|(partition, _)| partition != p) {
                        continue;
                    }
                    let selected = partition::select(&batch, p, count).map_err(local_sink_error)?;
                    if selected.num_rows() > 0 {
                        if sender.is_none() {
                            *sender = Some(open(p).map_err(local_sink_error)?);
                        }
                        sender
                            .as_mut()
                            .expect("opened")
                            .batch(&selected)
                            .map_err(local_sink_error)?;
                    }
                }
                Ok(())
            })?;
        let mut metrics = task_metrics(start, &output, files.len());
        metrics.production.operator_work_ns =
            output.operators.values().try_fold(0u64, |sum, m| {
                let ns = u64::try_from(m.work_time.as_nanos())
                    .map_err(|_| Error::Invalid("work observation overflow".into()))?;
                sum.checked_add(ns)
                    .ok_or_else(|| Error::Invalid("work observation overflow".into()))
            })?;
        metrics
    };
    metrics.production.wall_elapsed_ns = crate::materialization::ns(start);
    metrics.production.source_requests = files.len() as u64;
    metrics.production.source_bytes = files.iter().try_fold(0u64, |sum, path| {
        sum.checked_add(std::fs::metadata(path)?.len())
            .ok_or_else(|| Error::Invalid("source bytes overflow".into()))
    })?;
    metrics.production.consumer_count = 1;
    let mut staged = Vec::with_capacity(senders.len());
    for (p, sender) in senders.into_iter().enumerate() {
        if target.is_some_and(|(partition, _)| partition != p) {
            continue;
        }
        let sender = match sender {
            Some(sender) => sender,
            None => open(p)?,
        };
        let id = transfer(job.spec.job, 0, p as u64, task.0).with_attempt(attempt);
        let (sent, manifest, staging) = sender.finish_staged()?;
        if let Some(staging) = staging {
            staged.push((p, id, manifest.clone(), staging, sent.clone()));
        }
        metrics.manifests.insert(p as u64, manifest);
        metrics.sent.add(&sent);
        metrics.destinations.insert(p as u64, sent);
    }
    // Close every delivery stream before object I/O; a slow upload must not
    // leave other receivers waiting for their terminal past socket inactivity.
    for (p, id, manifest, staging, sent) in staged {
        let mut observation = metrics.production.clone();
        observation.output_bytes = sent.payload_bytes;
        observation.output_segments = sent.batches;
        let decision = job
            .spec
            .config
            .exchange
            .cost
            .as_ref()
            .expect("copy policy")
            .decide(observation)?;
        let fact = staging.complete(
            id,
            &manifest,
            decision,
            job.copy.as_ref().expect("copy backend"),
            &job.spec.config.exchange.object_store,
            &token,
        )?;
        metrics.materializations.insert(p as u64, fact);
    }
    token.check()?;
    metrics.production.output_bytes = metrics.sent.payload_bytes;
    metrics.production.output_segments = metrics.sent.batches;
    metrics.elapsed_ms = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);

    Ok(metrics)
}
fn final_task(
    job: &Job,
    gather: SocketAddr,
    task: TaskId,
    attempt: AttemptId,
) -> Result<TaskMetrics> {
    require(
        !job.final_started.swap(true, Ordering::AcqRel),
        "final task already started",
    )?;
    let start = Instant::now();
    let token = job.store.cancellation.clone();
    let open = || {
        Sender::new(
            gather,
            transfer(job.spec.job, 1, 0, task.0).with_attempt(attempt),
            token.clone(),
        )
    };
    let mut sender: Option<Sender> = None;
    let output = ade_exec::execute_merge_source_to_sink(
        job.store.reader(),
        job.spec.config.execution(),
        token.clone(),
        |batch| {
            if sender.is_none() {
                sender = Some(open().map_err(local_sink_error)?);
            }
            sender
                .as_mut()
                .expect("opened")
                .batch(&batch)
                .map_err(local_sink_error)?;
            #[cfg(feature = "research-hooks")]
            if attempt == AttemptId(0)
                && job.spec.config.research_exit_final_partition
                    == Some(task.0 - job.spec.workers.len() as u64)
                && sender
                    .as_ref()
                    .is_some_and(|sender| sender.metrics.batches == 1)
            {
                research_exit_after_first_gather_batch(job, task, &token)
                    .map_err(local_sink_error)?;
            }
            Ok(())
        },
    )?;
    job.store.validate_retention_drained()?;
    let mut metrics = task_metrics(start, &output, 0);
    let sender = match sender {
        Some(sender) => sender,
        None => open()?,
    };
    metrics.sent = sender.finish()?;
    metrics.received = job.store.metrics()?;
    metrics.retention = job.store.retention_metrics()?;
    metrics.elapsed_ms = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);
    Ok(metrics)
}
#[cfg(feature = "research-hooks")]
fn research_exit_after_first_gather_batch(
    job: &Job,
    task: TaskId,
    token: &CancellationToken,
) -> Result<()> {
    // The first batch was sent. The coordinator releases this gate only after
    // every original producer result has arrived, then verifies gather received it.
    let deadline = Instant::now() + crate::TASK_TIMEOUT;
    while !job.release_final.load(Ordering::Acquire) {
        token.check()?;
        require(Instant::now() < deadline, "failure boundary gate timed out")?;
        thread::sleep(Duration::from_millis(2));
    }
    eprintln!(
        "ADE research worker process exit job={:?} final={task:?} attempt=0 gather_batches_before_exit=1",
        job.spec.job
    );
    std::process::exit(86);
}
// A single RPC owns the whole bounded episode. Cancellation uses the original job token.
fn recover(
    state: &State,
    plan: &crate::recovery::RecoveryPlan,
    gather: SocketAddr,
) -> Result<Reply> {
    let original = job(state, plan.job)?;
    require(
        !state.poisoned.load(Ordering::Acquire) && !original.store.is_poisoned(),
        "recovery host poisoned",
    )?;
    require(
        !state.producer_busy.load(Ordering::Acquire)
            && !state.final_busy.load(Ordering::Acquire)
            && original.producer_done.load(Ordering::Acquire)
            && original.final_started.load(Ordering::Acquire),
        "recovery host not quiescent",
    )?;
    require(
        !original.recovery_started.swap(true, Ordering::AcqRel),
        "recovery already attempted",
    )?;
    let result = recover_inner(state, &original, plan, gather);
    if original.copy_poison.load(Ordering::Acquire) {
        state.poisoned.store(true, Ordering::Release);
    }
    if result.is_err() {
        original.store.cancellation.cancel();
    }
    result
}
#[allow(
    clippy::too_many_lines,
    reason = "One recovery episode validates every transfer before final aggregation."
)]
fn recover_inner(
    state: &State,
    original: &Job,
    plan: &crate::recovery::RecoveryPlan,
    gather: SocketAddr,
) -> Result<Reply> {
    let snapshot = original
        .spec
        .snapshot
        .as_ref()
        .ok_or_else(|| Error::Unrecoverable("missing source snapshot".into()))?;
    let count = original.spec.workers.len();
    require(
        snapshot.replayable
            && plan.host == state.endpoint.id
            && plan.partition.0 < count as u64
            && plan.partition.0 != state.endpoint.id.0
            && plan.closures.len() == count
            && plan.ranges.len() == count,
        "invalid recovery plan/lineage",
    )?;
    let expected = (0..count)
        .map(|p| transfer(plan.job, 0, plan.partition.0, p as u64).with_attempt(AttemptId(1)))
        .collect();
    let store = Arc::new(Store::new(
        &state.parent,
        expected,
        original.store.cancellation.clone(),
    )?);
    let receiving = store.clone();
    let receiver = Receiver::start(TcpListener::bind("127.0.0.1:0")?, move |_| {
        Ok(receiving.clone())
    })?;
    let replay = Job {
        spec: original.spec.clone(),
        parent: state.parent.clone(),
        copy: None,
        copy_poison: original.copy_poison.clone(),
        store,
        producer_done: AtomicBool::new(false),
        producer_started: AtomicBool::new(false),
        final_started: AtomicBool::new(false),
        recovery_started: AtomicBool::new(true),
        #[cfg(feature = "research-hooks")]
        release_final: AtomicBool::new(false),
    };
    let mut manifests = Vec::with_capacity(count);
    let mut work = crate::recovery::RecoveryMetrics::default();
    for (p, closure) in plan.closures.iter().enumerate() {
        require(
            closure.producer == TaskId(p as u64)
                && closure.original.logical
                    == transfer(plan.job, 0, plan.partition.0, p as u64).logical(),
            "recovery closure/source mismatch",
        )?;
        plan.ranges[p].validate(&closure.original)?;
        require(
            plan.ranges[p].start == 0 && plan.ranges[p].end_exclusive == closure.original.segments,
            "recovery requires full transfer range",
        )?;
        let manifest = if let Some(receipt) = &closure.restore {
            let objects = original
                .copy
                .as_ref()
                .ok_or_else(|| Error::Invalid("no restore backend".into()))?;
            let (manifest, elapsed) = crate::materialization::restore(
                receipt,
                &closure.original,
                &original.spec.config.exchange.object_store,
                objects,
                receiver.address,
                &original.store.cancellation,
            )?;
            work.object_read_requests += receipt.original.segments * 2;
            work.object_read_elapsed_ns += elapsed;
            manifest
        } else if closure.original.segments == 0 {
            // Preserve exact empty-transfer coverage without reopening source files.
            finish_empty_replay(
                receiver.address,
                transfer(plan.job, 0, plan.partition.0, p as u64).with_attempt(AttemptId(1)),
                original.store.cancellation.clone(),
            )?
            .1
        } else {
            let morsels = snapshot.producer_morsels(closure.producer, count);
            crate::lineage::SourceSnapshot::verify(&morsels)?;
            let files = morsels.iter().map(|m| m.path.clone()).collect::<Vec<_>>();
            let metrics = producer(
                &replay,
                &files,
                closure.producer,
                AttemptId(1),
                Some((
                    usize::try_from(plan.partition.0).expect("validated partition"),
                    receiver.address,
                )),
            )?;
            work.recomputed_operator_work_ns += metrics.production.operator_work_ns;
            work.recomputed_source_bytes += metrics.production.source_bytes;
            work.recomputed_source_requests += metrics.production.source_requests;
            metrics.manifests[&plan.partition.0].clone()
        };
        closure.original.validate_replay(&manifest)?;
        manifests.push(manifest);
    }
    let final_metrics = final_task(
        &replay,
        gather,
        TaskId(count as u64 + plan.partition.0),
        AttemptId(1),
    )?;
    drop(receiver);
    Ok(Reply::Recovered {
        final_metrics,
        manifests,
        work,
    })
}

fn finish_empty_replay(
    gather: SocketAddr,
    id: crate::ids::TransferId,
    token: CancellationToken,
) -> Result<(
    crate::metrics::NetworkMetrics,
    crate::lineage::TransferManifest,
)> {
    Sender::new(gather, id, token)?.finish_manifest()
}
#[allow(
    clippy::too_many_lines,
    reason = "Bounded control state transitions stay together."
)]
fn handle(state: &State, request: Request) -> Result<Reply> {
    match request {
        Request::Ping => Ok(Reply::Pong(state.endpoint.clone())),
        Request::PrepareJob(spec) => {
            spec.config.validate()?;
            crate::coordinator::validate_topology(&spec.workers)?;
            #[cfg(feature = "research-hooks")]
            require(
                spec.config
                    .exchange
                    .pressure
                    .target_partition
                    .is_none_or(|p| p < spec.workers.len() as u64),
                "research pressure target outside topology",
            )?;
            if let Some(snapshot) = &spec.snapshot {
                snapshot.validate(
                    &snapshot
                        .morsels
                        .iter()
                        .map(|m| m.path.clone())
                        .collect::<Vec<_>>(),
                )?;
            }
            #[cfg(feature = "research-hooks")]
            require(
                spec.config
                    .research_exit_final_partition
                    .is_none_or(|p| p < spec.workers.len() as u64),
                "failure target outside topology",
            )?;
            let own = spec
                .workers
                .iter()
                .find(|w| w.id == state.endpoint.id)
                .ok_or_else(|| Error::Invalid("worker absent from topology".into()))?;
            require(
                own.control == state.endpoint.control && own.exchange == state.endpoint.exchange,
                "worker endpoint mismatch",
            )?;
            let mut current = state.job.lock().expect("job lock");
            require(
                !state.poisoned.load(Ordering::Acquire),
                "worker is poisoned after an uncontained retention I/O operation",
            )?;
            require(
                current.is_none()
                    && !state.producer_busy.load(Ordering::Acquire)
                    && !state.final_busy.load(Ordering::Acquire),
                "worker already has active job",
            )?;
            let mut last_job_id = state.last_job_id.lock().expect("job ID watermark lock");
            require(
                last_job_id.is_none_or(|last| spec.job > last),
                "job ID must increase on a reused worker",
            )?;
            let expected = (0..spec.workers.len())
                .map(|p| transfer(spec.job, 0, state.endpoint.id.0, p as u64))
                .collect();
            let store = Arc::new(Store::configured_with_backend(
                &state.parent,
                expected,
                CancellationToken::default(),
                spec.config.exchange.clone(),
                state.object_backend.as_ref(),
            )?);
            let accepted_job_id = spec.job;
            let copy_poison = Arc::new(AtomicBool::new(false));
            let copy = if spec.config.exchange.cost.is_some() {
                require(
                    spec.snapshot.as_ref().is_some_and(|s| s.replayable),
                    "expected cost requires replayable snapshot",
                )?;
                require(
                    spec.config.exchange.object_store.durability
                        == crate::ObjectStoreDurability::WorkerProcessSurviving,
                    "expected cost requires worker-process-surviving backend",
                )?;
                Some(
                    crate::retention::Objects::configured(
                        &state.parent,
                        spec.config.exchange.object_store.clone(),
                        state.object_backend.as_ref(),
                        copy_poison.clone(),
                    )?
                    .complete(),
                )
            } else {
                None
            };
            *current = Some(Arc::new(Job {
                spec,
                parent: state.parent.clone(),
                copy,
                copy_poison,
                store,
                producer_done: AtomicBool::new(false),
                producer_started: AtomicBool::new(false),
                final_started: AtomicBool::new(false),
                recovery_started: AtomicBool::new(false),
                #[cfg(feature = "research-hooks")]
                release_final: AtomicBool::new(false),
            }));
            *last_job_id = Some(accepted_job_id);
            Ok(Reply::Ready)
        }
        #[cfg(feature = "research-hooks")]
        Request::ReleaseFinal { job: id } => {
            let _ = job(state, id)?;
            Ok(Reply::Ready)
        }
        Request::RecoverFinal { plan, gather } => recover(state, &plan, gather),
        Request::CleanupMaterializations { job: id } => {
            require(
                *state.last_job_id.lock().expect("job watermark") == Some(id),
                "cleanup job mismatch",
            )?;
            if let Some(backend) = &state.object_backend {
                crate::retention::Objects::configured(
                    &state.parent,
                    backend.config.clone(),
                    Some(backend),
                    Arc::default(),
                )?
                .complete()
                .cleanup_job(id)?;
            }
            Ok(Reply::Ready)
        }
        Request::CancelJob { job: id } => {
            let mut current = state.job.lock().expect("job lock");
            if current.as_ref().is_some_and(|j| j.spec.job == id) {
                let old = current.take().expect("job");
                if old.store.is_poisoned() || old.copy_poison.load(Ordering::Acquire) {
                    state.poisoned.store(true, Ordering::Release);
                }
                old.store.cancellation.cancel();
            }
            Ok(Reply::Ready)
        }
        Request::Shutdown => {
            state.stop.store(true, Ordering::Release);
            if let Some(job) = state.job.lock().expect("job lock").take() {
                job.store.cancellation.cancel();
            }
            Ok(Reply::Ready)
        }
        task => {
            let (id, task_id, attempt, is_final) = match &task {
                Request::RunProducerTask {
                    job, task, attempt, ..
                } => (*job, *task, *attempt, false),
                Request::RunFinalTask {
                    job, task, attempt, ..
                } => (*job, *task, *attempt, true),
                _ => unreachable!(),
            };
            let busy = if is_final {
                &state.final_busy
            } else {
                &state.producer_busy
            };
            require(
                busy.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok(),
                "worker task role already running",
            )?;
            let _guard = TaskGuard(busy);
            let job = job(state, id)?;
            let expected = state.endpoint.id.0
                + if is_final {
                    job.spec.workers.len() as u64
                } else {
                    0
                };
            require(
                task_id == TaskId(expected) && attempt == AttemptId(0),
                "task/attempt identity mismatch",
            )?;
            let result = match task {
                Request::RunProducerTask { files, .. } => {
                    require(
                        !job.producer_started.swap(true, Ordering::AcqRel),
                        "producer task already started",
                    )?;
                    if let Some(snapshot) = &job.spec.snapshot {
                        let morsels = snapshot.producer_morsels(task_id, job.spec.workers.len());
                        require(
                            morsels.iter().map(|m| &m.path).eq(files.iter()),
                            "producer source assignment mismatch",
                        )?;
                        crate::lineage::SourceSnapshot::verify(&morsels)?;
                    }
                    let result = producer(&job, &files, task_id, attempt, None);
                    if result.is_ok() {
                        job.producer_done.store(true, Ordering::Release);
                    }
                    result
                }
                Request::RunFinalTask { gather, .. } => final_task(&job, gather, task_id, attempt),
                _ => unreachable!(),
            };
            if result.is_err() {
                job.store.cancellation.cancel();
            }
            if job.store.is_poisoned() || job.copy_poison.load(Ordering::Acquire) {
                state.poisoned.store(true, Ordering::Release);
            }
            result.map(Reply::Task)
        }
    }
}
/// Serve with the deterministic ephemeral local object-store fixture until Shutdown.
/// The caller owns the parent directory and may remove it after killing/waiting a child.
/// # Errors
/// Returns binding, readiness, listener or socket setup errors.
pub fn serve(
    id: crate::ids::WorkerId,
    control: SocketAddr,
    exchange: SocketAddr,
    parent: &Path,
    ready: impl Write,
) -> Result<()> {
    serve_inner(id, control, exchange, parent, None, ready)
}

/// Serve with a caller-installed object-store backend for static or adaptive retention.
/// The backend name and durability capability must match each job's exchange config.
/// # Errors
/// Returns binding, readiness, listener or socket setup errors.
pub fn serve_with_object_store(
    id: crate::ids::WorkerId,
    control: SocketAddr,
    exchange: SocketAddr,
    parent: &Path,
    backend: crate::ObjectStoreBackend,
    ready: impl Write,
) -> Result<()> {
    serve_inner(id, control, exchange, parent, Some(backend), ready)
}

fn handle_socket(state: &State, socket: &mut std::net::TcpStream) {
    if socket.set_read_timeout(Some(crate::IO_TIMEOUT)).is_err()
        || socket.set_write_timeout(Some(crate::IO_TIMEOUT)).is_err()
    {
        return;
    }
    if let Ok(envelope) = protocol::read_message::<Envelope<Request>>(
        &mut protocol::DeadlineStream::new(socket, crate::IO_TIMEOUT),
    ) {
        #[cfg(feature = "research-hooks")]
        let release_job = match &envelope.body {
            Request::ReleaseFinal { job } => Some(*job),
            _ => None,
        };
        let body = handle(state, envelope.body).unwrap_or_else(|error| Reply::Error {
            code: match &error {
                Error::Exec(_) => "execution",
                Error::Io(_) => "io",
                _ => "protocol",
            }
            .into(),
            message: error.to_string(),
        });
        let written = protocol::write_message(
            &mut protocol::DeadlineStream::new(socket, crate::IO_TIMEOUT),
            &Envelope {
                request_id: envelope.request_id,
                body,
            },
        );
        // Publish only after the complete reply is on the socket;
        // process exit must not truncate the gate-release response.
        #[cfg(feature = "research-hooks")]
        if written.is_ok()
            && let Some(id) = release_job
            && let Ok(job) = job(state, id)
        {
            job.release_final.store(true, Ordering::Release);
        }
        #[cfg(not(feature = "research-hooks"))]
        let _ = written;
    }
}

fn serve_inner(
    id: crate::ids::WorkerId,
    control: SocketAddr,
    exchange: SocketAddr,
    parent: &Path,
    object_backend: Option<crate::ObjectStoreBackend>,
    mut ready: impl Write,
) -> Result<()> {
    let listener = TcpListener::bind(control)?;
    listener.set_nonblocking(true)?;
    let exchange_listener = TcpListener::bind(exchange)?;
    let endpoint = WorkerEndpoint {
        id,
        control: listener.local_addr()?,
        exchange: exchange_listener.local_addr()?,
    };
    let state = Arc::new(State {
        endpoint: endpoint.clone(),
        job: Mutex::new(None),
        last_job_id: Mutex::new(None),
        producer_busy: AtomicBool::new(false),
        final_busy: AtomicBool::new(false),
        poisoned: AtomicBool::new(false),
        stop: AtomicBool::new(false),
        parent: parent.into(),
        object_backend,
    });
    let receiving = state.clone();
    let receiver = Receiver::start(exchange_listener, move |header| {
        Ok(job(&receiving, header.id.job)?.store.clone())
    })?;
    protocol::write_message(&mut ready, &endpoint)?;
    ready.flush()?;
    let mut handlers: Vec<JoinHandle<()>> = Vec::new();
    let mut listener_error = None;
    while !state.stop.load(Ordering::Acquire) {
        let mut i = 0;
        while i < handlers.len() {
            if handlers[i].is_finished() {
                let _ = handlers.swap_remove(i).join();
            } else {
                i += 1;
            }
        }
        // One producer and one final task may overlap, plus bounded control operations.
        if handlers.len() < 8 {
            match listener.accept() {
                Ok((mut socket, _)) => {
                    // macOS inherits O_NONBLOCK from the accepting listener.
                    if socket.set_nonblocking(false).is_err() {
                        continue;
                    }
                    let state = state.clone();
                    handlers.push(thread::spawn(move || handle_socket(&state, &mut socket)));
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(e) => {
                    listener_error = Some(e);
                    break;
                }
            }
        }
        thread::sleep(Duration::from_millis(2));
    }
    if let Some(job) = state
        .job
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take()
    {
        job.store.cancellation.cancel();
    }
    for handler in handlers {
        let _ = handler.join();
    }
    drop(receiver);
    if let Some(error) = listener_error {
        return Err(error.into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{exchange::Header, ids::transfer};

    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "One worker fixture verifies watermark, stale attempts and poisoned recovery rejection together."
    )]
    fn cancelled_job_id_cannot_be_reused_and_stale_exchange_is_rejected() {
        let parent = tempfile::tempdir().unwrap();
        let control_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let control_address = control_listener.local_addr().unwrap();
        let exchange_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let exchange_address = exchange_listener.local_addr().unwrap();
        let endpoint = WorkerEndpoint {
            id: crate::ids::WorkerId(0),
            control: control_address,
            exchange: exchange_address,
        };
        let state = Arc::new(State {
            endpoint: endpoint.clone(),
            job: Mutex::new(None),
            last_job_id: Mutex::new(None),
            producer_busy: AtomicBool::new(false),
            final_busy: AtomicBool::new(false),
            poisoned: AtomicBool::new(false),
            stop: AtomicBool::new(false),
            parent: parent.path().into(),
            object_backend: None,
        });
        let receiving = state.clone();
        let receiver = Receiver::start(exchange_listener, move |header: &Header| {
            Ok(job(&receiving, header.id.job)?.store.clone())
        })
        .unwrap();
        let peer_control = TcpListener::bind("127.0.0.1:0").unwrap();
        let peer_exchange = TcpListener::bind("127.0.0.1:0").unwrap();
        let workers = vec![
            endpoint,
            WorkerEndpoint {
                id: crate::ids::WorkerId(1),
                control: peer_control.local_addr().unwrap(),
                exchange: peer_exchange.local_addr().unwrap(),
            },
        ];
        let spec = |id| JobSpec {
            job: id,
            workers: workers.clone(),
            config: crate::TaskConfig::default(),
            snapshot: None,
        };

        assert!(matches!(
            handle(&state, Request::PrepareJob(spec(JobId(42)))).unwrap(),
            Reply::Ready
        ));
        handle(&state, Request::CancelJob { job: JobId(42) }).unwrap();
        assert!(handle(&state, Request::PrepareJob(spec(JobId(42)))).is_err());
        assert!(matches!(
            handle(&state, Request::PrepareJob(spec(JobId(43)))).unwrap(),
            Reply::Ready
        ));

        let stale_sender = Sender::new(
            receiver.address,
            transfer(JobId(42), 0, 0, 0),
            ade_exec::CancellationToken::default(),
        )
        .unwrap();
        assert!(stale_sender.finish().is_err());
        let current = job(&state, JobId(43)).unwrap();
        assert_eq!(current.store.metrics().unwrap().transfers, 0);
        assert!(current.store.paths().is_err());

        let current_transfer = Sender::new(
            receiver.address,
            transfer(JobId(43), 0, 0, 0),
            ade_exec::CancellationToken::default(),
        )
        .unwrap();
        let metrics = current_transfer.finish().unwrap();
        assert_eq!(metrics.transfers, 1);
        assert_eq!(metrics.batches, 0);
        assert_eq!(current.store.metrics().unwrap().transfers, 1);
        assert!(
            handle(
                &state,
                Request::RunProducerTask {
                    job: JobId(43),
                    task: TaskId(0),
                    attempt: AttemptId(1),
                    files: Vec::new()
                }
            )
            .is_err()
        );
        current.store.poison();
        let plan = crate::recovery::RecoveryPlan {
            job: JobId(43),
            host: state.endpoint.id,
            partition: crate::ids::PartitionId(1),
            ranges: Vec::new(),
            closures: Vec::new(),
            reused_partitions: vec![0],
        };
        // Poisoning also cancels the session; either guard must refuse recovery.
        assert!(recover(&state, &plan, control_address).is_err());
        handle(&state, Request::CancelJob { job: JobId(43) }).unwrap();
        assert!(state.poisoned.load(Ordering::Acquire));
        assert!(handle(&state, Request::PrepareJob(spec(JobId(44)))).is_err());
    }

    #[test]
    fn empty_replay_emits_matching_attempt_terminal_without_source_access() {
        use crate::ids::{AttemptId, JobId, transfer};
        use std::net::TcpListener;

        let root = tempfile::tempdir().unwrap();
        let logical = transfer(JobId(90), 0, 1, 0);
        let original_store = Arc::new(
            Store::new(root.path(), [logical].into(), CancellationToken::default()).unwrap(),
        );
        let receiving = original_store.clone();
        let receiver = Receiver::start(TcpListener::bind("127.0.0.1:0").unwrap(), move |_| {
            Ok(receiving.clone())
        })
        .unwrap();
        let original = finish_empty_replay(receiver.address, logical, CancellationToken::default())
            .unwrap()
            .1;
        assert_eq!(original.segments, 0);
        drop(receiver);

        let replay_store = Arc::new(
            Store::new(
                root.path(),
                [logical.with_attempt(AttemptId(1))].into(),
                CancellationToken::default(),
            )
            .unwrap(),
        );
        let receiving = replay_store.clone();
        let receiver = Receiver::start(TcpListener::bind("127.0.0.1:0").unwrap(), move |_| {
            Ok(receiving.clone())
        })
        .unwrap();
        let replay = finish_empty_replay(
            receiver.address,
            logical.with_attempt(AttemptId(1)),
            CancellationToken::default(),
        )
        .unwrap()
        .1;
        original.validate_replay(&replay).unwrap();
        assert_eq!(replay_store.metrics().unwrap().batches, 0);
    }
}
