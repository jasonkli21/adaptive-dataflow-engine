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
    prepared: Mutex<Option<(ade_exec::PreparedAggregate, u64)>>,
    layout: Mutex<Option<crate::topology::Layout>>,
    probe_started: AtomicBool,
    probe_files: Mutex<Option<Vec<PathBuf>>>,
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
fn committed_layout(job: &Job) -> Result<Option<crate::topology::Layout>> {
    let layout = job.layout.lock().expect("layout lock").clone();
    require(
        job.spec.config.topology.policy != crate::topology::PartitionPolicy::SampledV1
            || layout.is_some(),
        "exchange/task before layout commitment",
    )?;
    Ok(layout)
}
#[derive(Debug)]
struct TimedSource {
    inner: Box<dyn ade_exec::BatchSource>,
    pulls: Arc<std::sync::atomic::AtomicU64>,
}
impl ade_exec::BatchSource for TimedSource {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.inner.schema()
    }
    fn next_batch(
        &mut self,
        token: &CancellationToken,
    ) -> ade_exec::Result<Option<arrow::record_batch::RecordBatch>> {
        let start = Instant::now();
        let result = self.inner.next_batch(token);
        self.pulls
            .fetch_add(crate::materialization::ns(start), Ordering::Relaxed);
        result
    }
    fn shutdown(&mut self) {
        self.inner.shutdown();
    }
}
struct State {
    endpoint: WorkerEndpoint,
    job: Mutex<Option<Arc<Job>>>,
    join: Mutex<Option<Arc<crate::join::Session>>>,
    // Retained after cancellation so delayed traffic can never target a reused ID.
    last_job_id: Mutex<Option<JobId>>,
    last_execution_namespace: Mutex<Option<String>>,
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
fn join_session(state: &State, id: JobId) -> Result<Arc<crate::join::Session>> {
    let current = state.join.lock().expect("join lock");
    let session = current
        .as_ref()
        .ok_or_else(|| Error::Invalid("no join job".into()))?;
    require(session.id() == id, "join identity mismatch")?;
    session.token.check()?;
    Ok(session.clone())
}
fn cancel_join(state: &State, id: Option<JobId>) {
    let mut current = state.join.lock().expect("join lock");
    if current
        .as_ref()
        .is_some_and(|s| id.is_none_or(|id| id == s.id()))
    {
        let old = current.take().expect("join job");
        if old.poisoned() {
            state.poisoned.store(true, Ordering::Release);
        }
        old.token.cancel();
    }
}
fn run_join(state: &State, id: JobId, gather: Option<SocketAddr>) -> Result<Reply> {
    let busy = if gather.is_some() {
        &state.final_busy
    } else {
        &state.producer_busy
    };
    require(!busy.swap(true, Ordering::AcqRel), "join task role busy")?;
    let _guard = TaskGuard(busy);
    let session = join_session(state, id)?;
    let result = if let Some(gather) = gather {
        session.finish(gather)
    } else {
        session.produce()
    };
    if result.is_err() {
        session.token.cancel();
    }
    if session.poisoned() {
        state.poisoned.store(true, Ordering::Release);
    }
    result.map(Reply::JoinTask)
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
        partial: output.partial.clone(),
        source_files,
        operator_work_ns: output
            .operators
            .values()
            .map(|m| u64::try_from(m.work_time.as_nanos()).unwrap_or(u64::MAX))
            .sum(),
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
fn producer_plan(job: &Job, files: &[PathBuf]) -> LocalPlan {
    let input = Box::new(LocalPlan::Project {
        input: Box::new(LocalPlan::ParquetScan {
            files: files.to_vec(),
            batch_size: job.spec.config.batch_size,
        }),
        columns: vec!["key".into(), "value_i64".into()],
    });
    if job.spec.config.partial.mode == ade_exec::partial::Mode::FixedHash {
        LocalPlan::HashAggregate {
            input,
            key: "key".into(),
            value: "value_i64".into(),
        }
    } else {
        LocalPlan::PartialAggregate {
            input,
            key: "key".into(),
            value: "value_i64".into(),
            config: job.spec.config.partial.clone(),
        }
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
    if job.spec.schedule.is_some() {
        require(
            target.is_none() && attempt == AttemptId(0) && files.is_empty(),
            "leased producer cannot replay or accept assigned files",
        )?;
        return leased_producer(job, task);
    }
    let start = Instant::now();
    let token = job.store.cancellation.clone();
    let layout = committed_layout(job)?;
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
    let mut packer = (job.spec.config.topology.policy != crate::topology::PartitionPolicy::Static)
        .then(|| partition::FramePacker::new(job.spec.workers.len()));
    let mut routing_ns = 0u64;
    let mut sending_ns = 0u64;
    let prepared = if attempt == AttemptId(0) {
        job.prepared.lock().expect("prepared task").take()
    } else {
        None
    };
    let prefix_rows = prepared.as_ref().map_or(0, |(p, _)| p.prefix_rows());
    let preparation_ns = prepared.as_ref().map_or(0, |(_, ns)| *ns);
    let mut metrics = if files.is_empty() {
        TaskMetrics::default()
    } else {
        let plan = producer_plan(job, files);
        let count = senders.len();
        let sink = |batch| {
            let route_start = Instant::now();
            let sent_before = sending_ns;
            let mut emit = |p: usize, selected: arrow::record_batch::RecordBatch| {
                let send_start = Instant::now();
                let sender = &mut senders[p];
                if sender.is_none() {
                    *sender = Some(open(p)?);
                }
                sender.as_mut().expect("opened").batch(&selected)?;
                sending_ns += crate::materialization::ns(send_start);
                Ok(())
            };
            if let Some(packer) = &mut packer {
                packer
                    .route(&batch, layout.as_ref(), target.map(|(p, _)| p), &mut emit)
                    .map_err(local_sink_error)?;
            } else {
                for p in 0..count {
                    if target.is_some_and(|(partition, _)| partition != p) {
                        continue;
                    }
                    let selected = partition::select(&batch, p, count).map_err(local_sink_error)?;
                    if selected.num_rows() > 0 {
                        emit(p, selected).map_err(local_sink_error)?;
                    }
                }
            }
            routing_ns +=
                crate::materialization::ns(route_start).saturating_sub(sending_ns - sent_before);
            Ok(())
        };
        let output = match prepared {
            Some((prepared, _)) => prepared.execute(sink)?,
            None => {
                ade_exec::execute_to_sink(plan, job.spec.config.execution(), token.clone(), sink)?
            }
        };
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
    if let Some(packer) = &mut packer {
        for (p, sender) in senders.iter_mut().enumerate() {
            if let Some(batch) = packer.finish(p)? {
                if sender.is_none() {
                    *sender = Some(open(p)?);
                }
                sender.as_mut().expect("opened").batch(&batch)?;
            }
        }
        metrics.routing_buffer_peak_rows = packer.peak_rows;
        metrics.routing_buffer_capacity_rows =
            job.spec.workers.len() * crate::exchange::MAX_BATCH_ROWS;
    }
    metrics.prefix_rows = prefix_rows;
    metrics.preparation_ns = preparation_ns;
    metrics.routing_ns = routing_ns;
    metrics.sending_ns = sending_ns;
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
        let mut fact = staging.complete(
            id,
            &manifest,
            decision,
            job.copy.as_ref().expect("copy backend"),
            &job.spec.config.exchange.object_store,
            &token,
        )?;
        if let Some(receipt) = &mut fact.receipt {
            receipt.topology_digest = layout.as_ref().map(|l| l.digest.clone());
        }
        fact.topology_digest = layout.as_ref().map(|l| l.digest.clone());
        metrics.materializations.insert(p as u64, fact);
    }
    token.check()?;
    metrics.production.output_bytes = metrics.sent.payload_bytes;
    metrics.production.output_segments = metrics.sent.batches;
    metrics.elapsed_ms = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);

    metrics.layout_digest = layout.as_ref().map(|l| l.digest.clone());
    Ok(metrics)
}
// A lease is not an exchange attempt. Append every file's contributions to
// this ONE sender set, and emit exactly one terminal per destination at the end.
#[allow(
    clippy::too_many_lines,
    reason = "One persistent producer session owns lease completion and terminal closure."
)]
fn leased_producer(job: &Job, task: TaskId) -> Result<TaskMetrics> {
    let start = Instant::now();
    let token = job.store.cancellation.clone();
    let schedule = job.spec.schedule.as_ref().expect("leased spec");
    let client = crate::scheduler::Client::start(
        schedule.clone(),
        crate::ids::WorkerId(task.0),
        &job.spec.config.scheduler,
        token.clone(),
    )?;
    let open = |p: usize| {
        Sender::typed(
            job.spec.workers[p].exchange,
            transfer(job.spec.job, 0, p as u64, task.0),
            token.clone(),
            crate::exchange::Payload::LeasedAggregate,
            Some(schedule.fence.digest.clone()),
        )
    };
    let mut senders: Vec<Option<Sender>> = (0..job.spec.workers.len()).map(|_| None).collect();
    let mut metrics = TaskMetrics::default();
    loop {
        let wait_started = Instant::now();
        let next = client.next()?;
        metrics.lease_wait_ns += crate::materialization::ns(wait_started);
        let Some(lease) = next else {
            break;
        };
        let source = job.spec.snapshot.as_ref().expect("leased source");
        require(
            source.morsels.get(
                usize::try_from(lease.morsel.id.0)
                    .map_err(|_| Error::Invalid("morsel index overflow".into()))?,
            ) == Some(&lease.morsel),
            "lease source differs from frozen catalog",
        )?;
        let verify_started = Instant::now();
        crate::scheduler::verify_sources(
            std::slice::from_ref(&lease.morsel),
            &token,
            start + crate::TASK_TIMEOUT,
        )?;
        metrics.source_verify_ns += crate::materialization::ns(verify_started);
        token.check()?;
        #[cfg(feature = "research-hooks")]
        if job.spec.config.scheduler.research_delay_worker == Some(task.0) {
            let until =
                Instant::now() + Duration::from_millis(job.spec.config.scheduler.research_delay_ms);
            while Instant::now() < until {
                token.check()?;
                thread::sleep(Duration::from_millis(5));
            }
        }
        let file_start = Instant::now();
        let output = ade_exec::execute_hash_partials_to_sink(
            producer_plan(job, std::slice::from_ref(&lease.morsel.path)),
            job.spec.config.execution(),
            token.clone(),
            |batch| {
                for p in 0..senders.len() {
                    let route_start = Instant::now();
                    let selected =
                        partition::select(&batch, p, senders.len()).map_err(local_sink_error)?;
                    metrics.routing_ns += crate::materialization::ns(route_start);
                    if selected.num_rows() > 0 {
                        let send_start = Instant::now();
                        if senders[p].is_none() {
                            senders[p] = Some(open(p).map_err(local_sink_error)?);
                        }
                        senders[p]
                            .as_mut()
                            .expect("opened")
                            .batch(&selected)
                            .map_err(local_sink_error)?;
                        #[cfg(feature = "research-hooks")]
                        if job.spec.config.scheduler.research_exit_after_emit_worker == Some(task.0)
                        {
                            std::process::exit(87);
                        }
                        metrics.sending_ns += crate::materialization::ns(send_start);
                    }
                }
                Ok(())
            },
        )?;
        let verify_started = Instant::now();
        crate::scheduler::verify_sources(
            std::slice::from_ref(&lease.morsel),
            &token,
            start + crate::TASK_TIMEOUT,
        )?;
        metrics.source_verify_ns += crate::materialization::ns(verify_started);
        let m = task_metrics(file_start, &output, 1);
        let ack_started = Instant::now();
        client.complete(crate::scheduler::Completion {
            lease: lease.clone(),
            input_rows: m.input_rows as u64,
            output_rows: m.output_rows as u64,
            work_ns: m.operator_work_ns,
        })?;
        metrics.lease_ack_ns += crate::materialization::ns(ack_started);
        metrics.source_files += 1;
        metrics.input_rows = metrics
            .input_rows
            .checked_add(m.input_rows)
            .ok_or_else(|| Error::Invalid("leased input rows overflow".into()))?;
        metrics.output_rows = metrics
            .output_rows
            .checked_add(m.output_rows)
            .ok_or_else(|| Error::Invalid("leased output rows overflow".into()))?;
        metrics.operator_work_ns = metrics
            .operator_work_ns
            .checked_add(m.operator_work_ns)
            .ok_or_else(|| Error::Invalid("leased work overflow".into()))?;
        metrics.spill_bytes += m.spill_bytes;
        metrics.managed_peak = metrics.managed_peak.max(m.managed_peak);
        metrics.production.source_bytes = metrics
            .production
            .source_bytes
            .checked_add(lease.morsel.bytes)
            .ok_or_else(|| Error::Invalid("leased bytes overflow".into()))?;
    }
    metrics.production.operator_work_ns = metrics.operator_work_ns;
    metrics.production.wall_elapsed_ns = crate::materialization::ns(start);
    metrics.production.source_requests = metrics.source_files as u64;
    metrics.production.consumer_count = 1;
    for (p, sender) in senders.into_iter().enumerate() {
        let sender = match sender {
            Some(s) => s,
            None => open(p)?,
        };
        let (sent, manifest) = sender.finish_manifest()?;
        metrics.sent.add(&sent);
        metrics.destinations.insert(p as u64, sent);
        metrics.manifests.insert(p as u64, manifest);
    }
    client.closed()?;
    metrics.production.output_bytes = metrics.sent.payload_bytes;
    metrics.production.output_segments = metrics.sent.batches;
    metrics.elapsed_ms = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);
    token.check()?;
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
        if let Some(schedule) = &job.spec.schedule {
            return Sender::typed(
                gather,
                transfer(job.spec.job, 1, 0, task.0).with_attempt(attempt),
                token.clone(),
                crate::exchange::Payload::LeasedAggregate,
                Some(schedule.fence.digest.clone()),
            );
        }
        Sender::new(
            gather,
            transfer(job.spec.job, 1, 0, task.0).with_attempt(attempt),
            token.clone(),
        )
    };
    let mut sender: Option<Sender> = None;
    let pulls = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let output = ade_exec::execute_merge_source_to_sink(
        Box::new(TimedSource {
            inner: job.store.reader(),
            pulls: pulls.clone(),
        }),
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
            if job.spec.schedule.is_some()
                && job.spec.config.scheduler.research_exit_final_worker
                    == Some(task.0 - job.spec.workers.len() as u64)
            {
                std::process::exit(88);
            }
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
    metrics.executed_final = true;
    metrics.layout_digest = committed_layout(job)?.as_ref().map(|l| l.digest.clone());
    metrics.source_pull_ns = pulls.load(Ordering::Relaxed);
    let sender = match sender {
        Some(sender) => sender,
        None => open()?,
    };
    if job.spec.schedule.is_some() {
        let (sent, manifest) = sender.finish_manifest()?;
        metrics.sent = sent;
        metrics.manifests.insert(0, manifest);
    } else {
        metrics.sent = sender.finish()?;
    }
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
    let layout = committed_layout(original)?;
    require(
        plan.layout_digest == layout.as_ref().map(|l| l.digest.clone()),
        "recovery layout mismatch",
    )?;
    if let Some(layout) = &layout {
        store.install_layout(layout.clone())?;
    }
    let replay = Job {
        prepared: Mutex::new(None),
        layout: Mutex::new(layout),
        probe_started: AtomicBool::new(true),
        probe_files: Mutex::new(None),
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
            require(
                receipt.topology_digest == plan.layout_digest,
                "restore layout mismatch",
            )?;
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
        Request::PrepareJoin(spec) => {
            let aggregate = state.job.lock().expect("job lock");
            let mut current = state.join.lock().expect("join lock");
            require(
                aggregate.is_none()
                    && current.is_none()
                    && !state.poisoned.load(Ordering::Acquire)
                    && !state.producer_busy.load(Ordering::Acquire)
                    && !state.final_busy.load(Ordering::Acquire),
                "worker has active/poisoned job",
            )?;
            let own = spec
                .workers
                .get(
                    usize::try_from(state.endpoint.id.0)
                        .map_err(|_| Error::Invalid("worker ID overflow".into()))?,
                )
                .ok_or_else(|| Error::Invalid("join endpoint absent".into()))?;
            require(
                own.id == state.endpoint.id
                    && own.control == state.endpoint.control
                    && own.exchange == state.endpoint.exchange,
                "join endpoint mismatch",
            )?;
            let mut last = state.last_job_id.lock().expect("job watermark");
            require(last.is_none_or(|id| spec.job > id), "join stale job")?;
            let session = crate::join::Session::new(
                spec.clone(),
                usize::try_from(state.endpoint.id.0)
                    .map_err(|_| Error::Invalid("worker ID overflow".into()))?,
                &state.parent,
            )?;
            *last = Some(spec.job);
            *state
                .last_execution_namespace
                .lock()
                .expect("execution namespace") = Some(spec.execution);
            *current = Some(Arc::new(session));
            Ok(Reply::Ready)
        }
        Request::ProbeJoin { job: id } => join_session(state, id)?.probe().map(Reply::JoinProbe),
        Request::CommitJoin { job: id, decision } => {
            join_session(state, id)?.commit(decision)?;
            Ok(Reply::Ready)
        }
        Request::RunJoinProducer { job: id } => run_join(state, id, None),
        Request::RunJoinFinal { job: id, gather } => run_join(state, id, Some(gather)),
        Request::PrepareJob(spec) => {
            require(
                uuid::Uuid::parse_str(&spec.execution_namespace).is_ok(),
                "invalid execution namespace",
            )?;
            spec.config.validate()?;
            require(
                spec.config.partial.mode == ade_exec::partial::Mode::FixedHash
                    || spec.snapshot.is_none(),
                "non-fixed partial source/replay graph is not admitted; disable recovery",
            )?;
            crate::coordinator::validate_topology(&spec.workers)?;
            spec.config.scheduler.validate(spec.workers.len())?;
            require(
                spec.config.scheduler.leased() == spec.schedule.is_some(),
                "missing/unsolicited schedule spec",
            )?;
            if let Some(schedule) = &spec.schedule {
                let source = spec
                    .snapshot
                    .as_ref()
                    .ok_or_else(|| Error::Invalid("leased source snapshot required".into()))?;
                require(
                    !source.replayable,
                    "leased source cannot authorize recovery",
                )?;
                schedule.validate(
                    spec.job,
                    &spec.execution_namespace,
                    &spec.workers,
                    &spec.config,
                    source,
                )?;
            }

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
                    && state.join.lock().expect("join lock").is_none()
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
            let store = Arc::new(if let Some(schedule) = &spec.schedule {
                let c = &spec.config.scheduler;
                Store::typed(
                    &state.parent,
                    expected,
                    CancellationToken::default(),
                    spec.config.exchange.clone(),
                    crate::exchange::Payload::LeasedAggregate,
                    schedule.fence.digest.clone(),
                    crate::exchange::IngressLimits {
                        rows: c.max_input_rows,
                        bytes: c.max_exchange_bytes,
                        batches: c.max_exchange_frames,
                    },
                    Some(crate::exchange::HashOwner {
                        workers: spec.workers.len(),
                        destination: usize::try_from(state.endpoint.id.0)
                            .map_err(|_| Error::Invalid("worker index overflow".into()))?,
                    }),
                )?
            } else {
                Store::configured_with_backend(
                    &state.parent,
                    expected,
                    CancellationToken::default(),
                    spec.config.exchange.clone(),
                    state.object_backend.as_ref(),
                )?
            });
            let accepted_job_id = spec.job;
            let accepted_execution_namespace = spec.execution_namespace.clone();
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
                    .complete(accepted_execution_namespace.clone())?,
                )
            } else {
                None
            };
            *current = Some(Arc::new(Job {
                prepared: Mutex::new(None),
                layout: Mutex::new(None),
                probe_started: AtomicBool::new(false),
                probe_files: Mutex::new(None),
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
            *state
                .last_execution_namespace
                .lock()
                .expect("execution namespace lock") = Some(accepted_execution_namespace);
            Ok(Reply::Ready)
        }
        #[cfg(feature = "research-hooks")]
        Request::ReleaseFinal { job: id } => {
            let _ = job(state, id)?;
            Ok(Reply::Ready)
        }
        Request::Probe {
            job: id,
            files,
            timeout_ms,
        } => {
            let job = job(state, id)?;
            require(
                job.spec.config.topology.policy == crate::topology::PartitionPolicy::SampledV1,
                "probe requires sampled_v1",
            )?;
            require(
                (1..=120_000).contains(&timeout_ms),
                "invalid probe deadline",
            )?;
            require(
                !state.producer_busy.swap(true, Ordering::AcqRel),
                "producer/probe role busy",
            )?;
            let _guard = TaskGuard(&state.producer_busy);
            if let Some(snapshot) = &job.spec.snapshot {
                let morsels =
                    snapshot.producer_morsels(TaskId(state.endpoint.id.0), job.spec.workers.len());
                require(
                    morsels.iter().map(|m| &m.path).eq(files.iter()),
                    "probe source assignment mismatch",
                )?;
                for morsel in morsels.iter().take(job.spec.config.topology.max_files) {
                    require(
                        std::fs::metadata(&morsel.path)?.len() == morsel.bytes,
                        "probe source size mismatch",
                    )?;
                }
            }
            require(
                !job.probe_started.swap(true, Ordering::AcqRel),
                "duplicate probe",
            )?;
            let result = crate::probe::probe(
                &files,
                state.endpoint.id.0,
                &job.spec.config.topology,
                &job.store.cancellation,
                Instant::now() + Duration::from_millis(timeout_ms),
            );
            match result {
                Ok(mut observation) => {
                    let preparation_started = Instant::now();
                    if !files.is_empty() {
                        let prepared = ade_exec::PreparedAggregate::new(
                            producer_plan(&job, &files),
                            job.spec.config.execution(),
                            job.store.cancellation.clone(),
                        )
                        .inspect_err(|_| job.store.cancellation.cancel())?;
                        observation.original_prefix_rows = prepared.prefix_rows();
                        observation.preparation_ns =
                            crate::materialization::ns(preparation_started);
                        *job.prepared.lock().expect("prepared task") =
                            Some((prepared, observation.preparation_ns));
                    }
                    *job.probe_files.lock().expect("probe files") = Some(files);
                    Ok(Reply::Probe(observation))
                }
                Err(error) => {
                    job.store.cancellation.cancel();
                    Err(error)
                }
            }
        }
        Request::CommitLayout { job: id, layout } => {
            let job = job(state, id)?;
            require(
                job.spec.config.topology.policy == crate::topology::PartitionPolicy::SampledV1
                    && job.probe_files.lock().expect("probe files").is_some(),
                "layout before successful probe",
            )?;
            layout.validate(job.spec.workers.len())?;
            let mut current = job.layout.lock().expect("layout lock");
            require(
                current.is_none()
                    && !job.producer_started.load(Ordering::Acquire)
                    && !job.final_started.load(Ordering::Acquire),
                "duplicate/late layout commit",
            )?;
            job.store.install_layout(layout.clone())?;
            *current = Some(layout);
            Ok(Reply::Ready)
        }
        Request::VerifyInactive { job: id } => {
            let job = job(state, id)?;
            let layout = committed_layout(&job)?
                .ok_or_else(|| Error::Invalid("inactive static destination".into()))?;
            require(
                !layout.active().contains(&state.endpoint.id.0),
                "active destination cannot be idle",
            )?;
            require(
                !state.final_busy.swap(true, Ordering::AcqRel),
                "final role busy",
            )?;
            let _guard = TaskGuard(&state.final_busy);
            require(
                !job.final_started.swap(true, Ordering::AcqRel),
                "duplicate inactive verification",
            )?;
            let mut source = job.store.reader();
            require(
                source.next_batch(&job.store.cancellation)?.is_none(),
                "inactive destination contains data",
            )?;
            job.store.validate_retention_drained()?;
            let received = job.store.metrics()?;
            require(
                received.transfers == job.spec.workers.len() as u64
                    && received.rows == 0
                    && received.batches == 0
                    && received.payload_bytes == 0,
                "invalid inactive terminals",
            )?;
            Ok(Reply::Task(TaskMetrics {
                layout_digest: Some(layout.digest),
                received,
                retention: job.store.retention_metrics()?,
                ..Default::default()
            }))
        }
        Request::RecoverFinal { plan, gather } => recover(state, &plan, gather),
        Request::CleanupMaterializations {
            job: id,
            execution_namespace,
        } => {
            require(
                *state.last_job_id.lock().expect("job watermark") == Some(id),
                "cleanup job mismatch",
            )?;
            require(
                state
                    .last_execution_namespace
                    .lock()
                    .expect("execution namespace lock")
                    .as_deref()
                    == Some(execution_namespace.as_str()),
                "cleanup execution namespace mismatch",
            )?;
            if let Some(backend) = &state.object_backend {
                crate::retention::Objects::configured(
                    &state.parent,
                    backend.config.clone(),
                    Some(backend),
                    Arc::default(),
                )?
                .complete(execution_namespace)?
                .cleanup_job(id)?;
            }
            Ok(Reply::Ready)
        }
        Request::CancelJob { job: id } => {
            cancel_join(state, Some(id));
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
            cancel_join(state, None);
            state.stop.store(true, Ordering::Release);
            // Wake a blocking accept; the listener checks stop before spawning a handler.
            protocol::wake_listener(state.endpoint.control);
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
            let layout = committed_layout(&job)?;
            if is_final {
                require(
                    layout
                        .as_ref()
                        .is_none_or(|l| l.active().contains(&state.endpoint.id.0)),
                    "final task on inactive destination",
                )?;
            }
            let result = match task {
                Request::RunProducerTask { files, .. } => {
                    if layout.is_some() {
                        require(
                            job.probe_files.lock().expect("probe files").as_ref() == Some(&files),
                            "producer differs from probed assignment",
                        )?;
                    }
                    require(
                        !job.producer_started.swap(true, Ordering::AcqRel),
                        "producer task already started",
                    )?;
                    if let Some(snapshot) = &job.spec.snapshot
                        && job.spec.schedule.is_none()
                    {
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
    listener.set_nonblocking(false)?;
    let exchange_listener = TcpListener::bind(exchange)?;
    let endpoint = WorkerEndpoint {
        id,
        control: listener.local_addr()?,
        exchange: exchange_listener.local_addr()?,
    };
    let state = Arc::new(State {
        endpoint: endpoint.clone(),
        job: Mutex::new(None),
        join: Mutex::new(None),
        last_job_id: Mutex::new(None),
        last_execution_namespace: Mutex::new(None),
        producer_busy: AtomicBool::new(false),
        final_busy: AtomicBool::new(false),
        poisoned: AtomicBool::new(false),
        stop: AtomicBool::new(false),
        parent: parent.into(),
        object_backend,
    });
    let receiving = state.clone();
    let receiver = Receiver::start(exchange_listener, move |header| {
        if let Some(join) = receiving.join.lock().expect("join lock").as_ref() {
            return join.store(header);
        }
        let job = job(&receiving, header.id.job)?;
        committed_layout(&job)?;
        Ok(job.store.clone())
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
                    if state.stop.load(Ordering::Acquire) {
                        break;
                    }
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
        } else {
            thread::sleep(Duration::from_millis(2));
        }
    }
    if let Some(job) = state
        .job
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take()
    {
        job.store.cancellation.cancel();
    }
    cancel_join(&state, None);
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
            join: Mutex::new(None),
            last_job_id: Mutex::new(None),
            last_execution_namespace: Mutex::new(None),
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
            schedule: None,
            job: id,
            execution_namespace: "8abade70-a479-4c7e-9bd8-989ed8f1a1d7".into(),
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
            layout_digest: None,
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
