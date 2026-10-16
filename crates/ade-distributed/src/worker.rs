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
    store: Arc<Store>,
    producer_done: AtomicBool,
    producer_started: AtomicBool,
    final_started: AtomicBool,
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
fn producer(state: &State, job: &Job, files: &[PathBuf]) -> Result<TaskMetrics> {
    require(
        !job.producer_done.load(Ordering::Acquire),
        "producer already completed",
    )?;
    let start = Instant::now();
    let token = job.store.cancellation.clone();
    // Do not hold idle receiver connections while a blocking aggregate builds.
    let open = |p: usize| {
        Sender::new(
            job.spec.workers[p].exchange,
            transfer(job.spec.job, 0, p as u64, state.endpoint.id.0),
            token.clone(),
        )
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
        task_metrics(start, &output, files.len())
    };
    for (p, sender) in senders.into_iter().enumerate() {
        let sender = match sender {
            Some(sender) => sender,
            None => open(p)?,
        };
        let sent = sender.finish()?;
        metrics.sent.add(&sent);
        metrics.destinations.insert(p as u64, sent);
    }
    token.check()?;
    metrics.elapsed_ms = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);
    job.producer_done.store(true, Ordering::Release);
    Ok(metrics)
}
fn final_task(state: &State, job: &Job, gather: SocketAddr) -> Result<TaskMetrics> {
    require(
        !job.final_started.swap(true, Ordering::AcqRel),
        "final task already started",
    )?;
    let start = Instant::now();
    let token = job.store.cancellation.clone();
    let open = || {
        Sender::new(
            gather,
            transfer(
                job.spec.job,
                1,
                0,
                job.spec.workers.len() as u64 + state.endpoint.id.0,
            ),
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
                .map_err(local_sink_error)
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
            *current = Some(Arc::new(Job {
                spec,
                store,
                producer_done: AtomicBool::new(false),
                producer_started: AtomicBool::new(false),
                final_started: AtomicBool::new(false),
            }));
            *last_job_id = Some(accepted_job_id);
            Ok(Reply::Ready)
        }
        Request::CancelJob { job: id } => {
            let mut current = state.job.lock().expect("job lock");
            if current.as_ref().is_some_and(|j| j.spec.job == id) {
                let old = current.take().expect("job");
                if old.store.is_poisoned() {
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
                    producer(state, &job, &files)
                }
                Request::RunFinalTask { gather, .. } => final_task(state, &job, gather),
                _ => unreachable!(),
            };
            if result.is_err() {
                job.store.cancellation.cancel();
            }
            if job.store.is_poisoned() {
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
                    handlers.push(thread::spawn(move || {
                        if socket.set_read_timeout(Some(crate::IO_TIMEOUT)).is_err()
                            || socket.set_write_timeout(Some(crate::IO_TIMEOUT)).is_err()
                        {
                            return;
                        }
                        if let Ok(envelope) = protocol::read_message::<Envelope<Request>>(
                            &mut protocol::DeadlineStream::new(&mut socket, crate::IO_TIMEOUT),
                        ) {
                            let body = handle(&state, envelope.body).unwrap_or_else(|error| {
                                Reply::Error {
                                    code: match &error {
                                        Error::Exec(_) => "execution",
                                        Error::Io(_) => "io",
                                        _ => "protocol",
                                    }
                                    .into(),
                                    message: error.to_string(),
                                }
                            });
                            let _ = protocol::write_message(
                                &mut protocol::DeadlineStream::new(&mut socket, crate::IO_TIMEOUT),
                                &Envelope {
                                    request_id: envelope.request_id,
                                    body,
                                },
                            );
                        }
                    }));
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
        current.store.poison();
        handle(&state, Request::CancelJob { job: JobId(43) }).unwrap();
        assert!(state.poisoned.load(Ordering::Acquire));
        assert!(handle(&state, Request::PrepareJob(spec(JobId(44)))).is_err());
    }
}
