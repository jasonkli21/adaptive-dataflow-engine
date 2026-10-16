//! Static stage barriers, gather completeness and owned localhost process lifecycle.
use crate::{
    Error, Result, TaskConfig, WorkerEndpoint,
    exchange::{self, Receiver, Store},
    ids::{AttemptId, JobId, TaskId, WorkerId, transfer},
    metrics::{DistributedMetrics, TaskMetrics},
    protocol::{self, JobSpec, Reply, Request},
    require,
};
use arrow::record_batch::RecordBatch;
use std::{
    collections::{BTreeMap, BTreeSet},
    net::TcpListener,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{Arc, mpsc},
    thread,
    time::{Duration, Instant},
};
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobState {
    Prepared,
    ProducersRunning,
    ExchangeComplete,
    FinalsRunning,
    GatherComplete,
    Succeeded,
    Failed,
}
pub struct StaticAggregateJob {
    pub id: JobId,
    pub files: Vec<PathBuf>,
    pub workers: Vec<WorkerEndpoint>,
    pub config: TaskConfig,
    pub task_timeout: Duration,
}
pub struct JobOutput {
    pub batches: Vec<RecordBatch>,
    pub metrics: DistributedMetrics,
    pub state: JobState,
}
/// # Errors
/// Rejects worker counts, IDs, or endpoints that cannot form the static topology.
pub fn validate_topology(workers: &[WorkerEndpoint]) -> Result<()> {
    require(
        (2..=4).contains(&workers.len()),
        "Phase 3 requires 2..4 workers",
    )?;
    let mut addresses = BTreeSet::new();
    for (i, worker) in workers.iter().enumerate() {
        require(
            worker.id == WorkerId(i as u64),
            "worker IDs must match ordered contiguous topology",
        )?;
        require(
            worker.control.port() != 0 && worker.exchange.port() != 0,
            "worker endpoints must have bound ports",
        )?;
        require(
            addresses.insert(worker.control) && addresses.insert(worker.exchange),
            "duplicate worker address",
        )?;
    }
    Ok(())
}
struct CancelOnDrop(ade_exec::CancellationToken);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}
fn control(
    job: &StaticAggregateJob,
    worker: &WorkerEndpoint,
    request: Request,
    stage: &'static str,
) -> Result<Reply> {
    protocol::rpc(worker, request, crate::IO_TIMEOUT).map_err(|error| Error::Task {
        job: job.id,
        worker: worker.id,
        stage,
        message: error.to_string(),
    })
}
fn cancel(workers: &[WorkerEndpoint], job: JobId) {
    thread::scope(|scope| {
        for worker in workers {
            scope.spawn(move || {
                let _ = protocol::rpc(worker, Request::CancelJob { job }, crate::IO_TIMEOUT);
            });
        }
    });
}
fn stage(
    job: &StaticAggregateJob,
    name: &'static str,
    requests: Vec<Request>,
) -> Result<BTreeMap<u64, TaskMetrics>> {
    let deadline = Instant::now() + job.task_timeout;
    thread::scope(|scope| {
        let (tx, rx) = mpsc::sync_channel(job.workers.len());
        for (worker, request) in job.workers.iter().zip(requests) {
            let tx = tx.clone();
            scope.spawn(move || {
                let response =
                    protocol::rpc_until(worker, request, deadline).and_then(|reply| match reply {
                        Reply::Task(metrics) => Ok(metrics),
                        _ => Err(Error::Invalid("unexpected task response".into())),
                    });
                let _ = tx.send((worker.id, response));
            });
        }
        drop(tx);
        let mut metrics = BTreeMap::new();
        for _ in &job.workers {
            let received = rx.recv_timeout(deadline.saturating_duration_since(Instant::now()));
            let (worker, result) = match received {
                Ok(result) => result,
                Err(error) => {
                    cancel(&job.workers, job.id);
                    return Err(Error::Task {
                        job: job.id,
                        worker: WorkerId(u64::MAX),
                        stage: name,
                        message: format!("stage timeout/disconnect: {error}"),
                    });
                }
            };
            match result {
                Ok(task) => {
                    metrics.insert(worker.0, task);
                }
                Err(error) => {
                    cancel(&job.workers, job.id);
                    return Err(Error::Task {
                        job: job.id,
                        worker,
                        stage: name,
                        message: error.to_string(),
                    });
                }
            }
        }
        Ok(metrics)
    })
}
/// Execute one complete static graph; errors never return partial output.
/// # Errors
/// Returns configuration, task, transport, deadline, or gather errors with no partial output.
#[allow(
    clippy::too_many_lines,
    reason = "Explicit static stage transitions and failure cleanup remain in one place."
)]
pub fn execute(job: &StaticAggregateJob) -> Result<JobOutput> {
    let started = Instant::now();
    validate_topology(&job.workers)?;
    job.config.validate()?;
    require(
        !job.task_timeout.is_zero(),
        "positive task timeout required",
    )?;
    require(!job.files.is_empty(), "job requires manifest files")?;
    let unique: BTreeSet<_> = job.files.iter().collect();
    require(unique.len() == job.files.len(), "duplicate source path")?;
    let mut state = JobState::Prepared;
    let result = (|| {
        for worker in &job.workers {
            match control(job, worker, Request::Ping, "ping")? {
                Reply::Pong(actual) => require(
                    actual.id == worker.id
                        && actual.control == worker.control
                        && actual.exchange == worker.exchange,
                    "Ping topology mismatch",
                )?,
                _ => return Err(Error::Invalid("unexpected Ping response".into())),
            }
        }
        for worker in &job.workers {
            let spec = JobSpec {
                job: job.id,
                workers: job.workers.clone(),
                config: job.config.clone(),
            };
            require(
                matches!(
                    control(job, worker, Request::PrepareJob(spec), "prepare")?,
                    Reply::Ready
                ),
                "unexpected prepare response",
            )?;
        }
        let parent = tempfile::tempdir()?;
        let token = ade_exec::CancellationToken::default();
        let expected = (0..job.workers.len())
            .map(|p| transfer(job.id, 1, 0, (job.workers.len() + p) as u64))
            .collect();
        let store = Arc::new(Store::new(parent.path(), expected, token.clone())?);
        let receiving = store.clone();
        let receiver = Receiver::start(TcpListener::bind("127.0.0.1:0")?, move |_| {
            Ok(receiving.clone())
        })?;
        let _cancel_gather = CancelOnDrop(token.clone());
        let mut assignments = vec![Vec::new(); job.workers.len()];
        for (i, file) in job.files.iter().enumerate() {
            assignments[i % job.workers.len()].push(file.clone());
        }
        state = JobState::ProducersRunning;
        let producers = stage(
            job,
            "producer",
            assignments
                .into_iter()
                .enumerate()
                .map(|(i, files)| Request::RunProducerTask {
                    job: job.id,
                    task: TaskId(i as u64),
                    attempt: AttemptId(0),
                    files,
                })
                .collect(),
        )?;
        state = JobState::ExchangeComplete;
        let requests = job
            .workers
            .iter()
            .map(|w| Request::RunFinalTask {
                job: job.id,
                task: TaskId(job.workers.len() as u64 + w.id.0),
                attempt: AttemptId(0),
                gather: receiver.address,
            })
            .collect();
        state = JobState::FinalsRunning;
        let finals = stage(job, "final", requests)?;
        let batches = exchange::collect(&store)?;
        state = JobState::GatherComplete;
        let metrics = DistributedMetrics {
            worker_count: job.workers.len(),
            elapsed_ms: 0,
            received: finals
                .iter()
                .map(|(id, m)| (*id, m.received.clone()))
                .collect(),
            producers,
            finals,
            gather: store.metrics()?,
        };
        token.cancel();
        drop(receiver);
        state = JobState::Succeeded;
        Ok(JobOutput {
            batches,
            metrics,
            state,
        })
    })();
    if result.is_err() {
        state = JobState::Failed;
        eprintln!("ADE job {:?} state={state:?}", job.id);
    }
    // Success also releases worker-local exchange stores. Tasks have completed their RPCs.
    cancel(&job.workers, job.id);
    result.map(|mut output| {
        output.metrics.elapsed_ms =
            u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        output
    })
}
/// Parent owns children and temp root. Drop always kill/waits before removing files.
pub struct LocalCluster {
    pub workers: Vec<WorkerEndpoint>,
    children: Vec<Child>,
    root: tempfile::TempDir,
}
impl LocalCluster {
    /// # Errors
    /// Returns process, readiness, or configuration failures; already spawned children are killed and waited.
    pub fn spawn(binary: &Path, workers: usize) -> Result<Self> {
        require((2..=4).contains(&workers), "workers must be 2..4")?;
        let mut cluster = Self {
            workers: Vec::new(),
            children: Vec::new(),
            root: tempfile::Builder::new().prefix("ade-workers-").tempdir()?,
        };
        for id in 0..workers {
            let mut child = Command::new(binary)
                .arg(id.to_string())
                .arg(cluster.root.path())
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .spawn()?;
            let mut stdout = child
                .stdout
                .take()
                .ok_or_else(|| Error::Invalid("worker readiness pipe unavailable".into()))?;
            cluster.children.push(child);
            let (tx, rx) = mpsc::sync_channel(1);
            let handle = thread::spawn(move || {
                let _ = tx.send(protocol::read_message::<WorkerEndpoint>(&mut stdout));
            });
            let ready = rx.recv_timeout(crate::IO_TIMEOUT);
            // On timeout, killing/waiting closes the readiness pipe before joining the reader.
            if ready.is_err()
                && let Some(child) = cluster.children.last_mut()
            {
                let _ = child.kill();
                let _ = child.wait();
            }
            let _ = handle.join();
            let endpoint =
                ready.map_err(|_| Error::Invalid("worker readiness timed out".into()))??;
            require(
                endpoint.id == WorkerId(id as u64)
                    && endpoint.control.ip().is_loopback()
                    && endpoint.exchange.ip().is_loopback(),
                "invalid worker readiness",
            )?;
            cluster.workers.push(endpoint);
        }
        Ok(cluster)
    }
    #[must_use]
    pub fn temp_root(&self) -> &Path {
        self.root.path()
    }

    /// Explicit failure injection for process integration tests.
    /// # Errors
    /// Returns an invalid index or process termination error.
    pub fn terminate(&mut self, index: usize) -> Result<()> {
        let child = self
            .children
            .get_mut(index)
            .ok_or_else(|| Error::Invalid("worker index out of range".into()))?;
        child.kill()?;
        child.wait()?;
        Ok(())
    }
}
impl Drop for LocalCluster {
    fn drop(&mut self) {
        for child in &mut self.children {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
