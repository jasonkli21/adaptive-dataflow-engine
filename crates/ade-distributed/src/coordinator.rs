//! Fixed worker stages, optional pre-exchange layout commitment, gather completeness and owned localhost process lifecycle.
use crate::{
    Error, Result, TaskConfig, WorkerEndpoint,
    ids::{AttemptId, JobId, TaskId, WorkerId},
    metrics::{DistributedMetrics, TaskMetrics},
    protocol::{self, JobSpec, Reply, Request},
    require,
};
use arrow::record_batch::RecordBatch;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::mpsc,
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
    pub snapshot: Option<crate::lineage::SourceSnapshot>,
}
pub struct JobOutput {
    pub batches: Vec<RecordBatch>,
    pub metrics: DistributedMetrics,
    pub state: JobState,
}
/// # Errors
/// Rejects worker counts, IDs, or endpoints that cannot form the fixed worker topology.
pub fn validate_topology(workers: &[WorkerEndpoint]) -> Result<()> {
    require(
        (2..=4).contains(&workers.len()),
        "distributed execution requires 2..4 workers",
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
struct TaskResults {
    producers: BTreeMap<u64, TaskMetrics>,
    finals: BTreeMap<u64, TaskMetrics>,
    lost: Vec<u64>,
}
fn tasks(
    job: &StaticAggregateJob,
    producers: Vec<Request>,
    finals: Vec<Request>,
    started: Instant,
) -> Result<TaskResults> {
    let task_started = Instant::now();
    let deadline = task_started + job.task_timeout;
    thread::scope(|scope| {
        let (tx, rx) = mpsc::sync_channel(2 * job.workers.len());
        // Spawn final readers first. A producer may win CPU dispatch; bounded handoff
        // safely waits until its reader is pulling, without a separate barrier RPC.
        for (is_final, requests) in [(true, finals), (false, producers)] {
            for (worker, request) in job.workers.iter().zip(requests) {
                let tx = tx.clone();
                scope.spawn(move || {
                    let response =
                        protocol::rpc_until(worker, request, deadline).and_then(|r| match r {
                            Reply::Task(m) => Ok(m),
                            _ => Err(Error::Invalid("unexpected task response".into())),
                        });
                    let _ = tx.send((worker.id, is_final, response));
                });
            }
        }
        drop(tx);
        let mut producers = BTreeMap::new();
        let mut finals = BTreeMap::new();
        let mut lost = Vec::new();
        #[cfg(feature = "research-hooks")]
        let mut released = false;
        for _ in 0..2 * job.workers.len() {
            let (worker, is_final, response) =
                match rx.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
                    Ok(r) => r,
                    Err(e) => {
                        cancel(&job.workers, job.id);
                        return Err(Error::Task {
                            job: job.id,
                            worker: WorkerId(u64::MAX),
                            stage: "overlap",
                            message: e.to_string(),
                        });
                    }
                };
            match response {
                Ok(mut m) => {
                    m.coordinator_finished_ns = crate::materialization::ns(started);
                    if is_final {
                        finals.insert(worker.0, m);
                    } else {
                        producers.insert(worker.0, m);
                    }
                }
                Err(Error::Io(error))
                    if is_final
                        && lost.is_empty()
                        && matches!(
                            error.kind(),
                            std::io::ErrorKind::UnexpectedEof
                                | std::io::ErrorKind::ConnectionReset
                                | std::io::ErrorKind::ConnectionAborted
                                | std::io::ErrorKind::BrokenPipe
                        ) =>
                {
                    // Transport loss is only a candidate. Confirm unavailability below.
                    lost.push(worker.0);
                }
                Err(e) => {
                    cancel(&job.workers, job.id);
                    return Err(Error::Task {
                        job: job.id,
                        worker,
                        stage: if is_final { "final" } else { "producer" },
                        message: e.to_string(),
                    });
                }
            }
            #[cfg(feature = "research-hooks")]
            if producers.len() == job.workers.len() && !released {
                released = true;
                if let Some(p) = job.config.research_exit_final_partition {
                    protocol::rpc_until(
                        &job.workers[usize::try_from(p).expect("validated partition")],
                        Request::ReleaseFinal { job: job.id },
                        deadline,
                    )?;
                }
            }
        }
        Ok(TaskResults {
            producers,
            finals,
            lost,
        })
    })
}
/// Execute one complete fixed-host aggregate graph; errors never return partial output.
/// # Errors
/// Returns configuration, task, transport, deadline, or gather errors with no partial output.
#[allow(
    clippy::too_many_lines,
    reason = "Explicit producer/final stage transitions and failure cleanup remain in one place."
)]
pub fn execute(job: &StaticAggregateJob) -> Result<JobOutput> {
    execute_inner(job, None, None)
}

pub(crate) fn execute_bounded(
    job: &StaticAggregateJob,
    output_limits: crate::bounded_output::OutputLimits,
    mut adapt: impl FnMut(RecordBatch) -> Result<RecordBatch>,
) -> Result<JobOutput> {
    output_limits.validate()?;
    execute_inner(job, Some(output_limits), Some(&mut adapt))
}

#[allow(
    clippy::too_many_lines,
    reason = "Explicit producer/final stage transitions and failure cleanup remain in one place."
)]
fn execute_inner(
    job: &StaticAggregateJob,
    output_limits: Option<crate::bounded_output::OutputLimits>,
    mut adapt: Option<&mut dyn FnMut(RecordBatch) -> Result<RecordBatch>>,
) -> Result<JobOutput> {
    let started = Instant::now();
    validate_topology(&job.workers)?;
    job.config.validate()?;
    job.config.scheduler.validate(job.workers.len())?;
    if job.config.scheduler.leased() {
        require(
            job.workers
                .iter()
                .all(|w| w.control.ip().is_loopback() && w.exchange.ip().is_loopback()),
            "leased scheduler v1 requires loopback registry",
        )?;
        let snapshot = job.snapshot.as_ref().ok_or_else(|| {
            Error::Invalid("leased scheduler requires verified source snapshot".into())
        })?;
        job.config
            .scheduler
            .validate_for_source(job.workers.len(), snapshot.morsels.len())?;
        require(
            !snapshot.replayable && snapshot.morsels.len() <= job.config.scheduler.max_morsels,
            "leased replay/source catalog refusal",
        )?;
    }

    require(
        job.config.partial.mode == ade_exec::partial::Mode::FixedHash || job.snapshot.is_none(),
        "non-fixed partial source/replay graph is not admitted; disable recovery",
    )?;
    require(
        !job.task_timeout.is_zero(),
        "positive task timeout required",
    )?;
    require(!job.files.is_empty(), "job requires manifest files")?;
    let unique: BTreeSet<_> = job.files.iter().collect();
    require(unique.len() == job.files.len(), "duplicate source path")?;
    if let Some(snapshot) = &job.snapshot {
        snapshot.validate(&job.files)?;
        if job.config.scheduler.leased() {
            let bytes = snapshot.morsels.iter().try_fold(0u64, |sum, m| {
                sum.checked_add(m.bytes)
                    .ok_or_else(|| Error::Invalid("source size overflow".into()))
            })?;
            require(
                bytes <= job.config.scheduler.max_source_bytes,
                "scheduler source byte cap",
            )?;
            crate::scheduler::verify_sources(
                &snapshot.morsels,
                &ade_exec::CancellationToken::default(),
                started + job.task_timeout.min(crate::TASK_TIMEOUT),
            )?;
            require(
                started.elapsed() < job.task_timeout,
                "scheduler admission deadline",
            )?;
        }
    }
    // JobId is intentionally cluster-local and remains part of logical transfer
    // identity. Shared object stores also need a globally unique execution scope.
    let execution_namespace = uuid::Uuid::new_v4().to_string();
    let scheduler = if job.config.scheduler.leased() {
        Some(crate::scheduler::Service::start(
            job.id,
            &execution_namespace,
            &job.workers,
            &job.config,
            job.snapshot
                .clone()
                .ok_or_else(|| Error::Invalid("leased snapshot missing".into()))?,
            job.task_timeout.saturating_sub(started.elapsed()),
        )?)
    } else {
        None
    };
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
                schedule: scheduler.as_ref().map(|s| s.spec.clone()),
                job: job.id,
                execution_namespace: execution_namespace.clone(),
                workers: job.workers.clone(),
                config: job.config.clone(),
                snapshot: job.snapshot.clone(),
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
        let mut assignments = vec![Vec::new(); job.workers.len()];
        for (i, file) in job.files.iter().enumerate() {
            assignments[i % job.workers.len()].push(file.clone());
        }
        let mut topology = None;
        let mut topology_commit_ns = 0;
        let mut topology_probe_ns = 0;
        if job.config.topology.policy == crate::topology::PartitionPolicy::SampledV1 {
            let probe_started = Instant::now();
            let deadline = probe_started + job.task_timeout;
            let observations = thread::scope(|scope| {
                let handles: Vec<_> = job
                    .workers
                    .iter()
                    .zip(&assignments)
                    .map(|(worker, files)| {
                        scope.spawn(move || {
                            match protocol::rpc_until(
                                worker,
                                Request::Probe {
                                    timeout_ms: u64::try_from(
                                        job.task_timeout.min(crate::TASK_TIMEOUT).as_millis(),
                                    )
                                    .unwrap_or(120_000),
                                    job: job.id,
                                    files: files.clone(),
                                },
                                deadline,
                            )? {
                                Reply::Probe(observation) => Ok(observation),
                                _ => Err(Error::Invalid("invalid probe response".into())),
                            }
                        })
                    })
                    .collect();
                handles
                    .into_iter()
                    .map(|h| {
                        h.join()
                            .map_err(|_| Error::Invalid("probe thread panic".into()))?
                    })
                    .collect::<Result<Vec<_>>>()
            })?;
            let decision =
                crate::topology::decide(&job.config.topology, job.workers.len(), observations)?;
            topology_probe_ns = crate::materialization::ns(probe_started);
            let commit_started = Instant::now();
            thread::scope(|scope| {
                let handles: Vec<_> = job
                    .workers
                    .iter()
                    .map(|worker| {
                        let layout = decision.layout.clone();
                        scope.spawn(move || {
                            require(
                                matches!(
                                    protocol::rpc_until(
                                        worker,
                                        Request::CommitLayout {
                                            job: job.id,
                                            layout
                                        },
                                        deadline
                                    )?,
                                    Reply::Ready
                                ),
                                "layout ACK missing",
                            )
                        })
                    })
                    .collect();
                for handle in handles {
                    handle
                        .join()
                        .map_err(|_| Error::Invalid("commit thread panic".into()))??;
                }
                Ok::<_, Error>(())
            })?;
            topology_commit_ns = crate::materialization::ns(commit_started);
            topology = Some(decision);
        }
        let active = topology.as_ref().map_or_else(
            || (0..job.workers.len() as u64).collect::<BTreeSet<_>>(),
            |d| d.layout.active(),
        );
        let layout_digest = topology.as_ref().map(|d| d.layout.digest.clone());
        #[cfg(feature = "research-hooks")]
        require(
            job.config
                .research_exit_final_partition
                .is_none_or(|p| active.contains(&p)),
            "failure target is inactive under committed layout",
        )?;
        let mut gather = crate::gather::Gather::with_context(
            parent.path(),
            job.id,
            job.workers.len(),
            active.clone(),
            scheduler
                .as_ref()
                .map(|s| (s.spec.fence.digest.as_str(), &job.config.scheduler)),
        )?;
        state = JobState::FinalsRunning;
        let producers = assignments
            .into_iter()
            .enumerate()
            .map(|(i, files)| Request::RunProducerTask {
                job: job.id,
                task: TaskId(i as u64),
                attempt: AttemptId(0),
                files: if scheduler.is_some() {
                    Vec::new()
                } else {
                    files
                },
            })
            .collect();
        let finals = job
            .workers
            .iter()
            .map(|w| {
                if active.contains(&w.id.0) {
                    Request::RunFinalTask {
                        job: job.id,
                        task: TaskId(job.workers.len() as u64 + w.id.0),
                        attempt: AttemptId(0),
                        gather: gather.address(),
                    }
                } else {
                    Request::VerifyInactive { job: job.id }
                }
            })
            .collect();
        let TaskResults {
            producers,
            mut finals,
            lost,
        } = tasks(job, producers, finals, started)?;
        crate::recovery::validate_producers(job.id, job.workers.len(), &producers)?;
        let schedule_metrics = scheduler
            .as_ref()
            .map(crate::scheduler::Service::finish)
            .transpose()?;
        if let Some(s) = &schedule_metrics {
            crate::scheduler::verify_metrics(s)?;
            require(
                lost.is_empty(),
                "leased final loss is fatal; replay forbidden",
            )?;
            for (w, m) in &producers {
                let completed: Vec<_> = s
                    .entries
                    .iter()
                    .filter_map(|e| e.completion.as_ref())
                    .filter(|c| c.lease.worker.0 == *w)
                    .collect();
                require(
                    m.source_files == completed.len()
                        && m.input_rows as u64
                            == completed.iter().map(|c| c.input_rows).sum::<u64>()
                        && m.output_rows as u64
                            == completed.iter().map(|c| c.output_rows).sum::<u64>()
                        && m.output_rows as u64 == m.sent.rows,
                    "leased source/producer conservation failure",
                )?;
            }
        }

        require(
            producers
                .values()
                .chain(finals.values())
                .all(|m| m.layout_digest == layout_digest),
            "task layout digest mismatch",
        )?;
        require(
            finals
                .iter()
                .all(|(p, m)| m.executed_final == active.contains(p)),
            "actual final task set mismatch",
        )?;
        require(
            lost.iter().all(|p| active.contains(p)),
            "inactive worker loss is unsupported",
        )?;
        if let Some(cost) = &job.config.exchange.cost {
            for producer in producers.values() {
                require(
                    producer.materializations.len() == job.workers.len(),
                    "materialization fact coverage mismatch",
                )?;
                for (partition, manifest) in &producer.manifests {
                    let fact = producer.materializations.get(partition).ok_or_else(|| {
                        Error::Invalid("missing materialization partition".into())
                    })?;
                    require(
                        fact.decision.observation.operator_work_ns
                            == producer.production.operator_work_ns
                            && fact.decision.observation.source_bytes
                                == producer.production.source_bytes
                            && fact.decision.observation.source_requests
                                == producer.production.source_requests,
                        "production observation mismatch",
                    )?;
                    require(
                        fact.topology_digest == layout_digest
                            && fact
                                .receipt
                                .as_ref()
                                .is_none_or(|r| r.topology_digest == layout_digest),
                        "materialization topology mismatch",
                    )?;
                    crate::recovery::validate_fact(
                        fact,
                        manifest,
                        cost,
                        &job.config.exchange.object_store,
                        &execution_namespace,
                    )?;
                }
            }
        }
        for (p, metrics) in &finals {
            if active.contains(p) {
                gather.commit(*p, AttemptId(0), &metrics.sent)?;
            }
        }
        let mut recovery = crate::recovery::RecoveryMetrics::default();
        if !lost.is_empty() {
            let recovery_start = Instant::now();
            let mut plan = crate::recovery::plan(
                job.id,
                job.workers.len(),
                job.snapshot.as_ref(),
                &producers,
                &finals.keys().copied().collect::<Vec<_>>(),
                &lost,
            )?;
            plan.layout_digest.clone_from(&layout_digest);
            plan.reused_partitions.retain(|p| active.contains(p));
            crate::recovery::select_cost_actions(
                &mut plan,
                &producers,
                &job.config.exchange,
                &execution_namespace,
            )?;
            require(
                protocol::rpc(
                    &job.workers[usize::try_from(lost[0])
                        .map_err(|_| Error::Invalid("invalid lost partition".into()))?],
                    Request::Ping,
                    Duration::from_millis(250),
                )
                .is_err(),
                "final task error without confirmed worker unavailability",
            )?;
            #[cfg(feature = "research-hooks")]
            if job.config.research_exit_final_partition == Some(plan.partition.0) {
                gather.wait_for_partial_prefix(plan.partition.0, Duration::from_secs(2))?;
            }
            let replay_snapshot = job
                .snapshot
                .as_ref()
                .ok_or_else(|| Error::Unrecoverable("missing source snapshot".into()))?;
            let address = gather.start_recovery(parent.path(), plan.partition.0)?;
            let reply = protocol::rpc(
                &job.workers[usize::try_from(plan.host.0)
                    .map_err(|_| Error::Invalid("invalid recovery host".into()))?],
                Request::RecoverFinal {
                    plan: plan.clone(),
                    gather: address,
                },
                job.task_timeout,
            )
            .map_err(|e| Error::Unrecoverable(format!("recovery episode failed: {e}")))?;
            let Reply::Recovered {
                mut final_metrics,
                manifests,
                work,
            } = reply
            else {
                return Err(Error::Unrecoverable("invalid recovery response".into()));
            };
            require(
                manifests.len() == plan.closures.len(),
                "replay manifest coverage mismatch",
            )?;
            for (closure, manifest) in plan.closures.iter().zip(&manifests) {
                closure.original.validate_replay(manifest)?;
            }
            final_metrics.coordinator_finished_ns = crate::materialization::ns(started);
            require(
                final_metrics.layout_digest == layout_digest && final_metrics.executed_final,
                "recovered layout/task mismatch",
            )?;
            gather.commit(plan.partition.0, AttemptId(1), &final_metrics.sent)?;
            finals.insert(plan.partition.0, final_metrics);
            recovery = crate::recovery::RecoveryMetrics {
                worker_failures_detected: 1,
                recovery_attempts: 1,
                recovered_partitions: 1,
                replayed_producer_tasks: plan
                    .closures
                    .iter()
                    .filter(|closure| closure.original.segments > 0 && closure.restore.is_none())
                    .count() as u64,
                replayed_morsels: plan
                    .closures
                    .iter()
                    .filter(|closure| closure.original.segments > 0 && closure.restore.is_none())
                    .map(|closure| {
                        replay_snapshot
                            .producer_morsels(closure.producer, job.workers.len())
                            .len() as u64
                    })
                    .sum(),
                lost_segments: plan.closures.iter().map(|c| c.original.segments).sum(),
                lost_payload_bytes: plan.closures.iter().map(|c| c.original.payload_bytes).sum(),
                restored_segments: plan
                    .closures
                    .iter()
                    .filter(|c| c.restore.is_some())
                    .map(|c| c.original.segments)
                    .sum(),
                restored_payload_bytes: plan
                    .closures
                    .iter()
                    .filter(|c| c.restore.is_some())
                    .map(|c| c.original.payload_bytes)
                    .sum(),
                object_read_requests: work.object_read_requests,
                object_read_elapsed_ns: work.object_read_elapsed_ns,
                recomputed_operator_work_ns: work.recomputed_operator_work_ns,
                recomputed_source_bytes: work.recomputed_source_bytes,
                recomputed_source_requests: work.recomputed_source_requests,
                recomputed_segments: plan
                    .closures
                    .iter()
                    .filter(|c| c.restore.is_none())
                    .map(|c| c.original.segments)
                    .sum(),
                recomputed_payload_bytes: plan
                    .closures
                    .iter()
                    .filter(|c| c.restore.is_none())
                    .map(|c| c.original.payload_bytes)
                    .sum(),
                recovery_elapsed_ms: u64::try_from(recovery_start.elapsed().as_millis())
                    .unwrap_or(u64::MAX),
                full_job_restart_avoided: true,
                planner_reason: if job.config.exchange.cost.is_some() {
                    "single_final_worker_loss_validated_restore_or_recompute"
                } else {
                    "single_final_worker_loss_recompute_file_lineage_no_durable_copy"
                }
                .into(),
                plan: Some(plan),
                replay_manifests: manifests,
                committed_attempts: BTreeMap::new(),
            };
        }
        recovery.committed_attempts = gather.attempts();
        // Validate conservation before any successful result can escape.
        let sent =
            producers
                .values()
                .fold(crate::metrics::NetworkMetrics::default(), |mut a, m| {
                    a.add(&m.sent);
                    a
                });
        let accepted =
            finals
                .values()
                .fold(crate::metrics::NetworkMetrics::default(), |mut a, m| {
                    a.add(&m.received);
                    a
                });
        require(
            sent.transfers == (job.workers.len() * job.workers.len()) as u64
                && sent.transfers == accepted.transfers
                && sent.batches == accepted.batches
                && sent.rows == accepted.rows
                && sent.payload_bytes == accepted.payload_bytes
                && accepted.rows == finals.values().map(|m| m.input_rows as u64).sum::<u64>(),
            "exchange conservation failure",
        )?;
        let (batches, gather_metrics) = if let Some(limits) = output_limits {
            let adapt = adapt.take().ok_or_else(|| {
                Error::Invalid("bounded gather requires an output adapter".into())
            })?;
            gather.collect_bounded(limits, adapt)?
        } else {
            gather.collect()?
        };
        state = JobState::GatherComplete;
        let mut adaptive = crate::adaptive::AdaptiveSummary::default();
        if job.config.exchange.mode == crate::ExchangeMode::Adaptive {
            for (partition, task) in &finals {
                if !active.contains(partition) {
                    continue;
                }
                if recovery.committed_attempts[partition] == AttemptId(1) {
                    continue;
                }
                let a = &task.retention.adaptive;
                require(
                    a.segments_by_tier.iter().sum::<u64>() == task.received.batches
                        && a.bytes_by_tier.iter().sum::<u64>() == task.received.payload_bytes
                        && a.transitions.len() <= 3
                        && a.extents <= 4,
                    "adaptive conservation/metadata bound failure",
                )?;
                adaptive.add(a);
            }
        }
        let inactive_ids: Vec<_> = finals
            .keys()
            .filter(|p| !active.contains(p))
            .copied()
            .collect();
        let inactive = inactive_ids
            .into_iter()
            .filter_map(|p| finals.remove(&p).map(|m| (p, m)))
            .collect();
        let metrics = DistributedMetrics {
            scheduler: schedule_metrics,
            topology,
            topology_commit_ns,
            topology_probe_ns,
            inactive,
            recovery,
            adaptive,
            worker_count: job.workers.len(),
            exchange_mode: job.config.exchange.mode,
            elapsed_ms: 0,
            received: finals
                .iter()
                .map(|(id, m)| (*id, m.received.clone()))
                .collect(),
            producers,
            finals,
            gather: gather_metrics,
        };
        drop(gather);
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
    let mut result = result;
    if job.config.exchange.cost.is_some() {
        let mut cleaned = false;
        for worker in &job.workers {
            if protocol::rpc(worker, Request::Ping, Duration::from_millis(250)).is_ok() {
                match protocol::rpc(
                    worker,
                    Request::CleanupMaterializations {
                        job: job.id,
                        execution_namespace: execution_namespace.clone(),
                    },
                    job.task_timeout,
                ) {
                    Ok(Reply::Ready) => {
                        cleaned = true;
                        break;
                    }
                    other => eprintln!("ADE materialization cleanup failed: {other:?}"),
                }
            }
        }
        if !cleaned {
            eprintln!(
                "ADE materialization cleanup failed for job {:?}; parent teardown required",
                job.id
            );
            if result.is_ok() {
                result = Err(Error::Invalid("materialization cleanup failed".into()));
            }
        }
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
        Self::spawn_inner(binary, workers, false)
    }
    /// Parent-owned fixture survives child process loss, never host loss.
    /// # Errors
    /// Returns process, filesystem and readiness failures.
    pub fn spawn_with_materialization(binary: &Path, workers: usize) -> Result<Self> {
        Self::spawn_inner(binary, workers, true)
    }
    fn spawn_inner(binary: &Path, workers: usize, materialize: bool) -> Result<Self> {
        require((2..=4).contains(&workers), "workers must be 2..4")?;
        let mut cluster = Self {
            workers: Vec::new(),
            children: Vec::new(),
            root: tempfile::Builder::new().prefix("ade-workers-").tempdir()?,
        };
        let shared = cluster.root.path().join("shared-objects");
        if materialize {
            std::fs::create_dir(&shared)?;
        }
        for id in 0..workers {
            let mut command = Command::new(binary);
            command.arg(id.to_string()).arg(cluster.root.path());
            if materialize {
                command.arg(&shared);
            }
            let mut child = command
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
