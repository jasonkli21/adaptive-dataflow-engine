//! Versioned, opt-in managed control. Legacy ADETCP12 is unchanged.
use crate::{
    Error, Result, WorkerEndpoint,
    protocol::{self, Reply, Request},
    require,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
    io::{Read, Write},
    time::Instant,
};
pub const MAGIC: &[u8; 8] = b"ADEOPS01";
pub const RUNTIME_ID: &str = env!("ADE_MANAGED_RUNTIME");
pub const DISK_BYTES: u64 = 64 * 1024 * 1024;
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Fence {
    pub version: u32,
    pub boot: String,
    pub store: String,
    pub owner: u64,
    pub instance: String,
    pub job: u64,
    pub disk_bytes: u64,
    pub scratch: String,
}
impl Fence {
    /// # Errors
    /// Refuses malformed, missing, foreign-version and empty owner identities.
    pub fn validate(&self) -> Result<()> {
        require(
            self.scratch.len() <= 4096
                && std::path::Path::new(&self.scratch).is_absolute()
                && (1..=DISK_BYTES).contains(&self.disk_bytes)
                && self.version == 1
                && self.owner > 0
                && self.job > 0
                && [&self.boot, &self.store, &self.instance]
                    .iter()
                    .all(|s| uuid::Uuid::parse_str(s).is_ok_and(|id| !id.is_nil())),
            "managed fence/version",
        )
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Boot {
    pub version: u32,
    pub store: String,
    pub instance: String,
    pub research_hooks: bool,
    pub runtime: String,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Ready {
    pub endpoint: WorkerEndpoint,
    pub boot: Boot,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum Operation {
    Hello,
    Claim,
    Drain,
    Call(Box<Request>),
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Message {
    pub fence: Fence,
    pub operation: Operation,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum Response {
    Hello(Box<Ready>),
    Ready,
    Drained { disk_bytes: u64 },
    Reply(Box<Reply>),
    Error { code: String, message: String },
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Answer {
    pub fence: Fence,
    pub response: Response,
}
/// # Errors
/// Refuses oversized data before writing a versioned bounded frame.
pub fn write_message(output: &mut impl Write, value: &impl Serialize) -> Result<()> {
    let mut bytes = crate::exchange::BoundedBuffer::new(protocol::MAX_CONTROL_BYTES);
    serde_json::to_writer(&mut bytes, value)?;
    output.write_all(MAGIC)?;
    output.write_all(
        &u32::try_from(bytes.0.len())
            .map_err(|_| Error::Invalid("managed frame length".into()))?
            .to_le_bytes(),
    )?;
    output.write_all(&bytes.0)?;
    Ok(())
}
/// # Errors
/// Refuses legacy/foreign versions and oversized/truncated managed frames.
pub fn read_message<T: DeserializeOwned>(input: &mut impl Read) -> Result<T> {
    let mut magic = [0; 8];
    input.read_exact(&mut magic)?;
    require(&magic == MAGIC, "managed protocol requires ADEOPS01")?;
    let mut length = [0; 4];
    input.read_exact(&mut length)?;
    let length = u32::from_le_bytes(length) as usize;
    require(length <= protocol::MAX_CONTROL_BYTES, "managed frame cap")?;
    let mut bytes = vec![0; length];
    input.read_exact(&mut bytes)?;
    Ok(serde_json::from_slice(&bytes)?)
}
/// # Errors
/// Rejects foreign/stale fences, deadline/transport failures and managed refusals.
pub fn rpc(endpoint: &WorkerEndpoint, operation: Operation, until: Instant) -> Result<Response> {
    let fence = endpoint
        .managed
        .as_ref()
        .ok_or_else(|| Error::Invalid("managed endpoint fence required".into()))?;
    fence.validate()?;
    let mut socket = protocol::connect(
        endpoint.control,
        until.saturating_duration_since(Instant::now()),
    )?;
    let mut stream =
        protocol::DeadlineStream::new(&mut socket, until.saturating_duration_since(Instant::now()));
    write_message(
        &mut stream,
        &Message {
            fence: fence.clone(),
            operation,
        },
    )?;
    let answer: Answer = read_message(&mut stream)?;
    require(answer.fence == *fence, "stale managed response")?;
    match answer.response {
        Response::Error { code, message } => Err(Error::Remote { code, message }),
        response => Ok(response),
    }
}
pub(crate) fn call(endpoint: &WorkerEndpoint, request: Request, until: Instant) -> Result<Reply> {
    match rpc(endpoint, Operation::Call(Box::new(request)), until)? {
        Response::Reply(reply) => match *reply {
            Reply::Error { code, message } => Err(Error::Remote { code, message }),
            reply => Ok(reply),
        },
        _ => Err(Error::Invalid("managed native response required".into())),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn oversized_truncated_and_foreign_managed_frames_refuse() {
        let mut bytes = MAGIC.to_vec();
        bytes.extend(u32::MAX.to_le_bytes());
        assert!(read_message::<Operation>(&mut bytes.as_slice()).is_err());
        let mut bytes = vec![];
        write_message(&mut bytes, &Operation::Hello).unwrap();
        bytes.pop();
        assert!(read_message::<Operation>(&mut bytes.as_slice()).is_err());
        assert!(write_message(&mut vec![], &"x".repeat(protocol::MAX_CONTROL_BYTES)).is_err());
    }
    #[test]
    fn stale_response_fence_is_refused_over_real_socket() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = WorkerEndpoint {
            id: crate::ids::WorkerId(0),
            control: listener.local_addr().unwrap(),
            exchange: "127.0.0.1:1".parse().unwrap(),
            managed: Some(Fence {
                version: 1,
                boot: uuid::Uuid::new_v4().to_string(),
                store: uuid::Uuid::new_v4().to_string(),
                owner: 1,
                instance: uuid::Uuid::new_v4().to_string(),
                job: 1,
                disk_bytes: DISK_BYTES,
                scratch: "/tmp/managed-test".into(),
            }),
        };
        let thread = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            let message: Message = read_message(&mut socket).unwrap();
            let mut fence = message.fence;
            fence.owner += 1;
            write_message(
                &mut socket,
                &Answer {
                    fence,
                    response: Response::Ready,
                },
            )
            .unwrap();
        });
        assert!(
            rpc(
                &endpoint,
                Operation::Claim,
                Instant::now() + std::time::Duration::from_secs(2)
            )
            .is_err()
        );
        thread.join().unwrap();
        let mut fence = endpoint.managed.unwrap();
        fence.version = 2;
        assert!(fence.validate().is_err());
        fence.version = 1;
        fence.disk_bytes = DISK_BYTES + 1;
        assert!(fence.validate().is_err());
    }
    #[test]
    fn managed_and_legacy_framing_refuse_each_other() {
        let mut bytes = vec![];
        write_message(&mut bytes, &Operation::Hello).unwrap();
        assert!(protocol::read_message::<Operation>(&mut bytes.as_slice()).is_err());
        let mut bytes = vec![];
        protocol::write_message(&mut bytes, &Request::Ping).unwrap();
        assert!(read_message::<Request>(&mut bytes.as_slice()).is_err());
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Terminal {
    pub partition: u64,
    pub rows: u64,
    pub batches: u64,
    pub bytes: u64,
    pub fingerprint: String,
}
/// Only the native streaming adapter can construct successful completion proof.
#[derive(Debug, Serialize)]
pub struct Completion {
    terminals: Vec<Terminal>,
    disk_bytes: u64,
}
impl Completion {
    #[must_use]
    pub fn terminals(&self) -> &[Terminal] {
        &self.terminals
    }
    #[must_use]
    pub fn disk_bytes(&self) -> u64 {
        self.disk_bytes
    }
}
#[derive(Debug, Clone, Copy)]
pub enum Stage {
    Prepared,
}
/// Execute through the existing native worker operators and conservation gates,
/// streaming only committed final partitions. Caller owns durable claim/fencing,
/// source filesystem identity, cancellation delivery and external publication.
/// # Errors
/// Returns no completion proof on any native/schema/source/quota/sink failure.
#[allow(
    clippy::too_many_lines,
    reason = "Two bounded native graphs share exact preflight/postflight and streaming validation."
)]
pub fn execute_to_sink(
    request: crate::physical::Request<'_>,
    root: &std::path::Path,
    token: &ade_exec::CancellationToken,
    sink: impl FnMut(arrow::record_batch::RecordBatch) -> Result<()>,
) -> Result<Completion> {
    execute_to_sink_observed(request, root, token, sink, |_| {})
}
/// Native stage observation for fault evidence; never changes the admitted graph.
/// # Errors
/// Refuses the same graph/source/fence/quota failures as `execute_to_sink`.
#[allow(
    clippy::too_many_lines,
    reason = "Shared native graph and terminal validation boundary."
)]
pub fn execute_to_sink_observed(
    request: crate::physical::Request<'_>,
    root: &std::path::Path,
    token: &ade_exec::CancellationToken,
    mut sink: impl FnMut(arrow::record_batch::RecordBatch) -> Result<()>,
    mut progress: impl FnMut(Stage),
) -> Result<Completion> {
    use crate::{
        ids::{AttemptId, TaskId, transfer},
        lineage::{SourceMorsel, SourceSnapshot},
        metrics::NetworkMetrics,
    };
    let crate::physical::Request {
        plan,
        catalog,
        workers,
        job,
        options,
    } = request;
    plan.validate().map_err(|e| Error::Invalid(e.to_string()))?;
    crate::coordinator::validate_topology(&workers)?;
    require(
        workers
            .iter()
            .all(|w| w.managed.as_ref().is_some_and(|f| f.job == job.0)),
        "managed fixed worker fence coverage",
    )?;
    let fence = workers
        .first()
        .and_then(|w| w.managed.as_ref())
        .ok_or_else(|| Error::Invalid("managed fence required".into()))?;
    require(
        workers.iter().all(|w| {
            w.managed.as_ref().is_some_and(|f| {
                f.store == fence.store && f.owner == fence.owner && f.instance == fence.instance
            })
        }),
        "managed shared instance fence mismatch",
    )?;
    let until = Instant::now() + options.timeout;
    require(
        !options.timeout.is_zero() && options.timeout <= crate::TASK_TIMEOUT,
        "managed task deadline",
    )?;
    catalog.verify(plan, token, until, true)?;
    let mut base = plan.output;
    while let ade_physical::Operator::Project { input, .. } = plan.nodes[base as usize].operator {
        base = input;
    }
    let mut output =
        crate::bounded_output::OutputBudget::new(crate::bounded_output::OutputLimits {
            rows: plan.max_output_rows,
            bytes: plan.max_output_bytes,
        })?;
    let mut emit = |batch| {
        token.check()?;
        require(Instant::now() < until, "managed gather deadline")?;
        let batch = crate::physical::adapt(plan, base, &batch)?;
        output.include(&batch)?;
        sink(batch)
    };
    let disk = workers[0]
        .managed
        .as_ref()
        .ok_or_else(|| Error::Invalid("managed fence".into()))?
        .disk_bytes;
    require(
        workers
            .iter()
            .all(|w| w.managed.as_ref().is_some_and(|f| f.disk_bytes == disk)),
        "managed quota cohort mismatch",
    )?;
    let budget = crate::disk::Budget::new(disk);
    let terminals = match plan.target {
        ade_physical::Target::DistributedAggregate => {
            require(
                !options.aggregate.spill_enabled
                    && options.aggregate.exchange.mode == crate::ExchangeMode::Buffered,
                "managed aggregate spill/exchange refusal",
            )?;
            let binding = &catalog.bindings[0];
            let source = &binding.source;
            let snapshot = SourceSnapshot {
                version: 1,
                manifest_fingerprint: source.manifest.clone(),
                schema_fingerprint: source.schema_identity.clone(),
                logical_fingerprint: source.logical.clone(),
                replayable: false,
                morsels: binding
                    .files
                    .iter()
                    .zip(&source.files)
                    .enumerate()
                    .map(|(i, (path, f))| SourceMorsel {
                        id: crate::ids::MorselId(i as u64),
                        path: path.clone(),
                        fingerprint: f.fingerprint.clone(),
                        bytes: f.bytes,
                    })
                    .collect(),
            };
            snapshot.validate(&binding.files)?;
            let spec = protocol::JobSpec {
                schedule: None,
                job,
                execution_namespace: fence.instance.clone(),
                workers: workers.clone(),
                config: options.aggregate.clone(),
                snapshot: Some(snapshot.clone()),
            };
            for worker in &workers {
                require(
                    matches!(
                        protocol::rpc_until(worker, Request::PrepareJob(spec.clone()), until)?,
                        Reply::Ready
                    ),
                    "managed prepare ACK",
                )?;
            }
            progress(Stage::Prepared);
            let mut gather = crate::gather::Gather::with_budget(
                root,
                job,
                workers.len(),
                (0..workers.len() as u64).collect(),
                None,
                Some(&budget),
            )?;
            let aggregate = crate::coordinator::StaticAggregateJob {
                id: job,
                files: binding.files.clone(),
                workers: workers.clone(),
                config: options.aggregate,
                task_timeout: until.saturating_duration_since(Instant::now()),
                snapshot: Some(snapshot),
            };
            let producers = (0..workers.len())
                .map(|i| Request::RunProducerTask {
                    job,
                    task: TaskId(i as u64),
                    attempt: AttemptId(0),
                    files: binding
                        .files
                        .iter()
                        .enumerate()
                        .filter(|(j, _)| j % workers.len() == i)
                        .map(|(_, p)| p.clone())
                        .collect(),
                })
                .collect();
            let finals = (0..workers.len())
                .map(|i| Request::RunFinalTask {
                    job,
                    task: TaskId((workers.len() + i) as u64),
                    attempt: AttemptId(0),
                    gather: gather.address(),
                })
                .collect();
            let tasks = crate::coordinator::tasks(&aggregate, producers, finals, Instant::now())?;
            require(
                tasks.lost.is_empty() && tasks.finals.len() == workers.len(),
                "managed engine worker loss; whole-job reconciliation required",
            )?;
            crate::recovery::validate_producers(job, workers.len(), &tasks.producers)?;
            let mut sent = NetworkMetrics::default();
            let mut accepted = NetworkMetrics::default();
            for p in tasks.producers.values() {
                sent.add(&p.sent);
            }
            for (partition, f) in &tasks.finals {
                require(
                    f.executed_final && f.input_rows as u64 == f.received.rows,
                    "managed final execution/row coverage",
                )?;
                accepted.add(&f.received);
                gather.commit(*partition, AttemptId(0), &f.sent)?;
                gather.verify_terminal(
                    *partition,
                    f.manifests
                        .get(&0)
                        .ok_or_else(|| Error::Invalid("managed final manifest required".into()))?,
                )?;
            }
            require(
                sent.transfers == (workers.len() * workers.len()) as u64
                    && same_network(&sent, &accepted),
                "managed aggregate exchange conservation",
            )?;
            gather.stream(|_, batch| emit(batch))?
        }
        ade_physical::Target::DistributedJoin => {
            require(
                !options.join.local.spill_enabled
                    && options.join.local.algorithm == ade_exec::join::Algorithm::HashOnly,
                "managed join spill/algorithm refusal",
            )?;
            let mut sources = vec![];
            for (binding, side) in catalog
                .bindings
                .iter()
                .zip([ade_exec::join::Side::Left, ade_exec::join::Side::Right])
            {
                let source = crate::join::Source::capture(
                    side,
                    binding
                        .files
                        .iter()
                        .zip(&binding.source.files)
                        .map(|(p, f)| (p.clone(), f.rows))
                        .collect(),
                )?;
                require(
                    source
                        .files
                        .iter()
                        .zip(&binding.source.files)
                        .all(|(a, b)| {
                            a.blake3 == b.fingerprint && a.bytes == b.bytes && a.rows == b.rows
                        }),
                    "managed join capture drift",
                )?;
                sources.push(source);
            }
            let spec = crate::join::Spec {
                job,
                execution: fence.instance.clone(),
                workers: workers.clone(),
                sources: sources
                    .try_into()
                    .map_err(|_| Error::Invalid("join source coverage".into()))?,
                config: options.join,
            };
            let (decision, probe_ns, commit_ns) = crate::join::prepare(&spec, until)?;
            progress(Stage::Prepared);
            let (stores, receiver) =
                crate::join::gather_budget(&spec, &decision, root, token, Some(&budget))?;
            let (producers, finals) = crate::join::tasks(&spec, until, receiver.address, token)?;
            let mut gather = NetworkMetrics::default();
            let mut terminals = vec![];
            for (partition, store) in stores.iter().enumerate() {
                let observed = store.metrics()?;
                require(
                    same_network(&observed, &finals[&(partition as u64)].gather),
                    "managed join gather conservation",
                )?;
                gather.add(&observed);
                terminals.push(Terminal {
                    partition: partition as u64,
                    rows: observed.rows,
                    batches: observed.batches,
                    bytes: observed.payload_bytes,
                    fingerprint: store.terminal(transfer(
                        job,
                        4,
                        0,
                        (workers.len() + partition) as u64,
                    ))?,
                });
            }
            for store in &stores {
                let mut reader = store.reader();
                while let Some(batch) = reader.next_batch(token)? {
                    emit(batch)?;
                }
                reader.shutdown();
                store.validate_retention_drained()?;
            }
            let metrics = crate::join::Metrics {
                spec,
                version: 1,
                decision,
                probe_ns,
                commit_ns,
                elapsed_ns: 0,
                producers,
                finals,
                gather,
                gather_retention: stores
                    .iter()
                    .enumerate()
                    .map(|(p, s)| Ok((p as u64, s.retention_metrics()?)))
                    .collect::<Result<_>>()?,
            };
            crate::join::validate_metrics(&metrics)?;
            drop(receiver);
            terminals
        }
        ade_physical::Target::Local => {
            return Err(Error::Invalid("managed distributed targets only".into()));
        }
    };
    catalog.verify(plan, token, until, false)?;
    token.check()?;
    require(Instant::now() < until, "managed native postflight deadline")?;
    Ok(Completion {
        terminals,
        disk_bytes: budget.used(),
    })
}
fn same_network(a: &crate::metrics::NetworkMetrics, b: &crate::metrics::NetworkMetrics) -> bool {
    a.transfers == b.transfers
        && a.rows == b.rows
        && a.batches == b.batches
        && a.payload_bytes == b.payload_bytes
}

/// Claim the entire fixed cohort only after the coordinator's durable claim.
/// # Errors
/// Any missing/mismatched boot or stale owner leaves reconciliation required.
pub fn claim(workers: &[WorkerEndpoint], until: Instant) -> Result<()> {
    crate::coordinator::validate_topology(workers)?;
    let common = workers
        .first()
        .and_then(|w| w.managed.as_ref())
        .ok_or_else(|| Error::Invalid("managed claim fence required".into()))?;
    require(
        workers.iter().all(|w| {
            w.managed.as_ref().is_some_and(|f| {
                f.store == common.store
                    && f.owner == common.owner
                    && f.job == common.job
                    && f.instance == common.instance
                    && f.disk_bytes == common.disk_bytes
            })
        }),
        "mixed managed claim contexts",
    )?;
    for worker in workers {
        let Response::Hello(ready) = rpc(worker, Operation::Hello, until)? else {
            return Err(Error::Invalid("managed capability hello required".into()));
        };
        require(
            ready.boot.version == 1
                && ready.boot.runtime == RUNTIME_ID
                && ready.boot.research_hooks == cfg!(feature = "research-hooks")
                && ready.endpoint.id == worker.id
                && ready.endpoint.control == worker.control
                && ready.endpoint.exchange == worker.exchange,
            "managed native feature/endpoint mismatch",
        )?;
        require(
            matches!(rpc(worker, Operation::Claim, until)?, Response::Ready),
            "managed claim ACK",
        )?;
    }
    Ok(())
}
/// Prove no old native tasks, stores, receiver writes or retention I/O owners remain.
/// # Errors
/// Unreachable/mismatched/poisoned/undrainable workers must never be retried automatically.
pub fn drain(workers: &[WorkerEndpoint], until: Instant) -> Result<Vec<u64>> {
    std::thread::scope(|scope| {
        let handles: Vec<_> = workers
            .iter()
            .map(|worker| {
                scope.spawn(move || match rpc(worker, Operation::Drain, until)? {
                    Response::Drained { disk_bytes } => Ok(disk_bytes),
                    _ => Err(Error::Invalid("managed drain ACK".into())),
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| {
                h.join()
                    .map_err(|_| Error::Invalid("drain thread panic".into()))?
            })
            .collect()
    })
}
