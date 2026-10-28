//! Bounded two-source join graph. All process losses fail closed; no aggregate replay is reused.
use crate::{
    Error, Result, WorkerEndpoint,
    exchange::{self, HashOwner, IngressLimits, Payload, Receiver, Sender, Store},
    ids::{JobId, transfer},
    join_policy::{self, Decision, Distribution, Observation, Policy},
    metrics::NetworkMetrics,
    protocol::{self, Reply, Request},
    require,
};
use ade_exec::{
    BatchSource, CancellationToken,
    join::Side,
    row_source::{self, ParquetRows},
};
use arrow::{
    array::{UInt32Array, UInt64Array},
    compute::take,
    record_batch::RecordBatch,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::Read,
    net::{SocketAddr, TcpListener},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub policy: Policy,
    pub build: Option<Side>,
    pub local: ade_exec::join::Config,
    pub probe_rows: u64,
    pub probe_files: usize,
    pub probe_read_bytes: u64,
    pub broadcast_rows: u64,
    pub broadcast_bytes: u64,
    pub broadcast_amplification_bytes: u64,
    #[cfg(feature = "research-hooks")]
    pub research_exit_final: Option<u64>,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            policy: Policy::ObservedV1,
            build: None,
            local: ade_exec::join::Config::default(),
            probe_rows: 4096,
            probe_files: 4,
            probe_read_bytes: 8 * 1024 * 1024,
            broadcast_rows: 4096,
            broadcast_bytes: 1024 * 1024,
            broadcast_amplification_bytes: 4 * 1024 * 1024,
            #[cfg(feature = "research-hooks")]
            research_exit_final: None,
        }
    }
}
impl Config {
    /// # Errors
    /// Rejects unbounded controls and non-reserving local configurations.
    pub fn validate(&self) -> Result<()> {
        self.local.validate()?;
        require(
            (1..=4096).contains(&self.probe_rows)
                && (1..=64).contains(&self.probe_files)
                && (1..=8 * 1024 * 1024).contains(&self.probe_read_bytes)
                && (1..=65536).contains(&self.broadcast_rows)
                && (1..=4 * 1024 * 1024).contains(&self.broadcast_bytes)
                && (1..=16 * 1024 * 1024).contains(&self.broadcast_amplification_bytes),
            "join policy bounds",
        )
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceFile {
    pub path: PathBuf,
    pub rows: u64,
    pub bytes: u64,
    pub blake3: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Source {
    pub side: Side,
    pub identity: String,
    pub files: Vec<SourceFile>,
}
impl Source {
    /// # Errors
    /// Rejects excess source files/bytes/rows and captures each file's exact bytes.
    pub fn capture(side: Side, files: Vec<(PathBuf, u64)>) -> Result<Self> {
        require(
            !files.is_empty() && files.len() <= 64,
            "join source file cap",
        )?;
        let mut captured = Vec::with_capacity(files.len());
        let mut total = 0u64;
        for (path, rows) in files {
            let bytes = path.metadata()?.len();
            total = total
                .checked_add(bytes)
                .ok_or_else(|| Error::Invalid("source length overflow".into()))?;
            require(total <= 64 * 1024 * 1024, "join source byte cap")?;
            captured.push(SourceFile {
                blake3: fingerprint(&path)?,
                path,
                rows,
                bytes,
            });
        }
        let identity = blake3::hash(&serde_json::to_vec(&(1u32, side, &captured))?)
            .to_hex()
            .to_string();
        let source = Self {
            side,
            identity,
            files: captured,
        };
        source.validate()?;
        Ok(source)
    }
    fn validate(&self) -> Result<()> {
        require(
            !self.files.is_empty() && self.files.len() <= 64,
            "source file count",
        )?;
        let mut paths = BTreeSet::new();
        let mut rows = 0u64;
        let mut bytes = 0u64;
        for f in &self.files {
            rows = rows
                .checked_add(f.rows)
                .ok_or_else(|| Error::Invalid("source rows overflow".into()))?;
            bytes = bytes
                .checked_add(f.bytes)
                .ok_or_else(|| Error::Invalid("source bytes overflow".into()))?;
            require(
                paths.insert(&f.path) && f.path.is_absolute() && f.blake3.len() == 64,
                "source identity/path",
            )?;
        }
        require(
            rows <= 1_000_000 && bytes <= 64 * 1024 * 1024,
            "source domain cap",
        )?;
        require(
            self.identity
                == blake3::hash(&serde_json::to_vec(&(1u32, self.side, &self.files))?)
                    .to_hex()
                    .as_str(),
            "source digest",
        )
    }
    fn assigned(&self, worker: usize, n: usize) -> Vec<SourceFile> {
        self.files
            .iter()
            .enumerate()
            .filter(|(i, _)| i % n == worker)
            .map(|(_, f)| f.clone())
            .collect()
    }
}
fn fingerprint(path: &Path) -> Result<String> {
    let mut file = File::open(path)?;
    let mut hasher = blake3::Hasher::new();
    let mut buf = vec![0; 64 * 1024];
    let mut total = 0;
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        total += n;
        require(total <= 64 * 1024 * 1024, "source fingerprint cap")?;
        hasher.update(&buf[..n]);
    }
    Ok(hasher.finalize().to_hex().to_string())
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Spec {
    pub job: JobId,
    pub execution: String,
    pub workers: Vec<WorkerEndpoint>,
    pub sources: [Source; 2],
    pub config: Config,
}
impl Spec {
    fn validate(&self) -> Result<()> {
        self.config.validate()?;
        crate::coordinator::validate_topology(&self.workers)?;
        require(
            uuid::Uuid::parse_str(&self.execution).is_ok(),
            "join execution UUID",
        )?;
        for (i, s) in self.sources.iter().enumerate() {
            s.validate()?;
            require(s.side.index() == i, "join source orientation")?;
        }
        let paths: BTreeSet<_> = self
            .sources
            .iter()
            .flat_map(|s| s.files.iter().map(|f| &f.path))
            .collect();
        require(
            paths.len() == self.sources.iter().map(|s| s.files.len()).sum::<usize>(),
            "join sides share paths",
        )?;
        #[cfg(feature = "research-hooks")]
        require(
            self.config
                .research_exit_final
                .is_none_or(|w| w < self.workers.len() as u64),
            "join failure target",
        )?;
        Ok(())
    }
    fn digest(&self, d: &Decision) -> Result<String> {
        let mut d = d.clone();
        d.digest.clear();
        Ok(blake3::hash(&serde_json::to_vec(&(
            1u32,
            self.job,
            &self.execution,
            &self.sources,
            &self.config,
            self.workers.len(),
            crate::partition::HASH_VERSION,
            [2u64, 3, 4],
            d,
        ))?)
        .to_hex()
        .to_string())
    }
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskMetrics {
    pub rows: [u64; 2],
    pub source: [row_source::Stats; 2],
    pub retention: [crate::RetentionMetrics; 2],
    pub sent: [NetworkMetrics; 2],
    pub received: [NetworkMetrics; 2],
    pub gather: NetworkMetrics,
    pub local: Option<ade_exec::join::Metrics>,
    pub elapsed_ns: u64,
    pub source_verify_ns: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Metrics {
    pub spec: Spec,
    pub version: u32,
    pub decision: Decision,
    pub probe_ns: u64,
    pub commit_ns: u64,
    pub elapsed_ns: u64,
    pub producers: BTreeMap<u64, TaskMetrics>,
    pub finals: BTreeMap<u64, TaskMetrics>,
    pub gather: NetworkMetrics,
    pub gather_retention: BTreeMap<u64, crate::RetentionMetrics>,
}
pub struct Output {
    pub batches: Vec<RecordBatch>,
    pub metrics: Metrics,
}
struct Committed {
    decision: Decision,
    stores: [Arc<Store>; 2],
}
pub(crate) struct Session {
    disk: Option<Arc<crate::disk::Budget>>,
    spec: Spec,
    worker: usize,
    parent: PathBuf,
    pub(crate) token: CancellationToken,
    probe: Mutex<Option<Vec<Observation>>>,
    committed: Mutex<Option<Committed>>,
    producer: AtomicBool,
    final_task: AtomicBool,
}
fn limits(rows: u64) -> IngressLimits {
    IngressLimits {
        rows: rows.max(1),
        bytes: 128 * 1024 * 1024,
        batches: 1_000_000,
    }
}
fn spill_config() -> crate::ExchangeConfig {
    crate::ExchangeConfig {
        mode: crate::ExchangeMode::Spill,
        ..Default::default()
    }
}
impl Session {
    #[cfg(test)]
    pub(crate) fn new(spec: Spec, worker: usize, parent: &Path) -> Result<Self> {
        Self::new_budget(spec, worker, parent, None)
    }
    pub(crate) fn new_budget(
        spec: Spec,
        worker: usize,
        parent: &Path,
        disk: Option<Arc<crate::disk::Budget>>,
    ) -> Result<Self> {
        spec.validate()?;
        require(worker < spec.workers.len(), "join worker")?;
        Ok(Self {
            disk,
            spec,
            worker,
            parent: parent.into(),
            token: CancellationToken::default(),
            probe: Mutex::new(None),
            committed: Mutex::new(None),
            producer: AtomicBool::new(false),
            final_task: AtomicBool::new(false),
        })
    }
    pub(crate) fn id(&self) -> JobId {
        self.spec.job
    }
    pub(crate) fn poisoned(&self) -> bool {
        self.committed
            .lock()
            .expect("join commit")
            .as_ref()
            .is_some_and(|c| c.stores.iter().any(|s| s.is_poisoned()))
    }
    pub(crate) fn store(&self, header: &exchange::Header) -> Result<Arc<Store>> {
        self.token.check()?;
        require(header.id.job == self.spec.job, "join job mismatch")?;
        let side = match header.id.exchange.0 {
            2 => 0,
            3 => 1,
            _ => return Err(Error::Invalid("join side/stage mismatch".into())),
        };
        let current = self.committed.lock().expect("join commit");
        let c = current
            .as_ref()
            .ok_or_else(|| Error::Invalid("join traffic before strategy ACK".into()))?;
        require(
            header.context.as_deref() == Some(c.decision.digest.as_str()),
            "join traffic context",
        )?;
        Ok(c.stores[side].clone())
    }
    pub(crate) fn probe(&self) -> Result<Vec<Observation>> {
        self.token.check()?;
        let mut saved = self.probe.lock().expect("join probe");
        require(saved.is_none(), "duplicate join probe")?;
        let mut result = Vec::with_capacity(2);
        for source in &self.spec.sources {
            let assigned = source.assigned(self.worker, self.spec.workers.len());
            let selected: Vec<_> = assigned
                .iter()
                .take(self.spec.config.probe_files)
                .map(|f| f.path.clone())
                .collect();
            let mut scan = ParquetRows::new(
                selected,
                row_source::Config {
                    mode: row_source::Mode::Prefix,
                    max_rows: usize::try_from(self.spec.config.probe_rows).expect("validated rows"),
                    max_files: self.spec.config.probe_files,
                    max_read_bytes: self.spec.config.probe_read_bytes,
                    batch_rows: 256,
                    ..Default::default()
                },
            )?;
            while scan.next_batch(&self.token)?.is_some() {}
            let stats = scan.stats();
            scan.shutdown();
            let assigned_rows = assigned.iter().map(|f| f.rows).sum();
            let complete = stats.complete
                && stats.completed_files == assigned.len()
                && stats.rows == assigned_rows;
            result.push(Observation {
                worker: self.worker as u64,
                side: source.side,
                rows: stats.rows,
                assigned_rows,
                assigned_files: assigned.len(),
                completed_files: stats.completed_files,
                read_bytes: stats.read_bytes,
                metadata_bytes: stats.metadata_bytes,
                decode_peak_bytes: stats.decode_chunk_peak_bytes,
                batch_peak_bytes: stats.peak_batch_bytes,
                complete,
            });
        }
        *saved = Some(result.clone());
        Ok(result)
    }
    pub(crate) fn commit(&self, decision: Decision) -> Result<()> {
        self.token.check()?;
        let observed = self.probe.lock().expect("join probe");
        let own = observed
            .as_ref()
            .ok_or_else(|| Error::Invalid("join commit before probe".into()))?;
        let chosen = join_policy::choose(
            &self.spec.config,
            decision.observations.clone(),
            self.spec.workers.len(),
        )?;
        require(
            decision.version == 1
                && decision.digest == self.spec.digest(&chosen)?
                && serde_json::to_value(&decision)?
                    == serde_json::to_value(Decision {
                        digest: decision.digest.clone(),
                        ..chosen
                    })?
                && &decision.observations[self.worker * 2..self.worker * 2 + 2] == own,
            "join decision mismatch",
        )?;
        let mut current = self.committed.lock().expect("join commit");
        require(
            current.is_none()
                && !self.producer.load(Ordering::Acquire)
                && !self.final_task.load(Ordering::Acquire),
            "duplicate/late join decision",
        )?;
        let mut stores = Vec::with_capacity(2);
        for side in 0..2 {
            let broadcast = decision.distribution == Distribution::BroadcastBuild
                && decision.build.index() == side;
            stores.push(Arc::new(Store::typed_budget(
                &self.parent,
                (0..self.spec.workers.len())
                    .map(|p| transfer(self.spec.job, side as u64 + 2, self.worker as u64, p as u64))
                    .collect(),
                self.token.clone(),
                if self.disk.is_some() {
                    crate::ExchangeConfig {
                        buffer_bytes: 1,
                        ..spill_config()
                    }
                } else {
                    spill_config()
                },
                Payload::JoinRows,
                decision.digest.clone(),
                limits(self.spec.config.local.max_input_rows_per_side),
                (!broadcast).then_some(HashOwner {
                    workers: self.spec.workers.len(),
                    destination: self.worker,
                }),
                self.disk.clone(),
            )?));
        }
        *current = Some(Committed {
            decision,
            stores: stores
                .try_into()
                .map_err(|_| Error::Invalid("join stores".into()))?,
        });
        Ok(())
    }
    fn committed(&self) -> Result<(Decision, [Arc<Store>; 2])> {
        let c = self.committed.lock().expect("join commit");
        let c = c
            .as_ref()
            .ok_or_else(|| Error::Invalid("join task before strategy ACK".into()))?;
        Ok((c.decision.clone(), c.stores.clone()))
    }
    pub(crate) fn produce(&self) -> Result<TaskMetrics> {
        self.token.check()?;
        let (decision, _) = self.committed()?;
        require(
            !self.producer.swap(true, Ordering::AcqRel),
            "duplicate join producer",
        )?;
        let start = Instant::now();
        let mut metrics = TaskMetrics::default();
        // Spill-retained consumers accept all left traffic without waiting for a right reader.
        // Close each complete side before opening the next: <=N receiver slots, no bilateral cycle.
        for side in 0..2 {
            let files = self.spec.sources[side].assigned(self.worker, self.spec.workers.len());
            let verify = Instant::now();
            for f in &files {
                self.token.check()?;
                require(
                    f.path.metadata()?.len() == f.bytes && fingerprint(&f.path)? == f.blake3,
                    "join source drift",
                )?;
            }
            metrics.source_verify_ns += ns(verify);
            let mut scan = ParquetRows::new(
                files.iter().map(|f| f.path.clone()).collect(),
                row_source::Config {
                    max_rows: usize::try_from(self.spec.config.local.max_input_rows_per_side)
                        .expect("validated rows"),
                    ..Default::default()
                },
            )?;
            let mut senders = self
                .spec
                .workers
                .iter()
                .enumerate()
                .map(|(p, w)| {
                    Sender::typed(
                        w.exchange,
                        transfer(self.spec.job, side as u64 + 2, p as u64, self.worker as u64),
                        self.token.clone(),
                        Payload::JoinRows,
                        Some(decision.digest.clone()),
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            while let Some(batch) = scan.next_batch(&self.token)? {
                metrics.rows[side] += batch.num_rows() as u64;
                for (p, sender) in senders.iter_mut().enumerate() {
                    let selected = if decision.distribution == Distribution::BroadcastBuild
                        && decision.build.index() == side
                    {
                        batch.clone()
                    } else {
                        select_rows(&batch, p, self.spec.workers.len())?
                    };
                    if selected.num_rows() > 0 {
                        sender.batch(&selected)?;
                    }
                }
            }
            let stats = scan.stats();
            metrics.source[side] = stats.clone();
            scan.shutdown();
            require(
                stats.complete && metrics.rows[side] == files.iter().map(|f| f.rows).sum::<u64>(),
                "join source row count/closure",
            )?;
            for sender in senders {
                metrics.sent[side].add(&sender.finish()?);
            }
        }
        metrics.elapsed_ns = ns(start);
        Ok(metrics)
    }
    pub(crate) fn finish(&self, gather: SocketAddr) -> Result<TaskMetrics> {
        self.token.check()?;
        let (decision, stores) = self.committed()?;
        require(
            !self.final_task.swap(true, Ordering::AcqRel),
            "duplicate join final",
        )?;
        let start = Instant::now();
        let mut sender = Sender::typed(
            gather,
            transfer(
                self.spec.job,
                4,
                0,
                self.spec.workers.len() as u64 + self.worker as u64,
            ),
            self.token.clone(),
            Payload::JoinPairs,
            Some(decision.digest),
        )?;
        let mut config = self.spec.config.local.clone();
        config.build = decision.build;
        let local = ade_exec::join::execute(
            stores[0].reader(),
            stores[1].reader(),
            &config,
            &self.token,
            Some(&self.parent),
            |batch| {
                sender
                    .batch(&batch)
                    .map_err(|e| ade_exec::Error::Invalid(e.to_string()))?;
                #[cfg(feature = "research-hooks")]
                if self.spec.config.research_exit_final == Some(self.worker as u64) {
                    eprintln!("ADE join research exit after gather frame");
                    std::process::exit(87);
                }
                Ok(())
            },
        )?;
        let mut received = [NetworkMetrics::default(), NetworkMetrics::default()];
        let mut retention = [
            crate::RetentionMetrics::default(),
            crate::RetentionMetrics::default(),
        ];
        for (side, store) in stores.iter().enumerate() {
            store.validate_retention_drained()?;
            received[side] = store.metrics()?;
            retention[side] = store.retention_metrics()?;
            require(
                received[side].transfers == self.spec.workers.len() as u64,
                "join side terminals",
            )?;
        }
        Ok(TaskMetrics {
            received,
            retention,
            gather: sender.finish()?,
            local: Some(local),
            elapsed_ns: ns(start),
            ..Default::default()
        })
    }
}
fn select_rows(batch: &RecordBatch, p: usize, n: usize) -> Result<RecordBatch> {
    let keys = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| Error::Invalid("join key schema".into()))?;
    let indices = UInt32Array::from(
        (0..batch.num_rows())
            .filter(|&i| {
                crate::partition::destination(keys.value(i), n).is_ok_and(|owner| owner == p)
            })
            .map(|i| u32::try_from(i).expect("bounded batch"))
            .collect::<Vec<_>>(),
    );
    let columns = batch
        .columns()
        .iter()
        .map(|a| take(a, &indices, None))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(RecordBatch::try_new(batch.schema(), columns)?)
}
fn ns(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX)
}
fn cancel(spec: &Spec) {
    thread::scope(|s| {
        for w in &spec.workers {
            s.spawn(move || {
                let _ = protocol::rpc(w, Request::CancelJob { job: spec.job }, crate::IO_TIMEOUT);
            });
        }
    });
}
/// Execute a full join with exact terminal and row conservation before bounded gather collection.
/// # Errors
/// Any admission, source, protocol, resource or process failure cancels every endpoint; no partial result is returned.
pub fn execute(spec: &Spec, timeout: Duration) -> Result<Output> {
    execute_inner_bounded(spec, timeout, None, None)
}

pub(crate) fn execute_bounded(
    spec: &Spec,
    timeout: Duration,
    output_limits: crate::bounded_output::OutputLimits,
    mut adapt: impl FnMut(RecordBatch) -> Result<RecordBatch>,
) -> Result<Output> {
    output_limits.validate()?;
    execute_inner_bounded(spec, timeout, Some(output_limits), Some(&mut adapt))
}

fn execute_inner_bounded(
    spec: &Spec,
    timeout: Duration,
    output_limits: Option<crate::bounded_output::OutputLimits>,
    adapt: Option<&mut dyn FnMut(RecordBatch) -> Result<RecordBatch>>,
) -> Result<Output> {
    require(
        output_limits.is_none() || adapt.is_some(),
        "bounded join gather requires an output adapter",
    )?;
    spec.validate()?;
    require(
        !timeout.is_zero() && timeout <= Duration::from_secs(120),
        "join deadline",
    )?;
    let started = Instant::now();
    let deadline = started + timeout;
    let result = execute_inner(spec, deadline, started, output_limits, adapt);
    cancel(spec);
    result
}
pub(crate) fn prepare(spec: &Spec, deadline: Instant) -> Result<(Decision, u64, u64)> {
    for w in &spec.workers {
        match protocol::rpc_until(w, Request::PrepareJoin(spec.clone()), deadline)? {
            Reply::Ready => {}
            _ => return Err(Error::Invalid("join prepare reply".into())),
        }
    }
    let probe = Instant::now();
    let mut observations = Vec::with_capacity(spec.workers.len() * 2);
    for w in &spec.workers {
        match protocol::rpc_until(w, Request::ProbeJoin { job: spec.job }, deadline)? {
            Reply::JoinProbe(o) => observations.extend(o),
            _ => return Err(Error::Invalid("join probe reply".into())),
        }
    }
    let mut decision = join_policy::choose(&spec.config, observations, spec.workers.len())?;
    decision.digest = spec.digest(&decision)?;
    let probe_ns = ns(probe);
    let commit = Instant::now();
    thread::scope(|scope| -> Result<()> {
        let joins = spec
            .workers
            .iter()
            .map(|w| {
                let d = decision.clone();
                scope.spawn(move || {
                    match protocol::rpc_until(
                        w,
                        Request::CommitJoin {
                            job: spec.job,
                            decision: d,
                        },
                        deadline,
                    )? {
                        Reply::Ready => Ok(()),
                        _ => Err(Error::Invalid("join ACK".into())),
                    }
                })
            })
            .collect::<Vec<_>>();
        for j in joins {
            j.join()
                .map_err(|_| Error::Invalid("join ACK panic".into()))??;
        }
        Ok(())
    })?;
    let commit_ns = ns(commit);
    Ok((decision, probe_ns, commit_ns))
}
type TaskSet = BTreeMap<u64, TaskMetrics>;
pub(crate) fn tasks(
    spec: &Spec,
    deadline: Instant,
    address: SocketAddr,
    token: &CancellationToken,
) -> Result<(TaskSet, TaskSet)> {
    thread::scope(|scope| -> Result<_> {
        let (tx, rx) = mpsc::sync_channel(spec.workers.len() * 2);
        for final_task in [true, false] {
            for w in &spec.workers {
                let tx = tx.clone();
                scope.spawn(move || {
                    let request = if final_task {
                        Request::RunJoinFinal {
                            job: spec.job,
                            gather: address,
                        }
                    } else {
                        Request::RunJoinProducer { job: spec.job }
                    };
                    let r = protocol::rpc_until(w, request, deadline).and_then(|r| match r {
                        Reply::JoinTask(m) => Ok(m),
                        _ => Err(Error::Invalid("join task reply".into())),
                    });
                    let _ = tx.send((w.id.0, final_task, r));
                });
            }
        }
        drop(tx);
        let mut producers = BTreeMap::new();
        let mut finals = BTreeMap::new();
        for _ in 0..spec.workers.len() * 2 {
            let message = rx.recv_timeout(deadline.saturating_duration_since(Instant::now()));
            let (w, f, r) = match message {
                Ok(v) => v,
                Err(e) => {
                    token.cancel();
                    cancel(spec);
                    return Err(Error::Unrecoverable(e.to_string()));
                }
            };
            match r {
                Ok(m) => {
                    if f {
                        finals.insert(w, m);
                    } else {
                        producers.insert(w, m);
                    }
                }
                Err(e) => {
                    token.cancel();
                    cancel(spec);
                    return Err(Error::Unrecoverable(format!("join worker {w} failed: {e}")));
                }
            }
        }
        Ok((producers, finals))
    })
}
fn gather(
    spec: &Spec,
    decision: &Decision,
    parent: &Path,
    token: &CancellationToken,
) -> Result<(Vec<Arc<Store>>, Receiver)> {
    gather_budget(spec, decision, parent, token, None)
}
pub(crate) fn gather_budget(
    spec: &Spec,
    decision: &Decision,
    parent: &Path,
    token: &CancellationToken,
    disk: Option<&Arc<crate::disk::Budget>>,
) -> Result<(Vec<Arc<Store>>, Receiver)> {
    let mut stores = Vec::with_capacity(spec.workers.len());
    for p in 0..spec.workers.len() {
        stores.push(Arc::new(Store::typed_budget(
            parent,
            [transfer(
                spec.job,
                4,
                0,
                spec.workers.len() as u64 + p as u64,
            )]
            .into(),
            token.clone(),
            if disk.is_some() {
                crate::ExchangeConfig {
                    mode: crate::ExchangeMode::LocalMaterialized,
                    ..spill_config()
                }
            } else {
                spill_config()
            },
            Payload::JoinPairs,
            decision.digest.clone(),
            limits(spec.config.local.max_output_rows),
            Some(HashOwner {
                workers: spec.workers.len(),
                destination: p,
            }),
            disk.cloned(),
        )?));
    }
    let receiving = stores.clone();
    let worker_count = spec.workers.len();
    let gather_receiver = Receiver::start(TcpListener::bind("127.0.0.1:0")?, move |h| {
        let p =
            h.id.producer
                .0
                .checked_sub(worker_count as u64)
                .and_then(|p| usize::try_from(p).ok())
                .ok_or_else(|| Error::Invalid("join gather producer".into()))?;
        receiving
            .get(p)
            .cloned()
            .ok_or_else(|| Error::Invalid("join gather destination".into()))
    })?;
    Ok((stores, gather_receiver))
}
fn execute_inner(
    spec: &Spec,
    deadline: Instant,
    started: Instant,
    output_limits: Option<crate::bounded_output::OutputLimits>,
    mut adapt: Option<&mut dyn FnMut(RecordBatch) -> Result<RecordBatch>>,
) -> Result<Output> {
    let (decision, probe_ns, commit_ns) = prepare(spec, deadline)?;
    let root = tempfile::tempdir()?;
    let token = CancellationToken::default();
    let (stores, gather_receiver) = gather(spec, &decision, root.path(), &token)?;
    let (producers, finals) = tasks(spec, deadline, gather_receiver.address, &token)?;

    let mut total_output = 0u64;
    let mut gather = NetworkMetrics::default();
    for (p, store) in stores.iter().enumerate() {
        let m = store.metrics()?;
        require(
            m.transfers == 1 && same_network(&m, &finals[&(p as u64)].gather),
            "join gather conservation",
        )?;
        total_output = total_output
            .checked_add(m.rows)
            .ok_or_else(|| Error::Invalid("join output overflow".into()))?;
        gather.add(&m);
    }
    require(
        total_output <= spec.config.local.max_output_rows,
        "global join output cap",
    )?;
    for side in 0..2 {
        let mut sent = NetworkMetrics::default();
        let mut received = NetworkMetrics::default();
        let mut rows = 0;
        for p in producers.values() {
            rows += p.rows[side];
            sent.add(&p.sent[side]);
        }
        for f in finals.values() {
            received.add(&f.received[side]);
        }
        let factor = if decision.distribution == Distribution::BroadcastBuild
            && decision.build.index() == side
        {
            spec.workers.len() as u64
        } else {
            1
        };
        require(
            same_network(&sent, &received)
                && rows == spec.sources[side].files.iter().map(|f| f.rows).sum::<u64>()
                && sent.rows == rows * factor
                && sent.transfers == (spec.workers.len() * spec.workers.len()) as u64,
            "join bilateral row/terminal conservation",
        )?;
    }
    let mut batches = Vec::new();
    let mut rows = 0u64;
    let mut output_budget = output_limits
        .map(crate::bounded_output::OutputBudget::new)
        .transpose()?;
    for store in &stores {
        let mut source = store.reader();
        while let Some(batch) = source.next_batch(&token)? {
            let batch = if let Some(adapt) = adapt.as_deref_mut() {
                adapt(batch)?
            } else {
                batch
            };
            rows = rows
                .checked_add(
                    u64::try_from(batch.num_rows())
                        .map_err(|_| Error::Invalid("join gather row overflow".into()))?,
                )
                .ok_or_else(|| Error::Invalid("join gather row overflow".into()))?;
            require(rows <= total_output, "join gather row overflow")?;
            if let Some(budget) = &mut output_budget {
                budget.include(&batch)?;
            }
            batches.push(batch);
        }
        source.shutdown();
        store.validate_retention_drained()?;
    }
    require(rows == total_output, "join gather incomplete")?;
    drop(gather_receiver);
    let output = Output {
        batches,
        metrics: Metrics {
            spec: spec.clone(),
            version: 1,
            decision,
            probe_ns,
            commit_ns,
            elapsed_ns: ns(started),
            producers,
            finals,
            gather,
            gather_retention: stores
                .iter()
                .enumerate()
                .map(|(p, s)| Ok((p as u64, s.retention_metrics()?)))
                .collect::<Result<_>>()?,
        },
    };
    validate_metrics(&output.metrics)?;
    Ok(output)
}
fn same_network(a: &NetworkMetrics, b: &NetworkMetrics) -> bool {
    a.transfers == b.transfers
        && a.batches == b.batches
        && a.rows == b.rows
        && a.payload_bytes == b.payload_bytes
}

/// Validate diagnostic strategy identity and measured bilateral transfer facts without source I/O.
/// # Errors
/// Refuses forged decisions/sources, incomplete worker inventories and contradictory actual work.
#[allow(
    clippy::too_many_lines,
    reason = "This pure audit checks one complete bounded graph before reporting its diagnostic evidence valid."
)]
pub fn validate_metrics(metrics: &Metrics) -> Result<()> {
    metrics.spec.validate()?;
    let spec = &metrics.spec;
    let d = &metrics.decision;
    let mut expected =
        join_policy::choose(&spec.config, d.observations.clone(), spec.workers.len())?;
    expected.digest = spec.digest(&expected)?;
    require(
        metrics.version == 1 && serde_json::to_value(&expected)? == serde_json::to_value(d)?,
        "join diagnostic decision digest",
    )?;
    let n = spec.workers.len();
    require(
        metrics.producers.len() == n
            && metrics.finals.len() == n
            && metrics.gather_retention.len() == n,
        "join diagnostic inventory",
    )?;
    let mut received = [NetworkMetrics::default(), NetworkMetrics::default()];
    let mut sent = [NetworkMetrics::default(), NetworkMetrics::default()];
    let mut gather = NetworkMetrics::default();
    for p in 0..n {
        let p64 = p as u64;
        let producer = metrics
            .producers
            .get(&p64)
            .ok_or_else(|| Error::Invalid("join producer inventory".into()))?;
        let final_task = metrics
            .finals
            .get(&p64)
            .ok_or_else(|| Error::Invalid("join final inventory".into()))?;
        let local = final_task
            .local
            .as_ref()
            .ok_or_else(|| Error::Invalid("missing local join facts".into()))?;
        require(
            local.cleaned
                && local.managed_current == 0
                && local.managed_peak <= spec.config.local.memory_limit_bytes
                && local.spill_fd_peak <= 1
                && local.hash_rows_peak <= spec.config.local.hash_rows
                && local.output_rows == final_task.gather.rows
                && final_task.gather.transfers == 1,
            "join local/gather facts",
        )?;
        let retention = metrics
            .gather_retention
            .get(&p64)
            .ok_or_else(|| Error::Invalid("gather retention inventory".into()))?;
        require(
            retention.current_buffer_bytes == 0 && retention.current_spill_backlog_bytes == 0,
            "gather not drained",
        )?;
        gather.add(&final_task.gather);
        for side in 0..2 {
            let assigned = spec.sources[side].assigned(p, n);
            let rows = assigned.iter().map(|f| f.rows).sum::<u64>();
            let o = &d.observations[p * 2 + side];
            require(
                o.assigned_files == assigned.len() && o.assigned_rows == rows,
                "join observation source assignment",
            )?;
            let source = &producer.source[side];
            let retained = &final_task.retention[side];
            require(
                producer.rows[side] == rows
                    && source.complete
                    && source.rows == rows
                    && source.completed_files == assigned.len()
                    && source.read_bytes <= 64 * 1024 * 1024
                    && source.metadata_bytes <= 1024 * 1024
                    && source.decode_chunk_peak_bytes <= 1024 * 1024
                    && source.peak_batch_bytes <= 64 * 1024
                    && local.input_rows[side] == final_task.received[side].rows
                    && final_task.received[side].transfers == n as u64
                    && retained.current_buffer_bytes == 0
                    && retained.current_spill_backlog_bytes == 0
                    && retained.peak_buffer_bytes <= spill_config().buffer_bytes,
                "join source/receive/buffer facts",
            )?;
            sent[side].add(&producer.sent[side]);
            received[side].add(&final_task.received[side]);
        }
    }
    require(
        same_network(&gather, &metrics.gather) && gather.rows <= spec.config.local.max_output_rows,
        "join diagnostic gather conservation",
    )?;
    for side in 0..2 {
        let factor = if d.distribution == Distribution::BroadcastBuild && d.build.index() == side {
            n as u64
        } else {
            1
        };
        require(
            same_network(&sent[side], &received[side])
                && sent[side].transfers == (n * n) as u64
                && sent[side].rows
                    == spec.sources[side].files.iter().map(|f| f.rows).sum::<u64>() * factor,
            "join diagnostic side conservation",
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn spec(root: &Path) -> Spec {
        let mut sides = Vec::new();
        for side in [Side::Left, Side::Right] {
            let path = root.join(format!("{side:?}.parquet"));
            let w = parquet::arrow::ArrowWriter::try_new(
                File::create(&path).unwrap(),
                ade_exec::join::input_schema(),
                None,
            )
            .unwrap();
            w.close().unwrap();
            sides.push(Source::capture(side, vec![(path, 0)]).unwrap());
        }
        Spec {
            job: JobId(1),
            execution: uuid::Uuid::new_v4().to_string(),
            sources: sides.try_into().unwrap(),
            workers: (0..2)
                .map(|w| WorkerEndpoint {
                    managed: None,
                    id: crate::ids::WorkerId(w),
                    control: format!("127.0.0.1:{}", 1000 + w * 2).parse().unwrap(),
                    exchange: format!("127.0.0.1:{}", 1001 + w * 2).parse().unwrap(),
                })
                .collect(),
            config: Config::default(),
        }
    }
    #[test]
    fn decision_fence_own_observations_and_stale_commit() {
        let root = tempfile::tempdir().unwrap();
        let spec = spec(root.path());
        let a = Session::new(spec.clone(), 0, root.path()).unwrap();
        let b = Session::new(spec.clone(), 1, root.path()).unwrap();
        assert!(a.produce().is_err());
        let h = exchange::Header::new(transfer(spec.job, 2, 0, 0));
        assert!(a.store(&h).is_err());
        let mut observations = a.probe().unwrap();
        observations.extend(b.probe().unwrap());
        assert!(a.probe().is_err());
        let mut d = join_policy::choose(&spec.config, observations, 2).unwrap();
        d.digest = spec.digest(&d).unwrap();
        let mut forged = d.clone();
        forged.observations[0].read_bytes += 1;
        forged.digest = spec.digest(&forged).unwrap();
        assert!(a.commit(forged).is_err());
        let mut stale = d.clone();
        stale.digest = "0".repeat(64);
        assert!(a.commit(stale).is_err());
        a.commit(d.clone()).unwrap();
        assert!(a.commit(d.clone()).is_err());
        let other = Spec {
            execution: uuid::Uuid::new_v4().to_string(),
            ..spec.clone()
        };
        assert_ne!(other.digest(&d).unwrap(), d.digest);
        let mut h = h;
        h.context = Some(d.digest);
        h.schema_version = 2;
        assert!(a.store(&h).is_ok());
        h.id.exchange.0 = 4;
        assert!(a.store(&h).is_err());
    }
    #[test]
    fn incomplete_prefix_and_forged_complete_never_authorize_broadcast() {
        let mut c = Config::default();
        let observations = (0..2)
            .flat_map(|w| {
                [Side::Left, Side::Right].map(move |side| Observation {
                    worker: w,
                    side,
                    rows: 8,
                    assigned_rows: 10000,
                    assigned_files: 1,
                    completed_files: 0,
                    read_bytes: 4096,
                    metadata_bytes: 100,
                    decode_peak_bytes: 65536,
                    batch_peak_bytes: 256,
                    complete: false,
                })
            })
            .collect::<Vec<_>>();
        let d = join_policy::choose(&c, observations.clone(), 2).unwrap();
        assert_eq!(d.distribution, Distribution::HashRepartition);
        c.policy = Policy::ForcedBroadcast;
        assert!(join_policy::choose(&c, observations.clone(), 2).is_err());
        let mut forged = observations;
        forged[1].complete = true;
        assert!(join_policy::choose(&c, forged.clone(), 2).is_err());
        forged[1].rows = u64::MAX;
        assert!(join_policy::choose(&c, forged, 2).is_err());
    }
}
