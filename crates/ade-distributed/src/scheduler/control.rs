//! Bounded coordinator RPC service and independent producer heartbeats.
use super::{
    Arc, AtomicBool, CancellationToken, Completion, Config, Deserialize, Duration, Error, Fence,
    Instant, JobId, JoinHandle, Lease, Ledger, Metrics, Mutex, Ordering, Result, Serialize,
    SourceSnapshot, Spec, TaskConfig, TcpListener, WorkerEndpoint, WorkerId, digest, protocol,
    require, thread,
};
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Rpc {
    pub(super) fence: Fence,
    pub(super) worker: WorkerId,
    pub(super) op: Op,
}
#[derive(Debug, Serialize, Deserialize)]
pub(super) enum Op {
    Ready,
    Acquire,
    Renew(Lease),
    Complete(Completion),
    Closed,
}
#[derive(Debug, Serialize, Deserialize)]
pub(super) enum Response {
    Ack,
    Wait,
    Finish,
    Lease(Lease),
    Failed(String),
}
fn elapsed(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX)
}
/// The service owns a single serialized ledger and one bounded socket handler.
pub(crate) struct Service {
    pub spec: Spec,
    pub(super) ledger: Arc<Mutex<Ledger>>,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
    start: Instant,
}
impl Service {
    pub(crate) fn start(
        job: JobId,
        execution: &str,
        workers: &[WorkerEndpoint],
        config: &TaskConfig,
        source: SourceSnapshot,
        timeout: Duration,
    ) -> Result<Self> {
        // Full diagnostics repeat a descriptor in source, lease and completion.
        // Reserve a conservative source-catalog share of the bounded verifier frame.
        let mut catalog = crate::exchange::BoundedBuffer::new(protocol::MAX_CONTROL_BYTES / 8);
        serde_json::to_writer(&mut catalog, &source)?;
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let fence = Fence {
            job,
            execution: execution.into(),
            epoch: 1,
            digest: digest(job, execution, workers, config, &source)?,
        };
        let spec = Spec {
            fence: fence.clone(),
            address: listener.local_addr()?,
        };
        let duration = u64::try_from(timeout.min(crate::TASK_TIMEOUT).as_millis())
            .map_err(|_| Error::Invalid("scheduler timeout overflow".into()))?;
        let ledger = Arc::new(Mutex::new(Ledger::new(
            fence,
            source,
            config.scheduler.clone(),
            workers.len(),
            duration,
        )?));
        {
            let mut state = ledger
                .lock()
                .map_err(|_| Error::Invalid("lease ledger poisoned".into()))?;
            state.metrics.task = Some(config.clone());
            state.metrics.registry = workers.to_vec();
        }
        let stop = Arc::new(AtomicBool::new(false));
        let stopping = stop.clone();
        let state = ledger.clone();
        let start = Instant::now();
        let handle = thread::spawn(move || {
            while !stopping.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((mut socket, _)) => {
                        let result = (|| {
                            socket.set_nonblocking(false)?;
                            let mut socket = protocol::DeadlineStream::new(
                                &mut socket,
                                Duration::from_millis(500),
                            );
                            let r: Rpc = protocol::read_message(&mut socket)?;
                            let size = serde_json::to_vec(&r)?.len() as u64;
                            let response = {
                                let mut l = state.lock().expect("lease ledger");
                                l.metrics.control_bytes += size;
                                match l.handle(r, elapsed(start)) {
                                    Ok(r) => r,
                                    Err(e) => {
                                        l.failed = Some(e.to_string());
                                        Response::Failed(e.to_string())
                                    }
                                }
                            };
                            state
                                .lock()
                                .map_err(|_| Error::Invalid("lease ledger poisoned".into()))?
                                .metrics
                                .control_bytes += serde_json::to_vec(&response)?.len() as u64;
                            protocol::write_message(&mut socket, &response)?;
                            Ok::<_, Error>(())
                        })();
                        if let Err(e) = result {
                            state.lock().expect("lease ledger").failed =
                                Some(format!("ambiguous lease control: {e}"));
                        }
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        let mut l = state.lock().expect("lease ledger");
                        if let Err(e) = l.check(elapsed(start)) {
                            l.failed = Some(e.to_string());
                        }
                        drop(l);
                        thread::sleep(Duration::from_millis(1));
                    }
                    Err(e) => {
                        state.lock().expect("lease ledger").failed = Some(e.to_string());
                        break;
                    }
                }
            }
        });
        Ok(Self {
            spec,
            ledger,
            stop,
            handle: Some(handle),
            start,
        })
    }
    pub(crate) fn finish(&self) -> Result<Metrics> {
        self.ledger
            .lock()
            .map_err(|_| Error::Invalid("lease ledger poisoned".into()))?
            .finish(elapsed(self.start))
    }
}
impl Drop for Service {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}
/// One independent renewal thread per producer; main and renewal RPCs serialize
/// under the lease lock, preventing a late renewal from racing a successful ACK.
pub(crate) struct Client {
    spec: Spec,
    worker: WorkerId,
    active: Arc<Mutex<Option<Lease>>>,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
    token: CancellationToken,
    deadline: Instant,
}
impl Client {
    pub(crate) fn start(
        spec: Spec,
        worker: WorkerId,
        c: &Config,
        token: CancellationToken,
    ) -> Result<Self> {
        let active = Arc::new(Mutex::new(None::<Lease>));
        let stop = Arc::new(AtomicBool::new(false));
        let heartbeat = Duration::from_millis(c.heartbeat_ms);
        let mut client = Self {
            spec: spec.clone(),
            worker,
            active: active.clone(),
            stop: stop.clone(),
            handle: None,
            token: token.clone(),
            deadline: Instant::now() + crate::TASK_TIMEOUT,
        };
        client.call(Op::Ready)?;
        let deadline = client.deadline;
        #[cfg(feature = "research-hooks")]
        let skip_heartbeats = c.research_no_heartbeat_worker == Some(worker.0);
        #[cfg(not(feature = "research-hooks"))]
        let skip_heartbeats = false;
        client.handle = Some(thread::spawn(move || {
            while !stop.load(Ordering::Acquire) {
                thread::park_timeout(heartbeat);
                if stop.load(Ordering::Acquire) {
                    break;
                }
                let l = active.lock().expect("active lease");
                if !skip_heartbeats
                    && let Some(lease) = l.as_ref()
                    && call(&spec, worker, Op::Renew(lease.clone()), deadline).is_err()
                {
                    token.cancel();
                    break;
                }
            }
        }));
        Ok(client)
    }
    fn call(&self, op: Op) -> Result<Response> {
        self.token.check()?;
        call(&self.spec, self.worker, op, self.deadline)
    }
    pub(crate) fn next(&self) -> Result<Option<Lease>> {
        loop {
            match self.call(Op::Acquire)? {
                Response::Lease(l) => {
                    require(l.worker == self.worker, "foreign lease reply")?;
                    *self.active.lock().expect("active lease") = Some(l.clone());
                    return Ok(Some(l));
                }
                Response::Finish => return Ok(None),
                Response::Wait => thread::sleep(Duration::from_millis(5)),
                _ => return Err(Error::Invalid("invalid acquire reply".into())),
            }
        }
    }
    pub(crate) fn complete(&self, c: Completion) -> Result<()> {
        let mut l = self.active.lock().expect("active lease");
        require(
            l.as_ref() == Some(&c.lease),
            "completion differs from active lease",
        )?;
        require(
            matches!(self.call(Op::Complete(c))?, Response::Ack),
            "completion ACK missing",
        )?;
        *l = None;
        Ok(())
    }
    pub(crate) fn closed(&self) -> Result<()> {
        require(
            matches!(self.call(Op::Closed)?, Response::Ack),
            "producer closure ACK missing",
        )
    }
}
fn call(spec: &Spec, worker: WorkerId, op: Op, deadline: Instant) -> Result<Response> {
    require(Instant::now() < deadline, "scheduler client deadline")?;
    let mut socket = protocol::connect(spec.address, Duration::from_millis(1000))?;
    let mut socket = protocol::DeadlineStream::new(&mut socket, Duration::from_millis(1000));
    protocol::write_message(
        &mut socket,
        &Rpc {
            fence: spec.fence.clone(),
            worker,
            op,
        },
    )?;
    match protocol::read_message(&mut socket)? {
        Response::Failed(s) => Err(Error::Invalid(s)),
        r => Ok(r),
    }
}
impl Drop for Client {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(h) = self.handle.take() {
            h.thread().unpark();
            let _ = h.join();
        }
    }
}

/// Hash bounded chunks with cancellation/deadline checks between OS reads.
/// A single blocked kernel read retains the documented non-preemptibility limit.
pub(crate) fn verify_sources(
    morsels: &[super::SourceMorsel],
    token: &CancellationToken,
    deadline: Instant,
) -> Result<()> {
    use std::io::Read;
    for morsel in morsels {
        token.check()?;
        require(
            Instant::now() < deadline,
            "scheduler source verification deadline",
        )?;
        let mut file = std::fs::File::open(&morsel.path)?;
        require(
            file.metadata()?.len() == morsel.bytes,
            "source snapshot size mismatch",
        )?;
        let mut digest = blake3::Hasher::new();
        let mut bytes = [0u8; 8192];
        let mut read = 0u64;
        loop {
            token.check()?;
            require(
                Instant::now() < deadline,
                "scheduler source verification deadline",
            )?;
            let n = file.read(&mut bytes)?;
            if n == 0 {
                break;
            }
            read = read
                .checked_add(n as u64)
                .ok_or_else(|| Error::Invalid("source verification byte overflow".into()))?;
            require(read <= morsel.bytes, "source grew during verification")?;
            digest.update(&bytes[..n]);
        }
        require(
            read == morsel.bytes && digest.finalize().to_hex().as_str() == morsel.fingerprint,
            "source snapshot fingerprint mismatch",
        )?;
    }
    Ok(())
}
