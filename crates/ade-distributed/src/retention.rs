//! Retention mechanisms. Routing, policy application and network identity live outside this module.
use crate::{Error, Result, exchange::MAX_FRAME_BYTES, ids::TransferId, require};
use ade_exec::CancellationToken;
use object_store::{ObjectStore, ObjectStoreExt, local::LocalFileSystem, path::Path};
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    path::Path as FsPath,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, Sender, SyncSender},
    },
    thread,
    time::{Duration, Instant},
};

const IO_POLL_INTERVAL: Duration = Duration::from_millis(10);
const RETENTION_IO_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExchangeMode {
    Direct,
    Adaptive,
    Buffered,
    Spill,
    #[default]
    LocalMaterialized,
    #[serde(rename = "object_store")]
    ObjectStoreMaterialized,
}
impl ExchangeMode {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::Adaptive => "adaptive",
            Self::Buffered => "buffered",
            Self::Spill => "spill",
            Self::LocalMaterialized => "local_materialized",
            Self::ObjectStoreMaterialized => "object_store",
        }
    }
    pub(crate) fn live(self) -> bool {
        matches!(self, Self::Direct | Self::Buffered | Self::Spill)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Durability assertion supplied by the worker's backend configuration.
pub enum ObjectStoreDurability {
    #[default]
    Ephemeral,
    Durable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectStoreConfig {
    pub backend: String,
    pub durability: ObjectStoreDurability,
}
impl Default for ObjectStoreConfig {
    fn default() -> Self {
        Self {
            backend: "local_filesystem".into(),
            durability: ObjectStoreDurability::Ephemeral,
        }
    }
}

/// A worker-session object store supplied by its caller.
#[derive(Clone)]
pub struct ObjectStoreBackend {
    config: ObjectStoreConfig,
    store: Arc<dyn ObjectStore>,
}
impl std::fmt::Debug for ObjectStoreBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectStoreBackend")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}
impl ObjectStoreBackend {
    /// # Errors
    /// Rejects an empty backend name.
    pub fn new(config: ObjectStoreConfig, store: Arc<dyn ObjectStore>) -> Result<Self> {
        require(
            !config.backend.trim().is_empty(),
            "object-store backend name required",
        )?;
        Ok(Self { config, store })
    }
}

/// Research-only deterministic final-consumer delay; available only to the
/// benchmark crate's `research-hooks` build.
#[cfg(feature = "research-hooks")]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PressureConfig {
    pub target_partition: Option<u64>,
    pub after_segments: u64,
    pub delay_ms: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExchangeConfig {
    pub mode: ExchangeMode,
    #[serde(default)]
    pub adaptive: crate::adaptive::AdaptiveConfig,
    #[cfg(feature = "research-hooks")]
    #[serde(default)]
    pub pressure: PressureConfig,
    pub buffer_bytes: usize,
    #[serde(default)]
    pub object_store: ObjectStoreConfig,
}
impl Default for ExchangeConfig {
    fn default() -> Self {
        Self {
            mode: ExchangeMode::LocalMaterialized,
            adaptive: crate::adaptive::AdaptiveConfig::default(),
            #[cfg(feature = "research-hooks")]
            pressure: PressureConfig::default(),
            buffer_bytes: 4 * MAX_FRAME_BYTES,
            object_store: ObjectStoreConfig::default(),
        }
    }
}
impl ExchangeConfig {
    /// # Errors
    /// Rejects zero/excessive limits; buffered mode must fit the largest valid frame.
    pub fn validate(&self) -> Result<()> {
        self.adaptive.validate()?;
        #[cfg(feature = "research-hooks")]
        require(
            self.pressure.delay_ms <= 2000
                && self.pressure.after_segments <= 1_000_000
                && self.pressure.target_partition.is_none_or(|p| p < 4),
            "invalid research pressure configuration",
        )?;
        #[cfg(feature = "research-hooks")]
        require(
            self.pressure.target_partition.is_some() || self.pressure.delay_ms == 0,
            "research delay requires target partition",
        )?;
        require(
            (1..=256 * MAX_FRAME_BYTES).contains(&self.buffer_bytes),
            "exchange buffer must be 1..=268435456 bytes",
        )?;
        require(
            self.mode != ExchangeMode::Buffered || self.buffer_bytes >= MAX_FRAME_BYTES,
            "buffered exchange requires at least max_frame_bytes",
        )?;
        require(
            !matches!(
                self.mode,
                ExchangeMode::ObjectStoreMaterialized | ExchangeMode::Adaptive
            ) || !self.object_store.backend.trim().is_empty(),
            "object-store backend name required",
        )
    }
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetentionMetrics {
    pub adaptive: crate::adaptive::AdaptiveMetrics,
    /// Current bytes in the in-memory exchange queue.
    pub current_buffer_bytes: usize,
    pub peak_buffer_bytes: usize,
    /// Encoded payload bytes queued in the local spill log.
    pub current_spill_backlog_bytes: usize,
    pub spill_write_bytes: u64,
    pub spill_read_bytes: u64,
    pub local_materialized_bytes: u64,
    pub local_materialized_files: u64,
    pub object_write_bytes: u64,
    pub object_read_bytes: u64,
    pub object_count: u64,
    pub object_store_backend: Option<String>,
    pub object_store_durability: Option<ObjectStoreDurability>,
    pub producer_wait_ns: u64,
    pub consumer_wait_ns: u64,
}
#[derive(Debug)]
struct BufferState {
    queue: VecDeque<Vec<u8>>,
    bytes: usize,
    spill_backlog_bytes: usize,
    waiting: bool,
    consumed: u64,
    metrics: RetentionMetrics,
}
/// One queue/spool per destination, with constant disk metadata regardless of segment count.
#[derive(Debug)]
pub(crate) struct Buffer {
    config: ExchangeConfig,
    state: Mutex<BufferState>,
    changed: Condvar,
    spool: Option<SpoolIo>,
    poisoned: Arc<AtomicBool>,
}
impl Buffer {
    #[cfg(test)]
    pub fn new(config: ExchangeConfig, root: &FsPath) -> Result<Self> {
        Self::new_with_poison(config, root, Arc::default())
    }
    pub(crate) fn new_with_poison(
        config: ExchangeConfig,
        root: &FsPath,
        poisoned: Arc<AtomicBool>,
    ) -> Result<Self> {
        let spool = if matches!(config.mode, ExchangeMode::Spill | ExchangeMode::Adaptive) {
            Some(SpoolIo::new(tempfile::tempfile_in(root)?, poisoned.clone()))
        } else {
            None
        };
        Ok(Self {
            config,
            state: Mutex::new(BufferState {
                queue: VecDeque::new(),
                bytes: 0,
                spill_backlog_bytes: 0,
                waiting: false,
                consumed: 0,
                metrics: RetentionMetrics::default(),
            }),
            changed: Condvar::new(),
            spool,
            poisoned,
        })
    }
    #[cfg(test)]
    pub(crate) fn replace_spill_log_for_test(&mut self, log: impl SpillLog) {
        self.spool = Some(SpoolIo::with_log(log, self.poisoned.clone()));
    }
    pub fn push(&self, encoded: Vec<u8>, token: &CancellationToken) -> Result<()> {
        self.push_as(encoded, token, self.config.mode)
    }
    pub(crate) fn observation(
        &self,
        incoming_bytes: usize,
    ) -> crate::adaptive::AdaptiveObservation {
        let s = self.state.lock().expect("buffer lock");
        crate::adaptive::AdaptiveObservation {
            incoming_bytes,
            buffer_bytes: s.bytes,
            capacity_bytes: self.config.buffer_bytes,
            spill_backlog_bytes: s.spill_backlog_bytes,
            consumer_ready: s.waiting,
            producer_wait_ns: s.metrics.producer_wait_ns,
            consumer_wait_ns: s.metrics.consumer_wait_ns,
            consumed_segments: s.consumed,
            ..Default::default()
        }
    }
    pub(crate) fn push_as(
        &self,
        encoded: Vec<u8>,
        token: &CancellationToken,
        mode: ExchangeMode,
    ) -> Result<()> {
        require(
            encoded.len() <= MAX_FRAME_BYTES,
            "spill frame exceeds segment cap",
        )?;
        let start = Instant::now();
        let mut s = self.state.lock().expect("buffer lock");
        loop {
            token.check()?;
            match mode {
                ExchangeMode::Direct if s.waiting && s.queue.is_empty() => break,
                ExchangeMode::Buffered
                    if encoded.len() <= self.config.buffer_bytes.saturating_sub(s.bytes) =>
                {
                    break;
                }
                ExchangeMode::Spill => break,
                _ => {
                    s = self
                        .changed
                        .wait_timeout(s, Duration::from_millis(10))
                        .expect("buffer wait")
                        .0;
                }
            }
        }
        let mut waited_ns = nanos(start);
        if mode == ExchangeMode::Spill
            && (self.config.mode == ExchangeMode::Adaptive
                || s.spill_backlog_bytes > 0
                || encoded.len() > self.config.buffer_bytes.saturating_sub(s.bytes))
        {
            if self.poisoned.load(Ordering::Acquire) {
                return Err(Error::Invalid("spill worker is poisoned".into()));
            }
            // Enqueue while holding only the short queue lock, preserving command order.
            // The spool thread performs file I/O without owning this shared state.
            let response = self
                .spool
                .as_ref()
                .expect("spill mode has a spool")
                .enqueue_write(encoded.clone())?;
            s.spill_backlog_bytes = s.spill_backlog_bytes.saturating_add(encoded.len());
            s.metrics.current_spill_backlog_bytes = s.spill_backlog_bytes;
            drop(s);
            let io_start = Instant::now();
            let bytes = SpoolIo::wait(&response, token, &self.poisoned)?;
            waited_ns = waited_ns.saturating_add(nanos(io_start));
            let mut s = self.state.lock().expect("buffer lock");
            s.metrics.spill_write_bytes = s.metrics.spill_write_bytes.saturating_add(bytes as u64);
            s.metrics.producer_wait_ns = s.metrics.producer_wait_ns.saturating_add(waited_ns);
            self.changed.notify_all();
            return Ok(());
        }
        s.bytes += encoded.len();
        s.metrics.current_buffer_bytes = s.bytes;
        s.metrics.peak_buffer_bytes = s.metrics.peak_buffer_bytes.max(s.bytes);
        s.queue.push_back(encoded);
        self.changed.notify_all();
        if mode == ExchangeMode::Direct {
            let start = Instant::now();
            while !s.queue.is_empty() {
                token.check()?;
                s = self
                    .changed
                    .wait_timeout(s, Duration::from_millis(10))
                    .expect("handoff wait")
                    .0;
            }
            waited_ns = waited_ns.saturating_add(nanos(start));
        }
        s.metrics.producer_wait_ns = s.metrics.producer_wait_ns.saturating_add(waited_ns);
        Ok(())
    }
    pub fn take(&self, token: &CancellationToken) -> Result<Option<Vec<u8>>> {
        let mut s = self.state.lock().expect("buffer lock");
        s.waiting = true;
        self.changed.notify_all();
        if let Some(frame) = s.queue.pop_front() {
            s.consumed = s.consumed.saturating_add(1);
            s.bytes -= frame.len();
            s.metrics.current_buffer_bytes = s.bytes;
            s.waiting = false;
            self.changed.notify_all();
            return Ok(Some(frame));
        }
        if s.spill_backlog_bytes > 0 {
            if self.poisoned.load(Ordering::Acquire) {
                return Err(Error::Invalid("spill worker is poisoned".into()));
            }
            // Earlier spill writes were enqueued under this same lock, so the read
            // follows them in the private I/O queue without holding state over I/O.
            let response = self
                .spool
                .as_ref()
                .expect("spill mode has a spool")
                .enqueue_read()?;
            drop(s);
            let io_start = Instant::now();
            let result = SpoolIo::wait(&response, token, &self.poisoned);
            let mut s = self.state.lock().expect("buffer lock");
            s.waiting = false;
            let frame = match result {
                Ok(Some(frame)) => frame,
                Ok(None) => {
                    self.changed.notify_all();
                    return Err(Error::Invalid(
                        "spill backlog did not contain a frame".into(),
                    ));
                }
                Err(error) => {
                    self.changed.notify_all();
                    return Err(error);
                }
            };
            s.consumed = s.consumed.saturating_add(1);
            s.spill_backlog_bytes = s.spill_backlog_bytes.saturating_sub(frame.len());
            s.metrics.current_spill_backlog_bytes = s.spill_backlog_bytes;
            s.metrics.spill_read_bytes = s
                .metrics
                .spill_read_bytes
                .saturating_add(frame.len() as u64);
            s.metrics.consumer_wait_ns = s.metrics.consumer_wait_ns.saturating_add(nanos(io_start));
            self.changed.notify_all();
            return Ok(Some(frame));
        }
        if !matches!(
            self.config.mode,
            ExchangeMode::Direct | ExchangeMode::Adaptive
        ) {
            s.waiting = false;
        }
        Ok(None)
    }
    pub fn waited(&self, start: Instant) {
        self.state
            .lock()
            .expect("buffer lock")
            .metrics
            .consumer_wait_ns += nanos(start);
    }
    pub fn metrics(&self) -> RetentionMetrics {
        self.state.lock().expect("buffer lock").metrics.clone()
    }
}

enum SpoolCommand {
    Write(Vec<u8>, SyncSender<std::io::Result<usize>>),
    Read(SyncSender<std::io::Result<Option<Vec<u8>>>>),
}

pub(crate) trait SpillLog: Send + 'static {
    fn write_frame(&mut self, bytes: &[u8]) -> std::io::Result<()>;
    fn read_frame(&mut self) -> std::io::Result<Option<Vec<u8>>>;
}

struct FileSpillLog {
    file: File,
    read_offset: u64,
    write_offset: u64,
}
impl FileSpillLog {
    fn new(file: File) -> Self {
        Self {
            file,
            read_offset: 0,
            write_offset: 0,
        }
    }
}
impl SpillLog for FileSpillLog {
    fn write_frame(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.file.seek(SeekFrom::Start(self.write_offset))?;
        self.file.write_all(
            &u32::try_from(bytes.len())
                .expect("bounded spill frame")
                .to_le_bytes(),
        )?;
        self.file.write_all(bytes)?;
        self.write_offset = self
            .write_offset
            .saturating_add(4 + u64::try_from(bytes.len()).unwrap_or(u64::MAX));
        Ok(())
    }
    fn read_frame(&mut self) -> std::io::Result<Option<Vec<u8>>> {
        if self.read_offset >= self.write_offset {
            return Ok(None);
        }
        self.file.seek(SeekFrom::Start(self.read_offset))?;
        let mut length = [0; 4];
        self.file.read_exact(&mut length)?;
        let length = u32::from_le_bytes(length) as usize;
        if length > MAX_FRAME_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid spill record length",
            ));
        }
        let mut bytes = vec![0; length];
        self.file.read_exact(&mut bytes)?;
        self.read_offset = self
            .read_offset
            .saturating_add(4 + u64::try_from(length).unwrap_or(u64::MAX));
        if self.read_offset == self.write_offset {
            self.file.set_len(0)?;
            self.read_offset = 0;
            self.write_offset = 0;
        }
        Ok(Some(bytes))
    }
}

/// A private blocking-I/O owner. Callers wait with a deadline/cancellation check;
/// a wedged filesystem can strand only this detached thread, then poison its worker.
#[derive(Debug)]
struct SpoolIo {
    commands: Sender<SpoolCommand>,
}
impl SpoolIo {
    fn new(file: File, poisoned: Arc<AtomicBool>) -> Self {
        Self::with_log(FileSpillLog::new(file), poisoned)
    }
    fn with_log(log: impl SpillLog, poisoned: Arc<AtomicBool>) -> Self {
        let (commands, receiver) = mpsc::channel();
        thread::spawn(move || {
            let mut log: Box<dyn SpillLog> = Box::new(log);
            while let Ok(command) = receiver.recv() {
                match command {
                    SpoolCommand::Write(bytes, response) => {
                        let result = log.write_frame(&bytes).map(|()| bytes.len());
                        if result.is_err() {
                            poisoned.store(true, Ordering::Release);
                        }
                        let _ = response.send(result);
                    }
                    SpoolCommand::Read(response) => {
                        let result = log.read_frame();
                        if result.is_err() {
                            poisoned.store(true, Ordering::Release);
                        }
                        let _ = response.send(result);
                    }
                }
            }
        });
        Self { commands }
    }
    fn enqueue_write(&self, bytes: Vec<u8>) -> Result<Receiver<std::io::Result<usize>>> {
        let (response, result) = mpsc::sync_channel(1);
        self.commands
            .send(SpoolCommand::Write(bytes, response))
            .map_err(|_| Error::Invalid("spill I/O worker stopped".into()))?;
        Ok(result)
    }
    fn enqueue_read(&self) -> Result<Receiver<std::io::Result<Option<Vec<u8>>>>> {
        let (response, result) = mpsc::sync_channel(1);
        self.commands
            .send(SpoolCommand::Read(response))
            .map_err(|_| Error::Invalid("spill I/O worker stopped".into()))?;
        Ok(result)
    }
    fn wait<T>(
        response: &Receiver<std::io::Result<T>>,
        token: &CancellationToken,
        poisoned: &AtomicBool,
    ) -> Result<T> {
        let deadline = Instant::now() + RETENTION_IO_TIMEOUT;
        loop {
            if poisoned.load(Ordering::Acquire) {
                token.cancel();
                return Err(Error::Invalid("spill I/O failed; worker poisoned".into()));
            }
            if token.check().is_err() {
                poisoned.store(true, Ordering::Release);
                return Err(Error::Invalid(
                    "spill I/O cancelled; worker poisoned".into(),
                ));
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                poisoned.store(true, Ordering::Release);
                token.cancel();
                return Err(Error::Invalid(
                    "spill I/O deadline exceeded; worker poisoned".into(),
                ));
            }
            match response.recv_timeout(remaining.min(IO_POLL_INTERVAL)) {
                Ok(Ok(value)) => return Ok(value),
                Ok(Err(error)) => {
                    poisoned.store(true, Ordering::Release);
                    return Err(Error::Io(error));
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    poisoned.store(true, Ordering::Release);
                    return Err(Error::Invalid("spill I/O worker disconnected".into()));
                }
            }
        }
    }
}
fn nanos(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

/// Provider-neutral bounded segment objects. Backend ownership is injected at the
/// worker-session boundary; the local filesystem remains the deterministic default.
pub(crate) struct Objects {
    backend: Arc<dyn ObjectStore>,
    root_guard: Option<Arc<tempfile::TempDir>>,
    config: ObjectStoreConfig,
    poisoned: Arc<AtomicBool>,
    operation_timeout: Duration,
    metrics: Mutex<RetentionMetrics>,
}
impl Objects {
    pub fn configured(
        parent: &FsPath,
        config: ObjectStoreConfig,
        supplied: Option<&ObjectStoreBackend>,
        poisoned: Arc<AtomicBool>,
    ) -> Result<Self> {
        let (backend, root): (Arc<dyn ObjectStore>, Option<Arc<tempfile::TempDir>>) =
            if let Some(supplied) = supplied {
                require(
                    supplied.config == config,
                    "worker object-store backend does not match job configuration",
                )?;
                (supplied.store.clone(), None)
            } else {
                require(
                    config == ObjectStoreConfig::default(),
                    "configured object-store backend is not installed on this worker",
                )?;
                let root = tempfile::Builder::new()
                    .prefix("ade-objects-")
                    .tempdir_in(parent)?;
                let root = Arc::new(root);
                let backend = Arc::new(LocalFileSystem::new_with_prefix(root.path())?);
                (backend, Some(root))
            };
        Ok(Self {
            backend,
            root_guard: root,
            config,
            poisoned,
            operation_timeout: RETENTION_IO_TIMEOUT,
            metrics: Mutex::new(RetentionMetrics::default()),
        })
    }
    fn key(id: TransferId, sequence: u64) -> Path {
        Path::from(format!(
            "job-{}/exchange-{}/partition-{}/producer-{}/attempt-{}/segment-{sequence}.arrow",
            id.job.0, id.exchange.0, id.partition.0, id.producer.0, id.attempt.0
        ))
    }
    pub fn put(
        &self,
        id: TransferId,
        sequence: u64,
        bytes: Vec<u8>,
        token: &CancellationToken,
    ) -> Result<()> {
        let length = bytes.len() as u64;
        let backend = self.backend.clone();
        self.run(
            async move {
                backend
                    .put(&Self::key(id, sequence), bytes.into())
                    .await
                    .map_err(Error::ObjectStore)
            },
            token,
        )?;
        let mut m = self.metrics.lock().expect("object metrics");
        m.object_count += 1;
        m.object_write_bytes += length;
        Ok(())
    }
    pub fn get(&self, id: TransferId, sequence: u64, token: &CancellationToken) -> Result<Vec<u8>> {
        let key = Self::key(id, sequence);
        let backend = self.backend.clone();
        let bytes = self.run(
            async move {
                let metadata = backend.head(&key).await?;
                if metadata.size > MAX_FRAME_BYTES as u64 {
                    return Err(Error::Invalid("object exceeds segment cap".into()));
                }
                Ok::<_, Error>(backend.get_range(&key, 0..metadata.size).await?)
            },
            token,
        )?;
        self.metrics
            .lock()
            .expect("object metrics")
            .object_read_bytes += bytes.len() as u64;
        Ok(bytes.to_vec())
    }
    fn run<T, F>(&self, operation: F, token: &CancellationToken) -> Result<T>
    where
        F: std::future::Future<Output = Result<T>> + Send + 'static,
        T: Send + 'static,
    {
        require(
            !self.poisoned.load(Ordering::Acquire),
            "object-store worker is poisoned",
        )?;
        token.check()?;
        let (response, result) = mpsc::sync_channel(1);
        let keepalive = self.root_guard.clone();
        thread::Builder::new()
            .name("ade-object-io".into())
            .spawn(move || {
                // The detached owner retains both its backend (in the future) and
                // any ephemeral filesystem root until a non-cooperative poll exits.
                let _keepalive = keepalive;
                let outcome = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => {
                        let outcome = runtime.block_on(operation);
                        // Provider work may have left blocking tasks behind. Keep
                        // teardown bounded on this detached owner too.
                        runtime.shutdown_timeout(Duration::ZERO);
                        outcome
                    }
                    Err(error) => Err(Error::Io(error)),
                };
                let _ = response.send(outcome);
            })
            .map_err(Error::Io)?;
        let deadline = Instant::now() + self.operation_timeout;
        loop {
            if token.check().is_err() {
                self.poisoned.store(true, Ordering::Release);
                return Err(Error::Invalid(
                    "object-store I/O cancelled; worker poisoned".into(),
                ));
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                self.poisoned.store(true, Ordering::Release);
                token.cancel();
                return Err(Error::Invalid(
                    "object-store I/O deadline exceeded; worker poisoned".into(),
                ));
            }
            match result.recv_timeout(remaining.min(IO_POLL_INTERVAL)) {
                Ok(Ok(value)) => return Ok(value),
                Ok(Err(error)) => {
                    self.poisoned.store(true, Ordering::Release);
                    return Err(error);
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    self.poisoned.store(true, Ordering::Release);
                    return Err(Error::Invalid("object-store I/O owner disconnected".into()));
                }
            }
        }
    }
    pub fn metrics(&self) -> RetentionMetrics {
        let mut metrics = self.metrics.lock().expect("object metrics").clone();
        metrics.object_store_backend = Some(self.config.backend.clone());
        metrics.object_store_durability = Some(self.config.durability);
        metrics
    }
}
// Object I/O owners are detached. Their captured backend and filesystem root outlive
// the failed exchange if a provider poll does not return promptly.

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc::{Receiver as TestReceiver, Sender as TestSender};

    #[derive(Clone, Copy)]
    enum SpillOperation {
        Write,
        Read,
    }
    struct SpillGate {
        block_write: AtomicBool,
        block_read: AtomicBool,
        started: TestSender<SpillOperation>,
        release: Mutex<TestReceiver<()>>,
    }
    impl SpillGate {
        fn pause_if_armed(&self, operation: SpillOperation) {
            let armed = match operation {
                SpillOperation::Write => &self.block_write,
                SpillOperation::Read => &self.block_read,
            };
            if armed.swap(false, Ordering::AcqRel) {
                self.started.send(operation).unwrap();
                self.release.lock().unwrap().recv().unwrap();
            }
        }
    }
    struct MemorySpillLog {
        gate: Arc<SpillGate>,
        frames: VecDeque<Vec<u8>>,
    }
    struct HangingStore {
        inner: object_store::memory::InMemory,
        put_started: TestSender<()>,
        get_started: TestSender<()>,
        put_release: Mutex<Option<TestReceiver<()>>>,
        get_release: Mutex<Option<TestReceiver<()>>>,
    }
    impl std::fmt::Debug for HangingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("HangingStore")
        }
    }
    impl std::fmt::Display for HangingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("hanging test object store")
        }
    }
    #[async_trait::async_trait]
    impl ObjectStore for HangingStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: object_store::PutPayload,
            options: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            let _ = self.put_started.send(());
            // Deliberately block in the first poll: Tokio timers cannot preempt this.
            self.put_release
                .lock()
                .unwrap()
                .take()
                .unwrap()
                .recv()
                .unwrap();
            self.inner.put_opts(location, payload, options).await
        }
        async fn put_multipart_opts(
            &self,
            location: &Path,
            options: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, options).await
        }
        async fn get_opts(
            &self,
            location: &Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            let _ = self.get_started.send(());
            // Deliberately block in the first poll, matching a non-cooperative provider.
            self.get_release
                .lock()
                .unwrap()
                .take()
                .unwrap()
                .recv()
                .unwrap();
            self.inner.get_opts(location, options).await
        }
        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<'static, object_store::Result<Path>>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }
        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }
    impl SpillLog for MemorySpillLog {
        fn write_frame(&mut self, bytes: &[u8]) -> std::io::Result<()> {
            self.gate.pause_if_armed(SpillOperation::Write);
            self.frames.push_back(bytes.to_vec());
            Ok(())
        }
        fn read_frame(&mut self) -> std::io::Result<Option<Vec<u8>>> {
            self.gate.pause_if_armed(SpillOperation::Read);
            Ok(self.frames.pop_front())
        }
    }
    fn blocking_spill_gate() -> (Arc<SpillGate>, TestReceiver<SpillOperation>, TestSender<()>) {
        let (started, started_rx) = mpsc::channel();
        let (release_tx, release) = mpsc::channel();
        (
            Arc::new(SpillGate {
                block_write: AtomicBool::new(false),
                block_read: AtomicBool::new(false),
                started,
                release: Mutex::new(release),
            }),
            started_rx,
            release_tx,
        )
    }

    #[test]
    fn bounded_buffer_backpressure_and_cancellation() {
        for mode in [ExchangeMode::Direct, ExchangeMode::Buffered] {
            let root = tempfile::tempdir().unwrap();
            let buffer = Arc::new(
                Buffer::new(
                    ExchangeConfig {
                        mode,
                        buffer_bytes: MAX_FRAME_BYTES,
                        ..Default::default()
                    },
                    root.path(),
                )
                .unwrap(),
            );
            let token = CancellationToken::default();
            if mode == ExchangeMode::Buffered {
                buffer.push(vec![0; MAX_FRAME_BYTES], &token).unwrap();
            }
            let writer = buffer.clone();
            let cancel = token.clone();
            let thread = std::thread::spawn(move || writer.push(vec![1; 12], &cancel));
            std::thread::sleep(Duration::from_millis(20));
            assert!(!thread.is_finished());
            token.cancel();
            assert!(thread.join().unwrap().is_err());
            assert!(buffer.metrics().peak_buffer_bytes <= MAX_FRAME_BYTES);
        }
    }
    #[test]
    fn spill_fifo_has_constant_metadata_and_exact_once_readback() {
        let root = tempfile::tempdir().unwrap();
        let buffer = Buffer::new(
            ExchangeConfig {
                mode: ExchangeMode::Spill,
                buffer_bytes: 10,
                ..Default::default()
            },
            root.path(),
        )
        .unwrap();
        let token = CancellationToken::default();
        for value in 0..100 {
            buffer.push(vec![value; 8], &token).unwrap();
        }
        for value in 0..100 {
            assert_eq!(buffer.take(&token).unwrap().unwrap(), vec![value; 8]);
        }
        assert!(buffer.take(&token).unwrap().is_none());
        let m = buffer.metrics();
        assert_eq!(m.peak_buffer_bytes, 8);
        assert_eq!(m.spill_write_bytes, 99 * 8);
        assert_eq!(m.spill_read_bytes, m.spill_write_bytes);
        assert_eq!(m.current_buffer_bytes, 0);
        assert_eq!(m.current_spill_backlog_bytes, 0);
    }
    #[test]
    fn blocked_spill_write_cancellation_releases_retention_state_and_poisons_worker() {
        let root = tempfile::tempdir().unwrap();
        let poisoned = Arc::new(AtomicBool::new(false));
        let mut buffer = Buffer::new_with_poison(
            ExchangeConfig {
                mode: ExchangeMode::Spill,
                buffer_bytes: 1,
                ..Default::default()
            },
            root.path(),
            poisoned.clone(),
        )
        .unwrap();
        let (gate, started, release) = blocking_spill_gate();
        gate.block_write.store(true, Ordering::Release);
        buffer.spool = Some(SpoolIo::with_log(
            MemorySpillLog {
                gate,
                frames: VecDeque::new(),
            },
            poisoned.clone(),
        ));
        let buffer = Arc::new(buffer);
        let token = CancellationToken::default();
        let writer = buffer.clone();
        let cancel = token.clone();
        let task = thread::spawn(move || writer.push(vec![7; 8], &cancel));
        assert!(matches!(
            started.recv_timeout(Duration::from_secs(1)),
            Ok(SpillOperation::Write)
        ));
        token.cancel();
        assert!(task.join().unwrap().is_err());
        assert!(poisoned.load(Ordering::Acquire));
        assert_eq!(buffer.metrics().current_spill_backlog_bytes, 8);
        release.send(()).unwrap();
    }
    #[test]
    fn blocked_spill_read_cancellation_releases_retention_state_and_poisons_worker() {
        let root = tempfile::tempdir().unwrap();
        let poisoned = Arc::new(AtomicBool::new(false));
        let mut buffer = Buffer::new_with_poison(
            ExchangeConfig {
                mode: ExchangeMode::Spill,
                buffer_bytes: 1,
                ..Default::default()
            },
            root.path(),
            poisoned.clone(),
        )
        .unwrap();
        let (gate, started, release) = blocking_spill_gate();
        let gate_for_test = gate.clone();
        buffer.spool = Some(SpoolIo::with_log(
            MemorySpillLog {
                gate,
                frames: VecDeque::new(),
            },
            poisoned.clone(),
        ));
        let token = CancellationToken::default();
        buffer.push(vec![9; 8], &token).unwrap();
        gate_for_test.block_read.store(true, Ordering::Release);
        let buffer = Arc::new(buffer);
        let reader = buffer.clone();
        let cancel = token.clone();
        let task = thread::spawn(move || reader.take(&cancel));
        assert!(matches!(
            started.recv_timeout(Duration::from_secs(1)),
            Ok(SpillOperation::Read)
        ));
        token.cancel();
        assert!(task.join().unwrap().is_err());
        assert!(poisoned.load(Ordering::Acquire));
        assert_eq!(buffer.metrics().current_spill_backlog_bytes, 8);
        release.send(()).unwrap();
    }
    #[test]
    fn object_keys_isolate_identity_and_backend_errors_propagate() {
        let root = tempfile::tempdir().unwrap();
        let objects = Objects::configured(
            root.path(),
            ObjectStoreConfig::default(),
            None,
            Arc::default(),
        )
        .unwrap();
        let token = CancellationToken::default();
        let id = crate::ids::transfer(crate::ids::JobId(1), 0, 0, 0);
        objects.put(id, 0, vec![1, 2, 3], &token).unwrap();
        assert_eq!(objects.get(id, 0, &token).unwrap(), vec![1, 2, 3]);
        assert!(
            objects
                .get(
                    crate::ids::transfer(crate::ids::JobId(2), 0, 0, 0),
                    0,
                    &token
                )
                .is_err()
        );
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(objects.backend.delete(&Objects::key(id, 0)))
            .unwrap();
        assert!(objects.get(id, 0, &token).is_err());
    }
    #[test]
    fn supplied_object_store_backend_is_used_and_reported() {
        let root = tempfile::tempdir().unwrap();
        let config = ObjectStoreConfig {
            backend: "memory_test".into(),
            durability: ObjectStoreDurability::Ephemeral,
        };
        let backend = ObjectStoreBackend::new(
            config.clone(),
            Arc::new(object_store::memory::InMemory::new()),
        )
        .unwrap();
        let objects =
            Objects::configured(root.path(), config.clone(), Some(&backend), Arc::default())
                .unwrap();
        let token = CancellationToken::default();
        let id = crate::ids::transfer(crate::ids::JobId(7), 0, 0, 0);
        objects.put(id, 0, vec![4, 5, 6], &token).unwrap();
        assert_eq!(objects.get(id, 0, &token).unwrap(), vec![4, 5, 6]);
        assert_eq!(
            objects.metrics().object_store_backend.as_deref(),
            Some("memory_test")
        );
    }
    #[test]
    fn hanging_object_store_put_and_get_obey_cancel_and_deadline() {
        let root = tempfile::tempdir().unwrap();
        let (put_started, put_started_rx) = mpsc::channel();
        let (get_started, _get_started_rx) = mpsc::channel();
        let (put_release_tx, put_release_rx) = mpsc::channel();
        let (_get_release_tx, get_release_rx) = mpsc::channel();
        let config = ObjectStoreConfig {
            backend: "hanging_test".into(),
            durability: ObjectStoreDurability::Ephemeral,
        };
        let backend = ObjectStoreBackend::new(
            config.clone(),
            Arc::new(HangingStore {
                inner: object_store::memory::InMemory::new(),
                put_started,
                get_started,
                put_release: Mutex::new(Some(put_release_rx)),
                get_release: Mutex::new(Some(get_release_rx)),
            }),
        )
        .unwrap();
        let poisoned = Arc::new(AtomicBool::new(false));
        let mut objects =
            Objects::configured(root.path(), config, Some(&backend), poisoned.clone()).unwrap();
        objects.operation_timeout = Duration::from_millis(100);
        let objects = Arc::new(objects);
        let id = crate::ids::transfer(crate::ids::JobId(8), 0, 0, 0);
        let token = CancellationToken::default();

        let writing = objects.clone();
        let write_token = token.clone();
        let write = thread::spawn(move || writing.put(id, 0, vec![1, 2, 3], &write_token));
        put_started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        token.cancel();
        assert!(write.join().unwrap().is_err());
        assert!(poisoned.load(Ordering::Acquire));
        assert!(objects.put(id, 1, vec![4], &token).is_err());
        let _ = put_release_tx.send(());

        let root = tempfile::tempdir().unwrap();
        let (put_started, _put_started_rx) = mpsc::channel();
        let (get_started, get_started_rx) = mpsc::channel();
        let (put_release_tx, put_release_rx) = mpsc::channel();
        let (get_release_tx, get_release_rx) = mpsc::channel();
        let backend = ObjectStoreBackend::new(
            ObjectStoreConfig {
                backend: "hanging_get_test".into(),
                durability: ObjectStoreDurability::Ephemeral,
            },
            Arc::new(HangingStore {
                inner: object_store::memory::InMemory::new(),
                put_started,
                get_started,
                put_release: Mutex::new(Some(put_release_rx)),
                get_release: Mutex::new(Some(get_release_rx)),
            }),
        )
        .unwrap();
        let config = backend.config.clone();
        let poisoned = Arc::new(AtomicBool::new(false));
        let mut objects =
            Objects::configured(root.path(), config, Some(&backend), poisoned.clone()).unwrap();
        objects.operation_timeout = Duration::from_millis(100);
        let objects = Arc::new(objects);
        let token = CancellationToken::default();
        let reading = objects.clone();
        let read_token = token.clone();
        let read = thread::spawn(move || reading.get(id, 0, &read_token));
        get_started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(read.join().unwrap().is_err());
        assert!(token.check().is_err());
        assert!(poisoned.load(Ordering::Acquire));
        let _ = get_release_tx.send(());
        let _ = put_release_tx.send(());
    }
}
