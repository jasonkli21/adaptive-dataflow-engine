//! Static retention mechanisms. Routing and network identity live outside this module.
use crate::{Error, Result, exchange::MAX_FRAME_BYTES, ids::TransferId, require};
use ade_exec::CancellationToken;
use object_store::{ObjectStore, ObjectStoreExt, local::LocalFileSystem, path::Path};
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    path::Path as FsPath,
    sync::{Arc, Condvar, Mutex},
    time::{Duration, Instant},
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExchangeMode {
    Direct,
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
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExchangeConfig {
    pub mode: ExchangeMode,
    pub buffer_bytes: usize,
}
impl Default for ExchangeConfig {
    fn default() -> Self {
        Self {
            mode: ExchangeMode::LocalMaterialized,
            buffer_bytes: 4 * MAX_FRAME_BYTES,
        }
    }
}
impl ExchangeConfig {
    /// # Errors
    /// Rejects zero/excessive limits; buffered mode must fit the largest valid frame.
    pub fn validate(&self) -> Result<()> {
        require(
            (1..=256 * MAX_FRAME_BYTES).contains(&self.buffer_bytes),
            "exchange buffer must be 1..=268435456 bytes",
        )?;
        require(
            self.mode != ExchangeMode::Buffered || self.buffer_bytes >= MAX_FRAME_BYTES,
            "buffered exchange requires at least max_frame_bytes",
        )
    }
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetentionMetrics {
    pub peak_buffer_bytes: usize,
    pub spill_write_bytes: u64,
    pub spill_read_bytes: u64,
    pub local_materialized_bytes: u64,
    pub local_materialized_files: u64,
    pub object_write_bytes: u64,
    pub object_read_bytes: u64,
    pub object_count: u64,
    pub producer_wait_ns: u64,
    pub consumer_wait_ns: u64,
}
#[derive(Debug)]
struct BufferState {
    queue: VecDeque<Vec<u8>>,
    bytes: usize,
    waiting: bool,
    spool: Option<File>,
    read_offset: u64,
    write_offset: u64,
    metrics: RetentionMetrics,
}
/// One queue/spool per destination, with constant disk metadata regardless of segment count.
#[derive(Debug)]
pub(crate) struct Buffer {
    config: ExchangeConfig,
    state: Mutex<BufferState>,
    changed: Condvar,
}
impl Buffer {
    pub fn new(config: ExchangeConfig, root: &FsPath) -> Result<Self> {
        let spool = if config.mode == ExchangeMode::Spill {
            Some(tempfile::tempfile_in(root)?)
        } else {
            None
        };
        Ok(Self {
            config,
            state: Mutex::new(BufferState {
                queue: VecDeque::new(),
                bytes: 0,
                waiting: false,
                spool,
                read_offset: 0,
                write_offset: 0,
                metrics: RetentionMetrics::default(),
            }),
            changed: Condvar::new(),
        })
    }
    pub fn push(&self, encoded: Vec<u8>, token: &CancellationToken) -> Result<()> {
        let start = Instant::now();
        let mut s = self.state.lock().expect("buffer lock");
        loop {
            token.check()?;
            match self.config.mode {
                ExchangeMode::Direct if s.waiting && s.queue.is_empty() => break,
                ExchangeMode::Buffered if encoded.len() <= self.config.buffer_bytes - s.bytes => {
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
        s.metrics.producer_wait_ns += nanos(start);
        if self.config.mode == ExchangeMode::Spill
            && (s.write_offset > s.read_offset
                || encoded.len() > self.config.buffer_bytes - s.bytes)
        {
            let offset = s.write_offset;
            let file = s.spool.as_mut().expect("spill file");
            file.seek(SeekFrom::Start(offset))?;
            file.write_all(
                &u32::try_from(encoded.len())
                    .expect("bounded frame")
                    .to_le_bytes(),
            )?;
            file.write_all(&encoded)?;
            s.write_offset += 4 + encoded.len() as u64;
            s.metrics.spill_write_bytes += encoded.len() as u64;
        } else {
            s.bytes += encoded.len();
            s.metrics.peak_buffer_bytes = s.metrics.peak_buffer_bytes.max(s.bytes);
            s.queue.push_back(encoded);
        }
        self.changed.notify_all();
        if self.config.mode == ExchangeMode::Direct {
            let start = Instant::now();
            while !s.queue.is_empty() {
                token.check()?;
                s = self
                    .changed
                    .wait_timeout(s, Duration::from_millis(10))
                    .expect("handoff wait")
                    .0;
            }
            s.metrics.producer_wait_ns += nanos(start);
        }
        Ok(())
    }
    pub fn take(&self) -> Result<Option<Vec<u8>>> {
        let mut s = self.state.lock().expect("buffer lock");
        s.waiting = true;
        self.changed.notify_all();
        if let Some(frame) = s.queue.pop_front() {
            s.bytes -= frame.len();
            s.waiting = false;
            self.changed.notify_all();
            return Ok(Some(frame));
        }
        if s.read_offset < s.write_offset {
            let offset = s.read_offset;
            let file = s.spool.as_mut().expect("spill file");
            file.seek(SeekFrom::Start(offset))?;
            let mut length = [0; 4];
            file.read_exact(&mut length)?;
            let length = u32::from_le_bytes(length) as usize;
            require(length <= MAX_FRAME_BYTES, "invalid spill record")?;
            let mut encoded = vec![0; length];
            file.read_exact(&mut encoded)?;
            s.read_offset += 4 + length as u64;
            s.metrics.spill_read_bytes += length as u64;
            // Reuse the file rather than growing it with consumed segments.
            if s.read_offset == s.write_offset {
                s.spool.as_mut().expect("spill file").set_len(0)?;
                s.read_offset = 0;
                s.write_offset = 0;
            }
            s.waiting = false;
            return Ok(Some(encoded));
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
fn nanos(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

/// Provider-neutral bounded segment objects; root is separate from local exchange files.
/// Backend capability, not this interface, determines durability. No recovery is provided.
pub(crate) struct Objects {
    backend: Arc<dyn ObjectStore>,
    runtime: tokio::runtime::Runtime,
    _root: tempfile::TempDir,
    metrics: Mutex<RetentionMetrics>,
}
impl Objects {
    pub fn local(parent: &FsPath) -> Result<Self> {
        let root = tempfile::Builder::new()
            .prefix("ade-objects-")
            .tempdir_in(parent)?;
        let backend = Arc::new(LocalFileSystem::new_with_prefix(root.path())?);
        Ok(Self {
            backend,
            runtime: tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?,
            _root: root,
            metrics: Mutex::new(RetentionMetrics::default()),
        })
    }
    fn key(id: TransferId, sequence: u64) -> Path {
        Path::from(format!(
            "job-{}/exchange-{}/partition-{}/producer-{}/attempt-{}/segment-{sequence}.arrow",
            id.job.0, id.exchange.0, id.partition.0, id.producer.0, id.attempt.0
        ))
    }
    pub fn put(&self, id: TransferId, sequence: u64, bytes: Vec<u8>) -> Result<()> {
        let length = bytes.len() as u64;
        self.runtime
            .block_on(self.backend.put(&Self::key(id, sequence), bytes.into()))?;
        let mut m = self.metrics.lock().expect("object metrics");
        m.object_count += 1;
        m.object_write_bytes += length;
        Ok(())
    }
    pub fn get(&self, id: TransferId, sequence: u64) -> Result<Vec<u8>> {
        let key = Self::key(id, sequence);
        let bytes = self.runtime.block_on(async {
            let metadata = self.backend.head(&key).await?;
            if metadata.size > MAX_FRAME_BYTES as u64 {
                return Err(Error::Invalid("object exceeds segment cap".into()));
            }
            Ok::<_, Error>(self.backend.get_range(&key, 0..metadata.size).await?)
        })?;
        self.metrics
            .lock()
            .expect("object metrics")
            .object_read_bytes += bytes.len() as u64;
        Ok(bytes.to_vec())
    }
    pub fn metrics(&self) -> RetentionMetrics {
        self.metrics.lock().expect("object metrics").clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bounded_buffer_backpressure_and_cancellation() {
        for mode in [ExchangeMode::Direct, ExchangeMode::Buffered] {
            let root = tempfile::tempdir().unwrap();
            let buffer = Arc::new(
                Buffer::new(
                    ExchangeConfig {
                        mode,
                        buffer_bytes: MAX_FRAME_BYTES,
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
            },
            root.path(),
        )
        .unwrap();
        let token = CancellationToken::default();
        for value in 0..100 {
            buffer.push(vec![value; 8], &token).unwrap();
        }
        for value in 0..100 {
            assert_eq!(buffer.take().unwrap().unwrap(), vec![value; 8]);
        }
        assert!(buffer.take().unwrap().is_none());
        let m = buffer.metrics();
        assert_eq!(m.peak_buffer_bytes, 8);
        assert_eq!(m.spill_write_bytes, 99 * 8);
        assert_eq!(m.spill_read_bytes, m.spill_write_bytes);
        assert_eq!(
            buffer
                .state
                .lock()
                .unwrap()
                .spool
                .as_ref()
                .unwrap()
                .metadata()
                .unwrap()
                .len(),
            0
        );
    }
    #[test]
    fn object_keys_isolate_identity_and_backend_errors_propagate() {
        let root = tempfile::tempdir().unwrap();
        let objects = Objects::local(root.path()).unwrap();
        let id = crate::ids::transfer(crate::ids::JobId(1), 0, 0, 0);
        objects.put(id, 0, vec![1, 2, 3]).unwrap();
        assert_eq!(objects.get(id, 0).unwrap(), vec![1, 2, 3]);
        assert!(
            objects
                .get(crate::ids::transfer(crate::ids::JobId(2), 0, 0, 0), 0)
                .is_err()
        );
        objects
            .runtime
            .block_on(objects.backend.delete(&Objects::key(id, 0)))
            .unwrap();
        assert!(objects.get(id, 0).is_err());
    }
}
