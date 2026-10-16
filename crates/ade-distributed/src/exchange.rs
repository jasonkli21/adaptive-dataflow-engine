//! Bounded TCP frames and ephemeral IPC retention. ACK follows file finalization.
use crate::{
    Error, Result, ids::TransferId, metrics::NetworkMetrics, partition::HASH_VERSION, protocol,
    require,
};
use ade_exec::CancellationToken;
use arrow::{
    datatypes::{DataType, Field, Schema, SchemaRef},
    ipc::{reader::StreamReader, writer::StreamWriter},
    record_batch::RecordBatch,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::{Cursor, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::Duration,
};
pub const MAX_FRAME_BYTES: usize = 1024 * 1024;
pub const MAX_BATCH_ROWS: usize = 1024;
/// Maximum four simultaneous producer streams per receiver; no unbounded accept queue in userspace.
const MAX_CONNECTIONS: usize = 4;
#[must_use]
pub fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::UInt64, false),
        Field::new("sum", DataType::Int64, false),
        Field::new("count", DataType::UInt64, false),
    ]))
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Header {
    pub id: TransferId,
    pub hash_version: u32,
    pub schema_version: u32,
}
impl Header {
    /// Construct the current private schema/hash version envelope.
    #[must_use]
    pub fn new(id: TransferId) -> Self {
        Self {
            id,
            hash_version: HASH_VERSION,
            schema_version: 1,
        }
    }
    fn validate(&self) -> Result<()> {
        require(
            self.hash_version == HASH_VERSION && self.schema_version == 1,
            "exchange hash/schema version mismatch",
        )
    }
}
/// Codec output cannot grow beyond its declared cap, including transient serialization.
pub(crate) struct BoundedBuffer(pub Vec<u8>, usize);
impl BoundedBuffer {
    /// Start an empty codec buffer with a hard encoded-length cap.
    pub fn new(cap: usize) -> Self {
        Self(Vec::new(), cap)
    }
}
impl Write for BoundedBuffer {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.1.saturating_sub(self.0.len()) {
            return Err(std::io::Error::other("encoded frame exceeds limit"));
        }
        // Exact reservation avoids geometric capacity growth beyond the cap.
        self.0.reserve_exact(bytes.len());
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
// Check advertised IPC allocations against the already bounded frame before Arrow's
// streaming decoder allocates metadata/body buffers. Compression is outside Phase 3.
fn validate_ipc_envelope(encoded: &[u8]) -> Result<()> {
    let mut offset = 0usize;
    let mut messages = 0;
    loop {
        let prefix = encoded
            .get(offset..offset + 8)
            .ok_or_else(|| Error::Invalid("truncated IPC prefix".into()))?;
        require(prefix[..4] == [255; 4], "IPC continuation marker required")?;
        let length = u32::from_le_bytes(prefix[4..8].try_into().expect("four bytes")) as usize;
        offset += 8;
        if length == 0 {
            return require(
                offset == encoded.len() && messages == 2,
                "IPC frame must be schema, batch, EOS",
            );
        }
        let end = offset
            .checked_add(length)
            .ok_or_else(|| Error::Invalid("IPC metadata length overflow".into()))?;
        let metadata = encoded
            .get(offset..end)
            .ok_or_else(|| Error::Invalid("IPC metadata exceeds frame".into()))?;
        let message = arrow::ipc::root_as_message(metadata)
            .map_err(|e| Error::Invalid(format!("invalid IPC metadata: {e}")))?;
        let body = usize::try_from(message.bodyLength())
            .map_err(|_| Error::Invalid("negative IPC body length".into()))?;
        offset = end
            .checked_add(body)
            .ok_or_else(|| Error::Invalid("IPC body length overflow".into()))?;
        require(offset <= encoded.len(), "IPC body exceeds frame")?;
        match messages {
            0 => require(
                message.header_type() == arrow::ipc::MessageHeader::Schema && body == 0,
                "IPC schema required",
            )?,
            1 => {
                let batch = message
                    .header_as_record_batch()
                    .ok_or_else(|| Error::Invalid("IPC record batch required".into()))?;
                require(
                    (0..=i64::try_from(MAX_BATCH_ROWS).expect("constant fits i64"))
                        .contains(&batch.length())
                        && batch.compression().is_none(),
                    "IPC row cap/compression violation",
                )?;
            }
            _ => return Err(Error::Invalid("too many IPC messages".into())),
        }
        messages += 1;
    }
}
fn validate_batch(batch: &RecordBatch) -> Result<()> {
    require(batch.schema() == schema(), "exchange schema mismatch")?;
    require(
        batch.num_rows() <= MAX_BATCH_ROWS,
        "exchange row cap exceeded",
    )?;
    require(
        batch.columns().iter().all(|a| a.null_count() == 0),
        "exchange nulls forbidden",
    )
}
pub struct Sender {
    id: TransferId,
    socket: TcpStream,
    pub metrics: NetworkMetrics,
    cancellation: CancellationToken,
}
impl Sender {
    /// # Errors
    /// Returns cancellation, connection, header encoding or socket errors.
    pub fn new(
        address: SocketAddr,
        id: TransferId,
        cancellation: CancellationToken,
    ) -> Result<Self> {
        cancellation.check()?;
        let mut socket = protocol::connect(address, crate::IO_TIMEOUT)?;
        protocol::write_message(
            &mut protocol::DeadlineStream::new(&mut socket, crate::IO_TIMEOUT),
            &Header::new(id),
        )?;
        Ok(Self {
            id,
            socket,
            metrics: NetworkMetrics::default(),
            cancellation,
        })
    }
    /// # Errors
    /// Rejects incompatible or oversized batches, cancellation, encoding and socket failures.
    pub fn batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.cancellation.check()?;
        validate_batch(batch)?;
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let mut encoded = BoundedBuffer::new(MAX_FRAME_BYTES);
        {
            let mut writer = StreamWriter::try_new(&mut encoded, &schema())?;
            writer.write(batch)?;
            writer.finish()?;
        }
        let mut socket = protocol::DeadlineStream::new(&mut self.socket, crate::IO_TIMEOUT);
        socket.write_all(
            &u32::try_from(encoded.0.len())
                .map_err(|_| Error::Invalid("frame length overflow".into()))?
                .to_le_bytes(),
        )?;
        socket.write_all(&encoded.0)?;
        self.metrics.rows += batch.num_rows() as u64;
        self.metrics.payload_bytes += encoded.0.len() as u64;
        self.metrics.peak_frame_bytes = self.metrics.peak_frame_bytes.max(encoded.0.len());
        Ok(())
    }
    /// # Errors
    /// Returns cancellation, transport, rejection, or mismatched completion ACK errors.
    pub fn finish(mut self) -> Result<NetworkMetrics> {
        self.cancellation.check()?;
        let mut socket = protocol::DeadlineStream::new(&mut self.socket, crate::IO_TIMEOUT);
        socket.write_all(&0u32.to_le_bytes())?;
        let ack: std::result::Result<TransferId, String> = protocol::read_message(&mut socket)?;
        let id = ack.map_err(|message| Error::Remote {
            code: "exchange".into(),
            message,
        })?;
        // Identity must match the original request, even on private connections.
        require(id == self.id, "ACK identity mismatch")?;
        self.metrics.transfers = 1;
        Ok(self.metrics)
    }
}
struct Registry {
    completed: BTreeMap<TransferId, (PathBuf, NetworkMetrics)>,
    started: BTreeSet<TransferId>,
}
/// One fixed expected transfer set and a unique execution-local root.
pub struct Store {
    root: tempfile::TempDir,
    expected: BTreeSet<TransferId>,
    registry: Mutex<Registry>,
    pub cancellation: CancellationToken,
}
impl Store {
    /// # Errors
    /// Returns invalid expected transfers or I/O errors creating the store/connection.
    pub fn new(
        parent: &Path,
        expected: BTreeSet<TransferId>,
        cancellation: CancellationToken,
    ) -> Result<Self> {
        require(
            !expected.is_empty() && expected.len() <= 16,
            "expected transfer set must contain 1..16 identities",
        )?;
        Ok(Self {
            root: tempfile::Builder::new()
                .prefix("ade-exchange-")
                .tempdir_in(parent)?,
            expected,
            registry: Mutex::new(Registry {
                completed: BTreeMap::new(),
                started: BTreeSet::new(),
            }),
            cancellation,
        })
    }
    /// # Errors
    /// Returns cancellation or incomplete-transfer errors.
    pub fn paths(&self) -> Result<Vec<PathBuf>> {
        self.cancellation.check()?;
        let registry = self
            .registry
            .lock()
            .map_err(|_| Error::Invalid("registry poisoned".into()))?;
        require(
            registry.completed.len() == self.expected.len(),
            "incomplete exchange",
        )?;
        Ok(registry
            .completed
            .values()
            .map(|(path, _)| path.clone())
            .collect())
    }
    /// # Errors
    /// Returns a poisoned transfer-registry error.
    pub fn metrics(&self) -> Result<NetworkMetrics> {
        let mut metrics = NetworkMetrics::default();
        for (_, m) in self
            .registry
            .lock()
            .map_err(|_| Error::Invalid("registry poisoned".into()))?
            .completed
            .values()
        {
            metrics.add(m);
        }
        Ok(metrics)
    }
    fn receive(&self, header: &Header, socket: &mut TcpStream) -> Result<()> {
        header.validate()?;
        self.cancellation.check()?;
        require(
            self.expected.contains(&header.id),
            "unexpected transfer identity",
        )?;
        {
            let mut registry = self.registry.lock().expect("registry lock");
            require(
                registry.started.insert(header.id),
                "duplicate transfer identity",
            )?;
        }
        // A failed identity remains consumed; Phase 3 never retries. NamedTempFile removes partials.
        let mut temporary = tempfile::NamedTempFile::new_in(self.root.path())?;
        let mut metrics = NetworkMetrics::default();
        {
            let mut writer = StreamWriter::try_new(temporary.as_file_mut(), &schema())?;
            loop {
                self.cancellation.check()?;
                let mut frame = protocol::DeadlineStream::new(socket, crate::IO_TIMEOUT);
                let mut length = [0; 4];
                frame.read_exact(&mut length)?;
                let length = u32::from_le_bytes(length) as usize;
                if length == 0 {
                    break;
                }
                require(length <= MAX_FRAME_BYTES, "exchange frame too large")?;
                let mut encoded = vec![0; length];
                frame.read_exact(&mut encoded)?;
                require(
                    encoded.ends_with(&[255, 255, 255, 255, 0, 0, 0, 0]),
                    "frame lacks IPC completion marker",
                )?;
                validate_ipc_envelope(&encoded)?;
                let mut reader = StreamReader::try_new(Cursor::new(encoded), None)?;
                require(
                    reader.schema() == schema(),
                    "exchange stream schema mismatch",
                )?;
                let batch = reader
                    .next()
                    .ok_or_else(|| Error::Invalid("frame lacks record batch".into()))??;
                validate_batch(&batch)?;
                require(
                    reader.next().is_none()
                        && reader.is_finished()
                        && reader.get_ref().position() == length as u64,
                    "frame must contain exactly one complete batch",
                )?;
                writer.write(&batch)?;
                metrics.rows += batch.num_rows() as u64;
                metrics.payload_bytes += length as u64;
                metrics.peak_frame_bytes = metrics.peak_frame_bytes.max(length);
            }
            writer.finish()?;
        }
        self.cancellation.check()?;
        let path = self.root.path().join(format!(
            "{}-{}-{}-{}-{}.arrow",
            header.id.job.0,
            header.id.exchange.0,
            header.id.partition.0,
            header.id.producer.0,
            header.id.attempt.0
        ));
        temporary
            .persist_noclobber(&path)
            .map_err(|e| Error::Io(e.error))?;
        metrics.transfers = 1;
        self.registry
            .lock()
            .expect("registry lock")
            .completed
            .insert(header.id, (path, metrics));
        Ok(())
    }
}
/// Owns the listener and all receiver threads; Drop stops acceptance and joins bounded I/O.
pub struct Receiver {
    pub address: SocketAddr,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}
impl Receiver {
    /// # Errors
    /// Returns listener setup errors.
    pub fn start(
        listener: TcpListener,
        resolve: impl Fn(&Header) -> Result<Arc<Store>> + Send + Sync + 'static,
    ) -> Result<Self> {
        let address = listener.local_addr()?;
        listener.set_nonblocking(true)?;
        let stop = Arc::new(AtomicBool::new(false));
        let stopping = stop.clone();
        let resolve = Arc::new(resolve);
        let handle = thread::spawn(move || {
            let mut handlers: Vec<JoinHandle<()>> = Vec::new();
            while !stopping.load(Ordering::Relaxed) {
                let mut i = 0;
                while i < handlers.len() {
                    if handlers[i].is_finished() {
                        let _ = handlers.swap_remove(i).join();
                    } else {
                        i += 1;
                    }
                }
                if handlers.len() < MAX_CONNECTIONS {
                    match listener.accept() {
                        Ok((mut socket, _)) => {
                            // macOS inherits O_NONBLOCK from the accepting listener.
                            if socket.set_nonblocking(false).is_err() {
                                continue;
                            }
                            let resolve = resolve.clone();
                            handlers.push(thread::spawn(move || {
                                if socket.set_read_timeout(Some(crate::IO_TIMEOUT)).is_err()
                                    || socket.set_write_timeout(Some(crate::IO_TIMEOUT)).is_err()
                                {
                                    return;
                                }
                                let received = (|| {
                                    let header: Header = protocol::read_message(
                                        &mut protocol::DeadlineStream::new(
                                            &mut socket,
                                            crate::IO_TIMEOUT,
                                        ),
                                    )?;
                                    let store = resolve(&header)?;
                                    store.receive(&header, &mut socket)?;
                                    Ok(header.id)
                                })();
                                let ack: std::result::Result<TransferId, String> = received
                                    .map_err(|e: Error| {
                                        eprintln!("ADE exchange receive failed: {e}");
                                        e.to_string()
                                    });
                                let _ = protocol::write_message(
                                    &mut protocol::DeadlineStream::new(
                                        &mut socket,
                                        crate::IO_TIMEOUT,
                                    ),
                                    &ack,
                                );
                            }));
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                        Err(_) => break,
                    }
                }
                thread::sleep(Duration::from_millis(2));
            }
            for handler in handlers {
                let _ = handler.join();
            }
        });
        Ok(Self {
            address,
            stop,
            handle: Some(handle),
        })
    }
}
impl Drop for Receiver {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}
/// Read gathered files only at the harness boundary, after full transfer verification.
/// # Errors
/// Returns incomplete-transfer, file, or IPC decode errors.
pub fn collect(store: &Store) -> Result<Vec<RecordBatch>> {
    let mut output = Vec::new();
    for path in store.paths()? {
        for batch in StreamReader::try_new(File::open(path)?, None)? {
            output.push(batch?);
        }
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{JobId, transfer};
    use arrow::array::{Int64Array, UInt64Array};
    fn batch() -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(UInt64Array::from(vec![0, 1, 2])),
                Arc::new(Int64Array::from(vec![2, 4, 6])),
                Arc::new(UInt64Array::from(vec![1, 1, 1])),
            ],
        )
        .unwrap()
    }
    #[test]
    fn loopback_finalizes_empty_and_rejects_duplicate_and_partial() {
        let parent = tempfile::tempdir().unwrap();
        let ids: Vec<_> = (0..3).map(|p| transfer(JobId(9), 0, 0, p)).collect();
        let store = Arc::new(
            Store::new(
                parent.path(),
                ids.iter().copied().collect(),
                CancellationToken::default(),
            )
            .unwrap(),
        );
        let receiving = store.clone();
        let receiver = Receiver::start(TcpListener::bind("127.0.0.1:0").unwrap(), move |_| {
            Ok(receiving.clone())
        })
        .unwrap();
        let mut sender =
            Sender::new(receiver.address, ids[0], CancellationToken::default()).unwrap();
        sender.batch(&batch()).unwrap();
        sender.batch(&batch()).unwrap();
        let sent = sender.finish().unwrap();
        assert_eq!(sent.rows, 6);
        assert!(sent.payload_bytes > 0);
        Sender::new(receiver.address, ids[1], CancellationToken::default())
            .unwrap()
            .finish()
            .unwrap();
        assert_eq!(store.metrics().unwrap().transfers, 2);
        assert_eq!(store.metrics().unwrap().rows, sent.rows);
        assert_eq!(store.metrics().unwrap().payload_bytes, sent.payload_bytes);
        assert!(
            Sender::new(receiver.address, ids[0], CancellationToken::default())
                .unwrap()
                .finish()
                .is_err()
        );
        // A disconnected frame is neither completed nor retained as a partial file.
        let mut socket = protocol::connect(receiver.address, crate::IO_TIMEOUT).unwrap();
        protocol::write_message(&mut socket, &Header::new(ids[2])).unwrap();
        socket.write_all(&10u32.to_le_bytes()).unwrap();
        socket.write_all(&[1, 2]).unwrap();
        socket.shutdown(std::net::Shutdown::Write).unwrap();
        let ack: std::result::Result<TransferId, String> =
            protocol::read_message(&mut socket).unwrap();
        assert!(ack.is_err());
        assert!(store.paths().is_err());
        assert_eq!(std::fs::read_dir(store.root.path()).unwrap().count(), 2);
        let paths: Vec<_> = store
            .registry
            .lock()
            .unwrap()
            .completed
            .values()
            .map(|(p, _)| p.clone())
            .collect();
        let rows: usize = paths
            .into_iter()
            .flat_map(|p| StreamReader::try_new(File::open(p).unwrap(), None).unwrap())
            .map(|b| b.unwrap().num_rows())
            .sum();
        assert_eq!(rows, 6);
        drop(receiver);
        drop(store);
        assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 0);
    }
    #[test]
    fn rejects_oversized_frame_and_wrong_schema_version() {
        let parent = tempfile::tempdir().unwrap();
        let id = transfer(JobId(1), 0, 0, 0);
        let store = Arc::new(
            Store::new(
                parent.path(),
                BTreeSet::from([id]),
                CancellationToken::default(),
            )
            .unwrap(),
        );
        let receiving = store.clone();
        let receiver = Receiver::start(TcpListener::bind("127.0.0.1:0").unwrap(), move |_| {
            Ok(receiving.clone())
        })
        .unwrap();
        let mut header = Header::new(id);
        header.schema_version = 99;
        let mut socket = protocol::connect(receiver.address, crate::IO_TIMEOUT).unwrap();
        protocol::write_message(&mut socket, &header).unwrap();
        let ack: std::result::Result<TransferId, String> =
            protocol::read_message(&mut socket).unwrap();
        assert!(ack.is_err());
        let mut socket = protocol::connect(receiver.address, crate::IO_TIMEOUT).unwrap();
        protocol::write_message(&mut socket, &Header::new(id)).unwrap();
        socket.write_all(&u32::MAX.to_le_bytes()).unwrap();
        let ack: std::result::Result<TransferId, String> =
            protocol::read_message(&mut socket).unwrap();
        assert!(ack.is_err());
        assert!(store.paths().is_err());
        assert_eq!(store.metrics().unwrap().transfers, 0);
    }
}
