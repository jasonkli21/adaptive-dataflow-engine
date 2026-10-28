//! Bounded segment protocol with mechanism-specific retention and common completion checks.
use crate::{
    Error, ExchangeConfig, ExchangeMode, Result,
    ids::TransferId,
    metrics::NetworkMetrics,
    partition::HASH_VERSION,
    protocol, require,
    retention::{Buffer, ObjectStoreBackend, Objects, RetentionMetrics},
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
/// Distinct primitive payload contracts over the existing bounded wire-v2 frames.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Payload {
    Aggregate,
    LeasedAggregate,
    JoinRows,
    JoinPairs,
}
impl Payload {
    fn version(self) -> u32 {
        match self {
            Self::Aggregate => 1,
            Self::LeasedAggregate => 4,
            Self::JoinRows => 2,
            Self::JoinPairs => 3,
        }
    }
    fn schema(self) -> SchemaRef {
        match self {
            Self::Aggregate | Self::LeasedAggregate => schema(),
            Self::JoinRows => ade_exec::join::input_schema(),
            Self::JoinPairs => ade_exec::join::output_schema(),
        }
    }
}
#[derive(Debug, Clone, Copy)]
pub(crate) struct IngressLimits {
    pub rows: u64,
    pub bytes: u64,
    pub batches: u64,
}
#[derive(Debug, Clone, Copy)]
pub(crate) struct HashOwner {
    pub workers: usize,
    pub destination: usize,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Header {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<String>,
    pub id: TransferId,
    pub hash_version: u32,
    pub schema_version: u32,
    pub wire_version: u32,
}
impl Header {
    /// Construct the current private schema/hash version envelope.
    #[must_use]
    pub fn new(id: TransferId) -> Self {
        Self {
            context: None,
            id,
            hash_version: HASH_VERSION,
            schema_version: 1,
            wire_version: 2,
        }
    }
    fn validate(&self) -> Result<()> {
        require(
            self.hash_version == HASH_VERSION
                && self.wire_version == 2
                && match self.schema_version {
                    1 => self.context.is_none(),
                    2..=4 => self
                        .context
                        .as_ref()
                        .is_some_and(|c| c.len() == 64 && c.bytes().all(|b| b.is_ascii_hexdigit())),
                    _ => false,
                },
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
// streaming decoder allocates metadata/body buffers. Compression is outside this wire contract.
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
fn validate_payload(batch: &RecordBatch, payload: Payload) -> Result<()> {
    require(
        batch.schema() == payload.schema(),
        "exchange schema mismatch",
    )?;
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
    payload: Payload,
    fingerprint: blake3::Hasher,
    staging: Option<crate::materialization::Staging>,
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
        Self::typed(address, id, cancellation, Payload::Aggregate, None)
    }
    pub(crate) fn typed(
        address: SocketAddr,
        id: TransferId,
        cancellation: CancellationToken,
        payload: Payload,
        context: Option<String>,
    ) -> Result<Self> {
        let mut header = Header::new(id);
        header.schema_version = payload.version();
        header.context = context;
        header.validate()?;
        cancellation.check()?;
        let mut socket = protocol::connect(address, crate::IO_TIMEOUT)?;
        protocol::write_message(
            &mut protocol::DeadlineStream::new(&mut socket, crate::IO_TIMEOUT),
            &header,
        )?;
        let mut fingerprint = blake3::Hasher::new();
        fingerprint.update(b"ade-transfer-v1");
        fingerprint.update(&serde_json::to_vec(&id.logical())?);
        if let Some(context) = &header.context {
            fingerprint.update(b"ade-join-context-v1");
            fingerprint.update(context.as_bytes());
        }
        Ok(Self {
            payload,
            fingerprint,
            staging: None,
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
        validate_payload(batch, self.payload)?;
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let mut encoded = BoundedBuffer::new(MAX_FRAME_BYTES);
        {
            let mut writer = StreamWriter::try_new(&mut encoded, &self.payload.schema())?;
            writer.write(batch)?;
            writer.finish()?;
        }
        self.send_encoded(&encoded.0, batch.num_rows() as u64)
    }
    pub(crate) fn stage(mut self, parent: &Path, poison: Arc<AtomicBool>) -> Result<Self> {
        self.staging = Some(crate::materialization::Staging::new(parent, poison)?);
        Ok(self)
    }
    pub(crate) fn encoded(&mut self, bytes: &[u8]) -> Result<()> {
        self.cancellation.check()?;
        require(bytes.len() <= MAX_FRAME_BYTES, "restore frame exceeds cap")?;
        let rows = decode_payload(bytes, self.payload)?.num_rows() as u64;
        require(rows > 0, "empty restored frame")?;
        self.send_encoded(bytes, rows)
    }
    fn send_encoded(&mut self, bytes: &[u8], rows: u64) -> Result<()> {
        if let Some(staging) = &mut self.staging {
            staging.push(bytes, &self.cancellation)?;
        }
        let mut socket = protocol::DeadlineStream::new(&mut self.socket, crate::IO_TIMEOUT);
        let size = u32::try_from(bytes.len())
            .expect("bounded frame")
            .to_le_bytes();
        let sequence = self.metrics.batches.to_le_bytes();
        protocol::write_all_vectored(
            &mut socket,
            &mut [
                std::io::IoSlice::new(&size),
                std::io::IoSlice::new(&sequence),
                std::io::IoSlice::new(bytes),
            ],
        )?;
        self.fingerprint.update(&self.metrics.batches.to_le_bytes());
        self.fingerprint.update(&(bytes.len() as u64).to_le_bytes());
        self.fingerprint.update(bytes);
        self.metrics.batches += 1;
        self.metrics.rows += rows;
        self.metrics.payload_bytes += bytes.len() as u64;
        self.metrics.peak_frame_bytes = self.metrics.peak_frame_bytes.max(bytes.len());
        Ok(())
    }
    /// # Errors
    /// Returns cancellation, transport, rejection, or mismatched completion ACK errors.
    pub fn finish(self) -> Result<NetworkMetrics> {
        self.finish_manifest().map(|(metrics, _)| metrics)
    }
    /// Return terminal evidence only after the destination ACK validates.
    /// # Errors
    /// Returns transport, cancellation or completion errors.
    pub fn finish_manifest(self) -> Result<(NetworkMetrics, crate::lineage::TransferManifest)> {
        self.finish_staged().map(|(m, t, _)| (m, t))
    }
    pub(crate) fn finish_staged(
        mut self,
    ) -> Result<(
        NetworkMetrics,
        crate::lineage::TransferManifest,
        Option<crate::materialization::Staging>,
    )> {
        self.cancellation.check()?;
        let mut socket = protocol::DeadlineStream::new(&mut self.socket, crate::IO_TIMEOUT);
        socket.write_all(&0u32.to_le_bytes())?;
        socket.write_all(&self.metrics.batches.to_le_bytes())?;
        if self.payload != Payload::Aggregate {
            let mut terminal = self.fingerprint.clone();
            terminal.update(b"terminal-v1");
            terminal.update(&self.metrics.batches.to_le_bytes());
            socket.write_all(terminal.finalize().as_bytes())?;
        }
        let ack: std::result::Result<TransferId, String> = protocol::read_message(&mut socket)?;
        let id = ack.map_err(|message| Error::Remote {
            code: "exchange".into(),
            message,
        })?;
        // Identity must match the original request, even on private connections.
        require(id == self.id, "ACK identity mismatch")?;
        self.metrics.transfers = 1;
        self.fingerprint.update(b"terminal-v1");
        self.fingerprint.update(&self.metrics.batches.to_le_bytes());
        let manifest = crate::lineage::TransferManifest {
            logical: self.id.logical(),
            attempt: self.id.attempt,
            segments: self.metrics.batches,
            rows: self.metrics.rows,
            payload_bytes: self.metrics.payload_bytes,
            fingerprint: self.fingerprint.finalize().to_hex().to_string(),
        };
        Ok((self.metrics, manifest, self.staging))
    }
}
#[derive(Default)]
struct AdaptiveState {
    tier: crate::adaptive::AdaptiveTier,
    metrics: crate::adaptive::AdaptiveMetrics,
    produced_bytes: u64,
    produced_segments: u64,
    // At most one suffix boundary per expected transfer, never per segment.
    object_starts: BTreeMap<TransferId, u64>,
}
struct Registry {
    completed: BTreeMap<TransferId, (PathBuf, NetworkMetrics)>,
    started: BTreeSet<TransferId>,
    // One accepted-prefix counter per in-flight transfer, never per segment.
    #[cfg(feature = "research-hooks")]
    observed_batches: BTreeMap<TransferId, u64>,
}
/// One fixed expected transfer set and a unique execution-local root.
pub struct Store {
    payload: Payload,
    context: Option<String>,
    terminals: Mutex<BTreeMap<TransferId, String>>,
    limits: Option<IngressLimits>,
    ingress: Mutex<NetworkMetrics>,
    hash_owner: Option<HashOwner>,
    layout: Mutex<Option<crate::topology::Layout>>,
    root: tempfile::TempDir,
    expected: BTreeSet<TransferId>,
    registry: Mutex<Registry>,
    config: ExchangeConfig,
    buffer: Buffer,
    adaptive: Mutex<AdaptiveState>,
    adaptive_metrics: Mutex<crate::adaptive::AdaptiveMetrics>,
    objects: Option<Objects>,
    poisoned: Arc<AtomicBool>,
    pub cancellation: CancellationToken,
    // Last field: ownership proof outlives every store file/TempDir destructor.
    disk: Option<Arc<crate::disk::Budget>>,
}
impl Store {
    #[allow(
        clippy::too_many_arguments,
        reason = "One typed retention boundary binds codec, limits and ownership together."
    )]
    pub(crate) fn typed(
        parent: &Path,
        expected: BTreeSet<TransferId>,
        token: CancellationToken,
        config: ExchangeConfig,
        payload: Payload,
        context: String,
        limits: IngressLimits,
        hash_owner: Option<HashOwner>,
    ) -> Result<Self> {
        Self::typed_budget(
            parent, expected, token, config, payload, context, limits, hash_owner, None,
        )
    }
    #[allow(
        clippy::too_many_arguments,
        reason = "Typed native retention binds ownership and one optional disk budget."
    )]
    pub(crate) fn typed_budget(
        parent: &Path,
        expected: BTreeSet<TransferId>,
        token: CancellationToken,
        config: ExchangeConfig,
        payload: Payload,
        context: String,
        limits: IngressLimits,
        hash_owner: Option<HashOwner>,
        disk: Option<Arc<crate::disk::Budget>>,
    ) -> Result<Self> {
        require(
            payload != Payload::Aggregate
                && limits.rows > 0
                && limits.bytes > 0
                && limits.batches > 0,
            "typed exchange bounds",
        )?;
        require(
            matches!(
                config.mode,
                ExchangeMode::Spill | ExchangeMode::LocalMaterialized
            ) && config.cost.is_none(),
            "join exchange requires bounded spill/local gather",
        )?;
        if let Some(owner) = hash_owner {
            require(
                (2..=4).contains(&owner.workers) && owner.destination < owner.workers,
                "join hash ownership",
            )?;
        }
        let mut store = Self::configured_budget(parent, expected, token, config, None, disk)?;
        store.payload = payload;
        store.context = Some(context);
        store.limits = Some(limits);
        store.hash_owner = hash_owner;
        Ok(store)
    }

    /// # Errors
    /// Returns invalid expected transfers or I/O errors creating the store/connection.
    pub fn new(
        parent: &Path,
        expected: BTreeSet<TransferId>,
        cancellation: CancellationToken,
    ) -> Result<Self> {
        Self::configured(parent, expected, cancellation, ExchangeConfig::default())
    }
    /// Create a destination session with static or adaptive retention.
    /// # Errors
    /// Returns configuration, backend or temporary storage errors.
    pub fn configured(
        parent: &Path,
        expected: BTreeSet<TransferId>,
        cancellation: CancellationToken,
        config: ExchangeConfig,
    ) -> Result<Self> {
        Self::configured_with_backend(parent, expected, cancellation, config, None)
    }
    /// Create a destination session with an optional worker-installed object backend.
    /// # Errors
    /// Returns configuration, backend or temporary storage errors.
    pub(crate) fn configured_with_backend(
        parent: &Path,
        expected: BTreeSet<TransferId>,
        cancellation: CancellationToken,
        config: ExchangeConfig,
        object_backend: Option<&ObjectStoreBackend>,
    ) -> Result<Self> {
        Self::configured_budget(parent, expected, cancellation, config, object_backend, None)
    }
    pub(crate) fn configured_budget(
        parent: &Path,
        expected: BTreeSet<TransferId>,
        cancellation: CancellationToken,
        config: ExchangeConfig,
        object_backend: Option<&ObjectStoreBackend>,
        disk: Option<Arc<crate::disk::Budget>>,
    ) -> Result<Self> {
        config.validate()?;
        require(
            !expected.is_empty() && expected.len() <= 16,
            "expected transfer set must contain 1..16 identities",
        )?;
        if config.mode == ExchangeMode::Adaptive {
            let first = expected.first().expect("nonempty validated");
            require(
                expected.iter().all(|id| {
                    id.job == first.job
                        && id.exchange == first.exchange
                        && id.partition == first.partition
                }),
                "adaptive session requires one destination partition",
            )?;
        }
        let root = tempfile::Builder::new()
            .prefix("ade-exchange-")
            .tempdir_in(parent)?;
        let poisoned = Arc::new(AtomicBool::new(false));
        let buffer =
            Buffer::new_budget(config.clone(), root.path(), poisoned.clone(), disk.clone())?;
        let objects = if matches!(
            config.mode,
            ExchangeMode::ObjectStoreMaterialized | ExchangeMode::Adaptive
        ) {
            Some(Objects::configured(
                parent,
                config.object_store.clone(),
                object_backend,
                poisoned.clone(),
            )?)
        } else {
            None
        };
        Ok(Self {
            payload: Payload::Aggregate,
            terminals: Mutex::default(),
            disk,
            context: None,
            limits: None,
            ingress: Mutex::default(),
            hash_owner: None,
            layout: Mutex::new(None),
            root,
            config,
            buffer,
            adaptive: Mutex::new(AdaptiveState::default()),
            adaptive_metrics: Mutex::default(),
            objects,
            poisoned,
            expected,
            registry: Mutex::new(Registry {
                completed: BTreeMap::new(),
                started: BTreeSet::new(),
                #[cfg(feature = "research-hooks")]
                observed_batches: BTreeMap::new(),
            }),
            cancellation,
        })
    }
    /// # Errors
    /// Returns cancellation or incomplete-transfer errors.
    pub(crate) fn install_layout(&self, layout: crate::topology::Layout) -> Result<()> {
        layout.validate(layout.workers)?;
        let registry = self.registry.lock().expect("registry lock");
        require(registry.started.is_empty(), "layout after exchange started")?;
        let mut current = self.layout.lock().expect("layout lock");
        require(current.is_none(), "layout already installed")?;
        *current = Some(layout);
        Ok(())
    }
    /// # Errors
    /// Requires completed locally materialized transfers.
    pub fn paths(&self) -> Result<Vec<PathBuf>> {
        self.cancellation.check()?;
        require(
            self.config.mode == ExchangeMode::LocalMaterialized,
            "paths require local materialization; use the common reader",
        )?;
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
    /// Number of fully decoded and retained data batches for a transfer,
    /// including an incomplete transfer without a terminal.
    #[cfg(feature = "research-hooks")]
    pub(crate) fn observed_batches(&self, id: TransferId) -> Result<u64> {
        let registry = self
            .registry
            .lock()
            .map_err(|_| Error::Invalid("registry poisoned".into()))?;
        if let Some((_, metrics)) = registry.completed.get(&id) {
            return Ok(metrics.batches);
        }
        Ok(registry.observed_batches.get(&id).copied().unwrap_or(0))
    }
    /// One common reader for live, spill and finalized materializations.
    #[must_use]
    pub fn reader(self: &Arc<Self>) -> Box<dyn ade_exec::BatchSource> {
        Box::new(Source {
            store: self.clone(),
            files: None,
            ipc: None,
            objects: Vec::new().into_iter(),
            object: None,
            consumed: 0,
            #[cfg(feature = "research-hooks")]
            pressure_applied: false,
        })
    }
    /// # Errors
    /// Returns registry or finalized-file metadata errors.
    pub fn retention_metrics(&self) -> Result<RetentionMetrics> {
        let mut m = self.buffer.metrics();
        if self.config.mode == ExchangeMode::Adaptive {
            m.adaptive = self
                .adaptive_metrics
                .lock()
                .map_err(|_| Error::Invalid("adaptive metrics poisoned".into()))?
                .clone();
        }
        if let Some(objects) = &self.objects {
            let objects = objects.metrics();
            m.object_count = objects.object_count;
            m.object_write_bytes = objects.object_write_bytes;
            m.object_read_bytes = objects.object_read_bytes;
            m.object_store_backend = objects.object_store_backend;
            m.object_store_durability = objects.object_store_durability;
            return Ok(m);
        }
        if self.config.mode == ExchangeMode::LocalMaterialized {
            let r = self
                .registry
                .lock()
                .map_err(|_| Error::Invalid("registry poisoned".into()))?;
            m.local_materialized_files = r.completed.len() as u64;
            for (path, _) in r.completed.values() {
                m.local_materialized_bytes += std::fs::metadata(path)?.len();
            }
        }
        Ok(m)
    }
    /// Require all live retention accounting to drain before a successful task reply.
    pub(crate) fn validate_retention_drained(&self) -> Result<()> {
        let metrics = self.retention_metrics()?;
        require(
            metrics.current_buffer_bytes == 0 && metrics.current_spill_backlog_bytes == 0,
            "successful exchange ended with retained backlog",
        )
    }
    pub(crate) fn terminal(&self, id: TransferId) -> Result<String> {
        self.terminals
            .lock()
            .expect("managed terminals")
            .get(&id)
            .cloned()
            .ok_or_else(|| Error::Invalid("missing managed receive terminal".into()))
    }
    pub(crate) fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Acquire)
    }
    #[cfg(test)]
    pub(crate) fn poison(&self) {
        self.poisoned.store(true, Ordering::Release);
        self.cancellation.cancel();
    }
    fn complete(&self) -> bool {
        self.registry.lock().expect("registry lock").completed.len() == self.expected.len()
    }
    fn adaptive_push(&self, id: TransferId, sequence: u64, encoded: Vec<u8>) -> Result<()> {
        use crate::adaptive::{AdaptiveTier, AdaptiveTransition, decide};
        // One bounded receiver slot per producer. This ingress lock serializes
        // policy/placement, never the queue lock used by the reader or cancellation.
        let mut s = self.adaptive.lock().expect("adaptive ingress");
        self.cancellation.check()?;
        let mut observation = self.buffer.observation(encoded.len());
        observation.produced_bytes = s.produced_bytes;
        observation.produced_segments = s.produced_segments;
        let mut next_tier = s.tier;
        let mut applied_transitions = Vec::new();
        while let Some((new, reason)) = decide(&self.config.adaptive, next_tier, &observation) {
            let transition = AdaptiveTransition {
                policy: "threshold_v1".into(),
                old: next_tier,
                new,
                boundary: s.produced_segments,
                transfer: id,
                sequence,
                reason,
                projected_buffer_pct: u64::try_from(
                    (observation
                        .buffer_bytes
                        .saturating_add(observation.incoming_bytes) as u128)
                        * 100
                        / observation.capacity_bytes.max(1) as u128,
                )
                .unwrap_or(u64::MAX),
                lag_segments: observation
                    .produced_segments
                    .saturating_sub(observation.consumed_segments),
                observation: observation.clone(),
            };
            debug_assert!(s.metrics.transitions.len() < 3);
            applied_transitions.push(transition);
            next_tier = new;
        }
        let tier = next_tier;
        let length = encoded.len() as u64;
        match tier {
            AdaptiveTier::ObjectStore => {
                self.objects.as_ref().expect("adaptive objects").put(
                    id,
                    sequence,
                    encoded,
                    &self.cancellation,
                )?;
                s.object_starts.entry(id).or_insert(sequence);
            }
            _ => self.buffer.push_as(
                encoded,
                &self.cancellation,
                match tier {
                    AdaptiveTier::Direct => ExchangeMode::Direct,
                    AdaptiveTier::Buffered => ExchangeMode::Buffered,
                    _ => ExchangeMode::Spill,
                },
            )?,
        }
        // Publish only after the chosen mechanism accepted the complete segment.
        // Snapshot readers can never mistake a failed placement for a transition.
        s.metrics.transitions.extend(applied_transitions);
        s.tier = tier;
        s.produced_bytes = s.produced_bytes.saturating_add(length);
        s.produced_segments = s.produced_segments.saturating_add(1);
        s.metrics.policy = "threshold_v1".into();
        s.metrics.extents = 1 + s.metrics.transitions.len();
        s.metrics.bytes_by_tier[tier.index()] =
            s.metrics.bytes_by_tier[tier.index()].saturating_add(length);
        s.metrics.segments_by_tier[tier.index()] =
            s.metrics.segments_by_tier[tier.index()].saturating_add(1);
        *self.adaptive_metrics.lock().expect("adaptive metrics") = s.metrics.clone();
        Ok(())
    }
    fn receive(&self, header: &Header, socket: &mut TcpStream) -> Result<()> {
        let result = self.receive_inner(header, socket);
        if result.is_err() {
            self.cancellation.cancel();
        }
        result
    }
    fn validate_ownership(&self, batch: &RecordBatch, header: &Header) -> Result<()> {
        if let Some(layout) = self.layout.lock().expect("layout lock").as_ref() {
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
                .ok_or_else(|| Error::Invalid("routing key type".into()))?;
            require(
                keys.values()
                    .iter()
                    .all(|key| layout.route(*key) as u64 == header.id.partition.0),
                "batch violates committed key ownership",
            )?;
        }
        if let Some(owner) = self.hash_owner {
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
                .ok_or_else(|| Error::Invalid("join routing key type".into()))?;
            require(
                keys.values().iter().all(|key| {
                    crate::partition::destination(*key, owner.workers)
                        .is_ok_and(|p| p == owner.destination)
                }),
                "batch violates join key ownership",
            )?;
        }
        Ok(())
    }
    #[allow(
        clippy::too_many_lines,
        reason = "Validation and atomic terminal publication share one receive ownership scope."
    )]
    fn receive_inner(&self, header: &Header, socket: &mut TcpStream) -> Result<()> {
        header.validate()?;
        require(
            header.schema_version == self.payload.version() && header.context == self.context,
            "exchange schema/strategy context mismatch",
        )?;
        self.cancellation.check()?;
        require(
            self.expected.contains(&header.id),
            "unexpected transfer identity",
        )?;
        require(
            self.registry
                .lock()
                .expect("registry lock")
                .started
                .insert(header.id),
            "duplicate transfer identity",
        )?;
        let mut temporary = if self.config.mode == ExchangeMode::LocalMaterialized {
            Some(tempfile::NamedTempFile::new_in(self.root.path())?)
        } else {
            None
        };
        let mut writer = temporary
            .as_mut()
            .map(|f| {
                StreamWriter::try_new(
                    crate::disk::Writer {
                        inner: f.as_file_mut(),
                        budget: self.disk.clone(),
                    },
                    &self.payload.schema(),
                )
            })
            .transpose()?;
        let mut metrics = NetworkMetrics::default();
        // Aggregate schema1 keeps its accepted receive path; terminal hashing
        // belongs only to the new context-bound join schemas.
        let mut fingerprint = (header.context.is_some() || self.disk.is_some()).then(|| {
            let mut digest = blake3::Hasher::new();
            digest.update(b"ade-transfer-v1");
            digest
                .update(&serde_json::to_vec(&header.id.logical()).expect("primitive transfer ID"));
            if let Some(context) = &header.context {
                digest.update(b"ade-join-context-v1");
                digest.update(context.as_bytes());
            }
            digest
        });
        loop {
            self.cancellation.check()?;
            let mut length = [0; 4];
            if self.payload == Payload::LeasedAggregate {
                // Idle between file morsels is valid. Poll cancellation without
                // losing a partially received length prefix or resetting the job deadline.
                let mut offset = 0;
                while offset < length.len() {
                    self.cancellation.check()?;
                    socket.set_read_timeout(Some(Duration::from_millis(100)))?;
                    match socket.read(&mut length[offset..]) {
                        Ok(0) => {
                            return Err(
                                std::io::Error::from(std::io::ErrorKind::UnexpectedEof).into()
                            );
                        }
                        Ok(n) => offset += n,
                        Err(e)
                            if matches!(
                                e.kind(),
                                std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
                            ) => {}
                        Err(e) => return Err(e.into()),
                    }
                }
            } else {
                protocol::DeadlineStream::new(socket, crate::IO_TIMEOUT).read_exact(&mut length)?;
            }
            let mut frame = protocol::DeadlineStream::new(socket, crate::IO_TIMEOUT);
            let length = u32::from_le_bytes(length) as usize;
            require(length <= MAX_FRAME_BYTES, "exchange frame too large")?;
            let mut sequence = [0; 8];
            frame.read_exact(&mut sequence)?;
            require(
                u64::from_le_bytes(sequence) == metrics.batches,
                "segment sequence/completion count mismatch",
            )?;
            if length == 0 {
                if self.payload != Payload::Aggregate {
                    let fingerprint = fingerprint.as_mut().expect("typed context validated");
                    fingerprint.update(b"terminal-v1");
                    fingerprint.update(&metrics.batches.to_le_bytes());
                    let mut terminal = [0; 32];
                    frame.read_exact(&mut terminal)?;
                    require(
                        terminal == *fingerprint.finalize().as_bytes(),
                        "join terminal fingerprint mismatch",
                    )?;
                }
                break;
            }
            let mut encoded = vec![0; length];
            frame.read_exact(&mut encoded)?;
            if let Some(fingerprint) = &mut fingerprint {
                fingerprint.update(&metrics.batches.to_le_bytes());
                fingerprint.update(&(length as u64).to_le_bytes());
                fingerprint.update(&encoded);
            }
            let batch = decode_payload(&encoded, self.payload)?;
            require(batch.num_rows() > 0, "empty data segment")?;
            self.validate_ownership(&batch, header)?;
            let rows = batch.num_rows() as u64;
            if let Some(limits) = self.limits {
                let mut ingress = self.ingress.lock().expect("ingress budget");
                let next_rows = ingress
                    .rows
                    .checked_add(rows)
                    .ok_or_else(|| Error::Invalid("join ingress rows overflow".into()))?;
                let next_bytes = ingress
                    .payload_bytes
                    .checked_add(length as u64)
                    .ok_or_else(|| Error::Invalid("join ingress bytes overflow".into()))?;
                let next_batches = ingress
                    .batches
                    .checked_add(1)
                    .ok_or_else(|| Error::Invalid("join ingress frames overflow".into()))?;
                require(
                    next_rows <= limits.rows
                        && next_bytes <= limits.bytes
                        && next_batches <= limits.batches,
                    "join ingress bound exceeded",
                )?;
                ingress.rows = next_rows;
                ingress.payload_bytes = next_bytes;
                ingress.batches = next_batches;
            }

            if let Some(writer) = &mut writer {
                writer.write(&batch)?;
            } else if self.config.mode == ExchangeMode::Adaptive {
                drop(batch);
                self.adaptive_push(header.id, metrics.batches, encoded)?;
            } else if let Some(objects) = &self.objects {
                objects.put(header.id, metrics.batches, encoded, &self.cancellation)?;
            } else {
                drop(batch);
                self.buffer.push(encoded, &self.cancellation)?;
            }
            metrics.batches += 1;
            metrics.rows += rows;
            metrics.payload_bytes += length as u64;
            metrics.peak_frame_bytes = metrics.peak_frame_bytes.max(length);
            #[cfg(feature = "research-hooks")]
            self.registry
                .lock()
                .expect("registry lock")
                .observed_batches
                .insert(header.id, metrics.batches);
        }
        if self.disk.is_some() {
            let digest = fingerprint.as_mut().expect("managed receive digest");
            if self.payload == Payload::Aggregate {
                digest.update(b"terminal-v1");
                digest.update(&metrics.batches.to_le_bytes());
            }
            self.terminals
                .lock()
                .expect("managed terminals")
                .insert(header.id, digest.finalize().to_hex().to_string());
        }
        if let Some(writer) = &mut writer {
            writer.finish()?;
        }
        drop(writer);
        self.cancellation.check()?;
        let path = self.root.path().join(format!(
            "{}-{}-{}-{}-{}.arrow",
            header.id.job.0,
            header.id.exchange.0,
            header.id.partition.0,
            header.id.producer.0,
            header.id.attempt.0
        ));
        if let Some(temporary) = temporary {
            temporary
                .persist_noclobber(&path)
                .map_err(|e| Error::Io(e.error))?;
        }
        metrics.transfers = 1;
        let mut registry = self.registry.lock().expect("registry lock");
        #[cfg(feature = "research-hooks")]
        registry.observed_batches.remove(&header.id);
        registry.completed.insert(header.id, (path, metrics));
        Ok(())
    }
}
pub(crate) fn decode(encoded: &[u8]) -> Result<RecordBatch> {
    decode_payload(encoded, Payload::Aggregate)
}
fn decode_payload(encoded: &[u8], payload: Payload) -> Result<RecordBatch> {
    validate_ipc_envelope(encoded)?;
    let mut reader = StreamReader::try_new(Cursor::new(encoded), None)?;
    require(
        reader.schema() == payload.schema(),
        "exchange stream schema mismatch",
    )?;
    let batch = reader
        .next()
        .ok_or_else(|| Error::Invalid("frame lacks record batch".into()))??;
    validate_payload(&batch, payload)?;
    require(
        reader.next().is_none()
            && reader.is_finished()
            && reader.get_ref().position() == encoded.len() as u64,
        "frame must contain exactly one complete batch",
    )?;
    Ok(batch)
}
struct Source {
    store: Arc<Store>,
    files: Option<std::vec::IntoIter<PathBuf>>,
    ipc: Option<StreamReader<File>>,
    objects: std::vec::IntoIter<(TransferId, u64, u64)>,
    object: Option<(TransferId, u64, u64)>,
    consumed: u64,
    #[cfg(feature = "research-hooks")]
    pressure_applied: bool,
}
impl std::fmt::Debug for Source {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ExchangeSource")
    }
}
impl ade_exec::BatchSource for Source {
    fn schema(&self) -> SchemaRef {
        self.store.payload.schema()
    }
    fn next_batch(&mut self, token: &CancellationToken) -> ade_exec::Result<Option<RecordBatch>> {
        #[cfg(feature = "research-hooks")]
        {
            let pressure = &self.store.config.pressure;
            if !self.pressure_applied
                && self.consumed >= pressure.after_segments
                && self
                    .store
                    .expected
                    .iter()
                    .next()
                    .is_some_and(|id| Some(id.partition.0) == pressure.target_partition)
            {
                self.pressure_applied = true;
                let deadline = std::time::Instant::now() + Duration::from_millis(pressure.delay_ms);
                while std::time::Instant::now() < deadline {
                    token.check()?;
                    self.store
                        .cancellation
                        .check()
                        .map_err(|e| ade_exec::Error::Invalid(e.to_string()))?;
                    thread::sleep(
                        Duration::from_millis(2)
                            .min(deadline.saturating_duration_since(std::time::Instant::now())),
                    );
                }
            }
        }
        let batch = self.next(token);
        if matches!(&batch, Ok(Some(_))) {
            self.consumed = self.consumed.saturating_add(1);
        }
        batch.map_err(|e| ade_exec::Error::Invalid(format!("exchange source: {e}")))
    }
}
impl Source {
    fn next(&mut self, token: &CancellationToken) -> Result<Option<RecordBatch>> {
        loop {
            token.check()?;
            self.store.cancellation.check()?;
            if self.store.config.mode.live() || self.store.config.mode == ExchangeMode::Adaptive {
                if let Some(encoded) = self.store.buffer.take(&self.store.cancellation)? {
                    return Ok(Some(decode_payload(&encoded, self.store.payload)?));
                }
                // Recheck after completion: the last segment may have arrived between checks.
                if self.store.complete() {
                    if self.store.config.mode == ExchangeMode::Adaptive {
                        if let Some(encoded) = self.store.buffer.take(&self.store.cancellation)? {
                            return Ok(Some(decode_payload(&encoded, self.store.payload)?));
                        }
                        break;
                    }
                    return self
                        .store
                        .buffer
                        .take(&self.store.cancellation)?
                        .map(|e| decode_payload(&e, self.store.payload))
                        .transpose();
                }
            } else if self.store.complete() {
                break;
            }
            let start = std::time::Instant::now();
            thread::sleep(Duration::from_millis(2));
            self.store.buffer.waited(start);
        }
        if self.files.is_none() {
            self.files = Some(if self.store.objects.is_some() {
                Vec::new().into_iter()
            } else {
                self.store.paths()?.into_iter()
            });
            self.objects = self
                .store
                .registry
                .lock()
                .expect("registry lock")
                .completed
                .iter()
                .filter_map(|(id, (_, m))| {
                    if self.store.config.mode == ExchangeMode::Adaptive {
                        self.store
                            .adaptive
                            .lock()
                            .expect("adaptive state")
                            .object_starts
                            .get(id)
                            .map(|first| (*id, *first, m.batches))
                    } else {
                        Some((*id, 0, m.batches))
                    }
                })
                .collect::<Vec<_>>()
                .into_iter();
        }
        if let Some(objects) = &self.store.objects {
            loop {
                if let Some((id, sequence, count)) = &mut self.object
                    && *sequence < *count
                {
                    let bytes = objects.get(*id, *sequence, &self.store.cancellation)?;
                    *sequence += 1;
                    return Ok(Some(decode_payload(&bytes, self.store.payload)?));
                }
                let Some((id, first, count)) = self.objects.next() else {
                    return Ok(None);
                };
                self.object = Some((id, first, count));
            }
        }
        loop {
            if let Some(reader) = &mut self.ipc
                && let Some(batch) = reader.next()
            {
                let batch = batch?;
                validate_payload(&batch, self.store.payload)?;
                return Ok(Some(batch));
            }
            let Some(path) = self.files.as_mut().expect("initialized").next() else {
                return Ok(None);
            };
            self.ipc = Some(StreamReader::try_new(File::open(path)?, None)?);
        }
    }
}
type ActiveReceivers = Arc<Mutex<std::collections::HashMap<thread::ThreadId, CancellationToken>>>;
struct ActiveGuard(ActiveReceivers, thread::ThreadId);
impl Drop for ActiveGuard {
    fn drop(&mut self) {
        self.0.lock().expect("active receivers").remove(&self.1);
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
        listener.set_nonblocking(false)?;
        let stop = Arc::new(AtomicBool::new(false));
        let stopping = stop.clone();
        let resolve = Arc::new(resolve);
        let active: ActiveReceivers = Arc::default();
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
                            if stopping.load(Ordering::Acquire) {
                                break;
                            }
                            // macOS inherits O_NONBLOCK from the accepting listener.
                            if socket.set_nonblocking(false).is_err() {
                                continue;
                            }
                            let resolve = resolve.clone();
                            let active = active.clone();
                            let stopping = stopping.clone();
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
                                    let id = thread::current().id();
                                    active
                                        .lock()
                                        .map_err(|_| {
                                            Error::Invalid(
                                                "active receiver registry poisoned".into(),
                                            )
                                        })?
                                        .insert(id, store.cancellation.clone());
                                    let _guard = ActiveGuard(active.clone(), id);
                                    if stopping.load(Ordering::Relaxed) {
                                        store.cancellation.cancel();
                                    }
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
                } else {
                    thread::sleep(Duration::from_millis(2));
                }
            }
            for token in active
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .values()
            {
                token.cancel();
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
        self.stop.store(true, Ordering::Release);
        // Shutdown must wake an idle accept, without creating a new protocol reader.
        protocol::wake_listener(self.address);
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
    visit_batches(store, |batch| {
        output.push(batch);
        Ok(())
    })?;
    Ok(output)
}

pub(crate) fn visit_batches(
    store: &Store,
    mut visit: impl FnMut(RecordBatch) -> Result<()>,
) -> Result<()> {
    for path in store.paths()? {
        for batch in StreamReader::try_new(File::open(path)?, None)? {
            visit(batch?)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn blocking_accept_shutdown_wakes_idle_and_all_busy_slots() {
        let parent = tempfile::tempdir().unwrap();
        let token = CancellationToken::default();
        let store = Arc::new(
            Store::configured(
                parent.path(),
                (0..4)
                    .map(|p| crate::ids::transfer(crate::ids::JobId(1), 0, 0, p))
                    .collect(),
                token.clone(),
                ExchangeConfig {
                    mode: ExchangeMode::Direct,
                    ..Default::default()
                },
            )
            .unwrap(),
        );
        for busy in [false, true] {
            let receiving = store.clone();
            let receiver = Receiver::start(
                TcpListener::bind(if busy { "127.0.0.1:0" } else { "0.0.0.0:0" }).unwrap(),
                move |_| Ok(receiving.clone()),
            )
            .unwrap();
            let mut producers = Vec::new();
            if busy {
                for p in 0..4 {
                    let address = receiver.address;
                    producers.push(thread::spawn(move || {
                        let mut sender = Sender::new(
                            address,
                            crate::ids::transfer(crate::ids::JobId(1), 0, 0, p),
                            CancellationToken::default(),
                        )
                        .unwrap();
                        sender
                            .batch(
                                &RecordBatch::try_new(
                                    schema(),
                                    vec![
                                        Arc::new(arrow::array::UInt64Array::from(vec![p])),
                                        Arc::new(arrow::array::Int64Array::from(vec![1])),
                                        Arc::new(arrow::array::UInt64Array::from(vec![1])),
                                    ],
                                )
                                .unwrap(),
                            )
                            .unwrap();
                        sender.finish()
                    }));
                }
                let deadline = std::time::Instant::now() + crate::IO_TIMEOUT;
                while store.registry.lock().unwrap().started.len() < MAX_CONNECTIONS {
                    assert!(std::time::Instant::now() < deadline);
                    thread::sleep(Duration::from_millis(1));
                }
            }
            let start = std::time::Instant::now();
            drop(receiver);
            assert!(start.elapsed() < crate::IO_TIMEOUT);
            for producer in producers {
                assert!(producer.join().unwrap().is_err());
            }
        }
        assert!(token.check().is_err());
    }
    use crate::ids::{JobId, transfer};
    use arrow::array::{Int64Array, UInt64Array};
    use std::sync::mpsc;

    struct StalledSpillLog {
        started: mpsc::Sender<()>,
        release: Mutex<mpsc::Receiver<()>>,
    }
    impl crate::retention::SpillLog for StalledSpillLog {
        fn write_frame(&mut self, _bytes: &[u8]) -> std::io::Result<()> {
            let _ = self.started.send(());
            self.release
                .lock()
                .expect("spill release lock")
                .recv()
                .expect("spill release signal");
            Ok(())
        }
        fn read_frame(&mut self) -> std::io::Result<Option<Vec<u8>>> {
            Ok(None)
        }
    }
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
    fn duplicate_completed_transfer_fails_the_session() {
        let parent = tempfile::tempdir().unwrap();
        let id = transfer(JobId(1), 0, 0, 0);
        let store =
            Arc::new(Store::new(parent.path(), [id].into(), CancellationToken::default()).unwrap());
        let receiving = store.clone();
        let receiver = Receiver::start(TcpListener::bind("127.0.0.1:0").unwrap(), move |_| {
            Ok(receiving.clone())
        })
        .unwrap();
        Sender::new(receiver.address, id, CancellationToken::default())
            .unwrap()
            .finish()
            .unwrap();
        // Send only the duplicate header: unread trailing TCP data can cause an
        // OS reset to supersede a rejection ACK, which is also a valid failure.
        let mut socket = protocol::connect(receiver.address, crate::IO_TIMEOUT).unwrap();
        protocol::write_message(&mut socket, &Header::new(id)).unwrap();
        let ack: std::result::Result<TransferId, String> =
            protocol::read_message(&mut socket).unwrap();
        assert!(ack.unwrap_err().contains("duplicate transfer identity"));
        assert!(store.cancellation.check().is_err());
        assert!(store.paths().is_err());
    }
    #[test]
    fn receiver_drop_unblocks_a_direct_handoff() {
        let parent = tempfile::tempdir().unwrap();
        let id = transfer(JobId(1), 0, 0, 0);
        let store = Arc::new(
            Store::configured(
                parent.path(),
                [id].into(),
                CancellationToken::default(),
                ExchangeConfig {
                    mode: ExchangeMode::Direct,
                    ..Default::default()
                },
            )
            .unwrap(),
        );
        let receiving = store.clone();
        let receiver = Receiver::start(TcpListener::bind("127.0.0.1:0").unwrap(), move |_| {
            Ok(receiving.clone())
        })
        .unwrap();
        let mut sender = Sender::new(receiver.address, id, CancellationToken::default()).unwrap();
        sender.batch(&batch()).unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while store.registry.lock().unwrap().started.is_empty() {
            assert!(std::time::Instant::now() < deadline);
            thread::sleep(Duration::from_millis(1));
        }
        drop(receiver);
        assert!(store.cancellation.check().is_err());
        assert!(sender.finish().is_err());
    }
    #[test]
    fn retention_snapshot_reports_live_buffer_and_spill_backlog_then_drains() {
        let parent = tempfile::tempdir().unwrap();
        let id = transfer(JobId(3), 0, 0, 0);
        let store = Store::configured(
            parent.path(),
            [id].into(),
            CancellationToken::default(),
            ExchangeConfig {
                mode: ExchangeMode::Spill,
                buffer_bytes: 4,
                ..Default::default()
            },
        )
        .unwrap();
        store
            .buffer
            .push(vec![1, 2, 3], &store.cancellation)
            .unwrap();
        let live = store.retention_metrics().unwrap();
        assert_eq!(live.current_buffer_bytes, 3);
        assert_eq!(live.current_spill_backlog_bytes, 0);
        store
            .buffer
            .push(vec![4, 5, 6, 7, 8], &store.cancellation)
            .unwrap();
        let live = store.retention_metrics().unwrap();
        assert_eq!(live.current_buffer_bytes, 3);
        assert_eq!(live.current_spill_backlog_bytes, 5);
        assert_eq!(
            store.buffer.take(&store.cancellation).unwrap().unwrap(),
            vec![1, 2, 3]
        );
        assert_eq!(
            store.buffer.take(&store.cancellation).unwrap().unwrap(),
            vec![4, 5, 6, 7, 8]
        );
        store.validate_retention_drained().unwrap();
    }
    #[test]
    fn receiver_shutdown_returns_while_spill_io_is_stalled() {
        for mode in [ExchangeMode::Spill, ExchangeMode::Adaptive] {
            let parent = tempfile::tempdir().unwrap();
            let id = transfer(JobId(4), 0, 0, 0);
            let mut store = Store::configured(
                parent.path(),
                [id].into(),
                CancellationToken::default(),
                ExchangeConfig {
                    mode,
                    buffer_bytes: 1,
                    ..Default::default()
                },
            )
            .unwrap();
            let (started, started_rx) = mpsc::channel();
            let (release_tx, release_rx) = mpsc::channel();
            store.buffer.replace_spill_log_for_test(StalledSpillLog {
                started,
                release: Mutex::new(release_rx),
            });
            let store = Arc::new(store);
            let receiving = store.clone();
            let receiver = Receiver::start(TcpListener::bind("127.0.0.1:0").unwrap(), move |_| {
                Ok(receiving.clone())
            })
            .unwrap();
            let address = receiver.address;
            let producer = thread::spawn(move || {
                let mut sender = Sender::new(address, id, CancellationToken::default()).unwrap();
                sender
                    .batch(&batch())
                    .and_then(|()| sender.finish().map(|_| ()))
            });
            started_rx.recv_timeout(Duration::from_secs(1)).unwrap();

            let (stopped, stopped_rx) = mpsc::channel();
            thread::spawn(move || {
                drop(receiver);
                let _ = stopped.send(());
            });
            let shutdown = stopped_rx.recv_timeout(Duration::from_secs(1));
            // Release the isolated helper even if the regression returns, so the test
            // process can clean up its detached test thread.
            let _ = release_tx.send(());
            assert!(shutdown.is_ok(), "receiver shutdown waited on spill I/O");
            assert!(store.cancellation.check().is_err());
            assert!(store.is_poisoned());
            assert!(producer.join().unwrap().is_err());
        }
    }
    #[test]
    fn mechanism_roundtrips_include_empty_transfer_and_metrics() {
        for mode in [
            ExchangeMode::Direct,
            ExchangeMode::Buffered,
            ExchangeMode::Spill,
            ExchangeMode::LocalMaterialized,
            ExchangeMode::ObjectStoreMaterialized,
            ExchangeMode::Adaptive,
        ] {
            let parent = tempfile::tempdir().unwrap();
            let ids = [transfer(JobId(1), 0, 0, 0), transfer(JobId(1), 0, 0, 1)];
            let store = Arc::new(
                Store::configured(
                    parent.path(),
                    ids.into(),
                    CancellationToken::default(),
                    ExchangeConfig {
                        mode,
                        buffer_bytes: if mode == ExchangeMode::Spill {
                            1
                        } else {
                            MAX_FRAME_BYTES
                        },
                        ..Default::default()
                    },
                )
                .unwrap(),
            );
            let receiving = store.clone();
            let receiver = Receiver::start(TcpListener::bind("127.0.0.1:0").unwrap(), move |_| {
                Ok(receiving.clone())
            })
            .unwrap();
            let address = receiver.address;
            let producer = thread::spawn(move || {
                let mut sender =
                    Sender::new(address, ids[0], CancellationToken::default()).unwrap();
                for _ in 0..5 {
                    sender.batch(&batch()).unwrap();
                }
                sender.finish().unwrap();
                Sender::new(address, ids[1], CancellationToken::default())
                    .unwrap()
                    .finish()
                    .unwrap();
            });
            let mut reader = store.reader();
            let mut rows = 0;
            while let Some(batch) = reader.next_batch(&store.cancellation).unwrap() {
                rows += batch.num_rows();
            }
            producer.join().unwrap();
            assert_eq!(rows, 15);
            assert_eq!(store.metrics().unwrap().batches, 5);
            assert_eq!(store.metrics().unwrap().transfers, 2);
            let m = store.retention_metrics().unwrap();
            match mode {
                ExchangeMode::Direct | ExchangeMode::Buffered => {
                    assert!(m.peak_buffer_bytes > 0);
                    assert_eq!(
                        m.spill_write_bytes + m.local_materialized_bytes + m.object_write_bytes,
                        0
                    );
                }
                ExchangeMode::Spill => {
                    assert_eq!(m.spill_write_bytes, m.spill_read_bytes);
                    assert!(m.spill_read_bytes > 0);
                    assert!(m.peak_buffer_bytes <= 1);
                }
                ExchangeMode::LocalMaterialized => {
                    assert_eq!(m.local_materialized_files, 2);
                    assert!(m.local_materialized_bytes > 0);
                }
                ExchangeMode::Adaptive => {
                    assert_eq!(m.adaptive.segments_by_tier.iter().sum::<u64>(), 5);
                    assert!(m.adaptive.transitions.len() <= 3);
                }
                ExchangeMode::ObjectStoreMaterialized => {
                    assert_eq!(m.object_count, 5);
                    assert_eq!(m.object_write_bytes, m.object_read_bytes);
                    assert!(m.object_read_bytes > 0);
                }
            }
            drop(reader);
            drop(receiver);
            drop(store);
            assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 0);
        }
    }
    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "One ordered scenario proves conservation at all three transitions."
    )]
    fn adaptive_all_boundaries_exact_readback_with_bounded_suffixes() {
        use crate::adaptive::AdaptiveTier;
        let parent = tempfile::tempdir().unwrap();
        let ids = [transfer(JobId(91), 0, 0, 0), transfer(JobId(91), 0, 0, 1)];
        let encode = |key| {
            let b = RecordBatch::try_new(
                schema(),
                vec![
                    Arc::new(UInt64Array::from(vec![key])),
                    Arc::new(Int64Array::from(vec![17])),
                    Arc::new(UInt64Array::from(vec![1])),
                ],
            )
            .unwrap();
            let mut bytes = BoundedBuffer::new(MAX_FRAME_BYTES);
            let mut writer = StreamWriter::try_new(&mut bytes, &schema()).unwrap();
            writer.write(&b).unwrap();
            writer.finish().unwrap();
            drop(writer);
            bytes.0
        };
        let size = encode(0).len();
        let store = Arc::new(
            Store::configured(
                parent.path(),
                ids.into(),
                CancellationToken::default(),
                ExchangeConfig {
                    mode: ExchangeMode::Adaptive,
                    buffer_bytes: size * 4,
                    adaptive: crate::adaptive::AdaptiveConfig {
                        buffer_high_pct: 80,
                        remote_after_bytes: 1,
                    },
                    ..Default::default()
                },
            )
            .unwrap(),
        );
        // Advertise readiness, then prove the first segment uses real rendezvous.
        assert!(store.buffer.take(&store.cancellation).unwrap().is_none());
        let writing = store.clone();
        let first = encode(0);
        let producer = thread::spawn(move || writing.adaptive_push(ids[0], 0, first).unwrap());
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        loop {
            if let Some(bytes) = store.buffer.take(&store.cancellation).unwrap() {
                assert_eq!(bytes, encode(0));
                break;
            }
            assert!(std::time::Instant::now() < deadline);
            thread::yield_now();
        }
        producer.join().unwrap();
        for sequence in 1..7 {
            store
                .adaptive_push(ids[0], sequence, encode(sequence))
                .unwrap();
        }
        store.adaptive_push(ids[1], 0, encode(7)).unwrap();
        // Terminal validation is covered at the protocol boundary by the matrix.
        for (id, count) in [(ids[0], 7), (ids[1], 1)] {
            store.registry.lock().unwrap().completed.insert(
                id,
                (
                    PathBuf::new(),
                    NetworkMetrics {
                        batches: count,
                        transfers: 1,
                        ..Default::default()
                    },
                ),
            );
        }
        let mut reader = store.reader();
        for key in 1..8 {
            let b = reader.next_batch(&store.cancellation).unwrap().unwrap();
            assert_eq!(
                b.column(0)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .value(0),
                key
            );
        }
        assert!(reader.next_batch(&store.cancellation).unwrap().is_none());
        let m = store.retention_metrics().unwrap();
        assert_eq!(m.adaptive.segments_by_tier, [1, 3, 1, 3]);
        assert_eq!(
            m.adaptive
                .transitions
                .iter()
                .map(|t| (t.boundary, t.new))
                .collect::<Vec<_>>(),
            vec![
                (1, AdaptiveTier::Buffered),
                (4, AdaptiveTier::Spill),
                (5, AdaptiveTier::ObjectStore)
            ]
        );
        assert_eq!(store.adaptive.lock().unwrap().object_starts.len(), 2);
        assert_eq!(m.spill_write_bytes, m.spill_read_bytes);
        assert_eq!(m.object_write_bytes, m.object_read_bytes);
        store.validate_retention_drained().unwrap();
        drop(reader);
        drop(store);
        assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 0);
    }
    #[test]
    fn adaptive_direct_wait_cancels_and_live_metrics_do_not_block() {
        let parent = tempfile::tempdir().unwrap();
        let id = transfer(JobId(92), 0, 0, 0);
        let store = Arc::new(
            Store::configured(
                parent.path(),
                [id].into(),
                CancellationToken::default(),
                ExchangeConfig {
                    mode: ExchangeMode::Adaptive,
                    ..Default::default()
                },
            )
            .unwrap(),
        );
        assert!(store.buffer.take(&store.cancellation).unwrap().is_none());
        let writing = store.clone();
        let producer = thread::spawn(move || writing.adaptive_push(id, 0, vec![1; 8]));
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while store.retention_metrics().unwrap().current_buffer_bytes == 0 {
            assert!(std::time::Instant::now() < deadline);
            thread::yield_now();
        }
        assert!(!producer.is_finished());
        store.cancellation.cancel();
        assert!(producer.join().unwrap().is_err());
    }
    #[test]
    fn sequence_and_completion_count_rejections_fail_the_session() {
        for sequence in [1, 99] {
            let parent = tempfile::tempdir().unwrap();
            let id = transfer(JobId(1), 0, 0, 0);
            let store = Arc::new(
                Store::new(parent.path(), [id].into(), CancellationToken::default()).unwrap(),
            );
            let receiving = store.clone();
            let receiver = Receiver::start(TcpListener::bind("127.0.0.1:0").unwrap(), move |_| {
                Ok(receiving.clone())
            })
            .unwrap();
            let mut socket = protocol::connect(receiver.address, crate::IO_TIMEOUT).unwrap();
            protocol::write_message(&mut socket, &Header::new(id)).unwrap();
            socket.write_all(&0u32.to_le_bytes()).unwrap();
            socket.write_all(&u64::to_le_bytes(sequence)).unwrap();
            let ack: std::result::Result<TransferId, String> =
                protocol::read_message(&mut socket).unwrap();
            assert!(ack.is_err());
            assert!(store.paths().is_err());
            assert_eq!(store.metrics().unwrap().transfers, 0);
        }
    }
    #[test]
    fn rejects_duplicate_gapped_segments_and_incorrect_final_counts() {
        for mode in [ExchangeMode::LocalMaterialized, ExchangeMode::Adaptive] {
            for (second, count) in [(0, 2), (2, 2), (1, 1)] {
                let parent = tempfile::tempdir().unwrap();
                let id = transfer(JobId(1), 0, 0, 0);
                let store = Arc::new(
                    Store::configured(
                        parent.path(),
                        [id].into(),
                        CancellationToken::default(),
                        ExchangeConfig {
                            mode,
                            ..Default::default()
                        },
                    )
                    .unwrap(),
                );
                let receiving = store.clone();
                let receiver =
                    Receiver::start(TcpListener::bind("127.0.0.1:0").unwrap(), move |_| {
                        Ok(receiving.clone())
                    })
                    .unwrap();
                let mut encoded = BoundedBuffer::new(MAX_FRAME_BYTES);
                let mut writer = StreamWriter::try_new(&mut encoded, &schema()).unwrap();
                writer.write(&batch()).unwrap();
                writer.finish().unwrap();
                drop(writer);
                let mut bytes = Vec::new();
                protocol::write_message(&mut bytes, &Header::new(id)).unwrap();
                for sequence in [0, second] {
                    bytes.extend_from_slice(&u32::try_from(encoded.0.len()).unwrap().to_le_bytes());
                    bytes.extend_from_slice(&u64::to_le_bytes(sequence));
                    bytes.extend_from_slice(&encoded.0);
                }
                bytes.extend_from_slice(&0u32.to_le_bytes());
                bytes.extend_from_slice(&u64::to_le_bytes(count));
                let mut socket = protocol::connect(receiver.address, crate::IO_TIMEOUT).unwrap();
                socket.write_all(&bytes).unwrap();
                let ack: Result<std::result::Result<TransferId, String>> =
                    protocol::read_message(&mut socket);
                assert!(!matches!(ack, Ok(Ok(_))));
                assert!(store.paths().is_err());
            }
        }
    }
    #[test]
    fn waiting_reader_cancels_and_incomplete_objects_are_not_visible() {
        for mode in [
            ExchangeMode::Direct,
            ExchangeMode::Buffered,
            ExchangeMode::Spill,
            ExchangeMode::LocalMaterialized,
            ExchangeMode::ObjectStoreMaterialized,
            ExchangeMode::Adaptive,
        ] {
            let parent = tempfile::tempdir().unwrap();
            let id = transfer(JobId(1), 0, 0, 0);
            let store = Arc::new(
                Store::configured(
                    parent.path(),
                    [id].into(),
                    CancellationToken::default(),
                    ExchangeConfig {
                        mode,
                        ..Default::default()
                    },
                )
                .unwrap(),
            );
            if let Some(objects) = &store.objects {
                let mut bytes = BoundedBuffer::new(MAX_FRAME_BYTES);
                let mut writer = StreamWriter::try_new(&mut bytes, &schema()).unwrap();
                writer.write(&batch()).unwrap();
                writer.finish().unwrap();
                drop(writer);
                objects.put(id, 0, bytes.0, &store.cancellation).unwrap();
            }
            let reading = store.clone();
            let reader = thread::spawn(move || reading.reader().next_batch(&reading.cancellation));
            thread::sleep(Duration::from_millis(20));
            assert!(!reader.is_finished());
            store.cancellation.cancel();
            assert!(reader.join().unwrap().is_err());
        }
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
        assert_eq!(sent.batches, 2);
        assert!(sent.payload_bytes > 0);
        let empty = Sender::new(receiver.address, ids[1], CancellationToken::default())
            .unwrap()
            .finish()
            .unwrap();
        assert_eq!(empty.transfers, 1);
        assert_eq!(empty.batches, 0);
        assert_eq!(store.metrics().unwrap().transfers, 2);
        assert_eq!(store.metrics().unwrap().rows, sent.rows);
        assert_eq!(store.metrics().unwrap().batches, sent.batches);
        assert_eq!(store.metrics().unwrap().payload_bytes, sent.payload_bytes);
        // A disconnected frame is neither completed nor retained as a partial file.
        let mut socket = protocol::connect(receiver.address, crate::IO_TIMEOUT).unwrap();
        protocol::write_message(&mut socket, &Header::new(ids[2])).unwrap();
        socket.write_all(&10u32.to_le_bytes()).unwrap();
        socket.write_all(&[1, 2]).unwrap();
        socket.shutdown(std::net::Shutdown::Write).unwrap();
        let ack: std::result::Result<TransferId, String> =
            protocol::read_message(&mut socket).unwrap();
        assert!(ack.is_err());
        assert!(
            Sender::new(receiver.address, ids[0], CancellationToken::default())
                .unwrap()
                .finish()
                .is_err()
        );
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
        for wrong_version in [true, false] {
            let parent = tempfile::tempdir().unwrap();
            let id = transfer(JobId(1), 0, 0, 0);
            let store = Arc::new(
                Store::new(parent.path(), [id].into(), CancellationToken::default()).unwrap(),
            );
            let receiving = store.clone();
            let receiver = Receiver::start(TcpListener::bind("127.0.0.1:0").unwrap(), move |_| {
                Ok(receiving.clone())
            })
            .unwrap();
            let mut header = Header::new(id);
            if wrong_version {
                header.schema_version = 99;
            }
            let mut bytes = Vec::new();
            protocol::write_message(&mut bytes, &header).unwrap();
            if !wrong_version {
                bytes.extend_from_slice(&u32::MAX.to_le_bytes());
            }
            let mut socket = protocol::connect(receiver.address, crate::IO_TIMEOUT).unwrap();
            socket.write_all(&bytes).unwrap();
            let ack: Result<std::result::Result<TransferId, String>> =
                protocol::read_message(&mut socket);
            assert!(!matches!(ack, Ok(Ok(_))));
            assert!(store.paths().is_err());
            assert_eq!(store.metrics().unwrap().transfers, 0);
        }
    }
    #[test]
    fn terminal_manifests_ignore_attempt_and_detect_content_sequence_and_empty() {
        let root = tempfile::tempdir().unwrap();
        let run = |attempt, batches: Vec<RecordBatch>| {
            let id = transfer(crate::ids::JobId(42), 0, 1, 0)
                .with_attempt(crate::ids::AttemptId(attempt));
            let store = Arc::new(
                Store::new(root.path(), [id].into(), CancellationToken::default()).unwrap(),
            );
            let receiving = store.clone();
            let receiver = Receiver::start(TcpListener::bind("127.0.0.1:0").unwrap(), move |_| {
                Ok(receiving.clone())
            })
            .unwrap();
            let mut sender =
                Sender::new(receiver.address, id, CancellationToken::default()).unwrap();
            for batch in batches {
                sender.batch(&batch).unwrap();
            }
            sender.finish_manifest().unwrap().1
        };
        let original = run(0, vec![batch()]);
        original.validate_replay(&run(1, vec![batch()])).unwrap();
        assert!(original.validate_replay(&run(2, vec![batch()])).is_err());
        assert!(
            original
                .validate_replay(&run(1, vec![batch(), batch()]))
                .is_err()
        );
        assert!(
            original
                .validate_replay(&run(1, vec![batch().slice(0, 1)]))
                .is_err()
        );
        let changed = RecordBatch::try_new(
            schema(),
            vec![
                batch().column(0).clone(),
                Arc::new(Int64Array::from(vec![2, 5, 6])),
                batch().column(2).clone(),
            ],
        )
        .unwrap();
        let mutated = run(1, vec![changed.clone()]);
        assert_eq!(
            (original.segments, original.rows, original.payload_bytes),
            (mutated.segments, mutated.rows, mutated.payload_bytes)
        );
        assert_ne!(original.fingerprint, mutated.fingerprint);
        assert!(original.validate_replay(&mutated).is_err());
        let ordered = run(0, vec![batch(), changed.clone()]);
        let reordered = run(1, vec![changed, batch()]);
        assert_eq!(
            (ordered.segments, ordered.rows, ordered.payload_bytes),
            (reordered.segments, reordered.rows, reordered.payload_bytes)
        );
        assert!(ordered.validate_replay(&reordered).is_err());
        let empty = run(0, vec![]);
        empty.validate_replay(&run(1, vec![])).unwrap();
        assert_ne!(empty.fingerprint, original.fingerprint);
    }
}

#[cfg(test)]
mod topology_tests {
    use super::*;
    #[test]
    fn misrouted_but_row_conserving_batch_is_rejected() {
        use crate::{
            ids::{JobId, transfer},
            topology::Layout,
        };
        use arrow::array::{Int64Array, UInt64Array};
        let root = tempfile::tempdir().unwrap();
        let id = transfer(JobId(800), 0, 0, 0);
        let store =
            Arc::new(Store::new(root.path(), [id].into(), CancellationToken::default()).unwrap());
        store
            .install_layout(Layout::static_layout(2).unwrap())
            .unwrap();
        assert!(
            store
                .install_layout(Layout::static_layout(2).unwrap())
                .is_err()
        );
        let receiving = store.clone();
        let receiver = Receiver::start(TcpListener::bind("127.0.0.1:0").unwrap(), move |_| {
            Ok(receiving.clone())
        })
        .unwrap();
        // hash_v1(0)%2 == 1, but counts/schema/transfer identity alone all look legal.
        let batch = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(UInt64Array::from(vec![0])),
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(UInt64Array::from(vec![1])),
            ],
        )
        .unwrap();
        let mut sender = Sender::new(receiver.address, id, CancellationToken::default()).unwrap();
        sender.batch(&batch).unwrap();
        assert!(sender.finish().is_err());
        assert!(store.cancellation.check().is_err());
    }
}

#[cfg(test)]
mod join_tests {
    use super::*;
    use crate::ids::{JobId, transfer};
    fn batch() -> RecordBatch {
        RecordBatch::try_new(
            ade_exec::join::input_schema(),
            vec![
                Arc::new(arrow::array::UInt64Array::from(vec![0, 0])),
                Arc::new(arrow::array::Int64Array::from(vec![1, 1])),
            ],
        )
        .unwrap()
    }
    #[test]
    fn typed_codec_context_route_ingress_terminal_and_stage_are_fenced() {
        let root = tempfile::tempdir().unwrap();
        let id = transfer(JobId(7), 2, 0, 0);
        let context = "a".repeat(64);
        for case in 0..7 {
            let owner = crate::partition::destination(0, 2).unwrap();
            let store = Arc::new(
                Store::typed(
                    root.path(),
                    [id].into(),
                    CancellationToken::default(),
                    crate::ExchangeConfig {
                        mode: crate::ExchangeMode::Spill,
                        ..Default::default()
                    },
                    Payload::JoinRows,
                    context.clone(),
                    IngressLimits {
                        rows: if case == 3 { 1 } else { 2 },
                        bytes: if case == 4 { 1 } else { 1024 * 1024 },
                        batches: 2,
                    },
                    Some(HashOwner {
                        workers: 2,
                        destination: if case == 2 { 1 - owner } else { owner },
                    }),
                )
                .unwrap(),
            );
            let receiving = store.clone();
            let receiver = Receiver::start(TcpListener::bind("127.0.0.1:0").unwrap(), move |_| {
                Ok(receiving.clone())
            })
            .unwrap();
            let mut sender = Sender::typed(
                receiver.address,
                if case == 5 {
                    transfer(JobId(7), 3, 0, 0)
                } else {
                    id
                },
                CancellationToken::default(),
                if case == 1 {
                    Payload::JoinPairs
                } else {
                    Payload::JoinRows
                },
                Some(if case == 0 {
                    "b".repeat(64)
                } else {
                    context.clone()
                }),
            )
            .unwrap();
            if case == 6 {
                let batch = batch();
                sender.batch(&batch).unwrap();
                sender.fingerprint = blake3::Hasher::new();
                assert!(sender.finish().is_err());
            } else {
                let data = if case == 1 {
                    RecordBatch::try_new(
                        ade_exec::join::output_schema(),
                        vec![
                            Arc::new(arrow::array::UInt64Array::from(vec![0])),
                            Arc::new(arrow::array::Int64Array::from(vec![1])),
                            Arc::new(arrow::array::Int64Array::from(vec![1])),
                        ],
                    )
                    .unwrap()
                } else {
                    batch()
                };
                let written = sender.batch(&data);
                assert!(written.is_err() || sender.finish().is_err());
            }
            assert_eq!(store.metrics().unwrap().transfers, 0);
            assert!(store.cancellation.check().is_err());
            drop(receiver);
        }
    }
}

#[cfg(test)]
mod leased_tests {
    use super::*;
    use crate::ids::JobId;
    #[test]
    fn leased_persistent_idle_session_hash_context_and_poison() {
        let root = tempfile::tempdir().unwrap();
        let id = crate::ids::transfer(JobId(1), 0, 0, 0);
        let batch = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(arrow::array::UInt64Array::from(vec![1])),
                Arc::new(arrow::array::Int64Array::from(vec![7])),
                Arc::new(arrow::array::UInt64Array::from(vec![1])),
            ],
        )
        .unwrap();
        for fault in 0..4 {
            let store = Arc::new(
                Store::typed(
                    root.path(),
                    [id].into(),
                    CancellationToken::default(),
                    ExchangeConfig {
                        mode: ExchangeMode::Spill,
                        ..ExchangeConfig::default()
                    },
                    Payload::LeasedAggregate,
                    "a".repeat(64),
                    IngressLimits {
                        rows: 10,
                        bytes: 1 << 20,
                        batches: 10,
                    },
                    None,
                )
                .unwrap(),
            );
            let receiving = store.clone();
            let receiver = Receiver::start(TcpListener::bind("127.0.0.1:0").unwrap(), move |_| {
                Ok(receiving.clone())
            })
            .unwrap();
            let mut sender = Sender::typed(
                receiver.address,
                id,
                CancellationToken::default(),
                Payload::LeasedAggregate,
                Some(if fault == 1 {
                    "b".repeat(64)
                } else {
                    "a".repeat(64)
                }),
            )
            .unwrap();
            if fault == 3 {
                store.poison();
            }
            let written = sender.batch(&batch);
            if fault == 0 {
                thread::sleep(crate::IO_TIMEOUT + Duration::from_millis(150));
                sender.batch(&batch).unwrap();
                let (sent, manifest) = sender.finish_manifest().unwrap();
                assert_eq!(sent.transfers, 1);
                assert_eq!(sent.batches, 2);
                assert_eq!(manifest.rows, 2);
                assert_eq!(store.metrics().unwrap().transfers, 1);
            } else {
                if fault == 2 {
                    sender.fingerprint = blake3::Hasher::new();
                }
                assert!(written.is_err() || sender.finish().is_err());
                assert_eq!(store.metrics().unwrap().transfers, 0);
            }
            drop(receiver);
        }
    }
}
