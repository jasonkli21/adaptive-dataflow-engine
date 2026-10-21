//! Complete-copy mechanism, orthogonal to consumer delivery. Metadata is O(transfers).
use crate::{
    Result,
    cost::Decision,
    ids::{AttemptId, TransferId},
    lineage::TransferManifest,
    require,
    retention::{Objects, SpoolIo},
};
use ade_exec::CancellationToken;
use serde::{Deserialize, Serialize};
use std::{
    path::Path,
    sync::{Arc, atomic::AtomicBool},
    time::Instant,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MaterializationReceipt {
    pub version: u32,
    pub original: TransferManifest,
    pub backend: crate::ObjectStoreConfig,
    /// complete-v1 + original `TransferId` + sequence; no per-segment keys.
    pub namespace: String,
    /// Unique execution scope; does not alter logical transfer identity.
    pub execution_namespace: String,
    pub write_bytes: u64,
    pub write_requests: u64,
    pub write_elapsed_ns: u64,
}
impl MaterializationReceipt {
    /// # Errors
    /// Refuses incomplete, foreign, suffix-only or worker-local copies.
    pub fn validate(
        &self,
        original: &TransferManifest,
        backend: &crate::ObjectStoreConfig,
        execution_namespace: &str,
    ) -> Result<()> {
        require(
            self.version == 1
                && self.namespace == "complete-v1"
                && self.execution_namespace == execution_namespace
                && self.original == *original
                && original.attempt == AttemptId(0)
                && self.backend == *backend
                && backend.durability == crate::ObjectStoreDurability::WorkerProcessSurviving
                && self.write_bytes == original.payload_bytes
                && self.write_requests == original.segments
                && blake3::Hash::from_hex(&original.fingerprint).is_ok(),
            "invalid complete materialization receipt",
        )
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MaterializationFact {
    pub decision: Decision,
    pub receipt: Option<MaterializationReceipt>,
    pub staging_write_bytes: u64,
    pub staging_read_bytes: u64,
    pub staging_write_ns: u64,
    pub staging_read_ns: u64,
}
pub(crate) fn restore(
    receipt: &MaterializationReceipt,
    original: &TransferManifest,
    backend: &crate::ObjectStoreConfig,
    objects: &Objects,
    address: std::net::SocketAddr,
    token: &CancellationToken,
) -> Result<(TransferManifest, u64)> {
    let execution_namespace = objects
        .execution_namespace()
        .ok_or_else(|| crate::Error::Invalid("restore requires an execution namespace".into()))?;
    receipt.validate(original, backend, execution_namespace)?;
    let id = TransferId {
        job: original.logical.job,
        exchange: original.logical.exchange,
        partition: original.logical.partition,
        producer: original.logical.producer,
        attempt: AttemptId(0),
    };
    let mut sender =
        crate::exchange::Sender::new(address, id.with_attempt(AttemptId(1)), token.clone())?;
    let start = Instant::now();
    for sequence in 0..original.segments {
        let bytes = objects.get(id, sequence, token)?;
        sender.encoded(&bytes)?;
    }
    let replay = sender.finish_manifest()?.1;
    original.validate_replay(&replay)?;
    Ok((replay, ns(start)))
}

pub(crate) fn ns(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX)
}
pub(crate) struct Staging {
    io: SpoolIo,
    poison: Arc<AtomicBool>,
    bytes: u64,
    write_ns: u64,
}
impl Staging {
    pub(crate) fn new(parent: &Path, poison: Arc<AtomicBool>) -> Result<Self> {
        Ok(Self {
            io: SpoolIo::new(tempfile::tempfile_in(parent)?, poison.clone()),
            poison,
            bytes: 0,
            write_ns: 0,
        })
    }
    pub(crate) fn push(&mut self, bytes: &[u8], token: &CancellationToken) -> Result<()> {
        let start = Instant::now();
        let pending = self.io.enqueue_write(bytes.to_vec())?;
        SpoolIo::wait(&pending, token, &self.poison)?;
        self.bytes += bytes.len() as u64;
        self.write_ns += ns(start);
        Ok(())
    }
    pub(crate) fn complete(
        self,
        id: TransferId,
        manifest: &TransferManifest,
        decision: Decision,
        objects: &Objects,
        backend: &crate::ObjectStoreConfig,
        token: &CancellationToken,
    ) -> Result<MaterializationFact> {
        require(
            self.bytes == manifest.payload_bytes,
            "staging coverage mismatch",
        )?;
        let mut fact = MaterializationFact {
            decision,
            receipt: None,
            staging_write_bytes: self.bytes,
            staging_write_ns: self.write_ns,
            staging_read_bytes: 0,
            staging_read_ns: 0,
        };
        if !fact.decision.persist {
            return Ok(fact);
        }
        require(
            backend.durability == crate::ObjectStoreDurability::WorkerProcessSurviving,
            "complete materialization requires worker-process-surviving backend",
        )?;
        let mut write_elapsed_ns = 0;
        let mut hash = blake3::Hasher::new();
        hash.update(b"ade-transfer-v1");
        hash.update(&serde_json::to_vec(&id.logical())?);
        let mut rows = 0;
        for sequence in 0..manifest.segments {
            token.check()?;
            let read = Instant::now();
            let pending = self.io.enqueue_read()?;
            let bytes = SpoolIo::wait(&pending, token, &self.poison)?
                .ok_or_else(|| crate::Error::Invalid("truncated staging".into()))?;
            fact.staging_read_ns += ns(read);
            fact.staging_read_bytes += bytes.len() as u64;
            rows += crate::exchange::decode(&bytes)?.num_rows() as u64;
            hash.update(&sequence.to_le_bytes());
            hash.update(&(bytes.len() as u64).to_le_bytes());
            hash.update(&bytes);
            let write = Instant::now();
            objects.put(id, sequence, bytes, token)?;
            write_elapsed_ns += ns(write);
        }
        let pending = self.io.enqueue_read()?;
        require(
            SpoolIo::wait(&pending, token, &self.poison)?.is_none(),
            "excess staging frames",
        )?;
        hash.update(b"terminal-v1");
        hash.update(&manifest.segments.to_le_bytes());
        require(
            hash.finalize().to_hex().as_str() == manifest.fingerprint
                && rows == manifest.rows
                && fact.staging_read_bytes == manifest.payload_bytes,
            "staged manifest mismatch",
        )?;
        let receipt = MaterializationReceipt {
            version: 1,
            original: manifest.clone(),
            backend: backend.clone(),
            namespace: "complete-v1".into(),
            execution_namespace: objects
                .execution_namespace()
                .ok_or_else(|| {
                    crate::Error::Invalid("materialization requires an execution namespace".into())
                })?
                .to_owned(),
            write_bytes: fact.staging_read_bytes,
            write_requests: manifest.segments,
            write_elapsed_ns,
        };
        receipt.validate(
            manifest,
            backend,
            objects.execution_namespace().expect("complete namespace"),
        )?;
        fact.receipt = Some(receipt);
        Ok(fact)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ObjectStoreBackend, ObjectStoreConfig, ObjectStoreDurability,
        cost::{CostConfig, Production},
        exchange::{Receiver, Sender, Store},
        ids::{JobId, transfer},
    };
    use arrow::{
        array::{Int64Array, UInt64Array},
        record_batch::RecordBatch,
    };
    use std::net::TcpListener;
    const RUN_A: &str = "8abade70-a479-4c7e-9bd8-989ed8f1a1d7";
    fn receive(root: &Path, id: TransferId) -> (Arc<Store>, Receiver) {
        let store = Arc::new(Store::new(root, [id].into(), CancellationToken::default()).unwrap());
        let receiving = store.clone();
        let receiver = Receiver::start(TcpListener::bind("127.0.0.1:0").unwrap(), move |_| {
            Ok(receiving.clone())
        })
        .unwrap();
        (store, receiver)
    }
    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "One lifecycle fixture covers complete-copy integrity and writer independence."
    )]
    fn full_copy_survives_writer_and_rejects_corruption_missing_forgery_and_ephemeral() {
        let root = tempfile::tempdir().unwrap();
        let id = transfer(JobId(42), 0, 1, 0);
        let config = ObjectStoreConfig {
            backend: "fixture".into(),
            durability: ObjectStoreDurability::WorkerProcessSurviving,
        };
        let backend = ObjectStoreBackend::new(
            config.clone(),
            Arc::new(object_store::memory::InMemory::new()),
        )
        .unwrap();
        let objects = || {
            Objects::configured(root.path(), config.clone(), Some(&backend), Arc::default())
                .unwrap()
                .complete(RUN_A.into())
                .unwrap()
        };
        let writer = objects();
        let (store, receiver) = receive(root.path(), id);
        let mut sender = Sender::new(receiver.address, id, CancellationToken::default())
            .unwrap()
            .stage(root.path(), Arc::default())
            .unwrap();
        let batch = RecordBatch::try_new(
            crate::exchange::schema(),
            vec![
                Arc::new(UInt64Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![5])),
                Arc::new(UInt64Array::from(vec![2])),
            ],
        )
        .unwrap();
        for _ in 0..20 {
            sender.batch(&batch).unwrap();
        }
        let (counts, manifest, staging) = sender.finish_staged().unwrap();
        let decision = CostConfig {
            mode: crate::cost::MaterializationMode::Always,
            ..Default::default()
        }
        .decide(Production {
            output_bytes: counts.payload_bytes,
            output_segments: counts.batches,
            consumer_count: 1,
            ..Default::default()
        })
        .unwrap();
        let fact = staging
            .unwrap()
            .complete(
                id,
                &manifest,
                decision,
                &writer,
                &config,
                &CancellationToken::default(),
            )
            .unwrap();
        let receipt = fact.receipt.unwrap();
        assert_eq!(receipt.write_requests, 20);
        drop(writer);
        drop(receiver);
        drop(store); // The writer session no longer exists.
        let survivor = objects();
        let (restored, receiver) = receive(root.path(), id.with_attempt(AttemptId(1)));
        let (replay, _) = restore(
            &receipt,
            &manifest,
            &config,
            &survivor,
            receiver.address,
            &CancellationToken::default(),
        )
        .unwrap();
        manifest.validate_replay(&replay).unwrap();
        assert_eq!(restored.metrics().unwrap().batches, 20);
        drop(receiver);
        let mut forged = receipt.clone();
        forged.original.rows += 1;
        assert!(forged.validate(&manifest, &config, RUN_A).is_err());
        forged = receipt.clone();
        forged.namespace = "delivery_suffix".into();
        assert!(forged.validate(&manifest, &config, RUN_A).is_err());
        let mut ephemeral = config.clone();
        ephemeral.durability = ObjectStoreDurability::Ephemeral;
        assert!(receipt.validate(&manifest, &ephemeral, RUN_A).is_err());
        assert!(
            receipt
                .validate(&manifest, &config, "7b9b1bbf-929e-4b5e-a86e-8ed9bb5ed5b4")
                .is_err()
        );
        // Corrupt an existing full-copy object; no aggregate/committed result escapes.
        survivor
            .put(id, 7, vec![0; 16], &CancellationToken::default())
            .unwrap();
        let (_store, receiver) = receive(root.path(), id.with_attempt(AttemptId(1)));
        assert!(
            restore(
                &receipt,
                &manifest,
                &config,
                &survivor,
                receiver.address,
                &CancellationToken::default()
            )
            .is_err()
        );
        drop(receiver);
        survivor.cleanup_job(id.job).unwrap();
        let (_store, receiver) = receive(root.path(), id.with_attempt(AttemptId(1)));
        assert!(
            restore(
                &receipt,
                &manifest,
                &config,
                &objects(),
                receiver.address,
                &CancellationToken::default()
            )
            .is_err()
        );
    }
    #[test]
    fn empty_transfer_is_complete_and_cancellation_discards_staging() {
        let root = tempfile::tempdir().unwrap();
        let id = transfer(JobId(43), 0, 1, 0);
        let config = ObjectStoreConfig {
            backend: "fixture".into(),
            durability: ObjectStoreDurability::WorkerProcessSurviving,
        };
        let backend = ObjectStoreBackend::new(
            config.clone(),
            Arc::new(object_store::memory::InMemory::new()),
        )
        .unwrap();
        let objects =
            Objects::configured(root.path(), config.clone(), Some(&backend), Arc::default())
                .unwrap()
                .complete(RUN_A.into())
                .unwrap();
        let (_store, receiver) = receive(root.path(), id);
        let sender = Sender::new(receiver.address, id, CancellationToken::default())
            .unwrap()
            .stage(root.path(), Arc::default())
            .unwrap();
        let (_, original, stage) = sender.finish_staged().unwrap();
        let decision = CostConfig {
            mode: crate::cost::MaterializationMode::Always,
            ..Default::default()
        }
        .decide(Production {
            consumer_count: 1,
            ..Default::default()
        })
        .unwrap();
        let fact = stage
            .unwrap()
            .complete(
                id,
                &original,
                decision,
                &objects,
                &config,
                &CancellationToken::default(),
            )
            .unwrap();
        drop(receiver);
        let (_store, receiver) = receive(root.path(), id.with_attempt(AttemptId(1)));
        assert!(
            restore(
                &fact.receipt.unwrap(),
                &original,
                &config,
                &objects,
                receiver.address,
                &CancellationToken::default()
            )
            .is_ok()
        );
        let mut stage = Staging::new(root.path(), Arc::default()).unwrap();
        let cancel = CancellationToken::default();
        cancel.cancel();
        assert!(stage.push(&[1], &cancel).is_err());
    }
}
