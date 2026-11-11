use crate::{
    Context, Error, Result,
    contract::{hash, identifier},
    local,
};
use arrow::{
    datatypes::{DataType, Schema, TimeUnit},
    ipc::writer::StreamWriter,
    record_batch::RecordBatch,
};
use rustix::fs::{AtFlags, linkat};
use std::{
    io::{self, BufReader, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

const MAX_BYTES: u64 = 16 * 1024 * 1024;
const MAGIC: &[u8; 8] = b"ADEIPC21";
const HEADER: usize = MAGIC.len() + 64 + 32;

/// Bounded sinks accept native batches and issue a receipt after publication.
/// A pending handle aborts on drop. A commit error can be ambiguous.
pub trait Sink {
    type Pending: PendingSink;
    type Published;

    /// # Errors
    /// Refuses invalid keys, source/plan digests or destination.
    fn prepare(
        &self,
        key: &str,
        plan_digest: &str,
        schema: &Schema,
        context: &Context,
    ) -> Result<Self::Pending>;

    /// # Errors
    /// Refuses absent or invalid artifacts.
    fn reopen(&self, key: &str, context: &Context) -> Result<Self::Published>;
}

pub trait PendingSink: Sized {
    type Receipt;

    /// # Errors
    /// Refuses incompatible schema, over-limit output or failed I/O.
    fn write(&mut self, batch: &RecordBatch, context: &Context) -> Result<()>;

    /// # Errors
    /// Refuses poisoned attempts or conflicting keys. A sync failure after
    /// publication is ambiguous and must be reconciled via reopen.
    fn commit(self, context: &Context) -> Result<Self::Receipt>;

    fn abort(self);
}

#[derive(Debug, PartialEq, Eq, serde::Serialize)]
pub struct Receipt {
    pub key: String,
    pub plan_digest: String,
    pub payload_blake3: String,
    pub payload_bytes: u64,
}

pub struct Published {
    pub receipt: Receipt,
    pub ipc: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct SinkCapabilitiesV1 {
    pub local_atomic_publication: bool,
    pub same_key_same_payload_idempotency: bool,
    pub external_exactly_once: bool,
    pub max_payload_bytes: u64,
}

pub struct LocalSink {
    root: std::fs::File,
    path: PathBuf,
}

impl LocalSink {
    #[must_use]
    pub const fn capabilities(&self) -> SinkCapabilitiesV1 {
        SinkCapabilitiesV1 {
            local_atomic_publication: true,
            same_key_same_payload_idempotency: true,
            external_exactly_once: false,
            max_payload_bytes: MAX_BYTES,
        }
    }

    /// Open an existing operator-controlled directory on a local filesystem.
    ///
    /// # Errors
    /// Refuses a missing or symlink root.
    pub fn new(root: &Path) -> Result<Self> {
        let root_fd = local::root(root)?;
        let metadata = root_fd.metadata()?;
        if metadata.permissions().readonly() {
            return Err(Error::Admission("writable sink directory required"));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if metadata.uid() != rustix::process::geteuid().as_raw() || metadata.mode() & 0o022 != 0
            {
                return Err(Error::Admission("operator-owned sink directory required"));
            }
        }
        Ok(Self {
            root: root_fd,
            path: root.to_path_buf(),
        })
    }
}

struct CappedFile {
    file: tempfile::NamedTempFile,
    hasher: blake3::Hasher,
    bytes: u64,
}

impl Write for CappedFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.len() as u64 > MAX_BYTES - self.bytes {
            return Err(io::Error::from(io::ErrorKind::FileTooLarge));
        }
        let n = self.file.write(buf)?;
        self.hasher.update(&buf[..n]);
        self.bytes += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

pub struct LocalPending {
    root: std::fs::File,
    key: String,
    plan: String,
    schema: Schema,
    poisoned: bool,
    writer: StreamWriter<CappedFile>,
}

fn name(key: &str) -> Result<String> {
    if !identifier(key) {
        return Err(Error::Admission("sink idempotency key"));
    }
    Ok(format!("{key}.ade-ipc"))
}

fn ipc_error(error: arrow::error::ArrowError) -> Error {
    match error {
        arrow::error::ArrowError::IoError(_, io) if io.kind() == io::ErrorKind::FileTooLarge => {
            Error::Limit("IPC output bytes")
        }
        arrow::error::ArrowError::IoError(_, io) => io.into(),
        _ => Error::Protocol("IPC output"),
    }
}

impl Sink for LocalSink {
    type Pending = LocalPending;
    type Published = Published;

    fn prepare(
        &self,
        key: &str,
        plan_digest: &str,
        schema: &Schema,
        context: &Context,
    ) -> Result<LocalPending> {
        context.check()?;
        name(key)?;
        if schema.fields().is_empty()
            || schema.fields().len() > 16
            || !schema.metadata().is_empty()
            || schema.fields().iter().any(|field| {
                field.name().len() > 128
                    || !field.metadata().is_empty()
                    || !matches!(
                        field.data_type(),
                        DataType::UInt64
                            | DataType::Int64
                            | DataType::Float64
                            | DataType::UInt32
                            | DataType::Utf8
                            | DataType::Timestamp(TimeUnit::Millisecond, None)
                    )
            })
        {
            return Err(Error::Admission("sink primitive output schema"));
        }
        if !hash(plan_digest) {
            return Err(Error::Admission("sink plan digest"));
        }
        let mut file = tempfile::Builder::new()
            .prefix(".ade-sink-")
            .tempfile_in(&self.path)?;
        file.write_all(&[0; HEADER])?;
        let writer = StreamWriter::try_new(
            CappedFile {
                file,
                hasher: blake3::Hasher::new(),
                bytes: 0,
            },
            schema,
        )
        .map_err(ipc_error)?;
        Ok(LocalPending {
            root: self.root.try_clone()?,
            key: key.into(),
            plan: plan_digest.into(),
            schema: schema.clone(),
            poisoned: false,
            writer,
        })
    }

    fn reopen(&self, key: &str, context: &Context) -> Result<Published> {
        reopen_at(&self.root, key, context)
    }
}

fn reopen_at(root: &std::fs::File, key: &str, context: &Context) -> Result<Published> {
    context.check()?;
    let file = local::regular_at(root, &name(key)?)?;
    let payload = file
        .metadata()?
        .len()
        .checked_sub(HEADER as u64)
        .filter(|&n| (1..=MAX_BYTES).contains(&n))
        .ok_or(Error::Limit("sink artifact size"))?;
    let mut file = BufReader::new(file);
    let mut header = [0; HEADER];
    file.read_exact(&mut header)?;
    let plan = std::str::from_utf8(&header[8..72]).map_err(|_| Error::Identity("sink envelope"))?;
    if &header[..8] != MAGIC || !hash(plan) {
        return Err(Error::Identity("sink envelope"));
    }
    let mut ipc = Vec::with_capacity(
        usize::try_from(payload).map_err(|_| Error::Limit("sink address space"))?,
    );
    let mut buf = [0; 8192];
    loop {
        context.check()?;
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        if (ipc.len() + n) as u64 > payload {
            return Err(Error::Identity("sink payload length"));
        }
        ipc.extend_from_slice(&buf[..n]);
    }
    let digest = artifact_digest(key, plan, blake3::hash(&ipc));
    if ipc.len() as u64 != payload || digest.as_bytes() != &header[72..] {
        return Err(Error::Identity("sink artifact content"));
    }
    Ok(Published {
        receipt: Receipt {
            key: key.into(),
            plan_digest: plan.into(),
            payload_blake3: ade_physical::fingerprint(&ipc),
            payload_bytes: payload,
        },
        ipc,
    })
}

fn artifact_digest(key: &str, plan: &str, payload: blake3::Hash) -> blake3::Hash {
    let mut hasher = blake3::Hasher::new_derive_key("ade-connectors local publication v1");
    hasher.update(plan.as_bytes());
    hasher.update(payload.as_bytes());
    hasher.update(key.as_bytes());
    hasher.finalize()
}

impl PendingSink for LocalPending {
    type Receipt = Receipt;

    fn write(&mut self, batch: &RecordBatch, context: &Context) -> Result<()> {
        if self.poisoned {
            return Err(Error::Protocol("poisoned sink attempt"));
        }
        self.poisoned = true;
        context.check()?;
        if batch.schema().as_ref() != &self.schema
            || batch.get_array_memory_size() as u64 > MAX_BYTES
        {
            return Err(Error::Limit("sink batch schema or byte cap"));
        }
        self.writer.write(batch).map_err(ipc_error)?;
        self.poisoned = false;
        Ok(())
    }

    fn commit(self, context: &Context) -> Result<Receipt> {
        context.check()?;
        if self.poisoned {
            return Err(Error::Protocol("poisoned sink attempt"));
        }
        let mut staged = self.writer.into_inner().map_err(ipc_error)?;
        let digest = staged.hasher.finalize();
        let receipt = Receipt {
            key: self.key,
            plan_digest: self.plan,
            payload_blake3: digest.to_hex().to_string(),
            payload_bytes: staged.bytes,
        };
        staged.file.seek(SeekFrom::Start(0))?;
        staged.file.write_all(MAGIC)?;
        staged.file.write_all(receipt.plan_digest.as_bytes())?;
        staged
            .file
            .write_all(artifact_digest(&receipt.key, &receipt.plan_digest, digest).as_bytes())?;
        staged.file.as_file().sync_all()?;
        context.check()?;
        let staged_name = staged
            .file
            .path()
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or(Error::Identity("sink temporary file name"))?;
        let staged_in_root = local::regular_at(&self.root, staged_name)?;
        let staged_meta = staged.file.as_file().metadata()?;
        let root_meta = staged_in_root.metadata()?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if staged_meta.dev() != root_meta.dev() || staged_meta.ino() != root_meta.ino() {
                return Err(Error::Identity("sink root changed during publication"));
            }
        }
        let target = name(&receipt.key)?;
        match linkat(
            &self.root,
            staged_name,
            &self.root,
            &target,
            AtFlags::empty(),
        ) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                if reopen_at(&self.root, &receipt.key, context)?.receipt != receipt {
                    return Err(Error::Conflict);
                }
            }
            Err(e) => return Err(io::Error::from(e).into()),
        }
        self.root.sync_all().map_err(|_| Error::Ambiguous)?;
        Ok(receipt)
    }

    fn abort(self) {
        drop(self);
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use arrow::{array::Int64Array, datatypes::Field};
    use std::{sync::Arc, time::Duration};

    #[test]
    fn publication_stays_anchored_when_root_path_is_replaced() {
        let parent = tempfile::tempdir().unwrap();
        let root = parent.path().join("output");
        let moved = parent.path().join("approved-output");
        std::fs::create_dir(&root).unwrap();
        let sink = LocalSink::new(&root).unwrap();
        let context = Context::new(Duration::from_secs(10)).unwrap();
        let schema = Schema::new(vec![Field::new("value", DataType::Int64, false)]);
        let mut pending = sink
            .prepare("artifact", &"a".repeat(64), &schema, &context)
            .unwrap();
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(Int64Array::from(vec![7]))])
                .unwrap();
        pending.write(&batch, &context).unwrap();

        std::fs::rename(&root, &moved).unwrap();
        std::fs::create_dir(&root).unwrap();
        std::fs::write(root.join("victim.txt"), b"must remain untouched").unwrap();

        pending.commit(&context).unwrap();
        assert!(moved.join("artifact.ade-ipc").is_file());
        assert!(!root.join("artifact.ade-ipc").exists());
        assert_eq!(
            std::fs::read(root.join("victim.txt")).unwrap(),
            b"must remain untouched"
        );
        assert_eq!(
            sink.reopen("artifact", &context).unwrap().receipt.key,
            "artifact"
        );
    }
}
