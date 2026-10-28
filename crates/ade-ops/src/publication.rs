//! Local IPC publication. Public standalone completion is a caller assertion;
//! the runner attaches opaque native proof internally before a durable job commit.
use crate::{Error, Result, check_time, fault, recipe::nonsymlink, require};
use ade_exec::{CancellationToken, physical::arrow_schema};
use ade_physical::Schema;
use arrow::{
    ipc::{reader::StreamReader, writer::StreamWriter},
    record_batch::RecordBatch,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    time::Instant,
};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Identity {
    pub store: Uuid,
    pub job: Uuid,
    pub instance: Uuid,
    pub owner: u64,
    pub engine_job: u64,
    pub plan_hash: String,
    pub source_hash: String,
    pub config_hash: String,
    pub workers_hash: String,
}
impl Identity {
    fn validate(&self) -> Result<()> {
        require(
            !self.store.is_nil()
                && !self.job.is_nil()
                && !self.instance.is_nil()
                && self.owner > 0
                && [
                    &self.plan_hash,
                    &self.source_hash,
                    &self.config_hash,
                    &self.workers_hash,
                ]
                .into_iter()
                .all(|s| hash(s)),
            "publication identity",
        )
    }
}
fn hash(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Terminal {
    pub partition: u32,
    pub attempt: u32,
    pub rows: u64,
    pub fingerprint: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Completion {
    pub workers: u32,
    pub terminals: Vec<Terminal>,
}
impl Completion {
    fn rows(&self) -> Result<u64> {
        require(
            (2..=4).contains(&self.workers) && self.terminals.len() == self.workers as usize,
            "complete fixed terminal set",
        )?;
        let mut partitions = BTreeSet::new();
        self.terminals.iter().try_fold(0u64, |rows, t| {
            require(
                t.partition < self.workers
                    && t.attempt <= 1
                    && partitions.insert(t.partition)
                    && hash(&t.fingerprint),
                "duplicate/foreign terminal",
            )?;
            rows.checked_add(t.rows)
                .ok_or(Error::Quota("terminal row overflow"))
        })
    }
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    pub rows: u64,
    /// Actual serialized IPC bytes, including schema and end-of-stream framing.
    pub bytes: u64,
}
impl Limits {
    fn validate(self) -> Result<()> {
        require(
            self.rows <= 1_000_000 && (1..=64 * 1024 * 1024).contains(&self.bytes),
            "publication limits",
        )
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub version: u32,
    pub identity: Identity,
    pub schema: Schema,
    pub completion: Completion,
    pub rows: u64,
    pub bytes: u64,
    pub file: String,
    pub blake3: String,
}
struct BudgetWriter {
    file: File,
    limit: u64,
    written: u64,
    hash: blake3::Hasher,
}
impl Write for BudgetWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let proposed = self
            .written
            .checked_add(bytes.len() as u64)
            .ok_or(std::io::ErrorKind::FileTooLarge)?;
        if proposed > self.limit {
            return Err(std::io::ErrorKind::FileTooLarge.into());
        }
        let n = self.file.write(bytes)?;
        self.hash.update(&bytes[..n]);
        self.written += n as u64;
        Ok(n)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}
/// A failed or dropped stage can leave private garbage. It never removes files
/// after a possibly successful link, and never overwrites the visible marker.
pub struct Publisher {
    root: PathBuf,
    identity: Identity,
    schema: Schema,
    completion: Completion,
    limits: Limits,
    token: CancellationToken,
    until: Instant,
    rows: u64,
    failed: bool,
    total_cap: Option<u64>,
    file_name: String,
    writer: StreamWriter<BudgetWriter>,
}
impl Publisher {
    /// Start one unique immutable attempt under an existing private directory.
    /// # Errors
    /// Refuses unsafe paths, conflicting marker, malformed identity/coverage/bounds.
    pub fn stage(
        root: &Path,
        identity: Identity,
        schema: Schema,
        completion: Completion,
        limits: Limits,
        token: CancellationToken,
        until: Instant,
    ) -> Result<Self> {
        identity.validate()?;
        limits.validate()?;
        completion.rows()?;
        validate_schema(&schema)?;
        private_root(root)?;
        check(&token, until)?;
        require(
            !root.join("COMMITTED.json").try_exists()?,
            "publication already exists",
        )?;
        // Exact attempt-derived name, never supplied by an untrusted manifest.
        let file_name = format!("attempt-{}.arrow", identity.instance);
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(root.join(&file_name))?;
        let writer = StreamWriter::try_new(
            BudgetWriter {
                file,
                limit: limits.bytes,
                written: 0,
                hash: blake3::Hasher::new(),
            },
            &arrow_schema(&schema),
        )
        .map_err(|e| Error::Invalid(format!("IPC stage: {e}")))?;
        Ok(Self {
            root: root.into(),
            identity,
            schema,
            completion,
            limits,
            token,
            until,
            rows: 0,
            failed: false,
            total_cap: None,
            file_name,
            writer,
        })
    }
    /// # Errors
    /// Refuses exact-schema, row, serialized-byte, deadline or cancellation violations.
    pub fn push(&mut self, batch: &RecordBatch) -> Result<()> {
        require(!self.failed, "aborted output stage")?;
        self.failed = true;
        check(&self.token, self.until)?;
        require(
            batch.schema() == arrow_schema(&self.schema) && batch.num_rows() <= 8192,
            "output schema/batch cap",
        )?;
        let rows = self
            .rows
            .checked_add(batch.num_rows() as u64)
            .ok_or(Error::Quota("output row overflow"))?;
        if rows > self.limits.rows {
            return Err(Error::Quota("output rows"));
        }
        crate::io_point("output_batch")?;
        self.writer
            .write(batch)
            .map_err(|e| Error::Invalid(format!("IPC stage write: {e}")))?;
        self.rows = rows;
        check(&self.token, self.until)?;
        self.failed = false;
        Ok(())
    }
    /// Finalize, independently re-read data, sync and publish using atomic hard link.
    /// Caller must validate real native terminals and source postflight before calling.
    /// # Errors
    /// A post-link error is an ambiguous publication: verify it; do not delete/retry.
    pub fn publish(self) -> Result<Manifest> {
        self.prepare()?.commit()
    }
    pub(crate) fn native_completion(
        &mut self,
        completion: &ade_distributed::managed::Completion,
        total_cap: u64,
    ) -> Result<()> {
        self.completion = Completion {
            workers: u32::try_from(completion.terminals().len())
                .map_err(|_| Error::Quota("native finals"))?,
            terminals: completion
                .terminals()
                .iter()
                .map(|t| {
                    Ok(Terminal {
                        partition: u32::try_from(t.partition)
                            .map_err(|_| Error::Quota("native partition"))?,
                        attempt: 0,
                        rows: t.rows,
                        fingerprint: t.fingerprint.clone(),
                    })
                })
                .collect::<Result<_>>()?,
        };
        self.completion.rows()?;
        self.total_cap = Some(total_cap);
        Ok(())
    }
    pub(crate) fn prepare(mut self) -> Result<Prepared> {
        require(!self.failed, "aborted output stage")?;
        check(&self.token, self.until)?;
        require(
            self.rows == self.completion.rows()?,
            "output/terminal row conservation",
        )?;
        self.writer
            .finish()
            .map_err(|e| Error::Invalid(format!("IPC finish: {e}")))?;
        let output = self
            .writer
            .into_inner()
            .map_err(|e| Error::Invalid(e.to_string()))?;
        crate::io_point("data_sync")?;
        output.file.sync_all()?;
        File::open(&self.root)?.sync_all()?;
        fault("output_synced");
        let manifest = Manifest {
            version: 1,
            identity: self.identity,
            schema: self.schema,
            completion: self.completion,
            rows: self.rows,
            bytes: output.written,
            file: self.file_name,
            blake3: output.hash.finalize().to_hex().to_string(),
        };
        verify_data(&self.root, &manifest, self.limits, &self.token, self.until)?;
        let bytes = serde_json::to_vec(&manifest)?;
        if bytes.len() > 65536 {
            return Err(Error::Quota("publication marker"));
        }
        if self.total_cap.is_some_and(|cap| {
            manifest
                .bytes
                .checked_add(bytes.len() as u64)
                .is_none_or(|total| total > cap)
        }) {
            return Err(Error::Quota("total staging bytes"));
        }
        let candidate = self
            .root
            .join(format!("manifest-{}.json", manifest.identity.instance));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&candidate)?;
        file.write_all(&bytes)?;
        crate::io_point("manifest_sync")?;
        file.sync_all()?;
        fault("manifest_synced");
        Ok(Prepared {
            root: self.root,
            manifest,
            candidate,
            token: self.token,
            until: self.until,
        })
    }
}
pub(crate) struct Prepared {
    root: PathBuf,
    manifest: Manifest,
    candidate: PathBuf,
    token: CancellationToken,
    until: Instant,
}
impl Prepared {
    pub(crate) fn token(&self) -> &CancellationToken {
        &self.token
    }
    pub(crate) fn manifest(&self) -> &Manifest {
        &self.manifest
    }
    pub(crate) fn commit(self) -> Result<Manifest> {
        check(&self.token, self.until)?;
        crate::io_point("link")?;
        fs::hard_link(&self.candidate, self.root.join("COMMITTED.json"))?;
        fault("marker_published");
        crate::io_point("marker_sync")?;
        File::open(&self.root)?.sync_all()?;
        fault("marker_durable");
        Ok(self.manifest)
    }
}
pub(crate) fn validate_record(manifest: &Manifest) -> Result<()> {
    manifest.identity.validate()?;
    validate_schema(&manifest.schema)?;
    require(
        manifest.version == 1
            && manifest.completion.rows()? == manifest.rows
            && manifest.rows <= 1_000_000
            && manifest.bytes <= 64 * 1024 * 1024
            && hash(&manifest.blake3)
            && manifest.file == format!("attempt-{}.arrow", manifest.identity.instance)
            && serde_json::to_vec(manifest)?.len() <= 65536,
        "bounded publication record",
    )
}
pub(crate) fn resume(
    root: &Path,
    manifest: &Manifest,
    limits: Limits,
    token: CancellationToken,
    until: Instant,
) -> Result<Prepared> {
    private_root(root)?;
    verify_data(root, manifest, limits, &token, until)?;
    let candidate = root.join(format!("manifest-{}.json", manifest.identity.instance));
    let file = regular(&candidate, 1)?;
    require(file.metadata()?.len() <= 65536, "candidate marker cap")?;
    let mut bytes = vec![];
    file.take(65537).read_to_end(&mut bytes)?;
    require(
        bytes == serde_json::to_vec(manifest)?,
        "staged manifest differs from native proof",
    )?;
    Ok(Prepared {
        root: root.into(),
        manifest: manifest.clone(),
        candidate,
        token,
        until,
    })
}
/// Verify the only consumer-visible entry point, against independently pinned identity,
/// schema and limits. Never discovers output by listing staging files.
/// # Errors
/// Refuses absent, foreign, corrupt, truncated, symlinked or inconsistent output.
pub fn verify(
    root: &Path,
    identity: &Identity,
    schema: &Schema,
    limits: Limits,
    token: &CancellationToken,
    until: Instant,
) -> Result<Manifest> {
    private_root(root)?;
    identity.validate()?;
    limits.validate()?;
    validate_schema(schema)?;
    check(token, until)?;
    let file = regular(&root.join("COMMITTED.json"), 2)?;
    require(file.metadata()?.len() <= 65536, "marker byte cap")?;
    let mut bytes = vec![];
    file.take(65537).read_to_end(&mut bytes)?;
    require(bytes.len() <= 65536, "marker byte cap")?;
    let manifest: Manifest = serde_json::from_slice(&bytes)?;
    require(
        manifest.version == 1 && &manifest.identity == identity && &manifest.schema == schema,
        "foreign marker/version/schema",
    )?;
    verify_data(root, &manifest, limits, token, until)?;
    Ok(manifest)
}
fn verify_data(
    root: &Path,
    manifest: &Manifest,
    limits: Limits,
    token: &CancellationToken,
    until: Instant,
) -> Result<()> {
    require(
        manifest.file == format!("attempt-{}.arrow", manifest.identity.instance)
            && hash(&manifest.blake3),
        "unsafe output reference",
    )?;
    require(
        manifest.rows == manifest.completion.rows()?
            && manifest.rows <= limits.rows
            && manifest.bytes <= limits.bytes,
        "manifest output limits/coverage",
    )?;
    let mut file = regular(&root.join(&manifest.file), 1)?;
    require(
        file.metadata()?.len() == manifest.bytes,
        "output size mismatch",
    )?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0; 8192];
    let mut bytes = 0u64;
    loop {
        check(token, until)?;
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        bytes = bytes
            .checked_add(n as u64)
            .ok_or(Error::Quota("output bytes overflow"))?;
        require(bytes <= manifest.bytes, "output grew during verification")?;
        hasher.update(&buffer[..n]);
    }
    require(
        bytes == manifest.bytes && hasher.finalize().to_hex().as_str() == manifest.blake3,
        "output checksum mismatch",
    )?;
    let reader = StreamReader::try_new(regular(&root.join(&manifest.file), 1)?, None)
        .map_err(|e| Error::Invalid(e.to_string()))?;
    require(
        reader.schema() == arrow_schema(&manifest.schema),
        "IPC output schema",
    )?;
    let mut rows = 0u64;
    for batch in reader {
        check(token, until)?;
        let batch = batch.map_err(|e| Error::Invalid(e.to_string()))?;
        require(
            batch.schema() == arrow_schema(&manifest.schema) && batch.num_rows() <= 8192,
            "IPC schema/batch cap",
        )?;
        for (array, field) in batch.columns().iter().zip(&manifest.schema) {
            require(
                field.nullable || array.null_count() == 0,
                "nonnull output field",
            )?;
        }
        rows = rows
            .checked_add(batch.num_rows() as u64)
            .ok_or(Error::Quota("output rows overflow"))?;
        require(rows <= manifest.rows, "IPC row cap")?;
    }
    check(token, until)?;
    require(rows == manifest.rows, "IPC row count")
}
fn validate_schema(schema: &Schema) -> Result<()> {
    let mut names = BTreeSet::new();
    require(
        !schema.is_empty()
            && schema.len() <= 16
            && schema
                .iter()
                .all(|f| !f.name.is_empty() && f.name.len() <= 128 && names.insert(&f.name)),
        "bounded unique output schema",
    )
}
fn check(token: &CancellationToken, until: Instant) -> Result<()> {
    token.check().map_err(|e| Error::Invalid(e.to_string()))?;
    check_time(until)
}
fn private_root(root: &Path) -> Result<()> {
    nonsymlink(root)?;
    let meta = fs::metadata(root)?;
    require(
        meta.is_dir() && meta.permissions().mode() & 0o777 == 0o700,
        "private publication root",
    )
}
fn regular(path: &Path, links: u64) -> Result<File> {
    nonsymlink(path)?;
    require(
        fs::metadata(path)?.is_file(),
        "output must be a regular file",
    )?;
    let file = File::open(path)?;
    let meta = file.metadata()?;
    require(
        meta.is_file() && meta.nlink() <= links && meta.permissions().mode() & 0o777 == 0o600,
        "private output regular file/link count",
    )?;
    Ok(file)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn counted_writes_cap_plus_one_overflow_and_actual_io_failure() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data");
        let file = File::create(&path).unwrap();
        let mut writer = BudgetWriter {
            file,
            limit: 8,
            written: 0,
            hash: blake3::Hasher::new(),
        };
        writer.write_all(b"12345678").unwrap();
        assert_eq!(writer.written, 8);
        assert!(writer.write_all(b"9").is_err());
        assert_eq!(fs::metadata(&path).unwrap().len(), 8);
        writer.written = u64::MAX;
        assert!(writer.write_all(b"1").is_err());
        assert_eq!(writer.written, u64::MAX);
        let mut read_only = BudgetWriter {
            file: File::open(&path).unwrap(),
            limit: 8,
            written: 0,
            hash: blake3::Hasher::new(),
        };
        assert!(read_only.write_all(b"1").is_err());
        assert_eq!(read_only.written, 0);
    }
}
