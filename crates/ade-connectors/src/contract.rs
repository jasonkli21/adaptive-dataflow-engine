use crate::{Error, Result};
use ade_physical::{Schema, Source as PhysicalSource, SourceFile, fingerprint};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

#[derive(Clone)]
pub struct Context {
    deadline: Instant,
    cancelled: Arc<AtomicBool>,
    #[cfg(feature = "batch")]
    pub(crate) native: ade_exec::CancellationToken,
}

impl Context {
    /// Create one deadline and cancellation scope across source and execution.
    ///
    /// # Errors
    /// Rejects zero or overlong deadlines.
    pub fn new(timeout: Duration) -> Result<Self> {
        if timeout.is_zero() || timeout > Duration::from_secs(120) {
            return Err(Error::Limit("timeout must be in (0, 120s]"));
        }
        Ok(Self {
            deadline: Instant::now() + timeout,
            cancelled: Arc::default(),
            #[cfg(feature = "batch")]
            native: ade_exec::CancellationToken::default(),
        })
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        #[cfg(feature = "batch")]
        self.native.cancel();
    }

    /// # Errors
    /// Returns cancellation or deadline failure.
    pub fn check(&self) -> Result<()> {
        if self.cancelled.load(Ordering::Acquire) {
            return Err(Error::Cancelled);
        }
        if Instant::now() >= self.deadline {
            return Err(Error::Deadline);
        }
        Ok(())
    }

    #[must_use]
    pub fn deadline(&self) -> Instant {
        self.deadline
    }
}

/// The SDK's bounded source contract supports whole, pinned Parquet splits,
/// serial extraction, complete replay and no source checkpoint or pushdown.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ConnectorSpecV1 {
    pub version: u32,
    pub kind: String,
    pub namespace: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CatalogEntryV1 {
    pub version: u32,
    pub name: String,
    pub connector: ConnectorSpecV1,
    pub schema: Schema,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PinnedSplit {
    pub key: String,
    pub bytes: u64,
    pub rows: u64,
    pub blake3: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ResolvedSnapshotV1 {
    pub version: u32,
    pub entry: CatalogEntryV1,
    /// Content digest of the snapshot authority (for example, pinned log history).
    pub authority: String,
    pub splits: Vec<PinnedSplit>,
    pub replay: Replay,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Replay {
    Pinned,
    OneShot,
}

/// Capabilities derived from the admitted connector contract.
#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
pub struct SourceCapabilitiesV1 {
    pub bounded: bool,
    pub replay: ReplayCapability,
    pub native_offsets: bool,
    pub checkpoint_restore: bool,
    pub pushdown: &'static str,
    pub max_splits: Option<u8>,
}

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReplayCapability {
    PinnedBytes,
    OneShot,
    TraceRelativeKafka,
}

pub(crate) fn identifier(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 64
        && s.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

pub(crate) fn relative_key(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 256
        && s.split('/').all(|part| {
            !part.is_empty()
                && part != "."
                && part != ".."
                && part
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
        })
}

pub(crate) fn hash(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}

fn digest<T: Serialize>(value: &T) -> Result<String> {
    let bytes = serde_json::to_vec(value).map_err(|_| Error::Protocol("snapshot encoding"))?;
    Ok(fingerprint(&bytes))
}

impl CatalogEntryV1 {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.version != 1
            || self.connector.version != 1
            || !identifier(&self.name)
            || !identifier(&self.connector.kind)
            || !hash(&self.connector.namespace)
        {
            return Err(Error::Admission("catalog entry or connector identity"));
        }
        let mut names = BTreeSet::new();
        if self.schema.is_empty()
            || self.schema.len() > 16
            || !self.schema.iter().all(|field| {
                !field.nullable && identifier(&field.name) && names.insert(field.name.as_str())
            })
        {
            return Err(Error::Admission(
                "catalog field names, count or nullability",
            ));
        }
        Ok(())
    }
}

impl ResolvedSnapshotV1 {
    /// Parse a capped, strict descriptor. The result still needs an independently
    /// trusted expected digest before staging.
    ///
    /// # Errors
    /// Refuses malformed, noncanonical or out-of-contract snapshots.
    pub fn from_json(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > ade_physical::MAX_PLAN_BYTES {
            return Err(Error::Limit("snapshot descriptor bytes"));
        }
        let snapshot: Self =
            serde_json::from_slice(bytes).map_err(|_| Error::Admission("snapshot JSON"))?;
        snapshot.physical()?;
        Ok(snapshot)
    }

    /// # Errors
    /// Refuses malformed or oversized snapshot descriptors.
    pub fn digest(&self) -> Result<String> {
        self.physical().map(|s| s.logical)
    }

    #[must_use]
    pub fn capabilities(&self) -> SourceCapabilitiesV1 {
        let (replay, max_splits) = match self.replay {
            Replay::Pinned => (ReplayCapability::PinnedBytes, 64),
            Replay::OneShot => (ReplayCapability::OneShot, 1),
        };
        SourceCapabilitiesV1 {
            bounded: true,
            replay,
            native_offsets: false,
            checkpoint_restore: false,
            pushdown: "none",
            max_splits: Some(max_splits),
        }
    }

    pub(crate) fn physical(&self) -> Result<PhysicalSource> {
        self.entry.validate()?;
        if self.version != 1
            || !hash(&self.authority)
            || self.splits.len() > 64
            || !self.splits.windows(2).all(|w| w[0].key < w[1].key)
            || !self
                .splits
                .iter()
                .all(|s| relative_key(&s.key) && s.key.ends_with(".parquet") && s.bytes >= 12)
        {
            return Err(Error::Admission(
                "snapshot version, authority or split inventory",
            ));
        }
        let identity = digest(self)?;
        let source = PhysicalSource {
            id: 0,
            table: "input".into(),
            manifest: identity.clone(),
            logical: identity,
            schema_identity: digest(&self.entry.schema)?,
            schema: self.entry.schema.clone(),
            files: self
                .splits
                .iter()
                .enumerate()
                .map(|(i, split)| SourceFile {
                    name: format!("{i:04}.parquet"),
                    bytes: split.bytes,
                    rows: split.rows,
                    fingerprint: split.blake3.clone(),
                })
                .collect(),
            replayable: self.replay == Replay::Pinned,
        };
        source
            .validate()
            .map_err(|_| Error::Admission("native source limits/schema"))?;
        Ok(source)
    }
}

/// Implementors resolve pinned metadata and return one sequential byte stream
/// per split. A reader closes on drop. Implementors must honor `Context` during
/// blocking I/O and enforce transport and decode bounds at their boundary.
pub trait Source {
    /// # Errors
    /// Refuses unsupported or unsafe connector configuration.
    fn validate(&self, spec: &ConnectorSpecV1) -> Result<()>;

    /// # Errors
    /// Refuses missing, mutable, unsupported or over-budget metadata.
    fn resolve(&self, entry: &CatalogEntryV1, context: &Context) -> Result<ResolvedSnapshotV1>;

    /// # Errors
    /// Refuses a missing or unsafe split or failed transport.
    fn open(&self, split: &PinnedSplit, context: &Context) -> Result<Box<dyn Read + '_>>;
}

#[derive(Default)]
pub struct Catalog {
    entries: BTreeMap<String, CatalogEntryV1>,
}

impl Catalog {
    /// # Errors
    /// Refuses invalid, duplicate or excessive entries.
    pub fn register(&mut self, entry: CatalogEntryV1) -> Result<()> {
        entry.validate()?;
        if self.entries.len() == 64 || self.entries.contains_key(&entry.name) {
            return Err(Error::Admission("duplicate or excessive catalog entry"));
        }
        self.entries.insert(entry.name.clone(), entry);
        Ok(())
    }

    /// # Errors
    /// Refuses incompatible sources, changed snapshots or failed extraction.
    pub fn stage(
        &self,
        name: &str,
        source: &dyn Source,
        expected: &str,
        parent: &Path,
        context: &Context,
    ) -> Result<Stage> {
        let started = Instant::now();
        if !hash(expected) {
            return Err(Error::Admission("expected snapshot digest"));
        }
        context.check()?;
        let entry = self
            .entries
            .get(name)
            .ok_or(Error::Admission("unknown catalog entry"))?;
        source.validate(&entry.connector)?;
        let snapshot = source.resolve(entry, context)?;
        let physical = snapshot.physical()?;
        if snapshot.entry != *entry
            || physical.logical != expected
            || snapshot.replay != Replay::Pinned
        {
            return Err(Error::Identity("catalog or snapshot authority"));
        }

        let dir = tempfile::Builder::new()
            .prefix(".ade-stage-")
            .tempdir_in(parent)?;
        let mut files = Vec::with_capacity(snapshot.splits.len());
        for (split, file) in snapshot.splits.iter().zip(&physical.files) {
            context.check()?;
            let mut reader = source.open(split, context)?;
            let path = dir.path().join(&file.name);
            copy_verified(&mut reader, &path, split, context)?;
            files.push(path);
        }
        context.check()?;
        #[cfg(feature = "batch")]
        let normalized = snapshot.entry.connector.kind != "iceberg";
        Ok(Stage {
            #[cfg(feature = "batch")]
            normalized,
            download_bytes: snapshot.splits.iter().map(|split| split.bytes).sum(),
            normalization_write_bytes: 0,
            peak_owned_stage_bytes: snapshot.splits.iter().map(|split| split.bytes).sum(),
            elapsed: started.elapsed(),
            _dir: dir,
            snapshot,
            #[cfg(feature = "batch")]
            physical,
            files,
        })
    }
}

fn copy_verified(
    reader: &mut dyn Read,
    path: &Path,
    split: &PinnedSplit,
    context: &Context,
) -> Result<()> {
    let mut output = File::create_new(path)?;
    let mut hasher = blake3::Hasher::new();
    let mut remaining = split.bytes;
    let mut buf = [0; 8192];
    loop {
        context.check()?;
        let limit = usize::try_from(remaining.saturating_add(1))
            .unwrap_or(usize::MAX)
            .min(buf.len());
        let n = reader.read(&mut buf[..limit])?;
        if n == 0 {
            break;
        }
        remaining = remaining
            .checked_sub(n as u64)
            .ok_or(Error::Identity("oversize split body"))?;
        let data = buf
            .get(..n)
            .ok_or(Error::Protocol("invalid adapter read count"))?;
        hasher.update(data);
        output.write_all(data)?;
    }
    if remaining != 0 || hasher.finalize().to_hex().as_str() != split.blake3 {
        return Err(Error::Identity("split size or content"));
    }
    Ok(())
}

/// Owns the private staged files. Dropping it cleans up only this attempt.
pub struct Stage {
    #[cfg(feature = "batch")]
    pub(crate) normalized: bool,
    pub(crate) elapsed: Duration,
    pub(crate) download_bytes: u64,
    pub(crate) normalization_write_bytes: u64,
    pub(crate) peak_owned_stage_bytes: u64,
    pub(crate) _dir: tempfile::TempDir,
    pub(crate) snapshot: ResolvedSnapshotV1,
    #[cfg(feature = "batch")]
    pub(crate) physical: PhysicalSource,
    pub(crate) files: Vec<PathBuf>,
}

impl Stage {
    #[must_use]
    pub fn snapshot(&self) -> &ResolvedSnapshotV1 {
        &self.snapshot
    }

    #[must_use]
    pub fn staged_bytes(&self) -> u64 {
        self.final_stage_bytes()
    }

    /// Bytes copied from source splits before any format normalization.
    #[must_use]
    pub fn download_bytes(&self) -> u64 {
        self.download_bytes
    }

    /// Bytes written by a format-normalization pass.
    #[must_use]
    pub fn normalization_write_bytes(&self) -> u64 {
        self.normalization_write_bytes
    }

    /// High-water estimate of bytes in owned stage files, excluding filesystem
    /// allocation granularity and directory metadata.
    #[must_use]
    pub fn peak_owned_stage_bytes(&self) -> u64 {
        self.peak_owned_stage_bytes
    }

    fn final_stage_bytes(&self) -> u64 {
        #[cfg(feature = "batch")]
        {
            self.physical.files.iter().map(|file| file.bytes).sum()
        }
        #[cfg(not(feature = "batch"))]
        {
            self.files
                .iter()
                .filter_map(|p| p.metadata().ok())
                .map(|m| m.len())
                .sum()
        }
    }

    #[must_use]
    pub fn stage_elapsed(&self) -> Duration {
        self.elapsed
    }

    #[must_use]
    pub fn file_count(&self) -> usize {
        self.files.len()
    }
}
