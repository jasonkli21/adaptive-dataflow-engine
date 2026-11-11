//! Local Delta reader for pinned JSON history and a simple protocol 1/2 table.
use crate::{
    CatalogEntryV1, ConnectorSpecV1, Context, Error, PinnedSplit, ResolvedSnapshotV1, Result,
    Source,
    contract::{hash, relative_key},
    local,
};
use ade_physical::{Field, Type, fingerprint};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::Read,
    path::Path,
};

const MAX_COMMITS: usize = 64;
const COMMIT_BYTES: u64 = 256 * 1024;
const MAX_ACTIONS: usize = 4096;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct VersionPin {
    pub version: u64,
    /// Ordered hashes of every JSON commit starting at version zero.
    pub commits: Vec<String>,
    /// Complete independently pinned inventory at the requested version.
    pub files: Vec<PinnedSplit>,
}

pub struct Delta {
    root: File,
    pin: VersionPin,
    entry: CatalogEntryV1,
}

impl Delta {
    /// # Errors
    /// Refuses unsupported connector kind, pin, schema or root.
    pub fn new(path: &Path, entry: CatalogEntryV1, pin: VersionPin) -> Result<Self> {
        if entry.connector.kind != "delta"
            || pin.commits.len() > MAX_COMMITS
            || pin.version.checked_add(1) != Some(pin.commits.len() as u64)
            || !pin.commits.iter().all(|digest| hash(digest))
        {
            return Err(Error::Admission("Delta version and contiguous commit pins"));
        }
        snapshot(&entry, &pin)?.digest()?;
        Ok(Self {
            root: local::root(path)?,
            pin,
            entry,
        })
    }
}

fn snapshot(entry: &CatalogEntryV1, pin: &VersionPin) -> Result<ResolvedSnapshotV1> {
    let bytes = serde_json::to_vec(pin).map_err(|_| Error::Protocol("Delta authority encoding"))?;
    Ok(ResolvedSnapshotV1 {
        version: 1,
        entry: entry.clone(),
        authority: fingerprint(&bytes),
        splits: pin.files.clone(),
        replay: crate::Replay::Pinned,
    })
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
enum Action {
    Protocol(Protocol),
    MetaData(Metadata),
    Add(Add),
    Remove(Remove),
    CommitInfo(serde::de::IgnoredAny),
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Protocol {
    min_reader_version: u32,
    min_writer_version: u32,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Metadata {
    id: String,
    name: Option<String>,
    description: Option<String>,
    format: Format,
    schema_string: String,
    partition_columns: Vec<String>,
    configuration: BTreeMap<String, String>,
    created_time: Option<i64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Format {
    provider: String,
    options: BTreeMap<String, String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Add {
    path: String,
    partition_values: BTreeMap<String, Option<String>>,
    size: u64,
    modification_time: i64,
    data_change: bool,
    stats: Option<String>,
    tags: Option<BTreeMap<String, String>>,
    deletion_vector: Option<serde_json::Value>,
    base_row_id: Option<i64>,
    default_row_commit_version: Option<i64>,
    clustering_provider: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Remove {
    path: String,
    deletion_timestamp: Option<i64>,
    data_change: bool,
    extended_file_metadata: Option<bool>,
    partition_values: Option<BTreeMap<String, Option<String>>>,
    size: Option<u64>,
    tags: Option<BTreeMap<String, String>>,
    deletion_vector: Option<serde_json::Value>,
    base_row_id: Option<i64>,
    default_row_commit_version: Option<i64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Struct {
    #[serde(rename = "type")]
    kind: String,
    fields: Vec<Column>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Column {
    name: String,
    #[serde(rename = "type")]
    kind: String,
    nullable: bool,
    metadata: BTreeMap<String, serde_json::Value>,
}

fn check_metadata(metadata: &Metadata, schema: &[Field]) -> Result<()> {
    if metadata.id.is_empty()
        || metadata.id.len() > 128
        || metadata.format.provider != "parquet"
        || !metadata.format.options.is_empty()
        || !metadata.partition_columns.is_empty()
        || !metadata.configuration.is_empty()
        || metadata.created_time.is_some_and(|t| t < 0)
        || metadata.name.as_ref().is_some_and(|n| n.len() > 256)
        || metadata
            .description
            .as_ref()
            .is_some_and(|d| d.len() > 4096)
    {
        return Err(Error::Admission("Delta table metadata"));
    }
    let columns: Struct = serde_json::from_str(&metadata.schema_string)
        .map_err(|_| Error::Protocol("Delta schema"))?;
    if columns.kind != "struct"
        || columns.fields.len() != schema.len()
        || !columns.fields.iter().zip(schema).all(|(external, native)| {
            external.name == native.name
                && !external.nullable
                && external.metadata.is_empty()
                && matches!(
                    (external.kind.as_str(), native.data_type),
                    ("long", Type::I64) | ("double", Type::F64) | ("string", Type::Utf8)
                )
        })
    {
        return Err(Error::Admission("Delta column schema or semantics"));
    }
    Ok(())
}

#[derive(Default)]
struct History {
    protocol: bool,
    metadata: bool,
    active: BTreeMap<String, u64>,
    actions: usize,
}

impl History {
    fn commit(&mut self, bytes: &[u8], initial: bool, schema: &[Field]) -> Result<()> {
        let mut paths = BTreeSet::new();
        if bytes.is_empty() {
            return Err(Error::Protocol("Delta empty commit"));
        }
        for line in bytes
            .strip_suffix(b"\n")
            .unwrap_or(bytes)
            .split(|&byte| byte == b'\n')
        {
            self.actions += 1;
            if self.actions > MAX_ACTIONS {
                return Err(Error::Limit("Delta actions"));
            }
            let action: Action = serde_json::from_slice(line)
                .map_err(|_| Error::Protocol("Delta action or feature"))?;
            match action {
                Action::Protocol(p) => {
                    if !initial
                        || self.protocol
                        || p.min_reader_version != 1
                        || p.min_writer_version != 2
                    {
                        return Err(Error::Admission("Delta protocol or upgrade"));
                    }
                    self.protocol = true;
                }
                Action::MetaData(m) => {
                    if !initial || self.metadata {
                        return Err(Error::Admission("Delta metadata evolution"));
                    }
                    check_metadata(&m, schema)?;
                    self.metadata = true;
                }
                Action::Add(a) => {
                    if !relative_key(&a.path)
                        || !a.path.ends_with(".parquet")
                        || !a.partition_values.is_empty()
                        || a.modification_time < 0
                        || !(12..=ade_physical::MAX_SOURCE_BYTES).contains(&a.size)
                        || !a.data_change
                        || a.stats.is_some_and(|s| s.len() > 64 * 1024)
                        || a.tags.is_some_and(|tags| !tags.is_empty())
                        || a.deletion_vector.is_some()
                        || a.base_row_id.is_some()
                        || a.default_row_commit_version.is_some()
                        || a.clustering_provider.is_some()
                        || self.active.contains_key(&a.path)
                        || !paths.insert(a.path.clone())
                    {
                        return Err(Error::Admission("Delta add semantics"));
                    }
                    self.active.insert(a.path, a.size);
                    if self.active.len() > 64 {
                        return Err(Error::Limit("Delta active files"));
                    }
                }
                Action::Remove(r) => {
                    if !r.data_change
                        || r.tags.is_some_and(|tags| !tags.is_empty())
                        || r.deletion_vector.is_some()
                        || r.base_row_id.is_some()
                        || r.default_row_commit_version.is_some()
                        || r.deletion_timestamp.is_some_and(|t| t < 0)
                        || r.partition_values.is_some_and(|p| !p.is_empty())
                        || (r.extended_file_metadata == Some(true) && r.size.is_none())
                        || !paths.insert(r.path.clone())
                    {
                        return Err(Error::Admission("Delta remove semantics"));
                    }
                    let size = self
                        .active
                        .remove(&r.path)
                        .ok_or(Error::Admission("Delta remove missing add"))?;
                    if r.size.is_some_and(|n| n != size) {
                        return Err(Error::Identity("Delta remove size"));
                    }
                }
                Action::CommitInfo(_) => {}
            }
        }
        if !self.protocol || !self.metadata {
            return Err(Error::Admission(
                "Delta missing version-zero protocol or metadata",
            ));
        }
        Ok(())
    }
}

impl Source for Delta {
    fn validate(&self, spec: &ConnectorSpecV1) -> Result<()> {
        if spec != &self.entry.connector {
            return Err(Error::Identity("Delta connector spec"));
        }
        Ok(())
    }

    fn resolve(&self, entry: &CatalogEntryV1, context: &Context) -> Result<ResolvedSnapshotV1> {
        if entry != &self.entry {
            return Err(Error::Identity("Delta catalog entry"));
        }
        match local::regular_at(&self.root, "_delta_log/_last_checkpoint") {
            Err(Error::Io(std::io::ErrorKind::NotFound)) => {}
            _ => return Err(Error::Admission("Delta checkpoints unsupported")),
        }
        let mut history = History::default();
        for (version, expected) in self.pin.commits.iter().enumerate() {
            context.check()?;
            let file = local::regular_at(&self.root, &format!("_delta_log/{version:020}.json"))?;
            let mut bytes = Vec::new();
            file.take(COMMIT_BYTES + 1).read_to_end(&mut bytes)?;
            if bytes.len() as u64 > COMMIT_BYTES {
                return Err(Error::Limit("Delta commit bytes"));
            }
            if fingerprint(&bytes) != *expected {
                return Err(Error::Identity("Delta commit digest"));
            }
            let bytes =
                std::str::from_utf8(&bytes).map_err(|_| Error::Protocol("Delta commit UTF-8"))?;
            history.commit(bytes.as_bytes(), version == 0, &entry.schema)?;
        }
        if !history
            .active
            .iter()
            .map(|(path, size)| (path.as_str(), *size))
            .eq(self
                .pin
                .files
                .iter()
                .map(|pin| (pin.key.as_str(), pin.bytes)))
        {
            return Err(Error::Identity("Delta active inventory differs from pin"));
        }
        snapshot(entry, &self.pin)
    }

    fn open(&self, split: &PinnedSplit, context: &Context) -> Result<Box<dyn Read + '_>> {
        context.check()?;
        Ok(Box::new(local::regular_at(&self.root, &split.key)?))
    }
}
