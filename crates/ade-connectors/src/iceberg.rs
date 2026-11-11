//! Pinned local Iceberg format-v1 snapshots, unpartitioned and add-only.
mod avro;
mod parquet_stage;

use crate::{
    CatalogEntryV1, ConnectorSpecV1, Context, Error, PinnedSplit, Replay, ResolvedSnapshotV1,
    Result, Source,
    contract::{hash, relative_key},
    local,
};
use ade_physical::{Field, Type, fingerprint};
use apache_avro::types::Value;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::Read,
    path::{Path, PathBuf},
};

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FilePin {
    pub key: String,
    pub blake3: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotPin {
    pub snapshot_id: i64,
    pub metadata: FilePin,
    /// Exactly the selected manifest list followed by the manifests sorted by key.
    pub manifest_list: FilePin,
    pub manifests: Vec<FilePin>,
    pub files: Vec<PinnedSplit>,
}

pub struct Iceberg {
    root: File,
    location: PathBuf,
    entry: CatalogEntryV1,
    pin: SnapshotPin,
}

impl Iceberg {
    /// # Errors
    /// Refuses an unsupported catalog entry, pin or local root.
    pub fn new(root: &Path, entry: CatalogEntryV1, pin: SnapshotPin) -> Result<Self> {
        if entry.connector.kind != "iceberg"
            || pin.snapshot_id <= 0
            || pin.manifests.is_empty()
            || pin.manifests.len() > 64
            || !pin.manifests.windows(2).all(|p| p[0].key < p[1].key)
            || !std::iter::once(&pin.metadata)
                .chain(std::iter::once(&pin.manifest_list))
                .chain(&pin.manifests)
                .all(|p| relative_key(&p.key) && hash(&p.blake3))
        {
            return Err(Error::Admission("Iceberg metadata inventory"));
        }
        descriptor(&entry, &pin)?.digest()?;
        Ok(Self {
            root: local::root(root)?,
            location: root.canonicalize()?,
            entry,
            pin,
        })
    }

    fn key(&self, value: &str) -> Result<String> {
        let path = file_path(value)?;
        let key = path
            .strip_prefix(&self.location)
            .ok()
            .and_then(Path::to_str)
            .filter(|s| relative_key(s))
            .ok_or(Error::Admission("Iceberg path outside table"))?;
        Ok(key.to_owned())
    }

    fn read(&self, pin: &FilePin, context: &Context) -> Result<Vec<u8>> {
        context.check()?;
        let mut bytes = Vec::new();
        local::regular_at(&self.root, &pin.key)?
            .take(1024 * 1024 + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() > 1024 * 1024 {
            return Err(Error::Limit("Iceberg metadata bytes"));
        }
        if fingerprint(&bytes) != pin.blake3 {
            return Err(Error::Identity("Iceberg metadata digest"));
        }
        Ok(bytes)
    }
}

fn file_path(s: &str) -> Result<PathBuf> {
    if s.len() > 4096 {
        return Err(Error::Admission("Iceberg file path length"));
    }
    if s.starts_with('/') {
        return Ok(PathBuf::from(s));
    }
    let uri = url::Url::parse(s).map_err(|_| Error::Admission("Iceberg file URI"))?;
    if uri.scheme() != "file" || uri.has_host() || uri.query().is_some() || uri.fragment().is_some()
    {
        return Err(Error::Admission("Iceberg local file URI required"));
    }
    uri.to_file_path()
        .map_err(|()| Error::Admission("Iceberg file URI"))
}

fn descriptor(entry: &CatalogEntryV1, pin: &SnapshotPin) -> Result<ResolvedSnapshotV1> {
    let bytes = serde_json::to_vec(pin).map_err(|_| Error::Protocol("Iceberg authority"))?;
    Ok(ResolvedSnapshotV1 {
        version: 1,
        entry: entry.clone(),
        authority: fingerprint(&bytes),
        splits: pin.files.clone(),
        replay: Replay::Pinned,
    })
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
struct Metadata {
    format_version: u32,
    table_uuid: String,
    location: String,
    last_updated_ms: i64,
    last_column_id: i32,
    schema: TableSchema,
    schemas: Option<Vec<TableSchema>>,
    current_schema_id: Option<i32>,
    partition_spec: Vec<serde_json::Value>,
    partition_specs: Option<Vec<PartitionSpec>>,
    default_spec_id: Option<i32>,
    last_partition_id: Option<i32>,
    properties: Option<BTreeMap<String, String>>,
    current_snapshot_id: i64,
    snapshots: Vec<TableSnapshot>,
    snapshot_log: Option<Vec<LogSnapshot>>,
    #[serde(rename = "metadata-log")]
    prior_log: Option<Vec<LogMetadata>>,
    sort_orders: Option<Vec<SortOrder>>,
    default_sort_order_id: Option<i32>,
    refs: Option<BTreeMap<String, SnapshotRef>>,
    statistics: Option<Vec<serde_json::Value>>,
    partition_statistics: Option<Vec<serde_json::Value>>,
}

#[derive(Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
struct TableSchema {
    #[serde(rename = "type")]
    kind: String,
    schema_id: Option<i32>,
    fields: Vec<Column>,
    identifier_field_ids: Option<Vec<i32>>,
}

#[derive(Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Column {
    id: i32,
    name: String,
    required: bool,
    #[serde(rename = "type")]
    kind: String,
    doc: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
struct PartitionSpec {
    spec_id: i32,
    fields: Vec<serde_json::Value>,
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
struct SortOrder {
    order_id: i32,
    fields: Vec<serde_json::Value>,
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
struct LogSnapshot {
    snapshot_id: i64,
    timestamp_ms: i64,
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
struct LogMetadata {
    metadata_file: String,
    timestamp_ms: i64,
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
struct SnapshotRef {
    snapshot_id: i64,
    #[serde(rename = "type")]
    kind: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
struct TableSnapshot {
    snapshot_id: i64,
    parent_snapshot_id: Option<i64>,
    timestamp_ms: i64,
    manifest_list: String,
    schema_id: Option<i32>,
    summary: BTreeMap<String, String>,
}

impl TableSchema {
    fn check(&self, native: &[Field]) -> Result<()> {
        let mut ids = BTreeSet::new();
        if self.kind != "struct"
            || self.schema_id.unwrap_or(0) != 0
            || self
                .identifier_field_ids
                .as_ref()
                .is_some_and(|ids| !ids.is_empty())
            || self.fields.len() != native.len()
            || !self.fields.iter().zip(native).all(|(ice, n)| {
                ice.id > 0
                    && ids.insert(ice.id)
                    && ice.required
                    && ice.name == n.name
                    && ice.doc.as_ref().is_none_or(|doc| doc.len() <= 4096)
                    && matches!(
                        (ice.kind.as_str(), n.data_type),
                        ("long", Type::I64) | ("double", Type::F64) | ("string", Type::Utf8)
                    )
            })
        {
            return Err(Error::Admission("Iceberg primitive schema"));
        }
        Ok(())
    }
}

impl Metadata {
    fn check(&self, ice: &Iceberg) -> Result<()> {
        self.schema.check(&ice.entry.schema)?;
        if self.format_version != 1
            || self.table_uuid.len() != 36
            || self.last_updated_ms < 0
            || file_path(&self.location)? != ice.location
            || self.last_column_id <= 0
            || self
                .schema
                .fields
                .iter()
                .any(|f| f.id > self.last_column_id)
            || self.current_schema_id.unwrap_or(0) != 0
            || self
                .schemas
                .as_ref()
                .is_some_and(|s| s.len() != 1 || s[0] != self.schema)
            || !self.partition_spec.is_empty()
            || self
                .partition_specs
                .as_ref()
                .is_some_and(|s| s.len() != 1 || s[0].spec_id != 0 || !s[0].fields.is_empty())
            || self.default_spec_id.unwrap_or(0) != 0
            || !matches!(self.last_partition_id, None | Some(999))
            || self.properties.as_ref().is_some_and(|p| {
                p.iter().any(|(key, value)| {
                    !matches!(
                        (key.as_str(), value.as_str()),
                        ("write.parquet.compression-codec", "uncompressed")
                            | ("write.avro.compression-codec", "null")
                    )
                })
            })
            || self
                .sort_orders
                .as_ref()
                .is_some_and(|o| o.len() != 1 || o[0].order_id != 0 || !o[0].fields.is_empty())
            || self.default_sort_order_id.unwrap_or(0) != 0
            || self.refs.as_ref().is_some_and(|refs| {
                refs.len() != 1
                    || refs.get("main").is_none_or(|main| {
                        main.kind != "branch" || main.snapshot_id != self.current_snapshot_id
                    })
            })
            || self.statistics.as_ref().is_some_and(|s| !s.is_empty())
            || self
                .partition_statistics
                .as_ref()
                .is_some_and(|s| !s.is_empty())
            || self.snapshots.is_empty()
            || self.snapshots.len() > 64
        {
            return Err(Error::Admission(
                "Iceberg protocol, table location or evolution",
            ));
        }
        if self.snapshot_log.as_ref().is_some_and(|log| {
            log.iter().any(|v| {
                v.timestamp_ms < 0
                    || !self
                        .snapshots
                        .iter()
                        .any(|s| s.snapshot_id == v.snapshot_id)
            })
        }) || self.prior_log.as_ref().is_some_and(|log| {
            log.len() > 64
                || log
                    .iter()
                    .any(|v| v.timestamp_ms < 0 || ice.key(&v.metadata_file).is_err())
        }) {
            return Err(Error::Admission("Iceberg metadata history"));
        }
        Ok(())
    }

    fn chain(&self) -> Result<Vec<i64>> {
        let mut ids = Vec::new();
        let mut parent = None;
        let mut time = 0;
        for snapshot in &self.snapshots {
            if snapshot.snapshot_id <= 0
                || ids.contains(&snapshot.snapshot_id)
                || snapshot.parent_snapshot_id != parent
                || snapshot.timestamp_ms < time
                || snapshot.schema_id.unwrap_or(0) != 0
                || snapshot.summary.get("operation").map(String::as_str) != Some("append")
            {
                return Err(Error::Admission("Iceberg append-only snapshot ancestry"));
            }
            ids.push(snapshot.snapshot_id);
            parent = Some(snapshot.snapshot_id);
            time = snapshot.timestamp_ms;
        }
        if parent != Some(self.current_snapshot_id) {
            return Err(Error::Identity("Iceberg current snapshot"));
        }
        Ok(ids)
    }
}

type Record = BTreeMap<String, Value>;

fn record(value: Value, allowed: &[&str]) -> Result<Record> {
    let Value::Record(fields) = value else {
        return Err(Error::Protocol("Iceberg Avro record"));
    };
    let mut map = BTreeMap::new();
    for (field, mut value) in fields {
        if let Value::Union(_, inner) = value {
            value = *inner;
        }
        if !allowed.contains(&field.as_str()) || map.insert(field, value).is_some() {
            return Err(Error::Admission(
                "unknown or duplicate Iceberg manifest field",
            ));
        }
    }
    Ok(map)
}

fn number(map: &Record, key: &str) -> Result<i64> {
    match map.get(key) {
        Some(Value::Long(n)) => Ok(*n),
        Some(Value::Int(n)) => Ok(i64::from(*n)),
        _ => Err(Error::Protocol("Iceberg numeric field")),
    }
}

fn text<'a>(map: &'a Record, key: &str) -> Result<&'a str> {
    match map.get(key) {
        Some(Value::String(s)) => Ok(s),
        _ => Err(Error::Protocol("Iceberg text field")),
    }
}

fn absent(map: &Record, key: &str) -> bool {
    matches!(map.get(key), None | Some(Value::Null))
}

impl Iceberg {
    #[allow(
        clippy::too_many_lines,
        reason = "Verify the pinned snapshot and manifest inventory in one pass."
    )]
    fn resolve_inventory(&self, context: &Context) -> Result<()> {
        let bytes = self.read(&self.pin.metadata, context)?;
        let metadata: Metadata = serde_json::from_slice(&bytes)
            .map_err(|_| Error::Protocol("Iceberg table metadata"))?;
        metadata.check(self)?;
        let ids = metadata.chain()?;
        let cut = ids
            .iter()
            .position(|id| *id == self.pin.snapshot_id)
            .ok_or(Error::Identity("Iceberg pinned snapshot missing"))?;
        if self.key(&metadata.snapshots[cut].manifest_list)? != self.pin.manifest_list.key {
            return Err(Error::Identity("Iceberg manifest list identity"));
        }
        let bytes = self.read(&self.pin.manifest_list, context)?;
        let (header, manifests) = avro::read(&bytes, context)?;
        let cut_id = self.pin.snapshot_id.to_string();
        let parent = metadata.snapshots[cut]
            .parent_snapshot_id
            .map_or_else(|| "null".into(), |id| id.to_string());
        if header.keys().any(|k| {
            ![
                "avro.codec",
                "snapshot-id",
                "parent-snapshot-id",
                "format-version",
            ]
            .contains(&k.as_str())
        }) || header.get("format-version").is_some_and(|v| v != b"1")
            || header
                .get("snapshot-id")
                .is_some_and(|v| v != cut_id.as_bytes())
            || header
                .get("parent-snapshot-id")
                .is_some_and(|v| v != parent.as_bytes())
        {
            return Err(Error::Identity("Iceberg manifest list header"));
        }
        if manifests.len() != self.pin.manifests.len() {
            return Err(Error::Identity("Iceberg manifest count"));
        }
        let mut seen = BTreeSet::new();
        let mut active = BTreeMap::new();
        for value in manifests {
            let manifest = record(
                value,
                &[
                    "manifest_path",
                    "manifest_length",
                    "partition_spec_id",
                    "added_snapshot_id",
                    "added_files_count",
                    "existing_files_count",
                    "deleted_files_count",
                    "partitions",
                    "key_metadata",
                    "added_rows_count",
                    "existing_rows_count",
                    "deleted_rows_count",
                ],
            )?;
            let key = self.key(text(&manifest, "manifest_path")?)?;
            if !seen.insert(key.clone())
                || !absent(&manifest, "key_metadata")
                || number(&manifest, "partition_spec_id")? != 0
                || !ids[..=cut].contains(&number(&manifest, "added_snapshot_id")?)
                || (!absent(&manifest, "partitions")
                    && manifest.get("partitions") != Some(&Value::Array(vec![])))
                || (!absent(&manifest, "deleted_files_count")
                    && number(&manifest, "deleted_files_count")? != 0)
                || (!absent(&manifest, "deleted_rows_count")
                    && number(&manifest, "deleted_rows_count")? != 0)
            {
                return Err(Error::Admission("Iceberg manifest list semantics"));
            }
            let pin = self
                .pin
                .manifests
                .binary_search_by(|p| p.key.cmp(&key))
                .ok()
                .map(|i| &self.pin.manifests[i])
                .ok_or(Error::Identity("Iceberg manifest pin missing"))?;
            let bytes = self.read(pin, context)?;
            if usize::try_from(number(&manifest, "manifest_length")?).ok() != Some(bytes.len()) {
                return Err(Error::Identity("Iceberg manifest length"));
            }
            let (header, entries) = avro::read(&bytes, context)?;
            check_manifest_header(&header, &metadata.schema)?;
            let counts = self.manifest(entries, &ids[..=cut], &mut active)?;
            for (field, expected) in [
                "existing_files_count",
                "added_files_count",
                "existing_rows_count",
                "added_rows_count",
            ]
            .into_iter()
            .zip(counts)
            {
                if !absent(&manifest, field) && number(&manifest, field)? != expected {
                    return Err(Error::Identity("Iceberg manifest count mismatch"));
                }
            }
        }
        if !active
            .iter()
            .map(|(k, (bytes, rows))| (k.as_str(), *bytes, *rows))
            .eq(self
                .pin
                .files
                .iter()
                .map(|p| (p.key.as_str(), p.bytes, p.rows)))
        {
            return Err(Error::Identity("Iceberg active inventory"));
        }
        Ok(())
    }

    fn manifest(
        &self,
        entries: Vec<Value>,
        ancestors: &[i64],
        active: &mut BTreeMap<String, (u64, u64)>,
    ) -> Result<[i64; 4]> {
        let mut counts = [0i64; 4];
        for value in entries {
            let mut entry = record(value, &["status", "snapshot_id", "data_file"])?;
            let status = usize::try_from(number(&entry, "status")?)
                .ok()
                .filter(|&n| n < 2)
                .ok_or(Error::Admission("Iceberg deleted or unknown entry"))?;
            if !ancestors.contains(&number(&entry, "snapshot_id")?) {
                return Err(Error::Admission("Iceberg entry snapshot ancestry"));
            }
            let file = record(
                entry
                    .remove("data_file")
                    .ok_or(Error::Protocol("Iceberg entry data file"))?,
                &[
                    "file_path",
                    "file_format",
                    "partition",
                    "record_count",
                    "file_size_in_bytes",
                    "block_size_in_bytes",
                    "column_sizes",
                    "value_counts",
                    "null_value_counts",
                    "nan_value_counts",
                    "lower_bounds",
                    "upper_bounds",
                    "key_metadata",
                    "split_offsets",
                    "sort_order_id",
                ],
            )?;
            if text(&file, "file_format")? != "PARQUET"
                || file.get("partition") != Some(&Value::Record(vec![]))
                || !absent(&file, "key_metadata")
                || (!absent(&file, "sort_order_id") && number(&file, "sort_order_id")? != 0)
            {
                return Err(Error::Admission("Iceberg data file semantics"));
            }
            let key = self.key(text(&file, "file_path")?)?;
            let bytes = number(&file, "file_size_in_bytes")?;
            let rows = number(&file, "record_count")?;
            if bytes < 12
                || rows < 0
                || active.len() == 64
                || active
                    .insert(key, (bytes.unsigned_abs(), rows.unsigned_abs()))
                    .is_some()
            {
                return Err(Error::Admission("Iceberg data file limits or duplicate"));
            }
            counts[status] += 1;
            counts[status + 2] = counts[status + 2]
                .checked_add(rows)
                .ok_or(Error::Limit("Iceberg rows"))?;
        }
        Ok(counts)
    }
}

fn check_manifest_header(header: &BTreeMap<String, Vec<u8>>, schema: &TableSchema) -> Result<()> {
    if header.keys().any(|k| {
        ![
            "avro.codec",
            "schema",
            "partition-spec",
            "partition-spec-id",
            "format-version",
            "content",
        ]
        .contains(&k.as_str())
    }) || header
        .get("partition-spec")
        .is_none_or(|v| serde_json::from_slice::<Vec<serde_json::Value>>(v).ok() != Some(vec![]))
        || header.get("partition-spec-id").is_some_and(|v| v != b"0")
        || header.get("format-version").is_some_and(|v| v != b"1")
        || header.get("content").is_some_and(|v| v != b"data")
        || header
            .get("schema")
            .and_then(|b| serde_json::from_slice::<TableSchema>(b).ok())
            .as_ref()
            != Some(schema)
    {
        return Err(Error::Admission(
            "Iceberg manifest header schema or partition spec",
        ));
    }
    Ok(())
}

impl Source for Iceberg {
    fn validate(&self, spec: &ConnectorSpecV1) -> Result<()> {
        if spec != &self.entry.connector {
            return Err(Error::Identity("Iceberg connector spec"));
        }
        Ok(())
    }

    fn resolve(&self, entry: &CatalogEntryV1, context: &Context) -> Result<ResolvedSnapshotV1> {
        if entry != &self.entry {
            return Err(Error::Identity("Iceberg catalog entry"));
        }
        self.resolve_inventory(context)?;
        descriptor(entry, &self.pin)
    }

    fn open(&self, split: &PinnedSplit, context: &Context) -> Result<Box<dyn Read + '_>> {
        context.check()?;
        Ok(Box::new(local::regular_at(&self.root, &split.key)?))
    }
}
