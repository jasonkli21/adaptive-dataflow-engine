//! Versioned dataset identity, bounded-batch generation and Parquet inspection.
use crate::{Result, require};
use arrow::{
    array::{
        ArrayRef, Float64Array, Int64Array, StringArray, TimestampMillisecondArray, UInt32Array,
        UInt64Array,
    },
    datatypes::{DataType, Field, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File},
    io::Read,
    path::{Component, Path},
    sync::Arc,
};

/// Versioned generator parameters. Row content is independent of file/batch boundaries.
#[derive(Debug, Clone, Serialize, Deserialize, clap::Args)]
#[serde(deny_unknown_fields)]
pub struct SyntheticConfig {
    #[arg(long, default_value_t = 100_000)]
    pub rows: u64,
    #[arg(long, default_value_t = 2)]
    pub files: usize,
    #[arg(long, default_value_t = 65536)]
    pub row_group_rows: usize,
    #[arg(long, default_value = "snappy")]
    pub compression: String,
    #[arg(long, default_value_t = 100)]
    pub key_cardinality: u64,
    #[arg(long, default_value = "uniform")]
    pub distribution: String,
    #[arg(long, default_value_t = 0.5)]
    pub hot_key_fraction: f64,
    #[arg(long, default_value_t = 1)]
    pub hot_key_count: u64,
    #[arg(long, default_value_t = 16)]
    pub payload_size: usize,
    #[arg(long, default_value_t = 42)]
    pub seed: u64,
}
impl Default for SyntheticConfig {
    fn default() -> Self {
        Self {
            rows: 100_000,
            files: 2,
            row_group_rows: 65536,
            compression: "snappy".into(),
            key_cardinality: 100,
            distribution: "uniform".into(),
            hot_key_fraction: 0.5,
            hot_key_count: 1,
            payload_size: 16,
            seed: 42,
        }
    }
}
impl SyntheticConfig {
    /// Checks versioned dataset or generator invariants.
    ///
    /// # Errors
    /// Returns an error for invalid versions/parameters, inconsistent or missing files, serialization, I/O or Parquet failures.
    pub fn validate(&self) -> Result<()> {
        require(
            self.rows > 0 && self.rows <= 1_000_000_000_000,
            "rows must be in 1..=10^12",
        )?;
        require(
            self.files > 0 && u64::try_from(self.files).is_ok_and(|files| files <= self.rows),
            "files must be in 1..=rows",
        )?;
        require(
            self.row_group_rows > 0 && self.payload_size <= 1_048_576,
            "invalid row group/payload size",
        )?;
        require(self.key_cardinality > 0, "key cardinality must be positive")?;
        require(
            matches!(
                self.distribution.as_str(),
                "uniform" | "hot-key" | "hash-bucket-skew"
            ),
            "distribution must be uniform, hot-key or hash-bucket-skew",
        )?;
        require(
            self.hot_key_fraction.is_finite() && (0.0..=1.0).contains(&self.hot_key_fraction),
            "hot fraction must be in [0,1]",
        )?;
        if self.distribution == "hot-key" {
            require(
                self.hot_key_count > 0 && self.hot_key_count < self.key_cardinality,
                "hot keys must be in 1..cardinality",
            )?;
        }
        if self.distribution == "hash-bucket-skew" {
            require(
                self.key_cardinality <= u64::MAX / 4,
                "hash bucket key domain overflow",
            )?;
        }
        compression(&self.compression)?;
        Ok(())
    }
}

/// Returns the contiguous row interval assigned to one file without overflow.
///
/// Remainder rows are assigned to the first files, so file lengths differ by at most one.
///
/// # Errors
/// Rejects invalid dimensions or indices; checked arithmetic reports any impossible overflow.
pub fn file_range(rows: u64, files: u64, index: u64) -> Result<(u64, u64)> {
    require(
        rows > 0 && files > 0 && files <= rows,
        "files must be in 1..=rows",
    )?;
    require(index < files, "file index must be less than file count")?;
    let base = rows / files;
    let remainder = rows % files;
    let start = base
        .checked_mul(index)
        .and_then(|offset| offset.checked_add(index.min(remainder)))
        .ok_or_else(|| crate::Error::Invalid("file range start overflow".into()))?;
    let length = base + u64::from(index < remainder);
    let end = start
        .checked_add(length)
        .ok_or_else(|| crate::Error::Invalid("file range end overflow".into()))?;
    Ok((start, end))
}
fn compression(s: &str) -> Result<Compression> {
    match s {
        "none" => Ok(Compression::UNCOMPRESSED),
        "snappy" => Ok(Compression::SNAPPY),
        "zstd" => Ok(Compression::ZSTD(ZstdLevel::default())),
        _ => Err(crate::Error::Invalid(
            "compression: use none, snappy, zstd".into(),
        )),
    }
}
/// One file, always named relative to the dataset root.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileManifest {
    pub relative_path: String,
    pub bytes: u64,
    pub rows: u64,
    pub row_groups: usize,
    pub fingerprint: String,
}
/// Dataset schema v1; physical encoding is distinct from logical generator identity.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatasetManifest {
    pub schema_version: u32,
    pub dataset_id: String,
    pub dataset_version: u32,
    pub required_schema: String,
    pub schema_fingerprint: String,
    pub generator: String,
    pub generator_version: String,
    pub encoding_version: String,
    pub format: String,
    pub row_count: u64,
    pub logical_fingerprint: String,
    pub synthetic: Option<SyntheticConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub join: Option<crate::join_dataset::Config>,
    pub scale_factor: Option<f64>,
    pub files: Vec<FileManifest>,
}
impl DatasetManifest {
    /// Checks versioned dataset or generator invariants.
    ///
    /// # Errors
    /// Returns an error for invalid versions/parameters, inconsistent or missing files, serialization, I/O or Parquet failures.
    pub fn validate(&self) -> Result<()> {
        require(
            self.schema_version == 1 && self.dataset_version == 1,
            "unsupported dataset schema/version",
        )?;
        require(
            !self.dataset_id.is_empty() && self.format == "parquet" && !self.files.is_empty(),
            "invalid dataset identity/format/files",
        )?;
        require(
            !self.logical_fingerprint.is_empty() && !self.schema_fingerprint.is_empty(),
            "missing dataset fingerprints",
        )?;
        let mut paths = std::collections::BTreeSet::new();
        for file in &self.files {
            require(
                Path::new(&file.relative_path)
                    .components()
                    .all(|c| matches!(c, Component::Normal(_)))
                    && file.relative_path.ends_with(".parquet"),
                "manifest paths must be relative Parquet paths without traversal",
            )?;
            require(paths.insert(&file.relative_path), "duplicate manifest path")?;
        }
        require(
            self.files
                .iter()
                .try_fold(0_u64, |n, f| n.checked_add(f.rows))
                == Some(self.row_count),
            "manifest row count mismatch",
        )?;
        require(
            self.required_schema == "synthetic-join-v1" || self.join.is_none(),
            "join parameters on nonjoin dataset",
        )?;
        match self.required_schema.as_str() {
            "synthetic-events-v1" => {
                let c = self
                    .synthetic
                    .as_ref()
                    .ok_or_else(|| crate::Error::Invalid("missing generator parameters".into()))?;
                c.validate()?;
                require(
                    c.rows == self.row_count
                        && c.files == self.files.len()
                        && self.generator_version
                            == if c.distribution == "hash-bucket-skew" {
                                "2"
                            } else {
                                "1"
                            },
                    "generator manifest mismatch",
                )?;
            }
            "synthetic-join-v1" => {
                let c = self
                    .join
                    .as_ref()
                    .ok_or_else(|| crate::Error::Invalid("missing join parameters".into()))?;
                c.validate()?;
                require(
                    self.synthetic.is_none()
                        && self.scale_factor.is_none()
                        && self.generator == "synthetic-join"
                        && self.generator_version == "1"
                        && self.files.len() == c.files_per_side * 2,
                    "join manifest generator",
                )?;
                for (side, rows) in [("left", c.left_rows), ("right", c.right_rows)] {
                    let selected: Vec<_> = self
                        .files
                        .iter()
                        .filter(|f| f.relative_path.starts_with(&format!("{side}/")))
                        .collect();
                    require(
                        selected.len() == c.files_per_side
                            && selected.iter().map(|f| f.rows).sum::<u64>() == rows,
                        "join side manifest",
                    )?;
                }
            }
            "tpch-v1" => {
                require(
                    self.synthetic.is_none(),
                    "TPC-H manifest cannot contain synthetic parameters",
                )?;
            }
            _ => return Err(crate::Error::Invalid("unsupported dataset schema".into())),
        }
        Ok(())
    }
    /// Hashes the complete manifest for physical dataset identity.
    ///
    /// # Errors
    /// Returns an error for invalid versions/parameters, inconsistent or missing files, serialization, I/O or Parquet failures.
    pub fn identity(&self) -> Result<String> {
        Ok(blake3::hash(&serde_json::to_vec(self)?)
            .to_hex()
            .to_string())
    }
}
/// Synthetic schema, stable under generator version 1.
#[must_use]
pub fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("row_id", DataType::UInt64, false),
        Field::new("key", DataType::UInt64, false),
        Field::new("secondary", DataType::UInt64, false),
        Field::new("value_i64", DataType::Int64, false),
        Field::new("value_f64", DataType::Float64, false),
        Field::new("category", DataType::UInt32, false),
        Field::new("payload", DataType::Utf8, false),
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ]))
}
fn mix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}
fn inverse_mix(mut x: u64) -> u64 {
    x ^= x >> 31;
    x ^= x >> 62;
    x = x.wrapping_mul(0x3196_42b2_d24d_8ec3);
    x ^= x >> 27;
    x ^= x >> 54;
    x = x.wrapping_mul(0x96de_1b17_3f11_9089);
    x ^= x >> 30;
    x ^= x >> 60;
    x.wrapping_sub(0x9e37_79b9_7f4a_7c15)
}
/// Direct deterministic row oracle: (key, secondary, integer value).
#[must_use]
#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "Validated rows <= 10^12 and finite fraction in [0,1] bound the floor conversion."
)]
pub fn row(c: &SyntheticConfig, id: u64) -> (u64, u64, i64) {
    let x = mix(id ^ c.seed);
    let hot_rows = (c.rows as f64 * c.hot_key_fraction).floor() as u64;
    let key = if c.distribution == "hash-bucket-skew" {
        // Invert the published SplitMix64 permutation: distinct keys deliberately
        // collide into physical hash bucket 0 for BOTH 2- and 4-worker controls.
        // This remains an engine-independent deterministic data fixture.
        inverse_mix(4 * (x % c.key_cardinality))
    } else if c.distribution == "hot-key" {
        if (u128::from(id + 1) * u128::from(hot_rows) / u128::from(c.rows))
            > (u128::from(id) * u128::from(hot_rows) / u128::from(c.rows))
        {
            x % c.hot_key_count
        } else {
            c.hot_key_count + x % (c.key_cardinality - c.hot_key_count)
        }
    } else {
        x % c.key_cardinality
    };
    (
        key,
        mix(x) % 1000,
        (mix(x ^ 0x11) % 2001).cast_signed() - 1000,
    )
}
fn batch(
    c: &SyntheticConfig,
    start: u64,
    end: u64,
    hash: &mut blake3::Hasher,
) -> Result<RecordBatch> {
    let mut ids = Vec::new();
    let mut keys = Vec::new();
    let mut secondary = Vec::new();
    let mut values = Vec::new();
    let mut floats = Vec::new();
    let mut cats = Vec::new();
    let mut payloads = Vec::new();
    let mut times = Vec::new();
    for id in start..end {
        let (key, second, value) = row(c, id);
        let cat = (second % 10) as u32;
        let time = 1_600_000_000_000_i64
            + i64::try_from(id)
                .map_err(|_| crate::Error::Invalid("row id exceeds timestamp range".into()))?;
        let float = f64::from(
            i32::try_from(value)
                .map_err(|_| crate::Error::Invalid("generator value out of range".into()))?,
        ) / 100.;
        let payload = std::iter::repeat_n(char::from(b'a' + (second % 26) as u8), c.payload_size)
            .collect::<String>();
        hash.update(&id.to_le_bytes());
        hash.update(&key.to_le_bytes());
        hash.update(&second.to_le_bytes());
        hash.update(&value.to_le_bytes());
        hash.update(&(float).to_bits().to_le_bytes());
        hash.update(&cat.to_le_bytes());
        hash.update(&(payload.len() as u64).to_le_bytes());
        hash.update(payload.as_bytes());
        hash.update(&time.to_le_bytes());
        ids.push(id);
        keys.push(key);
        secondary.push(second);
        values.push(value);
        floats.push(float);
        cats.push(cat);
        payloads.push(payload);
        times.push(time);
    }
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from(ids)),
        Arc::new(UInt64Array::from(keys)),
        Arc::new(UInt64Array::from(secondary)),
        Arc::new(Int64Array::from(values)),
        Arc::new(Float64Array::from(floats)),
        Arc::new(UInt32Array::from(cats)),
        Arc::new(StringArray::from(payloads)),
        Arc::new(TimestampMillisecondArray::from(times)),
    ];
    Ok(RecordBatch::try_new(schema(), arrays)?)
}
/// Streams a file through BLAKE3 without loading it into memory.
///
/// # Errors
/// Returns an error for invalid versions/parameters, inconsistent or missing files, serialization, I/O or Parquet failures.
pub fn hash_file(path: &Path) -> Result<String> {
    let mut reader = File::open(path)?;
    let mut hash = blake3::Hasher::new();
    let mut buf = vec![0_u8; 65536];
    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hash.update(&buf[..n]);
    }
    Ok(hash.finalize().to_hex().to_string())
}
pub(crate) fn describe(root: &Path, relative_path: String) -> Result<FileManifest> {
    let path = root.join(&relative_path);
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&path)?)?;
    Ok(FileManifest {
        relative_path,
        bytes: path.metadata()?.len(),
        rows: u64::try_from(reader.metadata().file_metadata().num_rows())
            .map_err(|_| crate::Error::Invalid("negative Parquet row count".into()))?,
        row_groups: reader.metadata().num_row_groups(),
        fingerprint: hash_file(&path)?,
    })
}
/// Generate into a new directory; refuse overwriting existing data.
/// Writes deterministic Parquet files and a manifest into a new directory.
///
/// # Errors
/// Returns an error for invalid versions/parameters, inconsistent or missing files, serialization, I/O or Parquet failures.
pub fn generate(output: &Path, dataset_id: &str, c: &SyntheticConfig) -> Result<DatasetManifest> {
    c.validate()?;
    require(!dataset_id.is_empty(), "dataset id cannot be empty")?;
    fs::create_dir(output)?;
    let generated = (|| {
        let mut hash = blake3::Hasher::new();
        let mut files = Vec::new();
        let file_count = u64::try_from(c.files)
            .map_err(|_| crate::Error::Invalid("file count exceeds u64".into()))?;
        for index in 0..c.files {
            let name = format!("part-{index:05}.parquet");
            let props = WriterProperties::builder()
                .set_compression(compression(&c.compression)?)
                .set_max_row_group_row_count(Some(c.row_group_rows))
                .build();
            let mut writer =
                ArrowWriter::try_new(File::create(output.join(&name))?, schema(), Some(props))?;
            let index = u64::try_from(index)
                .map_err(|_| crate::Error::Invalid("file index exceeds u64".into()))?;
            let (start, end) = file_range(c.rows, file_count, index)?;
            let mut cursor = start;
            while cursor < end {
                let next = (cursor + 8192).min(end);
                writer.write(&batch(c, cursor, next, &mut hash)?)?;
                cursor = next;
            }
            writer.close()?;
            files.push(describe(output, name)?);
        }
        let manifest = DatasetManifest {
            schema_version: 1,
            dataset_id: dataset_id.into(),
            dataset_version: 1,
            required_schema: "synthetic-events-v1".into(),
            schema_fingerprint: blake3::hash(b"synthetic-events-v1:row_id=u64,key=u64,secondary=u64,value_i64=i64,value_f64=f64,category=u32,payload=utf8,event_time=timestamp_ms;all_nonnull")
                .to_hex()
                .to_string(),
            generator: "synthetic-events".into(),
            generator_version: if c.distribution == "hash-bucket-skew" {"2"} else {"1"}.into(),
            encoding_version: "parquet-59.3.1".into(),
            format: "parquet".into(),
            row_count: c.rows,
            logical_fingerprint: hash.finalize().to_hex().to_string(),
            synthetic: Some(c.clone()),
            join: None,
            scale_factor: None,
            files,
        };
        manifest.validate()?;
        crate::result::write_atomic(&output.join("manifest.json"), &manifest)?;
        Ok(manifest)
    })();
    if generated.is_err() {
        let _ = fs::remove_dir_all(output);
    }
    generated
}
/// Load and validate file metadata. Full inspection also verifies stored checksums and schemas.
/// Checks manifest and Parquet metadata, and optionally full file checksums.
///
/// # Errors
/// Returns an error for invalid versions/parameters, inconsistent or missing files, serialization, I/O or Parquet failures.
pub fn inspect(root: &Path, full: bool) -> Result<DatasetManifest> {
    let m: DatasetManifest = serde_json::from_reader(File::open(root.join("manifest.json"))?)?;
    m.validate()?;
    for f in &m.files {
        let path = root.join(&f.relative_path);
        require(
            path.canonicalize()?.starts_with(root.canonicalize()?),
            "dataset file escapes root",
        )?;
        require(
            !fs::symlink_metadata(&path)?.file_type().is_symlink(),
            "dataset symlinks are unsupported",
        )?;
        require(
            path.metadata()?.len() == f.bytes,
            "dataset file size changed",
        )?;
        let b = ParquetRecordBatchReaderBuilder::try_new(File::open(&path)?)?;
        require(
            u64::try_from(b.metadata().file_metadata().num_rows())
                .map_err(|_| crate::Error::Invalid("negative Parquet row count".into()))?
                == f.rows
                && b.metadata().num_row_groups() == f.row_groups,
            "Parquet row metadata mismatch",
        )?;
        if m.synthetic.is_some() {
            require(
                b.schema().as_ref() == schema().as_ref(),
                "synthetic schema mismatch",
            )?;
        }
        if m.join.is_some() {
            require(
                b.schema() == &ade_exec::join::input_schema(),
                "join file schema mismatch",
            )?;
        }
        if full {
            require(
                hash_file(&path)? == f.fingerprint,
                "dataset checksum mismatch",
            )?;
        }
    }
    Ok(m)
}
/// Invoke the external generator, register all eight expected TPC-H-derived tables.
/// Generates and registers TPC-H-derived Parquet through tpchgen-cli.
///
/// # Errors
/// Returns an error for invalid versions/parameters, inconsistent or missing files, serialization, I/O or Parquet failures.
pub fn generate_tpch(output: &Path, scale: f64, tool: &Path) -> Result<DatasetManifest> {
    require(
        scale.is_finite() && scale > 0.,
        "scale factor must be positive",
    )?;
    require(!output.exists(), "output already exists")?;
    let version = std::process::Command::new(tool).arg("--version").output()?;
    require(version.status.success(), "tpchgen-cli --version failed")?;
    let generated = std::process::Command::new(tool)
        .args([
            "parquet",
            "--scale-factor",
            &scale.to_string(),
            "--output-dir",
        ])
        .arg(output)
        .arg("--quiet")
        .output()?;
    require(
        generated.status.success(),
        format!(
            "tpchgen-cli generation failed: {}",
            String::from_utf8_lossy(&generated.stderr)
        ),
    )?;
    register_tpch(
        output,
        scale,
        String::from_utf8_lossy(&version.stdout).trim().into(),
    )
}
/// Registers an existing local set of eight generated TPC-H-derived tables.
///
/// # Errors
/// Returns an error for invalid versions/parameters, inconsistent or missing files, serialization, I/O or Parquet failures.
pub fn register_tpch(root: &Path, scale: f64, version: String) -> Result<DatasetManifest> {
    fn walk(root: &Path, dir: &Path, names: &mut Vec<String>) -> Result<()> {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            require(
                !entry.file_type()?.is_symlink(),
                "TPC-H input symlinks are unsupported",
            )?;
            let path = entry.path();
            if path.is_dir() {
                walk(root, &path, names)?;
            } else if path.extension().is_some_and(|x| x == "parquet") {
                names.push(
                    path.strip_prefix(root)
                        .map_err(|e| crate::Error::Invalid(e.to_string()))?
                        .to_string_lossy()
                        .into_owned(),
                );
            }
        }
        Ok(())
    }
    let mut names = Vec::new();
    walk(root, root, &mut names)?;
    names.sort();
    for table in [
        "lineitem", "orders", "customer", "supplier", "part", "partsupp", "nation", "region",
    ] {
        require(
            names.iter().any(|p| {
                Path::new(p).components().any(|c| c.as_os_str() == table)
                    || Path::new(p).file_stem().is_some_and(|s| s == table)
            }),
            format!("missing TPC-H table: {table}"),
        )?;
    }
    let files = names
        .into_iter()
        .map(|p| describe(root, p))
        .collect::<Result<Vec<_>>>()?;
    let mut schemas = std::collections::BTreeSet::new();
    for file in &files {
        let reader =
            ParquetRecordBatchReaderBuilder::try_new(File::open(root.join(&file.relative_path))?)?;
        schemas.insert(format!("{:?}", reader.schema()));
    }
    let schema_fingerprint = blake3::hash(&serde_json::to_vec(&schemas)?)
        .to_hex()
        .to_string();
    let logical = blake3::hash(&serde_json::to_vec(&files)?)
        .to_hex()
        .to_string();
    let m = DatasetManifest {
        schema_version: 1,
        dataset_id: "tpch-derived".into(),
        dataset_version: 1,
        required_schema: "tpch-v1".into(),
        schema_fingerprint,
        generator: "tpchgen-cli".into(),
        generator_version: version,
        encoding_version: "external-parquet".into(),
        format: "parquet".into(),
        row_count: files.iter().map(|f| f.rows).sum(),
        logical_fingerprint: logical,
        synthetic: None,
        join: None,
        scale_factor: Some(scale),
        files,
    };
    m.validate()?;
    crate::result::write_atomic(&root.join("manifest.json"), &m)?;
    Ok(m)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_partition(rows: u64, files: u64) {
        let ranges = (0..files)
            .map(|index| file_range(rows, files, index).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(ranges.first().unwrap().0, 0);
        assert_eq!(ranges.last().unwrap().1, rows);
        for pair in ranges.windows(2) {
            assert_eq!(pair[0].1, pair[1].0);
        }
        let min = ranges.iter().map(|(start, end)| end - start).min().unwrap();
        let max = ranges.iter().map(|(start, end)| end - start).max().unwrap();
        assert!(max - min <= 1);
    }

    #[test]
    fn row_ranges_cover_ordinary_and_degenerate_partitions() {
        assert_eq!(
            (0..3)
                .map(|index| file_range(10, 3, index).unwrap())
                .collect::<Vec<_>>(),
            [(0, 4), (4, 7), (7, 10)]
        );
        assert_partition(10, 3);
        assert_eq!(
            file_range(1_000_000_000_000, 1, 0).unwrap(),
            (0, 1_000_000_000_000)
        );
        assert_partition(10, 10);
        assert_eq!(file_range(10, 10, 9).unwrap(), (9, 10));
    }

    #[test]
    fn row_ranges_are_safe_at_the_accepted_domain_limit() {
        let rows = 1_000_000_000_000;
        let files = rows - 1;
        let second_last = file_range(rows, files, files - 2).unwrap();
        let last = file_range(rows, files, files - 1).unwrap();
        assert_eq!(second_last.1, last.0);
        assert_eq!(last.1, rows);
        assert!(second_last.1 - second_last.0 <= 2);
        assert!(last.1 - last.0 <= 2);
        assert!(file_range(rows, files, files).is_err());
    }
}

#[cfg(test)]
mod topology_fixture_tests {
    use super::*;
    #[test]
    fn engineered_keys_are_distinct_and_in_one_base_hash_bucket() {
        let c = SyntheticConfig {
            distribution: "hash-bucket-skew".into(),
            key_cardinality: 1024,
            ..Default::default()
        };
        for x in [0, 1, 2, u64::MAX, 123_456_789] {
            assert_eq!(mix(inverse_mix(x)), x);
        }
        let mut keys = std::collections::BTreeSet::new();
        let mut shards = std::collections::BTreeSet::new();
        for id in 0..10000 {
            let key = row(&c, id).0;
            assert_eq!(mix(key) % 4, 0);
            keys.insert(key);
            shards.insert(mix(key) % 48);
        }
        assert!(keys.len() > 1000);
        assert_eq!(shards.len(), 12);
    }
}
