//! Deterministic two-relation fixture; generation is independent of ADE join execution.
use crate::{
    Result,
    dataset::{self, DatasetManifest},
    require,
};
use arrow::{
    array::{Int64Array, UInt64Array},
    record_batch::RecordBatch,
};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File},
    path::Path,
    sync::Arc,
};
#[derive(Debug, Clone, Serialize, Deserialize, clap::Args)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[arg(long, default_value_t = 65536)]
    pub left_rows: u64,
    #[arg(long, default_value_t = 128)]
    pub right_rows: u64,
    #[arg(long, default_value_t = 65536)]
    pub left_key_domain: u64,
    #[arg(long, default_value_t = 65536)]
    pub right_key_domain: u64,
    #[arg(long, default_value_t = 4)]
    pub files_per_side: usize,
    #[arg(long, default_value_t = 4096)]
    pub row_group_rows: usize,
    #[arg(long, default_value_t = 42)]
    pub seed: u64,
}
impl Config {
    /// # Errors
    /// Rejects fixture dimensions outside the bounded join contract.
    pub fn validate(&self) -> Result<()> {
        require(
            self.left_rows <= 1_000_000
                && self.right_rows <= 1_000_000
                && self.left_key_domain > 0
                && self.right_key_domain > 0
                && (1..=64).contains(&self.files_per_side)
                && (1..=65536).contains(&self.row_group_rows),
            "join generator bounds",
        )
    }
}
/// # Errors
/// Writes both bounded relations and their versioned, side-specific physical identity; never overwrites data.
pub fn generate(output: &Path, id: &str, c: &Config) -> Result<DatasetManifest> {
    c.validate()?;
    require(!id.is_empty(), "join dataset id")?;
    fs::create_dir(output)?;
    let generated = (|| {
        let mut files = Vec::new();
        let mut logical = blake3::Hasher::new();
        logical.update(b"synthetic-join-v1:left-v1:right-v1");
        logical.update(&serde_json::to_vec(c)?);
        for (side, rows, domain, salt) in [
            ("left", c.left_rows, c.left_key_domain, 0u64),
            ("right", c.right_rows, c.right_key_domain, 0x1234u64),
        ] {
            fs::create_dir(output.join(side))?;
            logical.update(side.as_bytes());
            for f in 0..c.files_per_side {
                let name = format!("{side}/part-{f:05}.parquet");
                let props = WriterProperties::builder()
                    .set_max_row_group_row_count(Some(c.row_group_rows))
                    .build();
                let mut writer = ArrowWriter::try_new(
                    File::create(output.join(&name))?,
                    ade_exec::join::input_schema(),
                    Some(props),
                )?;
                let n = c.files_per_side as u64;
                let mut cursor = rows / n * f as u64 + (f as u64).min(rows % n);
                let end = cursor + rows / n + u64::from((f as u64) < rows % n);
                while cursor < end {
                    let next = (cursor + 1024).min(end);
                    let mut keys = Vec::new();
                    let mut values = Vec::new();
                    for i in cursor..next {
                        let key = mix(i ^ c.seed ^ salt) % domain;
                        let value = (i % 17).cast_signed() - 8;
                        keys.push(key);
                        values.push(value);
                        logical.update(&key.to_le_bytes());
                        logical.update(&value.to_le_bytes());
                    }
                    writer.write(&RecordBatch::try_new(
                        ade_exec::join::input_schema(),
                        vec![
                            Arc::new(UInt64Array::from(keys)),
                            Arc::new(Int64Array::from(values)),
                        ],
                    )?)?;
                    cursor = next;
                }
                writer.close()?;
                files.push(dataset::describe(output, name)?);
            }
        }
        let manifest = DatasetManifest {
            schema_version: 1,
            dataset_id: id.into(),
            dataset_version: 1,
            required_schema: "synthetic-join-v1".into(),
            schema_fingerprint: blake3::hash(
                b"left-v1:key=u64,value_i64=i64;right-v1:key=u64,value_i64=i64;all_nonnull",
            )
            .to_hex()
            .to_string(),
            generator: "synthetic-join".into(),
            generator_version: "1".into(),
            encoding_version: "parquet-59.3.1".into(),
            format: "parquet".into(),
            row_count: c.left_rows + c.right_rows,
            logical_fingerprint: logical.finalize().to_hex().to_string(),
            synthetic: None,
            join: Some(c.clone()),
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
fn mix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn deterministic_sides_empty_and_manifest_separation() {
        let root = tempfile::tempdir().unwrap();
        let config = Config {
            left_rows: 100,
            right_rows: 60,
            left_key_domain: 8,
            right_key_domain: 8,
            files_per_side: 4,
            row_group_rows: 16,
            seed: 42,
        };
        let a = generate(&root.path().join("a"), "join", &config).unwrap();
        let b = generate(&root.path().join("b"), "join", &config).unwrap();
        assert_eq!(a.identity().unwrap(), b.identity().unwrap());
        dataset::inspect(&root.path().join("a"), true).unwrap();
        let mut changed = a.clone();
        changed.files[0].relative_path = "right/renamed.parquet".into();
        assert!(changed.validate().is_err());
        let mut changed = a.clone();
        changed.join.as_mut().unwrap().right_rows += 1;
        assert!(changed.validate().is_err());
        let empty = Config {
            left_rows: 0,
            right_rows: 0,
            ..config
        };
        let e = generate(&root.path().join("empty"), "empty", &empty).unwrap();
        assert_eq!(e.row_count, 0);
        dataset::inspect(&root.path().join("empty"), true).unwrap();
    }
}
