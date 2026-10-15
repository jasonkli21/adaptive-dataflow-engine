//! Versioned immutable result artifacts, atomic publication and integrity validation.
use crate::{
    Result,
    dataset::DatasetManifest,
    engine::{EngineInfo, ExecutionOutcome},
    environment::{BuildInfo, EnvironmentInfo, RepositoryInfo},
    require,
    spec::BenchmarkCase,
    stats::SummaryMetrics,
};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File},
    io::Write,
    path::Path,
};

/// Iteration process outcome, including unsuccessful executions.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IterationResult {
    pub index: u32,
    pub warmup: bool,
    pub process_time_ns: u64,
    pub exit_code: Option<i32>,
    pub outcome: Option<ExecutionOutcome>,
    pub error: Option<String>,
    pub cpu_time_ns: Option<u64>,
    pub peak_rss_bytes: Option<u64>,
}
/// Dataset checks performed by the parent before starting isolated children.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum DatasetVerification {
    Metadata,
    Full,
}
/// Run controls are independent from semantic workload specs.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RunConfig {
    pub warmups: u32,
    pub iterations: u32,
    pub cache_policy: String,
    pub log_level: String,
    pub dataset_verification: DatasetVerification,
}
/// Macrobenchmark JSON v1. Missing optional resource metrics mean unavailable, never zero.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BenchmarkResultV1 {
    pub schema_version: u32,
    pub run_id: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub repository: RepositoryInfo,
    pub environment: EnvironmentInfo,
    pub build: BuildInfo,
    pub engine: EngineInfo,
    pub workload: BenchmarkCase,
    pub dataset: DatasetManifest,
    pub dataset_manifest_fingerprint: String,
    pub run_config: RunConfig,
    pub valid: bool,
    pub warmups: Vec<IterationResult>,
    pub iterations: Vec<IterationResult>,
    pub summary: Option<SummaryMetrics>,
}
impl BenchmarkResultV1 {
    /// Loads a result and checks its schema and cross-field integrity.
    ///
    /// # Errors
    /// Returns an error for invalid schema/integrity, JSON serialization, I/O failure or an existing destination.
    pub fn load(path: &Path) -> Result<Self> {
        let value: Self = serde_json::from_reader(File::open(path)?)?;
        value.validate()?;
        Ok(value)
    }
    /// Validate schema plus cross-field integrity, including summaries and mandatory correctness.
    /// Checks schemas, input identity, iteration validity and recomputed statistics.
    ///
    /// # Errors
    /// Returns an error for invalid schema/integrity, JSON serialization, I/O failure or an existing destination.
    pub fn validate(&self) -> Result<()> {
        require(
            self.schema_version == 1,
            "unsupported result schema version",
        )?;
        self.workload.validate()?;
        self.dataset.validate()?;
        require(
            self.dataset_manifest_fingerprint == self.dataset.identity()?,
            "manifest identity mismatch",
        )?;
        require(
            self.dataset.required_schema == self.workload.dataset.required_schema,
            "result workload/input mismatch",
        )?;
        require(
            self.run_config.iterations > 0
                && self.iterations.len() == self.run_config.iterations as usize
                && self.warmups.len() == self.run_config.warmups as usize,
            "iteration count mismatch",
        )?;
        require(
            self.run_config.cache_policy == "warm" && self.run_config.log_level == "warn",
            "unsupported measurement policy",
        )?;
        let mut valid = true;
        let mut fingerprint = None;
        for (warmup, iterations) in [(true, &self.warmups), (false, &self.iterations)] {
            for (i, it) in iterations.iter().enumerate() {
                require(
                    it.index as usize == i && it.warmup == warmup,
                    "iteration index/warmup mismatch",
                )?;
                if let Some(o) = &it.outcome {
                    require(
                        o.schema_version == 1 && o.engine == self.engine,
                        "child schema/engine mismatch",
                    )?;
                    require(
                        !o.result_fingerprint.is_empty(),
                        "missing result fingerprint",
                    )?;
                    if let Some(previous) = fingerprint {
                        require(
                            previous == &o.result_fingerprint,
                            "inconsistent canonical output across children",
                        )?;
                    }
                    fingerprint = Some(&o.result_fingerprint);
                    valid &= o.validated && it.exit_code == Some(0) && it.error.is_none();
                } else {
                    valid = false;
                    require(it.error.is_some(), "failed child lacks diagnostic")?;
                }
            }
        }
        require(
            self.valid == valid,
            "run validity contradicts iteration correctness/status",
        )?;
        let expected = if valid {
            SummaryMetrics::new(
                &self
                    .iterations
                    .iter()
                    .filter_map(|i| i.outcome.as_ref().map(|o| o.query_time_ns))
                    .collect::<Vec<_>>(),
            )
        } else {
            None
        };
        require(self.summary == expected, "result summary mismatch")
    }
}
/// Publish complete JSON by same-directory hard-link (atomic no-clobber), then clean temporary file.
/// File data is synchronized before publication. Existing artifacts are never overwritten.
/// Publishes JSON without replacing an existing destination.
///
/// # Errors
/// Returns an error for invalid schema/integrity, JSON serialization, I/O failure or an existing destination.
pub fn write_atomic(path: &Path, value: &impl Serialize) -> Result<()> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    let mut temp = tempfile::NamedTempFile::new_in(parent)?;
    serde_json::to_writer_pretty(&mut temp, value)?;
    temp.write_all(b"\n")?;
    temp.flush()?;
    temp.as_file().sync_all()?;
    temp.persist_noclobber(path).map_err(|e| e.error)?;
    if let Ok(dir) = File::open(parent) {
        let _ = dir.sync_all();
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn atomic_no_clobber_and_cleanup() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("nested/result.json");
        write_atomic(&p, &vec![1, 2]).unwrap();
        assert!(write_atomic(&p, &vec![3]).is_err());
        assert_eq!(
            serde_json::from_reader::<_, Vec<u32>>(File::open(&p).unwrap()).unwrap(),
            vec![1, 2]
        );
        assert_eq!(fs::read_dir(p.parent().unwrap()).unwrap().count(), 1);
        assert!(write_atomic(d.path(), &1).is_err());
    }
}
