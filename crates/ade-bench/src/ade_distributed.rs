//! Distributed adapter; the benchmark harness retains independent oracle ownership.
use crate::{
    Error, Result,
    dataset::DatasetManifest,
    engine::{BenchmarkEngine, EngineInfo},
    require,
    spec::{BenchmarkCase, Query},
};
use ade_distributed::{
    TaskConfig,
    coordinator::{LocalCluster, StaticAggregateJob},
    ids::JobId,
};
use arrow::record_batch::RecordBatch;
use std::{
    path::{Path, PathBuf},
    sync::Mutex,
};
#[derive(Debug, Clone)]
pub struct AdeDistributedConfig {
    pub workers: usize,
    pub task: TaskConfig,
}
impl AdeDistributedConfig {
    #[must_use]
    pub fn info(&self) -> EngineInfo {
        let local = crate::ade_local::AdeLocalConfig {
            batch_size: self.task.batch_size,
            execution: self.task.execution(),
        };
        let mut info = local.info();
        info.name = "ade-distributed".into();
        info.configuration
            .insert("workers".into(), self.workers.to_string());
        info.configuration.insert(
            "output_batch_rows".into(),
            self.task.execution().output_batch_rows.to_string(),
        );
        info.configuration.insert("hash_version".into(), "1".into());
        info.configuration.insert(
            "max_frame_bytes".into(),
            ade_distributed::exchange::MAX_FRAME_BYTES.to_string(),
        );
        info.configuration.insert(
            "task_timeout_seconds".into(),
            ade_distributed::TASK_TIMEOUT.as_secs().to_string(),
        );
        info
    }
}
pub struct AdeDistributedEngine {
    config: AdeDistributedConfig,
    files: Vec<PathBuf>,
    cluster: LocalCluster,
    next_job: Mutex<u64>,
}
fn worker_binary() -> Result<PathBuf> {
    if let Some(path) = std::env::var_os("ADE_WORKER_BIN") {
        return Ok(path.into());
    }
    let executable = std::env::current_exe()?;
    let mut directory = executable
        .parent()
        .ok_or_else(|| Error::Invalid("executable directory unavailable".into()))?;
    if directory.file_name().is_some_and(|n| n == "deps") {
        directory = directory
            .parent()
            .ok_or_else(|| Error::Invalid("build directory unavailable".into()))?;
    }
    let binary = directory.join("ade-worker");
    require(
        binary.is_file(),
        "ade-worker missing: cargo build --workspace, or set ADE_WORKER_BIN",
    )?;
    Ok(binary)
}
#[allow(
    clippy::unused_async_trait_impl,
    reason = "Blocking static runtime uses the harness async adapter contract."
)]
impl BenchmarkEngine for AdeDistributedEngine {
    type Config = AdeDistributedConfig;
    fn parse_config(options: &[String]) -> Result<Self::Config> {
        let mut workers = None;
        let mut local = Vec::new();
        for option in options {
            if let Some(value) = option.strip_prefix("workers=") {
                require(
                    workers.is_none(),
                    "duplicate ade-distributed option: workers",
                )?;
                workers = Some(
                    value
                        .parse::<usize>()
                        .map_err(|_| Error::Invalid("invalid workers".into()))?,
                );
            } else {
                local.push(option.clone());
            }
        }
        let workers = workers.unwrap_or(2);
        require((2..=4).contains(&workers), "workers must be 2..4")?;
        let local = crate::ade_local::AdeLocalEngine::parse_config(&local)?;
        Ok(AdeDistributedConfig {
            workers,
            task: TaskConfig {
                batch_size: local.batch_size,
                memory_limit_bytes: local.execution.memory_limit_bytes,
                spill_enabled: local.execution.spill_enabled,
                prefetch_batches: local.execution.prefetch_batches,
            },
        })
    }
    async fn prepare(
        root: &Path,
        manifest: &DatasetManifest,
        case: &BenchmarkCase,
        config: &Self::Config,
    ) -> Result<Self> {
        require(
            case.query == Query::GroupBySum,
            "ade-distributed supports only synthetic GroupBySum in Phase 3",
        )?;
        case.validate()?;
        manifest.validate()?;
        config.task.validate()?;
        require(
            manifest.required_schema == "synthetic-events-v1",
            "ade-distributed requires synthetic-events-v1",
        )?;
        let files: Vec<_> = manifest
            .files
            .iter()
            .map(|f| root.join(&f.relative_path))
            .collect();
        for file in &files {
            let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                std::fs::File::open(file)?,
            )?;
            require(
                reader.schema().as_ref() == crate::dataset::schema().as_ref(),
                "ade-distributed requires the synthetic-events-v1 Arrow schema",
            )?;
        }
        let cluster = LocalCluster::spawn(&worker_binary()?, config.workers)?;
        Ok(Self {
            config: config.clone(),
            files,
            cluster,
            next_job: Mutex::new(0),
        })
    }
    async fn execute(&self) -> Result<Vec<RecordBatch>> {
        let id = {
            let mut next = self
                .next_job
                .lock()
                .map_err(|_| Error::Invalid("job identity lock poisoned".into()))?;
            let id = *next;
            *next = next
                .checked_add(1)
                .ok_or_else(|| Error::Invalid("job identity exhausted".into()))?;
            JobId(id)
        };
        let job = StaticAggregateJob {
            id,
            files: self.files.clone(),
            workers: self.cluster.workers.clone(),
            config: self.config.task.clone(),
            task_timeout: ade_distributed::TASK_TIMEOUT,
        };
        let output = ade_distributed::coordinator::execute(&job)?;
        tracing::debug!(metrics = %serde_json::to_string(&output.metrics)?, "ADE distributed execution");
        Ok(output.batches)
    }
    fn info(&self) -> EngineInfo {
        self.config.info()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn configuration_is_effective_and_strict() {
        let config = AdeDistributedEngine::parse_config(&[
            "workers=4".into(),
            "memory_limit_bytes=1048576".into(),
            "prefetch_batches=1".into(),
        ])
        .unwrap();
        assert_eq!(config.info().configuration["workers"], "4");
        assert_eq!(config.info().configuration["memory_limit_bytes"], "1048576");
        for options in [
            vec!["workers=1"],
            vec!["workers=2", "workers=3"],
            vec!["batch_size=0"],
            vec!["prefetch_batches=65"],
            vec!["unknown=true"],
        ] {
            assert!(
                AdeDistributedEngine::parse_config(
                    &options.into_iter().map(str::to_owned).collect::<Vec<_>>()
                )
                .is_err()
            );
        }
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("data");
        let manifest = crate::dataset::generate(
            &root,
            "phase3-config",
            &crate::dataset::SyntheticConfig {
                rows: 4,
                files: 1,
                ..Default::default()
            },
        )
        .unwrap();
        for name in ["scan", "scan-filter-project", "sort-key-rowid", "tpch-q1"] {
            let case = BenchmarkCase::load(
                &Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join(format!("../../benchmarks/specs/{name}.toml")),
            )
            .unwrap();
            let Err(error) = AdeDistributedEngine::prepare(&root, &manifest, &case, &config).await
            else {
                panic!("unsupported query accepted");
            };
            assert!(error.to_string().contains("only synthetic GroupBySum"));
        }
    }
}
