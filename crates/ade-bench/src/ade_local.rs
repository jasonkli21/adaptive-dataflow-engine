//! ADE-owned local adapter; canonical correctness remains in the benchmark harness.
use crate::{
    Error, Result,
    dataset::DatasetManifest,
    engine::{BenchmarkEngine, EngineInfo},
    require,
    spec::{BenchmarkCase, Query},
};
use ade_exec::{CancellationToken, LocalPlan};
use arrow::record_batch::RecordBatch;
use std::{collections::BTreeMap, path::Path};

/// Minimal effective local execution configuration.
#[derive(Debug, Clone)]
pub struct AdeLocalConfig {
    pub batch_size: usize,
}
impl AdeLocalConfig {
    /// Describes the effective local configuration without executing a plan.
    #[must_use]
    pub fn info(&self) -> EngineInfo {
        EngineInfo {
            name: "ade-local".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            configuration: BTreeMap::from([("batch_size".into(), self.batch_size.to_string())]),
        }
    }
}
/// Prepared provisional ADE local plan.
pub struct AdeLocalEngine {
    plan: LocalPlan,
    config: AdeLocalConfig,
}
#[allow(
    clippy::unused_async_trait_impl,
    reason = "Synchronous ADE runtime implements the shared async adapter boundary."
)]
impl BenchmarkEngine for AdeLocalEngine {
    type Config = AdeLocalConfig;
    fn parse_config(options: &[String]) -> Result<Self::Config> {
        let mut config = AdeLocalConfig { batch_size: 8192 };
        for option in options {
            let (key, value) = option
                .split_once('=')
                .ok_or_else(|| Error::Invalid("engine option must be key=value".into()))?;
            require(
                key == "batch_size",
                format!("unsupported ade-local option: {key}"),
            )?;
            config.batch_size = value
                .parse()
                .map_err(|_| Error::Invalid("invalid batch_size".into()))?;
        }
        require(config.batch_size > 0, "batch_size must be positive")?;
        Ok(config)
    }
    async fn prepare(
        root: &Path,
        manifest: &DatasetManifest,
        case: &BenchmarkCase,
        config: &Self::Config,
    ) -> Result<Self> {
        require(
            case.query != Query::TpchQ1,
            "ade-local Phase 1 does not support TPC-H Q1; use --engine datafusion",
        )?;
        case.validate()?;
        manifest.validate()?;
        require(
            manifest.required_schema == "synthetic-events-v1",
            "ade-local requires synthetic-events-v1",
        )?;
        require(config.batch_size > 0, "batch_size must be positive")?;
        let files = manifest
            .files
            .iter()
            .map(|f| root.join(&f.relative_path))
            .collect::<Vec<_>>();
        for file in &files {
            let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                std::fs::File::open(file)?,
            )?;
            require(
                builder.schema().as_ref() == crate::dataset::schema().as_ref(),
                "ade-local requires the synthetic-events-v1 Arrow schema",
            )?;
        }
        let scan = LocalPlan::ParquetScan {
            files,
            batch_size: config.batch_size,
        };
        let project = |input, names: &[&str]| LocalPlan::Project {
            input: Box::new(input),
            columns: names.iter().map(|s| (*s).into()).collect(),
        };
        let plan = match case.query {
            Query::Scan => project(scan, &["row_id", "key", "value_i64"]),
            Query::ScanFilterProject { threshold } => project(
                LocalPlan::Filter {
                    input: Box::new(scan),
                    column: "secondary".into(),
                    threshold,
                },
                &["key", "value_i64"],
            ),
            Query::GroupBySum => LocalPlan::HashAggregate {
                input: Box::new(project(scan, &["key", "value_i64"])),
                key: "key".into(),
                value: "value_i64".into(),
            },
            Query::TpchQ1 => unreachable!("rejected above"),
        };
        Ok(Self {
            plan,
            config: config.clone(),
        })
    }
    async fn execute(&self) -> Result<Vec<RecordBatch>> {
        let output = ade_exec::execute(self.plan.clone(), CancellationToken::default())?;
        tracing::debug!(metrics = ?output.metrics, "ADE local execution");
        Ok(output.batches)
    }
    fn info(&self) -> EngineInfo {
        self.config.info()
    }
}
