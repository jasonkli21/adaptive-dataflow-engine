//! ADE-owned local adapter; canonical correctness remains in the benchmark harness.
use crate::{
    Error, Result,
    dataset::DatasetManifest,
    engine::{BenchmarkEngine, EngineInfo},
    require,
    spec::{BenchmarkCase, Query},
};
use ade_exec::{CancellationToken, ExecutionConfig, LocalPlan};
use arrow::record_batch::RecordBatch;
use std::{collections::BTreeMap, path::Path};

/// Minimal effective local execution configuration.
#[derive(Debug, Clone)]
pub struct AdeLocalConfig {
    pub batch_size: usize,
    pub execution: ExecutionConfig,
}
impl AdeLocalConfig {
    /// Describes the effective local configuration without executing a plan.
    #[must_use]
    pub fn info(&self) -> EngineInfo {
        #[allow(unused_mut, reason = "Only the filter feature adds metadata.")]
        let mut info = EngineInfo {
            name: "ade-local".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            configuration: BTreeMap::from([
                ("batch_size".into(), self.batch_size.to_string()),
                (
                    "memory_limit_bytes".into(),
                    self.execution.memory_limit_bytes.to_string(),
                ),
                (
                    "spill_enabled".into(),
                    self.execution.spill_enabled.to_string(),
                ),
                (
                    "prefetch_batches".into(),
                    self.execution.prefetch_batches.to_string(),
                ),
            ]),
        };
        info
    }
}
/// Prepared provisional ADE local plan.
pub struct AdeLocalEngine {
    plan: LocalPlan,
    config: AdeLocalConfig,
    #[cfg(feature = "filter-kernels-v1")]
    filter: Option<ade_exec::filter_kernel::Request>,
    #[cfg(feature = "filter-kernels-v1")]
    filter_digest: Option<String>,
}

/// Feature-gated adapter configuration for the experimental filter kernels.
/// The legacy [`AdeLocalConfig`] remains externally constructible with its
/// original two fields when this feature is enabled.
#[cfg(feature = "filter-kernels-v1")]
#[derive(Debug, Clone)]
pub struct AdeLocalKernelConfig {
    pub local: AdeLocalConfig,
    filter: crate::local_filter::Options,
}

#[cfg(feature = "filter-kernels-v1")]
impl AdeLocalKernelConfig {
    /// Describes the effective local execution configuration without executing a plan.
    #[must_use]
    pub fn info(&self) -> EngineInfo {
        let mut info = self.local.info();
        if let Some(label) = self.filter.label() {
            info.configuration
                .insert("kernel_policy".into(), label.into());
            if let Some(pin) = &self.filter.digest {
                info.configuration
                    .insert("kernel_profile_blake3".into(), pin.clone());
            }
        }
        info
    }
}

fn parse_local_config(options: &[String]) -> Result<AdeLocalConfig> {
    let mut config = AdeLocalConfig {
        batch_size: 8192,
        execution: ExecutionConfig::default(),
    };
    let mut seen = std::collections::BTreeSet::new();
    for option in options {
        let (key, value) = option
            .split_once('=')
            .ok_or_else(|| Error::Invalid("engine option must be key=value".into()))?;
        require(
            seen.insert(key),
            format!("duplicate ade-local option: {key}"),
        )?;
        match key {
            "batch_size" => {
                config.batch_size = value
                    .parse()
                    .map_err(|_| Error::Invalid("invalid batch_size".into()))?;
            }
            "memory_limit_bytes" => {
                config.execution.memory_limit_bytes = value
                    .parse()
                    .map_err(|_| Error::Invalid("invalid memory_limit_bytes".into()))?;
            }
            "spill_enabled" => {
                config.execution.spill_enabled = value
                    .parse()
                    .map_err(|_| Error::Invalid("invalid spill_enabled".into()))?;
            }
            "prefetch_batches" => {
                config.execution.prefetch_batches = value
                    .parse()
                    .map_err(|_| Error::Invalid("invalid prefetch_batches".into()))?;
            }
            _ => {
                return Err(Error::Invalid(format!(
                    "unsupported ade-local option: {key}"
                )));
            }
        }
    }
    require(config.batch_size > 0, "batch_size must be positive")?;
    config.execution.validate()?;
    Ok(config)
}

#[cfg(feature = "filter-kernels-v1")]
fn parse_kernel_config(options: &[String]) -> Result<AdeLocalKernelConfig> {
    let mut local_options = Vec::new();
    let mut filter = crate::local_filter::Options::default();
    let mut seen = std::collections::BTreeSet::new();
    for option in options {
        let (key, value) = option
            .split_once('=')
            .ok_or_else(|| Error::Invalid("engine option must be key=value".into()))?;
        require(
            seen.insert(key),
            format!("duplicate ade-local option: {key}"),
        )?;
        if matches!(
            key,
            "kernel_policy" | "kernel_profile" | "kernel_profile_blake3"
        ) {
            filter.set(key, value)?;
        } else {
            local_options.push(option.clone());
        }
    }
    let local = parse_local_config(&local_options)?;
    filter.validate()?;
    require(
        filter.label().is_none() || local.batch_size <= ade_exec::filter_kernel::MAX_ROWS,
        "batch_size exceeds native filter batch limit",
    )?;
    Ok(AdeLocalKernelConfig { local, filter })
}

#[cfg(feature = "filter-kernels-v1")]
pub struct AdeLocalKernelEngine(AdeLocalEngine, EngineInfo);
#[allow(
    clippy::unused_async_trait_impl,
    reason = "Synchronous ADE runtime implements the shared async adapter boundary."
)]
impl BenchmarkEngine for AdeLocalEngine {
    type Config = AdeLocalConfig;
    fn parse_config(options: &[String]) -> Result<Self::Config> {
        parse_local_config(options)
    }
    async fn prepare(
        root: &Path,
        manifest: &DatasetManifest,
        case: &BenchmarkCase,
        config: &Self::Config,
    ) -> Result<Self> {
        require(
            case.query != Query::TpchQ1,
            "ade-local does not support TPC-H Q1; use --engine datafusion",
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
            Query::SortKeyRowid => LocalPlan::Sort {
                input: Box::new(project(scan, &["key", "row_id", "value_i64"])),
                key: "key".into(),
                row_id: "row_id".into(),
                value: "value_i64".into(),
            },
            Query::InnerEquiJoin{..} => return Err(Error::Invalid("ade-local join adapter not admitted; use ade-distributed-join or the typed local helper".into())),
            Query::TpchQ1 => unreachable!("rejected above"),
        };
        Ok(Self {
            plan,
            config: config.clone(),
            #[cfg(feature = "filter-kernels-v1")]
            filter: None,
            #[cfg(feature = "filter-kernels-v1")]
            filter_digest: None,
        })
    }
    async fn execute(&self) -> Result<Vec<RecordBatch>> {
        let legacy = || {
            ade_exec::execute_with_config(
                self.plan.clone(),
                self.config.execution.clone(),
                CancellationToken::default(),
            )
        };
        #[cfg(feature = "filter-kernels-v1")]
        let output = if let Some(request) = &self.filter {
            let (output, audit) = ade_exec::execute_with_filter_kernel(
                self.plan.clone(),
                self.config.execution.clone(),
                CancellationToken::default(),
                request.clone(),
            )?;
            eprintln!(
                "phase25-filter-v1 {}",
                serde_json::json!({
                    "audit": audit,
                    "profile_blake3": self.filter_digest,
                    "managed_limit_bytes": output.metrics.managed.limit,
                    "managed_peak_bytes": output.metrics.managed.peak,
                    "managed_retained_bytes": output.metrics.managed.current,
                    "output_rows": output.metrics.output_rows,
                }),
            );
            output
        } else {
            legacy()?
        };
        #[cfg(not(feature = "filter-kernels-v1"))]
        let output = legacy()?;
        tracing::debug!(
            managed_limit = output.metrics.managed.limit,
            managed_current = output.metrics.managed.current,
            managed_peak = output.metrics.managed.peak,
            reservation_attempts = output.metrics.managed.attempts,
            reservation_denials = output.metrics.managed.denials,
            pressure_events = output.metrics.managed.pressure_events,
            spill_files = output.metrics.spill.files,
            spill_events = output.metrics.spill.events,
            spill_write_bytes = output.metrics.spill.write_bytes,
            spill_read_bytes = output.metrics.spill.read_bytes,
            sort_runs = output.metrics.spill.sort_runs,
            sort_merges = output.metrics.spill.sort_merges,
            prefetch_produced = output.metrics.prefetch.produced,
            prefetch_consumed = output.metrics.prefetch.consumed,
            prefetch_queue_peak = output.metrics.prefetch.queue_peak,
            "ADE local execution"
        );
        Ok(output.batches)
    }
    fn info(&self) -> EngineInfo {
        self.config.info()
    }
}

#[cfg(feature = "filter-kernels-v1")]
#[allow(
    clippy::unused_async_trait_impl,
    reason = "Synchronous ADE runtime implements the shared async adapter boundary."
)]
impl BenchmarkEngine for AdeLocalKernelEngine {
    type Config = AdeLocalKernelConfig;

    fn parse_config(options: &[String]) -> Result<Self::Config> {
        parse_kernel_config(options)
    }

    async fn prepare(
        root: &Path,
        manifest: &DatasetManifest,
        case: &BenchmarkCase,
        config: &Self::Config,
    ) -> Result<Self> {
        require(
            config.filter.label().is_none()
                || matches!(case.query, Query::ScanFilterProject { .. }),
            "kernel_policy requires scan_filter_project",
        )?;
        let mut engine = AdeLocalEngine::prepare(root, manifest, case, &config.local).await?;
        engine.filter = config.filter.prepare(
            root,
            manifest,
            case,
            config.local.batch_size,
            &config.local.execution,
        );
        engine.filter_digest.clone_from(&config.filter.digest);
        Ok(Self(engine, config.info()))
    }

    async fn execute(&self) -> Result<Vec<RecordBatch>> {
        self.0.execute().await
    }

    fn info(&self) -> EngineInfo {
        self.1.clone()
    }
}
