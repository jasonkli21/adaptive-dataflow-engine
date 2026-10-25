//! Distributed adapter; the benchmark harness retains independent oracle ownership.
use crate::{
    Error, Result,
    dataset::DatasetManifest,
    engine::{BenchmarkEngine, EngineInfo},
    require,
    spec::{BenchmarkCase, Query},
};
use ade_distributed::{
    ExchangeConfig, ExchangeMode, TaskConfig,
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
    pub recovery_enabled: bool,
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
        add_partial_info(&mut info, &self.task.partial);
        add_topology_info(&mut info, &self.task.topology);
        info.name = "ade-distributed".into();
        info.configuration
            .insert("workers".into(), self.workers.to_string());
        info.configuration.insert(
            "output_batch_rows".into(),
            self.task.execution().output_batch_rows.to_string(),
        );
        info.configuration.insert(
            "exchange_mode".into(),
            self.task.exchange.mode.as_str().into(),
        );
        info.configuration.insert(
            "exchange_buffer_bytes".into(),
            self.task.exchange.buffer_bytes.to_string(),
        );
        if matches!(
            self.task.exchange.mode,
            ExchangeMode::ObjectStoreMaterialized | ExchangeMode::Adaptive
        ) {
            info.configuration.insert(
                "object_store_backend".into(),
                "local_filesystem_ephemeral".into(),
            );
        }
        if self.task.exchange.mode == ExchangeMode::Adaptive {
            info.configuration.insert(
                "adaptive_policy".into(),
                if self.task.exchange.cost.is_some() {
                    "expected_cost_v1"
                } else {
                    "threshold_v1"
                }
                .into(),
            );
            if let Some(cost) = &self.task.exchange.cost {
                add_cost_info(&mut info, cost);
            }
            info.configuration.insert(
                "adaptive_buffer_high_pct".into(),
                self.task.exchange.adaptive.buffer_high_pct.to_string(),
            );
            info.configuration.insert(
                "adaptive_remote_after_bytes".into(),
                self.task.exchange.adaptive.remote_after_bytes.to_string(),
            );
        }
        info.configuration.insert(
            "research_slow_partition".into(),
            self.task
                .exchange
                .pressure
                .target_partition
                .map_or_else(|| "off".into(), |p| p.to_string()),
        );
        info.configuration.insert(
            "research_delay_after_segments".into(),
            self.task.exchange.pressure.after_segments.to_string(),
        );
        info.configuration.insert(
            "research_delay_ms".into(),
            self.task.exchange.pressure.delay_ms.to_string(),
        );
        info.configuration
            .insert("recovery_enabled".into(), self.recovery_enabled.to_string());
        info.configuration
            .insert("recovery_policy".into(), "single_final_loss_v1".into());
        info.configuration.insert(
            "research_exit_final_partition".into(),
            self.task
                .research_exit_final_partition
                .map_or_else(|| "off".into(), |p| p.to_string()),
        );
        info.configuration
            .insert("exchange_wire_version".into(), "2".into());
        info.configuration.insert("hash_version".into(), "1".into());
        info.configuration
            .insert("control_protocol".into(), "ADETCP11".into());
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
fn add_partial_info(info: &mut EngineInfo, partial: &ade_exec::partial::Config) {
    info.configuration.insert(
        "aggregate_policy".into(),
        match partial.mode {
            ade_exec::partial::Mode::FixedHash => "fixed_hash",
            ade_exec::partial::Mode::AdaptiveV1 => "adaptive_v1",
            ade_exec::partial::Mode::RawPartial => "raw_partial",
        }
        .into(),
    );
    info.configuration.insert(
        "aggregate_sample_rows".into(),
        partial.sample_rows.to_string(),
    );
    info.configuration.insert(
        "aggregate_min_reduction_percent".into(),
        partial.min_reduction_percent.to_string(),
    );
}
fn add_topology_info(info: &mut EngineInfo, topology: &ade_distributed::topology::TopologyConfig) {
    if let Ok(serde_json::Value::Object(values)) = serde_json::to_value(topology) {
        for (key, value) in values {
            info.configuration.insert(
                format!("partition_{key}"),
                value
                    .as_str()
                    .map_or_else(|| value.to_string(), str::to_owned),
            );
        }
    }
}
fn add_cost_info(info: &mut EngineInfo, cost: &ade_distributed::cost::CostConfig) {
    for (key, value) in serde_json::to_value(cost)
        .expect("cost config")
        .as_object()
        .expect("object")
    {
        info.configuration.insert(
            format!("cost_{key}"),
            value
                .as_str()
                .map_or_else(|| value.to_string(), str::to_owned),
        );
    }
    info.configuration
        .insert("object_store_backend".into(), "shared_local_fixture".into());
    info.configuration.insert(
        "materialization_failure_domain".into(),
        "worker_process_surviving".into(),
    );
}
pub struct AdeDistributedEngine {
    config: AdeDistributedConfig,
    files: Vec<PathBuf>,
    snapshot: ade_distributed::lineage::SourceSnapshot,
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
    #[allow(
        clippy::too_many_lines,
        reason = "Single strict adapter option parser keeps duplicate and mode checks together."
    )]
    fn parse_config(options: &[String]) -> Result<Self::Config> {
        let mut topology_values = serde_json::Map::new();
        let mut partial_values = serde_json::Map::new();
        let mut workers = None;
        let mut exchange_mode = None;
        let mut exchange_buffer = None;
        let mut special = std::collections::BTreeMap::new();
        let mut local = Vec::new();
        let mut exit_final = None;
        let mut recovery_enabled = true;
        for option in options {
            if option.starts_with("aggregate_") {
                let (key, value) = option
                    .split_once('=')
                    .ok_or_else(|| Error::Invalid("aggregate option requires value".into()))?;
                let field = match key {
                    "aggregate_policy" => "mode",
                    "aggregate_sample_rows" => "sample_rows",
                    "aggregate_min_reduction_percent" => "min_reduction_percent",
                    _ => return Err(Error::Invalid("unsupported aggregate option".into())),
                };
                let value = if field == "mode" {
                    serde_json::Value::String(value.into())
                } else {
                    serde_json::Value::from(
                        value
                            .parse::<u64>()
                            .map_err(|_| Error::Invalid("invalid aggregate number".into()))?,
                    )
                };
                require(
                    partial_values.insert(field.into(), value).is_none(),
                    "duplicate aggregate option",
                )?;
                continue;
            }

            if option.starts_with("partition_") {
                let (key, value) = option
                    .split_once('=')
                    .ok_or_else(|| Error::Invalid("topology option requires value".into()))?;
                let field = key.strip_prefix("partition_").expect("prefix");
                let v = if field == "policy" {
                    serde_json::Value::String(value.to_owned())
                } else {
                    serde_json::Value::from(
                        value
                            .parse::<u64>()
                            .map_err(|_| Error::Invalid("invalid topology number".into()))?,
                    )
                };
                require(
                    topology_values.insert(field.to_owned(), v).is_none(),
                    "duplicate topology option",
                )?;
            } else if let Some(value) = option.strip_prefix("workers=") {
                require(
                    workers.is_none(),
                    "duplicate ade-distributed option: workers",
                )?;
                workers = Some(
                    value
                        .parse::<usize>()
                        .map_err(|_| Error::Invalid("invalid workers".into()))?,
                );
            } else if let Some(value) = option.strip_prefix("exchange_mode=") {
                require(exchange_mode.is_none(), "duplicate exchange_mode")?;
                exchange_mode = Some(match value {
                    "direct" => ExchangeMode::Direct,
                    "adaptive" => ExchangeMode::Adaptive,
                    "buffered" => ExchangeMode::Buffered,
                    "spill" => ExchangeMode::Spill,
                    "local_materialized" => ExchangeMode::LocalMaterialized,
                    "object_store" => ExchangeMode::ObjectStoreMaterialized,
                    _ => return Err(Error::Invalid("invalid exchange_mode".into())),
                });
            } else if let Some(value) = option.strip_prefix("exchange_buffer_bytes=") {
                require(exchange_buffer.is_none(), "duplicate exchange_buffer_bytes")?;
                exchange_buffer = Some(
                    value
                        .parse::<usize>()
                        .map_err(|_| Error::Invalid("invalid exchange_buffer_bytes".into()))?,
                );
            } else if option.starts_with("adaptive_")
                || option.starts_with("research_")
                || option.starts_with("recovery_")
                || option.starts_with("cost_")
            {
                let (key, value) = option
                    .split_once('=')
                    .ok_or_else(|| Error::Invalid("option requires value".into()))?;
                require(
                    key.starts_with("cost_")
                        || matches!(
                            key,
                            "recovery_enabled"
                                | "adaptive_policy"
                                | "adaptive_buffer_high_pct"
                                | "adaptive_remote_after_bytes"
                                | "research_slow_partition"
                                | "research_delay_after_segments"
                                | "research_delay_ms"
                                | "research_exit_final_partition"
                        ),
                    "unknown adaptive/research option",
                )?;
                require(
                    special.insert(key.to_owned(), value.to_owned()).is_none(),
                    "duplicate adaptive/research option",
                )?;
            } else {
                local.push(option.clone());
            }
        }
        require(
            topology_values.keys().all(|k| k == "policy")
                || topology_values
                    .get("policy")
                    .and_then(serde_json::Value::as_str)
                    == Some("sampled_v1"),
            "topology tuning requires sampled_v1",
        )?;
        let topology: ade_distributed::topology::TopologyConfig =
            serde_json::from_value(serde_json::Value::Object(topology_values))?;
        topology.validate()?;
        let workers = workers.unwrap_or(2);
        require((2..=4).contains(&workers), "workers must be 2..4")?;
        let local = crate::ade_local::AdeLocalEngine::parse_config(&local)?;
        let mut exchange = ExchangeConfig {
            mode: exchange_mode.unwrap_or_default(),
            buffer_bytes: exchange_buffer.unwrap_or(ExchangeConfig::default().buffer_bytes),
            ..Default::default()
        };
        require(
            exchange.mode == ExchangeMode::Adaptive
                || !special.keys().any(|k| k.starts_with("adaptive_")),
            "adaptive options require adaptive mode",
        )?;
        let expected = special
            .get("adaptive_policy")
            .is_some_and(|v| v == "expected_cost_v1");
        require(
            expected || !special.keys().any(|k| k.starts_with("cost_")),
            "cost options require expected_cost_v1",
        )?;
        require(
            !expected
                || special
                    .get("adaptive_remote_after_bytes")
                    .is_none_or(|v| v == "0"),
            "expected cost requires delivery remote escalation disabled",
        )?;
        let mut cost_values = serde_json::Map::new();
        for (key, value) in special {
            let number = || {
                value
                    .parse::<u64>()
                    .map_err(|_| Error::Invalid(format!("invalid {key}")))
            };
            match key.as_str() {
                "recovery_enabled" => {
                    recovery_enabled = value
                        .parse::<bool>()
                        .map_err(|_| Error::Invalid("invalid recovery_enabled".into()))?;
                }
                "adaptive_policy" => require(
                    value == "threshold_v1" || value == "expected_cost_v1",
                    "invalid adaptive_policy",
                )?,
                "adaptive_buffer_high_pct" => {
                    exchange.adaptive.buffer_high_pct = u8::try_from(number()?)
                        .map_err(|_| Error::Invalid("invalid adaptive percentage".into()))?;
                }
                "adaptive_remote_after_bytes" => exchange.adaptive.remote_after_bytes = number()?,
                "research_slow_partition" => {
                    let partition = number()?;
                    require(
                        partition < workers as u64,
                        "research partition outside topology",
                    )?;
                    exchange.pressure.target_partition = Some(partition);
                }
                "research_delay_after_segments" => exchange.pressure.after_segments = number()?,
                "research_delay_ms" => exchange.pressure.delay_ms = number()?,
                "research_exit_final_partition" => {
                    let p = number()?;
                    require(p < workers as u64, "failure target outside topology")?;
                    exit_final = Some(p);
                }
                key if key.starts_with("cost_") => {
                    let field = key.strip_prefix("cost_").expect("prefix");
                    let v = if field == "mode" {
                        serde_json::Value::String(value.clone())
                    } else {
                        serde_json::Value::from(number()?)
                    };
                    cost_values.insert(field.to_owned(), v);
                }
                _ => unreachable!(),
            }
        }
        if expected {
            exchange.cost = Some(serde_json::from_value(serde_json::Value::Object(
                cost_values,
            ))?);
            exchange.adaptive.remote_after_bytes = 0;
            exchange.object_store = ade_distributed::ObjectStoreConfig {
                backend: "shared_local_fixture".into(),
                durability: ade_distributed::ObjectStoreDurability::WorkerProcessSurviving,
            };
            require(recovery_enabled, "expected cost requires recovery_enabled")?;
        }
        let partial_tuned = partial_values.keys().any(|k| k != "mode");
        let partial: ade_exec::partial::Config =
            serde_json::from_value(serde_json::Value::Object(partial_values))?;
        partial.validate()?;
        require(
            !partial_tuned || partial.mode != ade_exec::partial::Mode::FixedHash,
            "aggregate tuning requires non-fixed policy",
        )?;
        require(
            partial.mode == ade_exec::partial::Mode::FixedHash || !recovery_enabled,
            "non-fixed partial recovery is not admitted; set recovery_enabled=false",
        )?;
        exchange.validate()?;
        Ok(AdeDistributedConfig {
            workers,
            recovery_enabled,
            task: TaskConfig {
                partial,
                topology,
                exchange,
                batch_size: local.batch_size,
                memory_limit_bytes: local.execution.memory_limit_bytes,
                spill_enabled: local.execution.spill_enabled,
                prefetch_batches: local.execution.prefetch_batches,
                research_exit_final_partition: exit_final,
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
            "ade-distributed supports only synthetic GroupBySum",
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
        let snapshot = ade_distributed::lineage::SourceSnapshot {
            version: 1,
            manifest_fingerprint: blake3::hash(&serde_json::to_vec(manifest)?)
                .to_hex()
                .to_string(),
            schema_fingerprint: manifest.schema_fingerprint.clone(),
            logical_fingerprint: manifest.logical_fingerprint.clone(),
            replayable: true,
            morsels: manifest
                .files
                .iter()
                .enumerate()
                .map(|(i, f)| ade_distributed::lineage::SourceMorsel {
                    id: ade_distributed::ids::MorselId(i as u64),
                    path: root.join(&f.relative_path),
                    fingerprint: f.fingerprint.clone(),
                    bytes: f.bytes,
                })
                .collect(),
        };
        snapshot.validate(&files)?;
        let cluster = if config.task.exchange.cost.is_some() {
            LocalCluster::spawn_with_materialization(&worker_binary()?, config.workers)?
        } else {
            LocalCluster::spawn(&worker_binary()?, config.workers)?
        };
        Ok(Self {
            config: config.clone(),
            files,
            snapshot,
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
            snapshot: self.config.recovery_enabled.then(|| self.snapshot.clone()),
        };
        let output = ade_distributed::coordinator::execute(&job)?;
        // The harness intentionally captures child stderr. This explicit diagnostic
        // sidecar retains actual observations from isolated measured children
        // without changing their outcome/result schema or independent oracle.
        if let Some(path) = std::env::var_os("ADE_DISTRIBUTED_METRICS_FILE") {
            use std::io::Write;
            let record = match std::env::var("ADE_BENCH_RUN_ID") {
                Ok(run_id) => {
                    let iteration_index = std::env::var("ADE_BENCH_ITERATION_INDEX")
                        .map_err(|_| Error::Invalid("metrics iteration identity missing".into()))?
                        .parse::<u32>()
                        .map_err(|_| Error::Invalid("metrics iteration identity invalid".into()))?;
                    let warmup = std::env::var("ADE_BENCH_WARMUP")
                        .map_err(|_| Error::Invalid("metrics warmup identity missing".into()))?
                        .parse::<bool>()
                        .map_err(|_| Error::Invalid("metrics warmup identity invalid".into()))?;
                    serde_json::json!({
                        "sidecar_version": 2,
                        "run_id": run_id,
                        "iteration_index": iteration_index,
                        "warmup": warmup,
                        "process_id": std::process::id(),
                        "metrics": output.metrics,
                    })
                }
                Err(std::env::VarError::NotPresent) => serde_json::json!({
                    "sidecar_version": 1,
                    "process_id": std::process::id(),
                    "metrics": output.metrics,
                }),
                Err(std::env::VarError::NotUnicode(_)) => {
                    return Err(Error::Invalid("metrics run identity is not UTF-8".into()));
                }
            };
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)?;
            serde_json::to_writer(&mut file, &record)?;
            file.write_all(b"\n")?;
        }
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
    #[test]
    fn cost_configuration_is_explicit_effective_and_strict() {
        let config = AdeDistributedEngine::parse_config(&[
            "exchange_mode=adaptive".into(),
            "adaptive_policy=expected_cost_v1".into(),
            "cost_failure_probability_ppm=900000".into(),
        ])
        .unwrap();
        assert_eq!(
            config.info().configuration["adaptive_policy"],
            "expected_cost_v1"
        );
        assert_eq!(
            config.info().configuration["cost_failure_probability_ppm"],
            "900000"
        );
        for options in [
            vec!["cost_failure_probability_ppm=1"],
            vec![
                "exchange_mode=adaptive",
                "adaptive_policy=expected_cost_v1",
                "cost_unknown=1",
            ],
            vec![
                "exchange_mode=adaptive",
                "adaptive_policy=expected_cost_v1",
                "cost_mode=unknown",
            ],
            vec![
                "exchange_mode=adaptive",
                "adaptive_policy=expected_cost_v1",
                "cost_object_read_bytes_per_second=0",
            ],
            vec![
                "exchange_mode=adaptive",
                "adaptive_policy=expected_cost_v1",
                "cost_failure_probability_ppm=1000001",
            ],
            vec![
                "exchange_mode=adaptive",
                "adaptive_policy=expected_cost_v1",
                "recovery_enabled=false",
            ],
            vec![
                "exchange_mode=adaptive",
                "adaptive_policy=expected_cost_v1",
                "cost_failure_probability_ppm=1",
                "cost_failure_probability_ppm=2",
            ],
        ] {
            assert!(
                AdeDistributedEngine::parse_config(
                    &options.into_iter().map(str::to_owned).collect::<Vec<_>>()
                )
                .is_err()
            );
        }
    }
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
            vec!["partition_policy=unknown"],
            vec!["partition_policy=sampled_v1", "partition_sample_rows=16385"],
            vec![
                "partition_policy=sampled_v1",
                "partition_sample_bytes=16777217",
            ],
            vec!["partition_policy=static", "partition_sample_rows=1"],
            vec!["partition_policy=sampled_v1", "partition_unknown=1"],
            vec!["partition_policy=sampled_v1", "partition_min_gain_pct=0"],
            vec!["partition_policy=sampled_v1", "partition_policy=static"],
            vec!["research_exit_final_partition=2"],
            vec![
                "research_exit_final_partition=0",
                "research_exit_final_partition=1",
            ],
            vec!["recovery_enabled=invalid"],
            vec!["exchange_mode=adaptive", "adaptive_policy=unknown"],
            vec!["adaptive_buffer_high_pct=80"],
            vec!["exchange_mode=adaptive", "adaptive_buffer_high_pct=0"],
            vec![
                "exchange_mode=adaptive",
                "adaptive_remote_after_bytes=18446744073709551615",
            ],
            vec![
                "exchange_mode=adaptive",
                "adaptive_policy=threshold_v1",
                "adaptive_policy=threshold_v1",
            ],
            vec!["research_delay_ms=1"],
            vec!["research_slow_partition=2"],
            vec!["research_slow_partition=0", "research_delay_ms=2001"],
            vec!["exchange_mode=direct", "exchange_mode=spill"],
            vec!["exchange_buffer_bytes=0"],
            vec!["exchange_buffer_bytes=1", "exchange_buffer_bytes=2"],
            vec!["exchange_mode=buffered", "exchange_buffer_bytes=10"],
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
