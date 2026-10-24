//! Explicit join adapter; aggregate adapter defaults/options remain independent.
use crate::{
    Error, Result,
    dataset::DatasetManifest,
    engine::{BenchmarkEngine, EngineInfo},
    require,
    spec::{BenchmarkCase, Query},
};
use ade_distributed::{
    coordinator::LocalCluster,
    ids::JobId,
    join::{self, Config, Source},
    join_policy::Policy,
};
use ade_exec::join::{Algorithm, Side};
use arrow::record_batch::RecordBatch;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::Mutex,
};
#[derive(Debug, Clone)]
pub struct JoinConfig {
    pub workers: usize,
    pub task: Config,
}
impl JoinConfig {
    #[must_use]
    pub fn info(&self) -> EngineInfo {
        let mut configuration = BTreeMap::from([
            ("workers".into(), self.workers.to_string()),
            ("exchange_mode".into(), "spill".into()),
            ("recovery_enabled".into(), "false".into()),
            ("control_protocol".into(), "ADETCP11".into()),
            ("exchange_wire_version".into(), "2".into()),
            ("hash_version".into(), "1".into()),
        ]);
        if let Ok(serde_json::Value::Object(values)) = serde_json::to_value(&self.task) {
            for (k, v) in values {
                configuration.insert(k, v.as_str().map_or_else(|| v.to_string(), str::to_owned));
            }
        }
        EngineInfo {
            name: "ade-distributed-join".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            configuration,
        }
    }
}
pub struct JoinEngine {
    config: JoinConfig,
    sources: [Source; 2],
    cluster: LocalCluster,
    next: Mutex<u64>,
    cap: u64,
}
#[allow(
    clippy::unused_async_trait_impl,
    reason = "Synchronous worker RPCs implement the common asynchronous benchmark adapter boundary."
)]
impl BenchmarkEngine for JoinEngine {
    type Config = JoinConfig;
    fn parse_config(options: &[String]) -> Result<JoinConfig> {
        let mut config = JoinConfig {
            workers: 2,
            task: Config::default(),
        };
        let mut seen = BTreeSet::new();
        for option in options {
            let (k, v) = option
                .split_once('=')
                .ok_or_else(|| Error::Invalid("join option must be key=value".into()))?;
            require(seen.insert(k), "duplicate join option")?;
            let invalid = || Error::Invalid(format!("invalid join option {k}"));
            match k {
                "workers" => config.workers = v.parse().map_err(|_| invalid())?,
                "join_policy" => {
                    config.task.policy = match v {
                        "observed_v1" => Policy::ObservedV1,
                        "forced_repartition" => Policy::ForcedRepartition,
                        "forced_broadcast" => Policy::ForcedBroadcast,
                        _ => return Err(invalid()),
                    }
                }
                "build_side" => {
                    config.task.build = match v {
                        "auto" => None,
                        "left" => Some(Side::Left),
                        "right" => Some(Side::Right),
                        _ => return Err(invalid()),
                    }
                }
                "join_algorithm" => {
                    config.task.local.algorithm = match v {
                        "auto" => Algorithm::Auto,
                        "hash_only" => Algorithm::HashOnly,
                        "grace" => Algorithm::Grace,
                        _ => return Err(invalid()),
                    }
                }
                "hash_rows" => config.task.local.hash_rows = v.parse().map_err(|_| invalid())?,
                "memory_limit_bytes" => {
                    config.task.local.memory_limit_bytes = v.parse().map_err(|_| invalid())?;
                }
                "spill_enabled" => {
                    config.task.local.spill_enabled = v.parse().map_err(|_| invalid())?;
                }
                "probe_rows" => config.task.probe_rows = v.parse().map_err(|_| invalid())?,
                "probe_files" => config.task.probe_files = v.parse().map_err(|_| invalid())?,
                "probe_read_bytes" => {
                    config.task.probe_read_bytes = v.parse().map_err(|_| invalid())?;
                }
                "broadcast_rows" => {
                    config.task.broadcast_rows = v.parse().map_err(|_| invalid())?;
                }
                "broadcast_bytes" => {
                    config.task.broadcast_bytes = v.parse().map_err(|_| invalid())?;
                }
                "broadcast_amplification_bytes" => {
                    config.task.broadcast_amplification_bytes = v.parse().map_err(|_| invalid())?;
                }
                "max_spill_bytes" => {
                    config.task.local.max_spill_bytes = v.parse().map_err(|_| invalid())?;
                }
                "exchange_mode" if v == "spill" => {}
                "recovery_enabled" if v == "false" => {}
                _ => return Err(Error::Invalid(format!("unsupported join option: {k}"))),
            }
        }
        require(
            (2..=4).contains(&config.workers),
            "join requires 2..4 workers",
        )?;
        config.task.validate()?;
        Ok(config)
    }
    async fn prepare(
        root: &Path,
        manifest: &DatasetManifest,
        case: &BenchmarkCase,
        config: &JoinConfig,
    ) -> Result<Self> {
        case.validate()?;
        manifest.validate()?;
        config.task.validate()?;
        let Query::InnerEquiJoin { max_output_rows } = case.query else {
            return Err(Error::Invalid("join adapter requires InnerEquiJoin".into()));
        };
        require(
            manifest.required_schema == "synthetic-join-v1",
            "join source schema",
        )?;
        let verified = crate::dataset::inspect(root, true)?;
        require(
            verified.identity()? == manifest.identity()?,
            "join manifest changed",
        )?;
        let mut sources = Vec::new();
        for (side, name) in [(Side::Left, "left/"), (Side::Right, "right/")] {
            let files = manifest
                .files
                .iter()
                .filter(|f| f.relative_path.starts_with(name))
                .map(|f| Ok((root.join(&f.relative_path).canonicalize()?, f.rows)))
                .collect::<Result<Vec<_>>>()?;
            let captured = Source::capture(side, files)?;
            // Keep manifest provenance across capture; do not adopt bytes that
            // changed after the preceding inspection as a new valid snapshot.
            for (actual, expected) in captured.files.iter().zip(
                manifest
                    .files
                    .iter()
                    .filter(|f| f.relative_path.starts_with(name)),
            ) {
                require(
                    actual.blake3 == expected.fingerprint
                        && actual.bytes == expected.bytes
                        && actual.rows == expected.rows,
                    "join source changed during snapshot capture",
                )?;
            }
            sources.push(captured);
        }
        let binary = worker_binary()?;
        let cluster = LocalCluster::spawn(&binary, config.workers)?;
        Ok(Self {
            config: config.clone(),
            sources: sources
                .try_into()
                .map_err(|_| Error::Invalid("join sources".into()))?,
            cluster,
            next: Mutex::new(0),
            cap: max_output_rows,
        })
    }
    async fn execute(&self) -> Result<Vec<RecordBatch>> {
        let mut next = self
            .next
            .lock()
            .map_err(|_| Error::Invalid("join identity lock".into()))?;
        let job = JobId(*next);
        *next = next
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("join identity overflow".into()))?;
        drop(next);
        let mut config = self.config.task.clone();
        config.local.max_output_rows = self.cap;
        let output = join::execute(
            &join::Spec {
                job,
                execution: uuid::Uuid::new_v4().to_string(),
                workers: self.cluster.workers.clone(),
                sources: self.sources.clone(),
                config,
            },
            ade_distributed::TASK_TIMEOUT,
        )?;
        if let Some(path) = std::env::var_os("ADE_DISTRIBUTED_METRICS_FILE") {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)?;
            serde_json::to_writer(
                &mut file,
                &serde_json::json!({"process_id":std::process::id(),"metrics":output.metrics}),
            )?;
            file.write_all(b"\n")?;
        }
        Ok(output.batches)
    }
    fn info(&self) -> EngineInfo {
        self.config.info()
    }
}
fn worker_binary() -> Result<PathBuf> {
    if let Some(path) = std::env::var_os("ADE_WORKER_BIN") {
        return Ok(path.into());
    }
    let executable = std::env::current_exe()?;
    let sibling = executable
        .parent()
        .ok_or_else(|| Error::Invalid("benchmark directory".into()))?
        .join("ade-worker");
    require(sibling.is_file(), "build ade-worker or set ADE_WORKER_BIN")?;
    Ok(sibling)
}
