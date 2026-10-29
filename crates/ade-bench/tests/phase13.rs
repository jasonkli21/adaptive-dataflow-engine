#![cfg(feature = "remote-s3")]
use ade_bench::{
    dataset::{self, DatasetManifest, SyntheticConfig},
    engine::{self, BenchmarkEngine, EngineInfo},
    spec::BenchmarkCase,
};
use ade_distributed::{coordinator::LocalCluster, ids::JobId, physical, remote};
use ade_physical::{Field, Operator, PlanV1, Source, SourceFile, Target, Type, fingerprint};
use arrow::record_batch::RecordBatch;
use std::{
    collections::BTreeMap,
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    time::Instant,
};
struct Captured(Vec<RecordBatch>);
impl BenchmarkEngine for Captured {
    type Config = Vec<RecordBatch>;
    fn parse_config(_: &[String]) -> ade_bench::Result<Self::Config> {
        unreachable!()
    }
    fn prepare(
        _: &Path,
        _: &DatasetManifest,
        _: &BenchmarkCase,
        config: &Self::Config,
    ) -> impl std::future::Future<Output = ade_bench::Result<Self>> {
        std::future::ready(Ok(Self(config.clone())))
    }
    fn execute(&self) -> impl std::future::Future<Output = ade_bench::Result<Vec<RecordBatch>>> {
        std::future::ready(Ok(self.0.clone()))
    }
    fn info(&self) -> EngineInfo {
        EngineInfo {
            name: "captured-native-output-oracle-only".into(),
            version: "1".into(),
            configuration: BTreeMap::new(),
        }
    }
}
fn plan(root: &Path, m: &DatasetManifest) -> PlanV1 {
    let fields = vec![
        Field::new("row_id", Type::U64, false),
        Field::new("key", Type::U64, false),
        Field::new("secondary", Type::U64, false),
        Field::new("value_i64", Type::I64, false),
        Field::new("value_f64", Type::F64, false),
        Field::new("category", Type::U32, false),
        Field::new("payload", Type::Utf8, false),
        Field::new("event_time", Type::TimestampMs, false),
    ];
    let source = Source {
        id: 0,
        table: "input".into(),
        manifest: fingerprint(&fs::read(root.join("manifest.json")).unwrap()),
        logical: m.logical_fingerprint.clone(),
        schema_identity: m.schema_fingerprint.clone(),
        schema: fields,
        files: m
            .files
            .iter()
            .map(|f| SourceFile {
                name: f.relative_path.clone(),
                bytes: f.bytes,
                rows: f.rows,
                fingerprint: f.fingerprint.clone(),
            })
            .collect(),
        replayable: true,
    };
    let mut p = PlanV1::new(Target::DistributedAggregate, vec![source]);
    p.push(Operator::Scan { source: 0 }).unwrap();
    p.push(Operator::GroupSumCount {
        input: 0,
        key: 1,
        value: 3,
    })
    .unwrap();
    p
}
fn write_new(path: &Path, bytes: &[u8]) {
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .unwrap();
    file.write_all(bytes).unwrap();
}
fn preserve(path: &Path, bytes: &[u8]) {
    if path.exists() {
        assert_eq!(fs::read(path).unwrap(), bytes);
    } else {
        write_new(path, bytes);
    }
}
fn isolated_samples(output: &Path) {
    let executable = std::env::current_exe().unwrap();
    let mut samples = Vec::new();
    for distribution in ["uniform", "hot-key"] {
        for workers in [2, 4] {
            for repetition in 0..4 {
                for mode in ["local_materialized", "remote_object"] {
                    let slot = format!("{distribution},{workers},{mode},{repetition}");
                    let status = std::process::Command::new(&executable)
                        .args([
                            "--exact",
                            "independent_oracle_and_all_local_remote_cost_samples",
                            "--ignored",
                            "--nocapture",
                            "--test-threads=1",
                        ])
                        .env("ADE_PHASE13_SLOT", &slot)
                        .status()
                        .unwrap();
                    assert!(status.success(), "isolated sample {slot}");
                    let row: serde_json::Value = serde_json::from_slice(
                        &fs::read(
                            output
                                .join(format!("{distribution}-{workers}-{mode}-{repetition}.json")),
                        )
                        .unwrap(),
                    )
                    .unwrap();
                    samples.push(row);
                }
            }
        }
    }
    assert_eq!(samples.len(), 32);
    write_new(
        &output.join("samples.json"),
        &serde_json::to_vec_pretty(&samples).unwrap(),
    );
}
#[test]
#[ignore = "source-bound same-data local/remote campaign requires separate private S3 fixture"]
#[allow(
    clippy::too_many_lines,
    reason = "A single predeclared paired campaign owns every sample, source identity and output validation."
)]
fn independent_oracle_and_all_local_remote_cost_samples() {
    let output =
        PathBuf::from(std::env::var("ADE_PHASE13_EVIDENCE").expect("evidence output required"));
    fs::create_dir_all(&output).unwrap();
    let Ok(slot) = std::env::var("ADE_PHASE13_SLOT") else {
        isolated_samples(&output);
        return;
    };
    let rt = tokio::runtime::Runtime::new().unwrap();
    let case = BenchmarkCase::load(
        &PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../benchmarks/specs/aggregate-low-cardinality.toml"),
    )
    .unwrap();
    let worker =
        PathBuf::from(std::env::var("ADE_REMOTE_WORKER").expect("feature-matched worker required"))
            .canonicalize()
            .unwrap();
    let mut samples = vec![];
    for distribution in ["uniform", "hot-key"] {
        if !slot.starts_with(&format!("{distribution},")) {
            continue;
        }
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        let data = root.join("data");
        let generator = SyntheticConfig {
            rows: 100_000,
            files: 4,
            row_group_rows: 8192,
            key_cardinality: 4096,
            distribution: distribution.into(),
            hot_key_fraction: 0.99,
            hot_key_count: 1,
            payload_size: 16,
            seed: 42,
            ..Default::default()
        };
        let manifest = dataset::generate(&data, "phase13-frozen", &generator).unwrap();
        let preserved = output.join(format!("data-{distribution}"));
        fs::create_dir_all(&preserved).unwrap();
        for f in &manifest.files {
            preserve(
                &preserved.join(&f.relative_path),
                &fs::read(data.join(&f.relative_path)).unwrap(),
            );
        }
        preserve(
            &preserved.join("manifest.json"),
            &fs::read(data.join("manifest.json")).unwrap(),
        );
        let plan = plan(&data, &manifest);
        let files = manifest
            .files
            .iter()
            .map(|f| data.join(&f.relative_path).canonicalize().unwrap())
            .collect::<Vec<_>>();
        fs::write(
            output.join(format!("{distribution}-manifest.json")),
            fs::read(data.join("manifest.json")).unwrap(),
        )
        .unwrap();
        for workers in [2, 4] {
            for repetition in 0..4 {
                for mode in ["local_materialized", "remote_object"] {
                    if slot != format!("{distribution},{workers},{mode},{repetition}") {
                        continue;
                    }
                    let run = remote::Run {
                        job: remote::Job {
                            version: 1,
                            job: JobId(1),
                            plan: plan.clone(),
                            files: files.clone(),
                            manifest: data.join("manifest.json"),
                            workers,
                            readers: workers,
                            store: remote::S3Config {
                                endpoint: std::env::var("ADE_REMOTE_ENDPOINT").unwrap(),
                                bucket: "ade-phase13-test".into(),
                                prefix: "campaign".into(),
                                region: "us-east-1".into(),
                                dev_allow_http: true,
                            },
                            limits: remote::Limits::default(),
                            rates: remote::Rates {
                                worker_placement: "loopback-compute".into(),
                                store_placement: "loopback-store".into(),
                                put_nano_usd: 5000,
                                head_nano_usd: 400,
                                get_nano_usd: 400,
                                egress_byte_nano_usd: 1,
                                retention_ms: 60000,
                                ..Default::default()
                            },
                        },
                        worker_binary: worker.clone(),
                        scratch_parent: root.clone(),
                    };
                    let start = Instant::now();
                    let (batches, receipt, cleanup) = if mode == "remote_object" {
                        let mut result =
                            remote::execute(&run, ade_exec::CancellationToken::default()).unwrap();
                        let wall_ns = start.elapsed().as_nanos();
                        let gc_start = Instant::now();
                        let gc = result.cleanup(true).unwrap();
                        let gc_ns = gc_start.elapsed().as_nanos();
                        (
                            result.batches,
                            serde_json::json!({"remote":result.receipt,"native_wall_ns":wall_ns}),
                            serde_json::json!({"observed":gc,"wall_ns":gc_ns}),
                        )
                    } else {
                        let cluster = LocalCluster::spawn(&worker, workers).unwrap();
                        let catalog = ade_exec::physical::Catalog {
                            bindings: vec![ade_exec::physical::Binding {
                                source: plan.sources[0].clone(),
                                files: files.clone(),
                            }],
                        };
                        let mut options = physical::Options::default();
                        options.aggregate.memory_limit_bytes = remote::TASK_MEMORY;
                        options.aggregate.spill_enabled = false;
                        options.aggregate.scheduler.recovery_enabled = false;
                        let result = physical::execute(physical::Request {
                            plan: &plan,
                            catalog: &catalog,
                            workers: cluster.workers.clone(),
                            job: JobId(1),
                            options,
                        })
                        .unwrap();
                        drop(cluster);
                        (
                            result.batches,
                            serde_json::json!({"native_wall_ns":start.elapsed().as_nanos()}),
                            serde_json::Value::Null,
                        )
                    };
                    let validation = rt
                        .block_on(engine::execute_validated_with::<Captured>(
                            &data, &manifest, &case, &batches,
                        ))
                        .unwrap();
                    let row = serde_json::json!({"coordinator_pid":std::process::id(),"evidence_version":1,"distribution":distribution,"workers":workers,"mode":mode,"repetition":repetition,"warmup":repetition==0,"manifest_hash":plan.sources[0].manifest,"plan_hash":plan.digest().unwrap(),"validation":validation,"execution":receipt,"cleanup":cleanup});
                    write_new(
                        &output.join(format!("{distribution}-{workers}-{mode}-{repetition}.json")),
                        &serde_json::to_vec_pretty(&row).unwrap(),
                    );
                    eprintln!(
                        "ADE_PHASE13_SAMPLE distribution={distribution} workers={workers} mode={mode} repetition={repetition} oracle=true"
                    );
                    samples.push(row);
                }
            }
        }
    }
    assert_eq!(
        samples.len(),
        1,
        "each coordinator process executes exactly one slot"
    );
}
