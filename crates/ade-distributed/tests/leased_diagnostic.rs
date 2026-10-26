//! Local native diagnostic only. No timing assertions or CI release gates.
use ade_distributed::{
    ExchangeMode, TaskConfig,
    coordinator::{self, LocalCluster, StaticAggregateJob},
    ids::{JobId, MorselId},
    lineage::{SourceMorsel, SourceSnapshot},
    scheduler::{Config, Policy},
};
use arrow::array::{Array, Int64Array, UInt64Array};
use parquet::arrow::{ProjectionMask, arrow_reader::ParquetRecordBatchReaderBuilder};
use std::{
    collections::BTreeMap,
    fs::File,
    io::Write,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

fn source(root: &Path) -> (Vec<PathBuf>, SourceSnapshot, BTreeMap<u64, (i128, u64)>) {
    let bytes = std::fs::read(root.join("manifest.json")).unwrap();
    let manifest: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let files = manifest["files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|file| root.join(file["relative_path"].as_str().unwrap()))
        .collect::<Vec<_>>();
    let snapshot = SourceSnapshot {
        version: 1,
        replayable: false,
        manifest_fingerprint: blake3::hash(&bytes).to_hex().to_string(),
        schema_fingerprint: manifest["schema_fingerprint"].as_str().unwrap().into(),
        logical_fingerprint: manifest["logical_fingerprint"].as_str().unwrap().into(),
        morsels: files
            .iter()
            .zip(manifest["files"].as_array().unwrap())
            .enumerate()
            .map(|(id, (path, file))| SourceMorsel {
                id: MorselId(id as u64),
                path: path.clone(),
                bytes: file["bytes"].as_u64().unwrap(),
                fingerprint: file["fingerprint"].as_str().unwrap().into(),
            })
            .collect(),
    };
    SourceSnapshot::verify(&snapshot.morsels).unwrap();
    let mut expected = BTreeMap::<u64, (i128, u64)>::new();
    for path in &files {
        let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap()).unwrap();
        let key = builder.schema().index_of("key").unwrap();
        let value = builder.schema().index_of("value_i64").unwrap();
        let mask = ProjectionMask::roots(builder.parquet_schema(), [key, value]);
        for batch in builder
            .with_projection(mask)
            .with_batch_size(8192)
            .build()
            .unwrap()
        {
            let batch = batch.unwrap();
            let keys = batch
                .column_by_name("key")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            let values = batch
                .column_by_name("value_i64")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                let entry = expected.entry(keys.value(row)).or_default();
                entry.0 += i128::from(values.value(row));
                entry.1 += 1;
            }
        }
    }
    (files, snapshot, expected)
}

#[test]
#[ignore = "local full-fixture diagnostic; never a timing gate or isolated macrobenchmark"]
fn leased_frozen_fixture_diagnostic() {
    let root = PathBuf::from(
        std::env::var_os("ADE_PHASE10_DIAGNOSTIC_DATA").expect("diagnostic data directory"),
    );
    let path =
        std::env::var_os("ADE_PHASE10_DIAGNOSTIC_OUTPUT").expect("new diagnostic output path");
    let mut output = std::io::BufWriter::new(
        std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(path)
            .unwrap(),
    );
    for regime in ["imbalanced", "uniform"] {
        let (files, snapshot, expected) = source(&root.join(format!("frozen-{regime}")));
        for workers in [2, 4] {
            for policy in [Policy::Static, Policy::LeasedV1] {
                for iteration in 0..4 {
                    let cluster =
                        LocalCluster::spawn(Path::new(env!("CARGO_BIN_EXE_ade-worker")), workers)
                            .unwrap();
                    let mut config = TaskConfig {
                        memory_limit_bytes: 33_554_432,
                        batch_size: 8192,
                        prefetch_batches: 0,
                        scheduler: Config {
                            policy,
                            recovery_enabled: false,
                            ..Config::default()
                        },
                        ..TaskConfig::default()
                    };
                    config.exchange.mode = ExchangeMode::Spill;
                    config.exchange.buffer_bytes = 1_048_576;
                    let job = StaticAggregateJob {
                        id: JobId(0),
                        files: files.clone(),
                        workers: cluster.workers.clone(),
                        snapshot: (policy != Policy::Static).then(|| snapshot.clone()),
                        config,
                        task_timeout: Duration::from_secs(120),
                    };
                    let started = Instant::now();
                    let result = coordinator::execute(&job).unwrap();
                    let wall_ns = started.elapsed().as_nanos();
                    let mut actual = BTreeMap::new();
                    for batch in &result.batches {
                        let keys = batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<UInt64Array>()
                            .unwrap();
                        let sums = batch
                            .column(1)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap();
                        let counts = batch
                            .column(2)
                            .as_any()
                            .downcast_ref::<UInt64Array>()
                            .unwrap();
                        for row in 0..keys.len() {
                            assert!(
                                actual
                                    .insert(
                                        keys.value(row),
                                        (i128::from(sums.value(row)), counts.value(row))
                                    )
                                    .is_none()
                            );
                        }
                    }
                    assert_eq!(actual, expected);
                    let record = serde_json::json!({"kind":"native_diagnostic_not_acceptance", "regime":regime,
                        "workers":workers, "policy":policy, "warmup":iteration==0, "iteration":iteration,
                        "wall_ns":wall_ns,"source":snapshot,"config":job.config,"metrics":result.metrics});
                    serde_json::to_writer(&mut output, &record).unwrap();
                    output.write_all(b"\n").unwrap();
                    output.flush().unwrap();
                    println!("{regime} {workers} {policy:?} {iteration} {wall_ns}");
                }
            }
        }
    }
}
