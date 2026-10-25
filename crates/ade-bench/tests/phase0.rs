#![allow(
    clippy::too_many_lines,
    reason = "Integration scenarios retain setup, mutations and assertions in one readable fixture."
)]
use ade_bench::{
    dataset::{self, SyntheticConfig},
    engine::{self, EngineConfig},
    result::BenchmarkResultV1,
    spec::BenchmarkCase,
};
use arrow::array::{
    Float64Array, Int64Array, StringArray, TimestampMillisecondArray, UInt32Array, UInt64Array,
};
use std::{
    fs,
    path::Path,
    process::{Command, Output},
};
fn spec(name: &str) -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(format!("../../benchmarks/specs/{name}.toml"))
}
fn cli(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_ade-bench"))
        .args(args)
        .output()
        .unwrap()
}
fn cli_at(args: &[&str], cwd: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_ade-bench"))
        .current_dir(cwd)
        .args(args)
        .output()
        .unwrap()
}
fn success(output: Output) -> Output {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    output
}
#[test]
fn deterministic_readback_and_distribution() {
    let d = tempfile::tempdir().unwrap();
    let config = SyntheticConfig {
        rows: 101,
        files: 3,
        row_group_rows: 7,
        distribution: "hot-key".into(),
        key_cardinality: 11,
        hot_key_fraction: 0.5,
        payload_size: 3,
        ..Default::default()
    };
    let a = dataset::generate(&d.path().join("a"), "fixture", &config).unwrap();
    let mut reshaped = config.clone();
    reshaped.files = 2;
    reshaped.row_group_rows = 11;
    reshaped.compression = "zstd".into();
    let b = dataset::generate(&d.path().join("b"), "fixture", &reshaped).unwrap();
    assert_eq!(a.logical_fingerprint, b.logical_fingerprint);
    assert_ne!(a.identity().unwrap(), b.identity().unwrap());
    reshaped.seed += 1;
    reshaped.compression = "none".into();
    let c = dataset::generate(&d.path().join("c"), "fixture", &reshaped).unwrap();
    assert_ne!(a.logical_fingerprint, c.logical_fingerprint);
    for (name, manifest) in [("b", &b), ("c", &c)] {
        let mut rows = 0;
        for file in &manifest.files {
            let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                fs::File::open(d.path().join(name).join(&file.relative_path)).unwrap(),
            )
            .unwrap()
            .build()
            .unwrap();
            for batch in reader {
                rows += batch.unwrap().num_rows() as u64;
            }
        }
        assert_eq!(rows, config.rows);
    }

    assert_eq!(a.files.len(), 3);
    assert_eq!(a.files.iter().map(|f| f.rows).sum::<u64>(), 101);
    assert_eq!(
        (0..101)
            .filter(|&i| dataset::row(&config, i).0 == 0)
            .count(),
        50
    );
    assert!((0..101).all(|i| dataset::row(&config, i).0 < 11));
    dataset::inspect(&d.path().join("a"), true).unwrap();
    // Read every logical field back, including columns unused by core queries.
    let mut read_rows = 0;
    for file in &a.files {
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            fs::File::open(d.path().join("a").join(&file.relative_path)).unwrap(),
        )
        .unwrap()
        .build()
        .unwrap();
        for batch in reader {
            let batch = batch.unwrap();
            for r in 0..batch.num_rows() {
                let id = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .value(r);
                let (key, secondary, value) = dataset::row(&config, id);
                assert_eq!(
                    batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .unwrap()
                        .value(r),
                    key
                );
                assert_eq!(
                    batch
                        .column(2)
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .unwrap()
                        .value(r),
                    secondary
                );
                assert_eq!(
                    batch
                        .column(3)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(r),
                    value
                );
                assert_eq!(
                    batch
                        .column(4)
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap()
                        .value(r)
                        .to_bits(),
                    (f64::from(i32::try_from(value).unwrap()) / 100.).to_bits()
                );
                assert_eq!(
                    batch
                        .column(5)
                        .as_any()
                        .downcast_ref::<UInt32Array>()
                        .unwrap()
                        .value(r),
                    (secondary % 10) as u32
                );
                assert_eq!(
                    batch
                        .column(6)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .value(r)
                        .len(),
                    config.payload_size
                );
                assert_eq!(
                    batch
                        .column(7)
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .unwrap()
                        .value(r),
                    1_600_000_000_000_i64 + i64::try_from(id).unwrap()
                );
                assert!(batch.columns().iter().all(|a| !a.is_null(r)));
                read_rows += 1;
            }
        }
    }
    assert_eq!(read_rows, config.rows);

    let path = d.path().join("a").join(&a.files[0].relative_path);
    let original = fs::read(&path).unwrap();
    let mut changed_bytes = original.clone();
    changed_bytes[8] ^= 1; // Data page bytes, leaving the footer and file size intact.
    fs::write(&path, changed_bytes).unwrap();
    assert!(dataset::inspect(&d.path().join("a"), false).is_ok());
    assert!(dataset::inspect(&d.path().join("a"), true).is_err());
    fs::write(path, original).unwrap();
    let round: dataset::DatasetManifest =
        serde_json::from_str(&serde_json::to_string(&a).unwrap()).unwrap();
    round.validate().unwrap();
    let mut bad = round.clone();
    bad.schema_version = 2;
    assert!(bad.validate().is_err());
    bad = round;
    bad.files[0].relative_path = "../x.parquet".into();
    assert!(bad.validate().is_err());
    fs::write(d.path().join("a").join(&a.files[0].relative_path), "broken").unwrap();
    assert!(dataset::inspect(&d.path().join("a"), true).is_err());
    let mut invalid = config;
    invalid.key_cardinality = 0;
    assert!(invalid.validate().is_err());
    invalid.key_cardinality = 1;
    assert!(invalid.validate().is_err());
}
#[tokio::test]
async fn all_synthetic_workloads_and_errors() {
    let d = tempfile::tempdir().unwrap();
    for (name, cardinality, distribution) in [
        ("low", 3, "uniform"),
        ("high", 997, "uniform"),
        ("skew", 997, "hot-key"),
    ] {
        let root = d.path().join(name);
        let c = SyntheticConfig {
            rows: 1001,
            files: 2,
            key_cardinality: cardinality,
            distribution: distribution.into(),
            ..Default::default()
        };
        let m = dataset::generate(&root, name, &c).unwrap();
        for name in [
            "scan",
            "scan-filter-project",
            "aggregate-low-cardinality",
            "aggregate-high-cardinality",
            "aggregate-skew",
        ] {
            let s = BenchmarkCase::load(&spec(name)).unwrap();
            let outcome = engine::execute_validated(&root, &m, &s, &EngineConfig::default())
                .await
                .unwrap();
            assert!(outcome.validated);
            assert!(outcome.rows_output > 0);
            if name == "scan" {
                assert_eq!(outcome.rows_output, 1001);
            }
            if name == "scan-filter-project" {
                assert_eq!(
                    outcome.rows_output,
                    (0..1001).filter(|&i| dataset::row(&c, i).1 < 250).count() as u64
                );
            }
        }
        let s = BenchmarkCase::load(&spec("scan")).unwrap();
        assert!(
            engine::execute_validated(
                &root,
                &m,
                &s,
                &EngineConfig {
                    batch_size: 0,
                    ..Default::default()
                }
            )
            .await
            .is_err()
        );
        assert!(
            engine::execute_validated(&d.path().join("missing"), &m, &s, &EngineConfig::default())
                .await
                .is_err()
        );
        // Syntactically valid manifest but wrong oracle parameters must fail correctness.
        let mut wrong = m.clone();
        wrong.synthetic.as_mut().unwrap().seed += 1;
        assert!(
            engine::execute_validated(&root, &wrong, &s, &EngineConfig::default())
                .await
                .is_err()
        );
    }
    assert!(EngineConfig::from_options(&["unknown=1".into()]).is_err());
    assert!(EngineConfig::from_options(&["target_partitions=0".into()]).is_err());
}
#[test]
fn cli_process_roundtrip_and_comparison_integrity() {
    let d = tempfile::tempdir().unwrap();
    let root = d.path().join("data");
    let output = d.path().join("result.json");
    success(cli(&["--help"]));
    let env = success(cli(&["env", "--format", "json"]));
    serde_json::from_slice::<serde_json::Value>(&env.stdout).unwrap();
    success(cli(&[
        "data",
        "generate",
        "synthetic",
        "--dataset-id",
        "ci",
        "--rows",
        "100",
        "--files",
        "2",
        "--output",
        root.to_str().unwrap(),
    ]));
    success(cli(&["data", "inspect", root.to_str().unwrap()]));
    let s = spec("aggregate-skew");
    success(cli(&[
        "validate",
        "--spec",
        s.to_str().unwrap(),
        "--dataset",
        root.to_str().unwrap(),
    ]));
    success(cli(&[
        "run",
        "--engine",
        "datafusion",
        "--spec",
        s.to_str().unwrap(),
        "--dataset",
        root.to_str().unwrap(),
        "--verify-data",
        "full",
        "--warmups",
        "1",
        "--iterations",
        "2",
        "--output",
        output.to_str().unwrap(),
    ]));
    let result = BenchmarkResultV1::load(&output).unwrap();
    assert!(result.valid);
    assert_eq!(
        result.run_config.dataset_verification,
        ade_bench::result::DatasetVerification::Full
    );
    assert_eq!(result.iterations.len(), 2);
    assert_eq!(result.warmups.len(), 1);
    success(cli(&["check-result", output.to_str().unwrap()]));
    success(cli(&[
        "compare",
        output.to_str().unwrap(),
        output.to_str().unwrap(),
        "--format",
        "json",
    ]));
    let compare = ade_bench::compare::compare(&result, &result).unwrap();
    assert_eq!(compare.classification, "neutral");
    let mut faster = result.clone();
    for it in &mut faster.iterations {
        it.outcome.as_mut().unwrap().query_time_ns /= 2;
    }
    faster.summary = ade_bench::stats::SummaryMetrics::new(
        &faster
            .iterations
            .iter()
            .map(|i| i.outcome.as_ref().unwrap().query_time_ns)
            .collect::<Vec<_>>(),
    );
    assert_eq!(
        ade_bench::compare::compare(&result, &faster)
            .unwrap()
            .classification,
        "improvement"
    );
    assert_eq!(
        ade_bench::compare::compare(&faster, &result)
            .unwrap()
            .classification,
        "regression"
    );
    let mut changed = result.clone();
    changed.environment.arch = "different".into();
    assert!(
        ade_bench::compare::compare(&result, &changed)
            .unwrap()
            .warnings
            .iter()
            .any(|w| w == "hardware/OS differs")
    );
    changed = result.clone();
    changed.build.rustc.push_str(" changed");
    assert!(
        ade_bench::compare::compare(&result, &changed)
            .unwrap()
            .warnings
            .iter()
            .any(|warning| warning == "compiler/build differs")
    );
    changed = result.clone();
    changed.workload.description.push_str(" (wording update)");
    let comparison = ade_bench::compare::compare(&result, &changed).unwrap();
    assert!(
        comparison
            .warnings
            .iter()
            .any(|warning| warning == "workload description/tags differ")
    );
    changed = result.clone();
    changed
        .workload
        .tags
        .insert("note".into(), "informational".into());
    assert!(ade_bench::compare::compare(&result, &changed).is_ok());
    changed = result.clone();
    changed.workload.query = ade_bench::spec::Query::Scan;
    assert!(ade_bench::compare::compare(&result, &changed).is_err());
    changed = result.clone();
    changed.schema_version = 99;
    assert!(changed.validate().is_err());
    changed = result.clone();
    changed.workload.id = "different".into();
    assert!(ade_bench::compare::compare(&result, &changed).is_err());
    changed = result.clone();
    changed.valid = false;
    assert!(changed.validate().is_err());
    changed = result.clone();
    changed.iterations[0].outcome.as_mut().unwrap().validated = false;
    changed.valid = false;
    changed.summary = None;
    changed.validate().unwrap();
    assert!(ade_bench::compare::compare(&result, &changed).is_err());
    changed = result.clone();
    changed.dataset.logical_fingerprint = "different".into();
    changed.dataset_manifest_fingerprint = changed.dataset.identity().unwrap();
    assert!(ade_bench::compare::compare(&result, &changed).is_err());
    assert!(
        !cli(&[
            "run",
            "--spec",
            s.to_str().unwrap(),
            "--dataset",
            root.to_str().unwrap(),
            "--output",
            output.to_str().unwrap()
        ])
        .status
        .success()
    );
    let unsupported_output = d.path().join("unsupported.json");
    assert!(
        !cli(&[
            "run",
            "--engine",
            "missing-engine",
            "--spec",
            s.to_str().unwrap(),
            "--dataset",
            root.to_str().unwrap(),
            "--output",
            unsupported_output.to_str().unwrap()
        ])
        .status
        .success()
    );
    assert!(!unsupported_output.exists());
    // Exercise parent diagnostics for an actual failed child, preserving an immutable invalid result.
    let mut manifest = dataset::inspect(&root, false).unwrap();
    manifest.synthetic.as_mut().unwrap().key_cardinality = 1;
    fs::write(
        root.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest).unwrap(),
    )
    .unwrap();
    let failed = d.path().join("failed.json");
    assert!(
        !cli(&[
            "run",
            "--spec",
            s.to_str().unwrap(),
            "--dataset",
            root.to_str().unwrap(),
            "--warmups",
            "0",
            "--iterations",
            "1",
            "--output",
            failed.to_str().unwrap()
        ])
        .status
        .success()
    );
    let invalid = BenchmarkResultV1::load(&failed).unwrap();
    assert!(!invalid.valid);
    assert!(invalid.summary.is_none());
    assert!(invalid.iterations[0].error.is_some());
}

#[test]
fn source_provenance_is_independent_of_runtime_git_repository() {
    let directory = tempfile::tempdir().unwrap();
    let unrelated = directory.path().join("unrelated-repo");
    fs::create_dir(&unrelated).unwrap();
    if !Command::new("git")
        .arg("--version")
        .status()
        .is_ok_and(|s| s.success())
    {
        return;
    }
    let run_git = |args: &[&str]| {
        let status = Command::new("git")
            .current_dir(&unrelated)
            .args(args)
            .status()
            .unwrap();
        assert!(status.success(), "git {args:?} failed");
    };
    run_git(&["init", "--quiet"]);
    run_git(&["config", "user.name", "ADE test"]);
    run_git(&["config", "user.email", "ade-test@example.invalid"]);
    fs::write(unrelated.join("input.txt"), "unrelated source\n").unwrap();
    run_git(&["add", "input.txt"]);
    run_git(&["commit", "--quiet", "-m", "unrelated repository"]);

    let local = serde_json::from_slice::<serde_json::Value>(
        &success(cli(&["env", "--format", "json"])).stdout,
    )
    .unwrap();
    let from_other = serde_json::from_slice::<serde_json::Value>(
        &success(cli_at(&["env", "--format", "json"], &unrelated)).stdout,
    )
    .unwrap();
    assert_eq!(local["repository"], from_other["repository"]);
}

#[tokio::test]
async fn q1_cutoff_and_tolerance_reference() {
    use arrow::{
        array::{ArrayRef, Date32Array, Float64Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use parquet::arrow::ArrowWriter;
    use std::sync::Arc;
    let directory = tempfile::tempdir().unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("l_returnflag", DataType::Utf8, false),
        Field::new("l_linestatus", DataType::Utf8, false),
        Field::new("l_quantity", DataType::Float64, false),
        Field::new("l_extendedprice", DataType::Float64, false),
        Field::new("l_discount", DataType::Float64, false),
        Field::new("l_tax", DataType::Float64, false),
        Field::new("l_shipdate", DataType::Date32, false),
    ]));
    let cutoff = i32::try_from(
        (chrono::NaiveDate::from_ymd_opt(1998, 9, 2).unwrap()
            - chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
        .num_days(),
    )
    .unwrap();
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec!["A", "A", "N", "N"])),
        Arc::new(StringArray::from(vec!["F", "F", "O", "O"])),
        Arc::new(Float64Array::from(vec![1., 3., 5., 999.])),
        Arc::new(Float64Array::from(vec![10., 20., 30., 999.])),
        Arc::new(Float64Array::from(vec![0.1, 0.2, 0.3, 0.])),
        Arc::new(Float64Array::from(vec![0.01, 0.02, 0.03, 0.])),
        Arc::new(Date32Array::from(vec![
            cutoff,
            cutoff - 1,
            cutoff,
            cutoff + 1,
        ])),
    ];
    let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
    let file = directory.path().join("lineitem.parquet");
    let mut writer = ArrowWriter::try_new(fs::File::create(&file).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let manifest = dataset::DatasetManifest {
        schema_version: 1,
        dataset_id: "q1-fixture".into(),
        dataset_version: 1,
        required_schema: "tpch-v1".into(),
        schema_fingerprint: "test-lineitem-v1".into(),
        generator: "test-fixture".into(),
        generator_version: "1".into(),
        encoding_version: "test".into(),
        format: "parquet".into(),
        row_count: 4,
        logical_fingerprint: dataset::hash_file(&file).unwrap(),
        synthetic: None,
        join: None,
        scale_factor: Some(0.001),
        files: vec![dataset::FileManifest {
            relative_path: "lineitem.parquet".into(),
            bytes: file.metadata().unwrap().len(),
            rows: 4,
            row_groups: 1,
            fingerprint: dataset::hash_file(&file).unwrap(),
        }],
    };
    let case = BenchmarkCase::load(&spec("tpch-q1")).unwrap();
    let baseline =
        engine::execute_validated(directory.path(), &manifest, &case, &EngineConfig::default())
            .await
            .unwrap();
    assert_eq!(baseline.rows_output, 2);
    let candidate = engine::execute_validated(
        directory.path(),
        &manifest,
        &case,
        &EngineConfig {
            target_partitions: 4,
            batch_size: 2,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(baseline.result_fingerprint, candidate.result_fingerprint);
    assert!(dataset::register_tpch(directory.path(), 0.001, "test".into()).is_err());
}

#[test]
fn weighted_file_layout_preserves_logical_rows_and_manifest_verification() {
    let root = tempfile::tempdir().unwrap();
    let c = SyntheticConfig {
        rows: 3200,
        files: 4,
        ..SyntheticConfig::default()
    };
    let balanced = dataset::generate(&root.path().join("balanced"), "weighted-test", &c).unwrap();
    let mut skew = c.clone();
    skew.file_row_weights = vec![13, 1, 1, 1];
    let weighted =
        dataset::generate(&root.path().join("weighted"), "weighted-test", &skew).unwrap();
    assert_eq!(balanced.logical_fingerprint, weighted.logical_fingerprint);
    assert_ne!(balanced.identity().unwrap(), weighted.identity().unwrap());
    assert_eq!(
        weighted.files.iter().map(|f| f.rows).collect::<Vec<_>>(),
        [2600, 200, 200, 200]
    );
    dataset::inspect(&root.path().join("weighted"), true).unwrap();
    assert!(
        !serde_json::to_string(&c)
            .unwrap()
            .contains("file_row_weights")
    );
    skew.file_row_weights = vec![0, 1, 1, 1];
    assert!(skew.validate().is_err());
    skew.file_row_weights = vec![1, 1];
    assert!(skew.validate().is_err());
}
