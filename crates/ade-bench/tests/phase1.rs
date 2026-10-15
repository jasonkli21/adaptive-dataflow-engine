use ade_bench::{
    ade_local::AdeLocalEngine,
    dataset::{self, SyntheticConfig},
    engine::{self, BenchmarkEngine, EngineRequest},
    spec::BenchmarkCase,
};
use std::path::Path;
fn spec(name: &str) -> BenchmarkCase {
    BenchmarkCase::load(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join(format!("../../benchmarks/specs/{name}.toml")),
    )
    .unwrap()
}
#[tokio::test]
async fn phase1_adapter_validates_all_synthetic_cases() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("data");
    let manifest = dataset::generate(
        &root,
        "phase1",
        &SyntheticConfig {
            rows: 101,
            files: 3,
            row_group_rows: 7,
            key_cardinality: 53,
            distribution: "hot-key".into(),
            ..Default::default()
        },
    )
    .unwrap();
    let request = |name: &str| EngineRequest {
        name: name.into(),
        options: vec!["batch_size=11".into()],
    };
    assert_eq!(
        request("ade-local").resolve().unwrap().info().configuration["batch_size"],
        "11"
    );
    for name in [
        "scan",
        "scan-filter-project",
        "aggregate-low-cardinality",
        "aggregate-high-cardinality",
        "aggregate-skew",
    ] {
        let case = spec(name);
        let ade = engine::execute_validated_request(&root, &manifest, &case, &request("ade-local"))
            .await
            .unwrap();
        let df = engine::execute_validated_request(&root, &manifest, &case, &request("datafusion"))
            .await
            .unwrap();
        assert!(ade.validated && df.validated);
        assert_eq!(ade.result_fingerprint, df.result_fingerprint);
        assert_eq!(ade.rows_output, df.rows_output);
    }
    let config = AdeLocalEngine::parse_config(&[]).unwrap();
    let error = AdeLocalEngine::prepare(&root, &manifest, &spec("tpch-q1"), &config)
        .await
        .err()
        .unwrap();
    assert!(error.to_string().contains("does not support TPC-H Q1"));
    for option in [
        "batch_size=0",
        "batch_size=-1",
        "batch_size=bad",
        "target_partitions=2",
        "batch_size",
    ] {
        assert!(AdeLocalEngine::parse_config(&[option.into()]).is_err());
    }
    std::fs::remove_file(root.join(&manifest.files[0].relative_path)).unwrap();
    assert!(
        AdeLocalEngine::prepare(&root, &manifest, &spec("scan"), &config)
            .await
            .is_err()
    );
}
