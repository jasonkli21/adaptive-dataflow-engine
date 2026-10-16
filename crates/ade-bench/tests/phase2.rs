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
async fn phase2_adapter_ordered_sort_pressure_and_configuration() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("data");
    let manifest = dataset::generate(
        &root,
        "phase2",
        &SyntheticConfig {
            rows: 5000,
            files: 3,
            row_group_rows: 64,
            key_cardinality: 701,
            ..Default::default()
        },
    )
    .unwrap();
    for name in [
        "scan",
        "scan-filter-project",
        "aggregate-low-cardinality",
        "aggregate-high-cardinality",
        "aggregate-skew",
        "sort-key-rowid",
    ] {
        let case = spec(name);
        let baseline = engine::execute_validated_request(
            &root,
            &manifest,
            &case,
            &EngineRequest {
                name: "datafusion".into(),
                options: vec![],
            },
        )
        .await
        .unwrap();
        for constrained in [false, true] {
            // Large scan output has no spillable state, so it uses the default budget.
            if constrained && matches!(name, "scan" | "scan-filter-project") {
                continue;
            }
            let mut options = vec!["batch_size=64".into(), "prefetch_batches=2".into()];
            if constrained {
                options.push("memory_limit_bytes=262144".into());
            }
            let request = EngineRequest {
                name: "ade-local".into(),
                options,
            };
            let result = engine::execute_validated_request(&root, &manifest, &case, &request)
                .await
                .unwrap();
            assert!(result.validated);
            assert_eq!(result.result_fingerprint, baseline.result_fingerprint);
            let configuration = request.resolve().unwrap().info().configuration;
            assert_eq!(configuration["prefetch_batches"], "2");
            assert_eq!(configuration["spill_enabled"], "true");
            assert!(configuration.contains_key("memory_limit_bytes"));
        }
    }
    for option in [
        "memory_limit_bytes=0",
        "memory_limit_bytes=-1",
        "memory_limit_bytes=18446744073709551616",
        "spill_enabled=1",
        "spill_enabled=TRUE",
        "prefetch_batches=-1",
        "prefetch_batches=65",
        "unknown=true",
    ] {
        assert!(
            AdeLocalEngine::parse_config(&[option.into()]).is_err(),
            "{option}"
        );
    }
    assert!(AdeLocalEngine::parse_config(&["batch_size=1".into(), "batch_size=2".into()]).is_err());
}
