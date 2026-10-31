use ade_bench::dataset::{self, SyntheticConfig};
use ade_frontend::{Catalog, DataFrame, Target, execution::Options};
use arrow::{array::Int64Array, record_batch::RecordBatch};
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

fn fixture(rows: u64, hot: bool) -> (tempfile::TempDir, PathBuf, Catalog) {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("data");
    dataset::generate(
        &root,
        "frontend",
        &SyntheticConfig {
            rows,
            files: 2.min(usize::try_from(rows).unwrap()),
            key_cardinality: 17,
            distribution: if hot { "hot-key" } else { "uniform" }.into(),
            row_group_rows: 32,
            ..SyntheticConfig::default()
        },
    )
    .unwrap();
    let mut catalog = Catalog::default();
    catalog
        .register_manifest("input", &root, &root.join("manifest.json"))
        .unwrap();
    (dir, root, catalog)
}
async fn oracle(catalog: &Catalog, sql: &str) -> Vec<RecordBatch> {
    let ctx = SessionContext::new();
    for b in &catalog.native().bindings {
        let files = b
            .files
            .iter()
            .map(|p| p.to_str().unwrap())
            .collect::<Vec<_>>();
        let table = ctx
            .read_parquet(files, ParquetReadOptions::default())
            .await
            .unwrap()
            .into_view();
        ctx.register_table(&b.source.table, table).unwrap();
    }
    ctx.sql(sql).await.unwrap().collect().await.unwrap()
}
fn rows(batches: &[RecordBatch], ordered: bool) -> Vec<Vec<String>> {
    let mut rows = vec![];
    for b in batches {
        for i in 0..b.num_rows() {
            rows.push(
                b.columns()
                    .iter()
                    .map(|a| arrow::util::display::array_value_to_string(a, i).unwrap())
                    .collect(),
            );
        }
    }
    if !ordered {
        rows.sort();
    }
    rows
}
async fn compare(
    c: &Catalog,
    sql: &str,
    target: Target,
    options: Options,
    ordered: bool,
) -> ade_physical::PlanV1 {
    let plan = c
        .plan_sql(sql, target)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    #[cfg(feature = "unified-planner")]
    let unified_output = {
        let logical = c.plan_sql_unified(sql, target).await.unwrap();
        let controls = ade_logical::Controls {
            workers: options.workers,
            distributed_available: options.worker_binary.is_some(),
            ..ade_logical::Controls::default()
        };
        let (out, decision) = c
            .execute_unified_until(
                &logical,
                &controls,
                options.clone(),
                std::time::Instant::now() + options.timeout,
            )
            .unwrap();
        assert_eq!(decision.semantic_digest, logical.digest().unwrap());
        Some(out)
    };
    let out = c
        .execute(&plan, options)
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let expected = oracle(c, sql).await;
    #[cfg(feature = "unified-planner")]
    if let Some(u) = unified_output {
        assert_eq!(
            rows(u.batches(), ordered),
            rows(&expected, ordered),
            "unified {sql}"
        );
    }

    if let (Some(a), Some(b)) = (out.batches().first(), expected.first()) {
        assert_eq!(a.schema(), b.schema(), "{sql}");
    }
    assert_eq!(
        rows(out.batches(), ordered),
        rows(&expected, ordered),
        "{sql}"
    );
    assert!(
        out.diagnostics["runtime"]["native_entrypoint"]
            .as_str()
            .unwrap()
            .starts_with("ade_")
    );
    match &out.native {
        ade_frontend::execution::NativeOutput::Local(native) => assert!(
            native
                .metrics
                .operators
                .values()
                .any(|m| m.name == "parquet-scan")
        ),
        ade_frontend::execution::NativeOutput::Distributed(native) => match &native.metrics {
            ade_distributed::physical::Metrics::Aggregate(m) => assert!(m.producers.len() >= 2),
            ade_distributed::physical::Metrics::Join(m) => assert!(m.finals.len() >= 2),
        },
    }
    plan
}
#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "One fixture matrix compares equivalent SQL and typed builders against an independent oracle."
)]
async fn sql_local_oracle_and_dataframe_equivalence() {
    for (n, hot) in [(1, false), (257, false), (257, true)] {
        let (_dir, _root, c) = fixture(n, hot);
        for sql in [
            "SELECT row_id, key, value_i64 FROM input",
            "SELECT value_i64 AS v, key AS k FROM input WHERE secondary < 25",
            "SELECT key, value_i64 FROM input WHERE secondary < 0",
            "SELECT key, value_i64 FROM input WHERE secondary < 18446744073709551615",
            "SELECT key, SUM(value_i64) AS sum, COUNT(*) AS count FROM input GROUP BY key",
            "SELECT COUNT(*) AS n, key AS k, SUM(value_i64) AS s FROM input GROUP BY key",
            "SELECT key, SUM(value_i64) AS sum, COUNT(*) AS count FROM input WHERE secondary < 0 GROUP BY key",
        ] {
            compare(&c, sql, Target::Local, Options::default(), false).await;
        }
        compare(
            &c,
            "SELECT key, row_id, value_i64 FROM input ORDER BY key ASC, row_id ASC LIMIT 100",
            Target::Local,
            Options::default(),
            true,
        )
        .await;
        compare(
            &c,
            "SELECT value_i64, row_id, key FROM input ORDER BY key ASC, row_id ASC LIMIT 7",
            Target::Local,
            Options::default(),
            true,
        )
        .await;
        let sql = c
            .plan_sql(
                "SELECT value_i64 AS v, key AS k FROM input WHERE secondary < 25",
                Target::Local,
            )
            .await
            .unwrap();
        let rust = DataFrame::scan(&c, "input", Target::Local)
            .unwrap()
            .filter_lt("secondary", 25)
            .unwrap()
            .select(&[("value_i64", "v"), ("key", "k")])
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(sql.digest().unwrap(), rust.digest().unwrap());
        let a = c.execute(&rust, Options::default()).unwrap();
        assert_eq!(
            rows(a.batches(), false),
            rows(
                &oracle(
                    &c,
                    "SELECT value_i64 AS v, key AS k FROM input WHERE secondary < 25"
                )
                .await,
                false
            )
        );
        let rust = DataFrame::scan(&c, "input", Target::Local)
            .unwrap()
            .group_sum_count("key", "value_i64")
            .unwrap()
            .select(&[("count", "n"), ("sum", "s"), ("key", "k")])
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(
            rows(
                c.execute(&rust, Options::default()).unwrap().batches(),
                false
            ),
            rows(
                &oracle(
                    &c,
                    "SELECT COUNT(*) AS n, SUM(value_i64) AS s, key AS k FROM input GROUP BY key"
                )
                .await,
                false
            )
        );
        let rust = DataFrame::scan(&c, "input", Target::Local)
            .unwrap()
            .sort_key_row_id()
            .unwrap()
            .limit(7)
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(
            rows(
                c.execute(&rust, Options::default()).unwrap().batches(),
                true
            ),
            rows(
                &oracle(
                    &c,
                    "SELECT key,row_id,value_i64 FROM input ORDER BY key,row_id LIMIT 7"
                )
                .await,
                true
            )
        );
    }
}
#[tokio::test]
async fn unsupported_sql_and_target_matrix() {
    let (_dir, _root, c) = fixture(8, false);
    for sql in [
        "SELECT key FROM input LIMIT 1",
        "SELECT DISTINCT key FROM input",
        "SELECT key FROM input; SELECT key FROM input",
        "CREATE TABLE x (a INT)",
        "CREATE EXTERNAL TABLE x STORED AS PARQUET LOCATION '/tmp/x'",
        "DELETE FROM input",
        "SELECT key FROM '/tmp/x.parquet'",
        "SELECT CAST(key AS BIGINT) FROM input",
        "SELECT key + 1 FROM input",
        "SELECT abs(value_i64) FROM input",
        "SELECT key FROM input WHERE secondary < -1",
        "SELECT key FROM input WHERE secondary < 18446744073709551616",
        "SELECT key FROM input WHERE secondary <= 25",
        "SELECT key FROM input WHERE secondary < 1.5",
        "SELECT key FROM input WHERE CAST(secondary AS BIGINT) < 25",
        "SELECT key,SUM(DISTINCT value_i64),COUNT(*) FROM input GROUP BY key",
        "SELECT key,COUNT(value_i64),SUM(value_i64) FROM input GROUP BY key",
        "SELECT key FROM input ORDER BY key DESC",
        "WITH t AS (SELECT key FROM input) SELECT key FROM t",
        "SELECT * FROM input a LEFT JOIN input b ON a.key=b.key",
        "SELECT * FROM read_parquet('/tmp/forbidden.parquet')",
        "SELECT * FROM generate_series(1,10)",
        "SELECT COUNT(*) OVER () FROM input",
        "SELECT key FROM input ORDER BY key,row_id LIMIT 1 OFFSET 1",
    ] {
        assert!(
            c.plan_sql(sql, Target::Local).await.is_err(),
            "unexpected admission: {sql}"
        );
    }
    assert!(c.plan_sql(&" ".repeat(16385), Target::Local).await.is_err());
    assert!(
        c.plan_sql(
            "SELECT key,SUM(value_i64),COUNT(*) FROM input WHERE secondary<25 GROUP BY key",
            Target::DistributedAggregate
        )
        .await
        .is_err()
    );
    assert!(
        DataFrame::scan(&c, "input", Target::Local)
            .unwrap()
            .limit(1)
            .is_err()
    );
}
#[tokio::test]
async fn source_drift_caps_cancellation_and_forged_graph_no_result() {
    let (_dir, _root, c) = fixture(16, false);
    let mut p = c
        .plan_sql("SELECT key,value_i64 FROM input", Target::Local)
        .await
        .unwrap();
    let expired = std::time::Instant::now()
        .checked_sub(std::time::Duration::from_millis(1))
        .unwrap();
    assert!(c.execute_until(&p, Options::default(), expired).is_err());
    p.max_output_rows = 1;
    assert!(c.execute(&p, Options::default()).is_err());
    p.max_output_rows = 100;
    let mut options = Options::default();
    options.local.memory_limit_bytes = 1;
    assert!(c.execute(&p, options).is_err());
    let options = Options {
        timeout: std::time::Duration::from_nanos(1),
        ..Options::default()
    };
    assert!(c.execute(&p, options).is_err());
    p.max_output_bytes = 1;
    assert!(c.execute(&p, Options::default()).is_err());
    p.max_output_bytes = 1024 * 1024;
    let token = ade_exec::CancellationToken::default();
    token.cancel();
    assert!(
        ade_exec::physical::execute(
            &p,
            &c.bind(&p).unwrap(),
            ade_exec::ExecutionConfig::default(),
            1024,
            token,
            std::time::Duration::from_secs(1)
        )
        .is_err()
    );
    let mut forged = p.clone();
    forged.nodes[0].schema[0].nullable = true;
    assert!(c.execute(&forged, Options::default()).is_err());
    let mut forged = p.clone();
    forged.sources[0].files[0].fingerprint = "0".repeat(64);
    assert!(c.execute(&forged, Options::default()).is_err());
    std::fs::OpenOptions::new()
        .append(true)
        .open(&c.native().bindings[0].files[0])
        .unwrap()
        .write_all(b"drift")
        .unwrap();
    assert!(c.execute(&p, Options::default()).is_err());
}

fn accounted_output_bytes(batches: &[RecordBatch]) -> u64 {
    batches
        .iter()
        .map(|batch| u64::try_from(batch.get_array_memory_size() + 128).unwrap())
        .sum()
}

#[tokio::test]
async fn distributed_output_bytes_are_checked_while_gathering() {
    let (_dir, _root, c) = fixture(257, false);
    let mut aggregate = c
        .plan_sql(
            "SELECT key,SUM(value_i64) AS sum,COUNT(*) AS count FROM input GROUP BY key",
            Target::DistributedAggregate,
        )
        .await
        .unwrap();
    let mut aggregate_options = Options {
        worker_binary: Some(worker()),
        ..Options::default()
    };
    aggregate_options.aggregate.batch_size = 1;
    let aggregate_output = c.execute(&aggregate, aggregate_options.clone()).unwrap();
    assert!(aggregate_output.batches().len() > 1);
    let aggregate_exact = accounted_output_bytes(aggregate_output.batches());
    aggregate.max_output_bytes = aggregate_exact;
    assert!(c.execute(&aggregate, aggregate_options.clone()).is_ok());
    aggregate.max_output_bytes = aggregate_exact - 1;
    assert!(c.execute(&aggregate, aggregate_options.clone()).is_err());
    aggregate.max_output_bytes = 1;
    assert!(c.execute(&aggregate, aggregate_options).is_err());

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("join");
    ade_bench::join_dataset::generate(
        &root,
        "join-output-budget",
        &ade_bench::join_dataset::Config {
            left_rows: 31,
            right_rows: 17,
            left_key_domain: 5,
            right_key_domain: 5,
            files_per_side: 2,
            row_group_rows: 16,
            seed: 42,
        },
    )
    .unwrap();
    let mut join_catalog = Catalog::default();
    for name in ["ade_left", "ade_right"] {
        join_catalog
            .register_manifest(name, &root, &root.join("manifest.json"))
            .unwrap();
    }
    let mut join = join_catalog
        .plan_sql(
            "SELECT l.key FROM ade_left l INNER JOIN ade_right r ON l.key=r.key",
            Target::DistributedJoin,
        )
        .await
        .unwrap();
    let mut join_options = Options {
        worker_binary: Some(worker()),
        ..Options::default()
    };
    join_options.join.local.output_batch_rows = 1;
    let join_output = join_catalog.execute(&join, join_options.clone()).unwrap();
    assert!(join_output.batches().len() > 1);
    let join_exact = accounted_output_bytes(join_output.batches());
    join.max_output_bytes = join_exact;
    assert!(join_catalog.execute(&join, join_options.clone()).is_ok());
    join.max_output_bytes = join_exact - 1;
    assert!(join_catalog.execute(&join, join_options.clone()).is_err());
    join.max_output_bytes = 1;
    assert!(join_catalog.execute(&join, join_options).is_err());
}

#[tokio::test]
async fn allowlisted_schema_fingerprints_are_recomputed_from_canonical_definitions() {
    let (_dir, root, catalog) = fixture(8, false);
    let events_fingerprint = ade_physical::fingerprint(
        b"synthetic-events-v1:row_id=u64,key=u64,secondary=u64,value_i64=i64,value_f64=f64,category=u32,payload=utf8,event_time=timestamp_ms;all_nonnull",
    );
    assert_eq!(
        catalog.native().bindings[0].source.schema_identity,
        events_fingerprint
    );
    let manifest = root.join("manifest.json");
    let mut forged: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&manifest).unwrap()).unwrap();
    forged["schema_fingerprint"] = "0".repeat(64).into();
    let forged_manifest = root.join("forged-schema.json");
    std::fs::write(&forged_manifest, serde_json::to_vec(&forged).unwrap()).unwrap();
    assert!(
        Catalog::default()
            .register_manifest("input", &root, &forged_manifest)
            .is_err()
    );

    let join_root = root.join("join-fixture");
    ade_bench::join_dataset::generate(
        &join_root,
        "join-schema-fingerprint",
        &ade_bench::join_dataset::Config {
            left_rows: 4,
            right_rows: 3,
            left_key_domain: 2,
            right_key_domain: 2,
            files_per_side: 2,
            row_group_rows: 16,
            seed: 42,
        },
    )
    .unwrap();
    let join_manifest = join_root.join("manifest.json");
    let mut join_catalog = Catalog::default();
    join_catalog
        .register_manifest("ade_left", &join_root, &join_manifest)
        .unwrap();
    assert_eq!(
        join_catalog.native().bindings[0].source.schema_identity,
        ade_physical::fingerprint(
            b"left-v1:key=u64,value_i64=i64;right-v1:key=u64,value_i64=i64;all_nonnull"
        )
    );
    let mut forged: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&join_manifest).unwrap()).unwrap();
    forged["schema_fingerprint"] = "0".repeat(64).into();
    let forged_manifest = join_root.join("forged-schema.json");
    std::fs::write(&forged_manifest, serde_json::to_vec(&forged).unwrap()).unwrap();
    assert!(
        Catalog::default()
            .register_manifest("ade_left", &join_root, &forged_manifest)
            .is_err()
    );
}

#[test]
fn manifest_registration_refuses_an_expired_caller_deadline() {
    let (_dir, root, _) = fixture(8, false);
    let deadline = std::time::Instant::now()
        .checked_sub(std::time::Duration::from_millis(1))
        .unwrap();
    assert!(
        Catalog::default()
            .register_manifest_until("input", &root, &root.join("manifest.json"), deadline,)
            .is_err()
    );
}
use std::io::Write;
fn replace_values(root: &Path, values: &[i64], nullable: bool) {
    let path = root.join("part-00000.parquet");
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("manifest.json")).unwrap()).unwrap();
    let name = manifest["files"][0]["relative_path"].as_str().unwrap();
    let actual = root.join(name);
    let mut fields = dataset::schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect::<Vec<_>>();
    fields[3] = fields[3].clone().with_nullable(nullable);
    let schema = Arc::new(arrow::datatypes::Schema::new(fields));
    let read = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
        std::fs::File::open(&actual).unwrap(),
    )
    .unwrap()
    .with_batch_size(1024)
    .build()
    .unwrap()
    .next()
    .unwrap()
    .unwrap();
    let mut arrays = read.columns().to_vec();
    arrays[1] = Arc::new(arrow::array::UInt64Array::from(vec![0; values.len()]));
    arrays[3] = Arc::new(Int64Array::from(values.to_vec()));
    let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(std::fs::File::create(&path).unwrap(), schema, None)
            .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    std::fs::rename(&path, &actual).unwrap();
    manifest["files"][0]["bytes"] = std::fs::metadata(&actual).unwrap().len().into();
    manifest["files"][0]["fingerprint"] = dataset::hash_file(&actual).unwrap().into();
    std::fs::write(
        root.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest).unwrap(),
    )
    .unwrap();
}
#[tokio::test]
async fn signed_sum_final_overflow_and_nullable_source_refusal() {
    let (_dir, root, _) = fixture(4, false);
    replace_values(&root, &[i64::MAX, 1], false);
    let mut c = Catalog::default();
    c.register_manifest("input", &root, &root.join("manifest.json"))
        .unwrap();
    let p = c
        .plan_sql(
            "SELECT key,SUM(value_i64) AS sum,COUNT(*) AS count FROM input GROUP BY key",
            Target::Local,
        )
        .await
        .unwrap();
    assert!(
        c.execute(&p, Options::default())
            .unwrap_err_native()
            .contains("overflow")
    );
    #[cfg(feature = "unified-planner")]
    {
        let logical = ade_frontend::unified::from_bound(&p).unwrap();
        assert!(
            c.execute_unified_until(
                &logical,
                &ade_logical::Controls::default(),
                Options::default(),
                std::time::Instant::now() + std::time::Duration::from_secs(10)
            )
            .err()
            .unwrap()
            .to_string()
            .contains("overflow")
        );
    }

    replace_values(&root, &[-9, 4], false);
    let mut c = Catalog::default();
    c.register_manifest("input", &root, &root.join("manifest.json"))
        .unwrap();
    compare(
        &c,
        "SELECT key,SUM(value_i64) AS sum,COUNT(*) AS count FROM input GROUP BY key",
        Target::Local,
        Options::default(),
        false,
    )
    .await;
    replace_values(&root, &[-9, 4], true);
    assert!(
        Catalog::default()
            .register_manifest("input", &root, &root.join("manifest.json"))
            .is_err()
    );
}
trait ErrorText {
    fn unwrap_err_native(self) -> String;
}
impl ErrorText for ade_frontend::Result<ade_frontend::execution::Output> {
    fn unwrap_err_native(self) -> String {
        match self {
            Ok(_) => panic!("unexpected success"),
            Err(e) => e.to_string(),
        }
    }
}
fn worker() -> PathBuf {
    let query_binary = PathBuf::from(env!("CARGO_BIN_EXE_ade-query"));
    let worker = query_binary.with_file_name(format!("ade-worker{}", std::env::consts::EXE_SUFFIX));
    assert!(
        worker.is_file(),
        "ade-worker is missing from this Cargo target directory: {}. Build it with matching features into the same target directory before running this focused test (cargo build -p ade-distributed --bin ade-worker --features research-hooks), or run cargo test --workspace.",
        worker.display()
    );
    worker
}
#[tokio::test]
async fn real_distributed_aggregate_and_join_oracle() {
    let (_dir, _root, c) = fixture(128, false);
    for workers in [2, 4] {
        let p = compare(
            &c,
            "SELECT key,SUM(value_i64) AS sum,COUNT(*) AS count FROM input GROUP BY key",
            Target::DistributedAggregate,
            Options {
                workers,
                worker_binary: Some(worker()),
                ..Options::default()
            },
            false,
        )
        .await;
        assert_eq!(p.target, Target::DistributedAggregate);
    }
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("join");
    ade_bench::join_dataset::generate(
        &root,
        "join",
        &ade_bench::join_dataset::Config {
            left_rows: 31,
            right_rows: 17,
            left_key_domain: 5,
            right_key_domain: 5,
            files_per_side: 2,
            row_group_rows: 16,
            seed: 42,
        },
    )
    .unwrap();
    let mut c = Catalog::default();
    for name in ["ade_left", "ade_right"] {
        c.register_manifest(name, &root, &root.join("manifest.json"))
            .unwrap();
    }
    for sql in [
        "SELECT l.key,l.value_i64 AS left_value,r.value_i64 AS right_value FROM ade_left l INNER JOIN ade_right r ON l.key=r.key",
        "SELECT r.value_i64 AS rv,l.key AS k,l.value_i64 AS lv FROM ade_left l INNER JOIN ade_right r ON l.key=r.key",
    ] {
        compare(
            &c,
            sql,
            Target::DistributedJoin,
            Options {
                worker_binary: Some(worker()),
                ..Options::default()
            },
            false,
        )
        .await;
    }
    assert!(
        c.plan_sql(
            "SELECT l.key FROM ade_left l LEFT JOIN ade_right r ON l.key=r.key",
            Target::DistributedJoin
        )
        .await
        .is_err()
    );
    let mut p = c
        .plan_sql(
            "SELECT l.key FROM ade_left l INNER JOIN ade_right r ON l.key=r.key",
            Target::DistributedJoin,
        )
        .await
        .unwrap();
    p.max_output_rows = 1;
    assert!(
        c.execute(
            &p,
            Options {
                worker_binary: Some(worker()),
                ..Options::default()
            }
        )
        .is_err()
    );
}

#[test]
fn cli_explain_execute_and_no_output_on_refusal() {
    let (_dir, root, _) = fixture(8, false);
    // Extra directory files must never enter the registered source list.
    std::fs::write(root.join("unlisted.parquet"), b"not parquet").unwrap();
    let binary = env!("CARGO_BIN_EXE_ade-query");
    let help = std::process::Command::new(binary)
        .arg("--help")
        .output()
        .unwrap();
    assert!(String::from_utf8_lossy(&help.stdout).contains("End-to-end budget"));
    let expired = std::process::Command::new(binary)
        .args(["execute", "--dataset"])
        .arg(&root)
        .args(["--sql", "SELECT key FROM input", "--timeout-ms", "0"])
        .output()
        .unwrap();
    assert!(!expired.status.success());
    assert_eq!(expired.stdout, [] as [u8; 0]);
    let oversized_sql = root.join("oversized.sql");
    std::fs::write(&oversized_sql, vec![b' '; 16 * 1024 + 1]).unwrap();
    let refused = std::process::Command::new(binary)
        .args(["execute", "--dataset"])
        .arg(&root)
        .arg("--sql-file")
        .arg(oversized_sql)
        .output()
        .unwrap();
    assert!(!refused.status.success());
    assert_eq!(refused.stdout, [] as [u8; 0]);
    for action in ["plan-sql", "explain", "execute"] {
        let output = std::process::Command::new(binary)
            .args([action, "--dataset"])
            .arg(&root)
            .args(["--sql", "SELECT key,value_i64 FROM input"])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let text = String::from_utf8(output.stdout).unwrap();
        assert!(text.contains("plan_hash"));
        if action == "execute" {
            assert!(text.contains("ade_exec::physical::execute"));
        }
    }
    let output = std::process::Command::new(binary)
        .args(["execute", "--dataset"])
        .arg(&root)
        .args(["--sql", "SELECT key FROM input LIMIT 1"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert_eq!(output.stdout, [] as [u8; 0]);
    let output = std::process::Command::new(binary)
        .args(["execute", "--dataset"])
        .arg(&root)
        .args(["--sql", "SELECT key FROM input", "--max-output-rows", "1"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert_eq!(output.stdout, [] as [u8; 0]);
}

#[test]
fn manifest_traversal_symlink_identity_and_duplicate_registration_refuse() {
    let (_dir, root, mut c) = fixture(4, false);
    assert!(
        c.register_manifest("input", &root, &root.join("manifest.json"))
            .is_err()
    );
    assert!(
        Catalog::default()
            .register_manifest("arbitrary", &root, &root.join("manifest.json"))
            .is_err()
    );
    let mut m: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("manifest.json")).unwrap()).unwrap();
    m["files"][0]["relative_path"] = "../escape.parquet".into();
    let oversized = root.join("oversized.json");
    std::fs::write(&oversized, vec![b' '; 1024 * 1024 + 1]).unwrap();
    assert!(
        Catalog::default()
            .register_manifest("input", &root, &oversized)
            .is_err()
    );
    let manifest = root.join("forged.json");
    std::fs::write(&manifest, serde_json::to_vec(&m).unwrap()).unwrap();
    assert!(
        Catalog::default()
            .register_manifest("input", &root, &manifest)
            .is_err()
    );
    m["files"][0]["relative_path"] = "escape.parquet".into();
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink("/tmp", root.join("escape.parquet")).unwrap();
        std::fs::write(&manifest, serde_json::to_vec(&m).unwrap()).unwrap();
        assert!(
            Catalog::default()
                .register_manifest("input", &root, &manifest)
                .is_err()
        );
    }
}

#[tokio::test]
async fn join_four_workers_forced_baseline_empty_and_failure_controls() {
    for (left_rows, right_rows, workers) in [(31, 17, 4), (0, 0, 2)] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("join");
        ade_bench::join_dataset::generate(
            &root,
            "join-controls",
            &ade_bench::join_dataset::Config {
                left_rows,
                right_rows,
                left_key_domain: 5,
                right_key_domain: 5,
                files_per_side: 2,
                row_group_rows: 16,
                seed: 42,
            },
        )
        .unwrap();
        let mut catalog = Catalog::default();
        for name in ["ade_left", "ade_right"] {
            catalog
                .register_manifest(name, &root, &root.join("manifest.json"))
                .unwrap();
        }
        let mut options = Options {
            workers,
            worker_binary: Some(worker()),
            ..Options::default()
        };
        options.join.policy = ade_distributed::join_policy::Policy::ForcedRepartition;
        options.join.local.algorithm = ade_exec::join::Algorithm::Grace;
        compare(&catalog,"SELECT l.key,l.value_i64 AS left_value,r.value_i64 AS right_value FROM ade_left l INNER JOIN ade_right r ON l.key=r.key",Target::DistributedJoin,options,false).await;
        if left_rows == 0 {
            let mut plan = catalog
                .plan_sql(
                    "SELECT l.key,l.value_i64 AS left_value,r.value_i64 AS right_value FROM ade_left l INNER JOIN ade_right r ON l.key=r.key",
                    Target::DistributedJoin,
                )
                .await
                .unwrap();
            plan.max_output_bytes = 1;
            let output = catalog
                .execute(
                    &plan,
                    Options {
                        workers,
                        worker_binary: Some(worker()),
                        ..Options::default()
                    },
                )
                .unwrap();
            assert_eq!(output.batches().len(), 0);
        }
    }
}

#[cfg(feature = "unified-planner")]
#[tokio::test]
async fn unified_two_actual_targets_and_forced_controls() {
    use ade_logical::{Choice, Controls};
    let mut observed = vec![];
    for n in [128, 12000] {
        let (_dir, _root, c) = fixture(n, false);
        let sql = "SELECT key,SUM(value_i64) AS sum,COUNT(*) AS count FROM input GROUP BY key";
        let logical = DataFrame::scan(&c, "input", Target::Local)
            .unwrap()
            .group_sum_count("key", "value_i64")
            .unwrap()
            .build_logical()
            .unwrap();
        let controls = Controls {
            distributed_available: true,
            ..Controls::default()
        };
        let options = Options {
            worker_binary: Some(worker()),
            ..Options::default()
        };
        let expected = oracle(&c, sql).await;
        let (out, d) = c
            .execute_unified_until(
                &logical,
                &controls,
                options.clone(),
                std::time::Instant::now() + options.timeout,
            )
            .unwrap();
        observed.push(d.chosen);
        assert_eq!(rows(out.batches(), false), rows(&expected, false));
        for forced in [Choice::Local, Choice::DistributedAggregate] {
            let controls = Controls {
                forced: Some(forced),
                ..controls.clone()
            };
            let (out, d) = c
                .execute_unified_until(
                    &logical,
                    &controls,
                    options.clone(),
                    std::time::Instant::now() + options.timeout,
                )
                .unwrap();
            assert_eq!(d.chosen, forced);
            assert_eq!(rows(out.batches(), false), rows(&expected, false));
        }
        let controls = Controls {
            enabled: false,
            ..controls
        };
        assert_eq!(
            logical.lower_batch(&controls).unwrap().1.chosen,
            Choice::Local
        );
        let expired = std::time::Instant::now()
            .checked_sub(std::time::Duration::from_millis(1))
            .unwrap();
        assert!(
            c.execute_unified_until(&logical, &controls, options.clone(), expired)
                .is_err()
        );
        let mut tiny = logical.clone();
        if let ade_logical::Computation::ParquetManifest {
            max_output_bytes, ..
        } = &mut tiny.computation
        {
            *max_output_bytes = 1;
        }
        assert!(
            c.execute_unified_until(
                &tiny,
                &controls,
                options.clone(),
                std::time::Instant::now() + options.timeout
            )
            .is_err()
        );
    }
    assert_eq!(observed, [Choice::Local, Choice::DistributedAggregate]);
}

#[cfg(feature = "unified-planner")]
#[tokio::test]
async fn unified_source_binding_and_drift_refuse_without_results() {
    let (_dir, _root, catalog) = fixture(16, false);
    let logical = catalog
        .plan_sql_unified("SELECT key,value_i64 FROM input", Target::Local)
        .await
        .unwrap();
    let mut forged = logical.clone();
    if let ade_logical::Computation::ParquetManifest { sources, .. } = &mut forged.computation {
        sources[0].files[0].fingerprint = "0".repeat(64);
    }
    let controls = ade_logical::Controls::default();
    assert!(
        catalog
            .execute_unified_until(
                &forged,
                &controls,
                Options::default(),
                std::time::Instant::now() + std::time::Duration::from_secs(10)
            )
            .is_err()
    );
    std::fs::OpenOptions::new()
        .append(true)
        .open(&catalog.native().bindings[0].files[0])
        .unwrap()
        .write_all(b"changed")
        .unwrap();
    assert!(
        catalog
            .execute_unified_until(
                &logical,
                &controls,
                Options::default(),
                std::time::Instant::now() + std::time::Duration::from_secs(10)
            )
            .is_err()
    );
}
