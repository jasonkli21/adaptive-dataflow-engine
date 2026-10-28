mod support;
use ade_ops::{JobState, Limits, Store, runner};
use std::fs;
#[test]
#[allow(clippy::too_many_lines)] // Both native targets, immutable marker and safe GC in one end-to-end scenario.
fn durable_native_both_shapes_preserve_values_schema_and_duplicate_bags() {
    for join in [false, true] {
        for n in [2, 4] {
            let (dir, mut recipe) = support::fixture(join);
            let root = support::state_path(&dir);
            let mut store = Store::init(&root, Limits::default()).unwrap();
            let (_workers, endpoints) =
                support::managed_workers(&dir.path().canonicalize().unwrap(), store.id(), n);
            recipe.workers = endpoints;
            recipe.max_attempts = 3;
            let submitted = store.submit("native", recipe.clone()).unwrap();
            drop(store);
            let succeeded = runner::run(&root, submitted.id).unwrap();
            assert_eq!(
                succeeded.state,
                JobState::Succeeded,
                "{:?}",
                succeeded.attempts
            );
            assert_eq!(succeeded.attempt, 1);
            let output = runner::verify_output(&root, submitted.id).unwrap();
            assert_eq!(
                output.schema,
                recipe.plan.nodes[recipe.plan.output as usize].schema
            );
            assert_eq!(output.completion.workers, u32::try_from(n).unwrap());
            assert!(output.bytes < recipe.staging_bytes);
            let file = root
                .join("jobs")
                .join(submitted.id.to_string())
                .join(&output.file);
            let reader =
                arrow::ipc::reader::StreamReader::try_new(fs::File::open(file).unwrap(), None)
                    .unwrap();
            let mut actual = vec![];
            for batch in reader {
                let batch = batch.unwrap();
                for i in 0..batch.num_rows() {
                    actual.push(
                        (0..batch.num_columns())
                            .map(|c| {
                                arrow::util::display::array_value_to_string(batch.column(c), i)
                                    .unwrap()
                            })
                            .collect::<Vec<_>>(),
                    );
                }
            }
            actual.sort();
            let mut expected = if join {
                vec![
                    vec!["1", "-9", "-9"],
                    vec!["1", "-9", "4"],
                    vec!["1", "4", "-9"],
                    vec!["1", "4", "4"],
                    vec!["2", "7", "7"],
                ]
            } else {
                vec![vec!["1", "-5", "2"], vec!["2", "7", "1"]]
            }
            .into_iter()
            .map(|r| r.into_iter().map(String::from).collect::<Vec<_>>())
            .collect::<Vec<_>>();
            expected.sort();
            assert_eq!(actual, expected);
            eprintln!(
                "ADE_NATIVE_ORACLE join={join} workers={n} canonical={} ipc={}",
                blake3::hash(&serde_json::to_vec(&actual).unwrap()).to_hex(),
                output.blake3
            );
            let marker = root
                .join("jobs")
                .join(submitted.id.to_string())
                .join("COMMITTED.json");
            let original = fs::read(&marker).unwrap();
            assert_eq!(
                runner::cancel(&root, submitted.id).unwrap().state,
                JobState::Succeeded
            );
            assert_eq!(
                runner::run(&root, submitted.id).unwrap().state,
                JobState::Succeeded
            );
            assert_eq!(fs::read(&marker).unwrap(), original);
            assert_eq!(
                runner::gc(&root, submitted.id, false).unwrap().files.len(),
                0
            );
            assert_eq!(
                runner::gc(&root, submitted.id, true).unwrap().files.len(),
                0
            );
            // A missing successful marker is corruption, never permission to GC
            // the once-committed data or candidate that can diagnose/recover it.
            fs::remove_file(&marker).unwrap();
            assert!(runner::gc(&root, submitted.id, true).is_err());
            assert!(
                root.join("jobs")
                    .join(submitted.id.to_string())
                    .join(&output.file)
                    .exists()
            );
            fs::hard_link(
                root.join("jobs")
                    .join(submitted.id.to_string())
                    .join(format!("manifest-{}.json", output.identity.instance)),
                &marker,
            )
            .unwrap();
            runner::verify_output(&root, submitted.id).unwrap();
        }
    }
}
#[test]
fn output_caps_and_source_drift_never_publish() {
    for attack in ["cap", "drift"] {
        let (dir, mut recipe) = support::fixture(false);
        let root = support::state_path(&dir);
        let mut store = Store::init(&root, Limits::default()).unwrap();
        let (_workers, endpoints) =
            support::managed_workers(&dir.path().canonicalize().unwrap(), store.id(), 2);
        recipe.workers = endpoints;
        if attack == "cap" {
            recipe.staging_bytes = 1;
        }
        let submitted = store.submit("negative", recipe.clone()).unwrap();
        drop(store);
        if attack == "drift" {
            let path = recipe.inputs[0].root.join("a.parquet");
            let mut bytes = fs::read(&path).unwrap();
            bytes[10] ^= 1;
            fs::write(path, bytes).unwrap();
        }
        let failed = runner::run(&root, submitted.id).unwrap();
        assert_eq!(failed.state, JobState::Failed, "{:?}", failed.attempts);
        assert!(
            !root
                .join("jobs")
                .join(submitted.id.to_string())
                .join("COMMITTED.json")
                .exists()
        );
        assert!(runner::verify_output(&root, submitted.id).is_err());
        runner::gc(&root, submitted.id, true).unwrap();
    }
}

#[test]
#[allow(clippy::too_many_lines)] // Independent scalar/bag oracle plus explicit overflow rejection.
fn independent_canonical_oracles_empty_skew_signed_and_overflow() {
    for join in [false, true] {
        for values in [
            vec![],
            vec![(7, -11)],
            vec![(1, -9), (1, 4), (2, 7), (1, -5)],
            (0..257)
                .map(|i| {
                    (
                        if i % 3 == 0 {
                            u64::try_from(i).unwrap()
                        } else {
                            1
                        },
                        i64::from(i) - 129,
                    )
                })
                .collect(),
        ] {
            let (dir, mut recipe) = support::fixture(join);
            let right = vec![(1, -3), (1, 2), (7, 5)];
            support::rewrite(&mut recipe, &values, &right);
            let root = support::state_path(&dir);
            let mut store = Store::init(&root, Limits::default()).unwrap();
            let (_workers, endpoints) =
                support::managed_workers(&dir.path().canonicalize().unwrap(), store.id(), 2);
            recipe.workers = endpoints;
            let job = store.submit("canonical", recipe.clone()).unwrap();
            drop(store);
            let result = runner::run(&root, job.id).unwrap();
            assert_eq!(result.state, JobState::Succeeded, "{:?}", result.attempts);
            let manifest = runner::verify_output(&root, job.id).unwrap();
            let path = root
                .join("jobs")
                .join(job.id.to_string())
                .join(manifest.file);
            let reader =
                arrow::ipc::reader::StreamReader::try_new(fs::File::open(path).unwrap(), None)
                    .unwrap();
            let mut actual = vec![];
            for batch in reader {
                let batch = batch.unwrap();
                assert_eq!(
                    batch.schema(),
                    ade_exec::physical::arrow_schema(
                        &recipe.plan.nodes[recipe.plan.output as usize].schema
                    )
                );
                for i in 0..batch.num_rows() {
                    actual.push(
                        (0..batch.num_columns())
                            .map(|c| {
                                arrow::util::display::array_value_to_string(batch.column(c), i)
                                    .unwrap()
                            })
                            .collect::<Vec<_>>(),
                    );
                }
            }
            actual.sort();
            let mut expected = vec![];
            if join {
                for (key, left) in &values {
                    for (right_key, right) in &right {
                        if key == right_key {
                            expected.push(vec![
                                key.to_string(),
                                left.to_string(),
                                right.to_string(),
                            ]);
                        }
                    }
                }
            } else {
                let mut groups = std::collections::BTreeMap::<u64, (i128, i64)>::new();
                for (key, value) in &values {
                    let group = groups.entry(*key).or_default();
                    group.0 += i128::from(*value);
                    group.1 += 1;
                }
                for (key, (sum, count)) in groups {
                    expected.push(vec![key.to_string(), sum.to_string(), count.to_string()]);
                }
            }
            expected.sort();
            assert_eq!(actual, expected);
            eprintln!(
                "ADE_NATIVE_ORACLE join={join} workers=2 canonical={} ipc={}",
                blake3::hash(&serde_json::to_vec(&actual).unwrap()).to_hex(),
                manifest.blake3
            );
        }
    }
    let (dir, mut recipe) = support::fixture(false);
    support::rewrite(&mut recipe, &[(1, i64::MAX), (1, 1)], &[]);
    let root = support::state_path(&dir);
    let mut store = Store::init(&root, Limits::default()).unwrap();
    let (_workers, endpoints) =
        support::managed_workers(&dir.path().canonicalize().unwrap(), store.id(), 2);
    recipe.workers = endpoints;
    recipe.max_attempts = 3;
    let job = store.submit("overflow", recipe).unwrap();
    drop(store);
    let failed = runner::run(&root, job.id).unwrap();
    assert_eq!(failed.state, JobState::Failed);
    assert_eq!(failed.attempt, 1);
    assert!(
        !root
            .join("jobs")
            .join(job.id.to_string())
            .join("COMMITTED.json")
            .exists()
    );
}
#[test]
fn real_runtime_quota_exhaustion_and_exact_output_staging_boundary() {
    for join in [false, true] {
        let (dir, mut recipe) = support::fixture(join);
        let root = support::state_path(&dir);
        let mut store = Store::init(&root, Limits::default()).unwrap();
        let (workers, endpoints) =
            support::managed_workers(&dir.path().canonicalize().unwrap(), store.id(), 2);
        recipe.workers = endpoints;
        recipe.runtime_bytes = 1;
        let job = store.submit("disk-cap", recipe).unwrap();
        drop(store);
        let failed = runner::run(&root, job.id).unwrap();
        assert!(
            matches!(failed.state, JobState::Failed | JobState::NeedsOperator),
            "{:?}",
            failed.attempts
        );
        assert!(
            !root
                .join("jobs")
                .join(job.id.to_string())
                .join("COMMITTED.json")
                .exists()
        );
        if join {
            let lines: Vec<_> = workers.witnesses.try_iter().collect();
            assert!(
                lines
                    .iter()
                    .any(|line| line.contains("managed disk write quota")),
                "{lines:?}"
            );
        }
    }
    // Publication's existing tests establish exact serialized cap and cap+1;
    // this integration checks the total native IPC+manifest staging domain.
    let (dir, mut recipe) = support::fixture(false);
    let root = support::state_path(&dir);
    let mut store = Store::init(&root, Limits::default()).unwrap();
    let (_workers, endpoints) =
        support::managed_workers(&dir.path().canonicalize().unwrap(), store.id(), 2);
    recipe.workers = endpoints;
    recipe.staging_bytes = 512;
    let job = store.submit("total-stage-cap", recipe).unwrap();
    drop(store);
    assert_eq!(runner::run(&root, job.id).unwrap().state, JobState::Failed);
    assert!(
        !root
            .join("jobs")
            .join(job.id.to_string())
            .join("COMMITTED.json")
            .exists()
    );
}
#[test]
fn source_physical_byte_cap_and_one_million_input_rows_are_real_admissions() {
    for count in [64 * 1024 * 1024, 64 * 1024 * 1024 + 1] {
        let (dir, mut recipe) = support::fixture(false);
        support::pad_source(&mut recipe, count);
        let root = support::state_path(&dir);
        let mut store = Store::init(&root, Limits::default()).unwrap();
        if count > 64 * 1024 * 1024 {
            assert!(store.submit("source-cap", recipe).is_err());
            continue;
        }
        let (_workers, endpoints) =
            support::managed_workers(&dir.path().canonicalize().unwrap(), store.id(), 2);
        recipe.workers = endpoints;
        let job = store.submit("source-cap", recipe).unwrap();
        drop(store);
        assert_eq!(
            runner::run(&root, job.id).unwrap().state,
            JobState::Succeeded
        );
        runner::verify_output(&root, job.id).unwrap();
    }
    let (dir, mut recipe) = support::fixture(false);
    support::rewrite(&mut recipe, &vec![(1, -1); 1_000_000], &[]);
    let root = support::state_path(&dir);
    let mut store = Store::init(&root, Limits::default()).unwrap();
    let (_workers, endpoints) =
        support::managed_workers(&dir.path().canonicalize().unwrap(), store.id(), 2);
    recipe.workers = endpoints;
    let job = store.submit("million", recipe.clone()).unwrap();
    drop(store);
    assert_eq!(
        runner::run(&root, job.id).unwrap().state,
        JobState::Succeeded
    );
    let manifest = runner::verify_output(&root, job.id).unwrap();
    assert_eq!(manifest.rows, 1);
    recipe.plan.sources[0].files[0].rows = 1_000_001;
    assert!(recipe.validate().is_err());
}
#[test]
fn one_million_join_rows_stream_and_one_over_global_row_cap_cannot_publish() {
    for cap in [1_000_000, 999_999] {
        let (dir, mut recipe) = support::fixture(true);
        support::rewrite(&mut recipe, &vec![(7, 1); 1000], &vec![(7, -1); 1000]);
        let input = recipe.plan.output;
        recipe
            .plan
            .push(ade_physical::Operator::Project {
                input,
                columns: vec![ade_physical::Selection {
                    column: 0,
                    alias: "key".into(),
                }],
            })
            .unwrap();
        recipe.plan.max_output_rows = cap;
        recipe.staging_bytes = 64 * 1024 * 1024;
        let root = support::state_path(&dir);
        let mut store = Store::init(&root, Limits::default()).unwrap();
        let (_workers, endpoints) =
            support::managed_workers(&dir.path().canonicalize().unwrap(), store.id(), 2);
        recipe.workers = endpoints;
        let job = store.submit("million-output", recipe).unwrap();
        drop(store);
        let result = runner::run(&root, job.id).unwrap();
        if cap == 999_999 {
            assert_eq!(result.state, JobState::Failed, "{:?}", result.attempts);
            assert!(
                !root
                    .join("jobs")
                    .join(job.id.to_string())
                    .join("COMMITTED.json")
                    .exists()
            );
            continue;
        }
        assert_eq!(result.state, JobState::Succeeded, "{:?}", result.attempts);
        let manifest = runner::verify_output(&root, job.id).unwrap();
        let reader = arrow::ipc::reader::StreamReader::try_new(
            fs::File::open(
                root.join("jobs")
                    .join(job.id.to_string())
                    .join(&manifest.file),
            )
            .unwrap(),
            None,
        )
        .unwrap();
        let mut rows = 0;
        for batch in reader {
            let batch = batch.unwrap();
            let key = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
                .unwrap();
            assert_eq!(batch.num_columns(), 1);
            assert!(key.values().iter().all(|v| *v == 7));
            rows += batch.num_rows();
        }
        assert_eq!(rows, 1_000_000);
        assert_eq!(manifest.rows, 1_000_000);
    }
}

#[test]
fn total_staging_exact_cap_and_one_less_count_ipc_plus_manifest() {
    let (dir, mut recipe) = support::fixture(false);
    let root = support::state_path(&dir);
    let mut store = Store::init(&root, Limits::default()).unwrap();
    let (_workers, workers) =
        support::managed_workers(&dir.path().canonicalize().unwrap(), store.id(), 2);
    recipe.workers = workers;
    let first = store.submit("size", recipe.clone()).unwrap();
    drop(store);
    assert_eq!(
        runner::run(&root, first.id).unwrap().state,
        JobState::Succeeded
    );
    let manifest = runner::verify_output(&root, first.id).unwrap();
    let total = manifest.bytes + serde_json::to_vec(&manifest).unwrap().len() as u64;
    for (name, cap, expected) in [
        ("exact", total, JobState::Succeeded),
        ("less", total - 1, JobState::Failed),
    ] {
        recipe.staging_bytes = cap;
        let job = Store::open(&root)
            .unwrap()
            .submit(name, recipe.clone())
            .unwrap();
        let result = runner::run(&root, job.id).unwrap();
        assert_eq!(result.state, expected, "cap={cap} {:?}", result.attempts);
        assert_eq!(
            root.join("jobs")
                .join(job.id.to_string())
                .join("COMMITTED.json")
                .exists(),
            expected == JobState::Succeeded
        );
    }
}
