use ade_ops::publication::{self, Completion, Identity, Limits, Publisher, Terminal};
use ade_physical::{Field, Type};
use arrow::{array::Int64Array, record_batch::RecordBatch};
use std::{
    fs,
    os::unix::fs::{PermissionsExt, symlink},
    sync::Arc,
    time::{Duration, Instant},
};
use uuid::Uuid;
fn identity() -> Identity {
    Identity {
        store: Uuid::new_v4(),
        job: Uuid::new_v4(),
        instance: Uuid::new_v4(),
        owner: 1,
        engine_job: 7,
        plan_hash: "a".repeat(64),
        source_hash: "b".repeat(64),
        config_hash: "c".repeat(64),
        workers_hash: "d".repeat(64),
    }
}
fn schema() -> ade_physical::Schema {
    vec![Field::new("v", Type::I64, false)]
}
fn completion(rows: u64) -> Completion {
    Completion {
        workers: 2,
        terminals: vec![
            Terminal {
                partition: 0,
                attempt: 0,
                rows,
                fingerprint: "e".repeat(64),
            },
            Terminal {
                partition: 1,
                attempt: 0,
                rows: 0,
                fingerprint: "f".repeat(64),
            },
        ],
    }
}
fn until() -> Instant {
    Instant::now() + Duration::from_secs(5)
}
fn batch() -> RecordBatch {
    RecordBatch::try_new(
        ade_exec::physical::arrow_schema(&schema()),
        vec![Arc::new(Int64Array::from(vec![-9, 4, 7]))],
    )
    .unwrap()
}
fn limits() -> Limits {
    Limits {
        rows: 3,
        bytes: 4096,
    }
}
fn stage(root: &std::path::Path, id: Identity, cap: Limits) -> Publisher {
    Publisher::stage(
        root,
        id,
        schema(),
        completion(3),
        cap,
        ade_exec::CancellationToken::default(),
        until(),
    )
    .unwrap()
}
#[test]
fn exact_bytes_and_rows_atomic_publication_independent_read_and_late_cancel() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    let id = identity();
    let mut publisher = stage(&root, id.clone(), limits());
    publisher.push(&batch()).unwrap();
    assert!(!root.join("COMMITTED.json").exists());
    let manifest = publisher.publish().unwrap();
    assert!(manifest.bytes > 0);
    assert_eq!(manifest.rows, 3);
    let verified = publication::verify(
        &root,
        &id,
        &schema(),
        limits(),
        &ade_exec::CancellationToken::default(),
        until(),
    )
    .unwrap();
    assert_eq!(manifest.blake3, verified.blake3);
    let bytes = fs::read(root.join("COMMITTED.json")).unwrap();
    let reader = arrow::ipc::reader::StreamReader::try_new(
        fs::File::open(root.join(&manifest.file)).unwrap(),
        None,
    )
    .unwrap();
    let values: Vec<_> = reader
        .flat_map(|b| {
            b.unwrap()
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(values, vec![-9, 4, 7]);
    assert!(
        Publisher::stage(
            &root,
            identity(),
            schema(),
            completion(3),
            limits(),
            ade_exec::CancellationToken::default(),
            until()
        )
        .is_err()
    );
    let token = ade_exec::CancellationToken::default();
    token.cancel();
    assert!(publication::verify(&root, &id, &schema(), limits(), &token, until()).is_err());
    assert_eq!(fs::read(root.join("COMMITTED.json")).unwrap(), bytes);
    for delta in [0, 1] {
        let d = tempfile::tempdir().unwrap();
        let r = d.path().canonicalize().unwrap();
        fs::set_permissions(&r, fs::Permissions::from_mode(0o700)).unwrap();
        let next = identity();
        let mut p = stage(
            &r,
            next.clone(),
            Limits {
                rows: 3,
                bytes: manifest.bytes - delta,
            },
        );
        let wrote = p.push(&batch());
        let result = if wrote.is_ok() {
            p.publish()
        } else {
            assert!(p.publish().is_err());
            assert!(!r.join("COMMITTED.json").exists());
            continue;
        };
        if delta == 0 {
            assert_eq!(result.unwrap().bytes, manifest.bytes);
        } else {
            assert!(result.is_err());
            assert!(!r.join("COMMITTED.json").exists());
        }
    }
}
#[test]
fn failed_append_aborts_even_when_prior_rows_match_claimed_completion() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    let mut p = stage(&root, identity(), limits());
    p.push(&batch()).unwrap();
    assert!(p.push(&batch()).is_err());
    assert!(p.publish().is_err());
    assert!(!root.join("COMMITTED.json").exists());
}
#[test]
fn cancellation_deadline_schema_nullability_and_coverage_fail_without_marker() {
    for attack in [
        "cancel",
        "deadline",
        "schema",
        "null",
        "coverage",
        "duplicate",
        "overflow",
        "rows",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        let token = ade_exec::CancellationToken::default();
        let mut complete = completion(3);
        let mut cap = limits();
        match attack {
            "coverage" => complete.terminals.pop().map(|_| ()).unwrap(),
            "duplicate" => complete.terminals[1].partition = 0,
            "overflow" => {
                complete.terminals[0].rows = u64::MAX;
                complete.terminals[1].rows = 1;
            }
            "rows" => cap.rows = 2,
            _ => (),
        }
        let stage = Publisher::stage(
            &root,
            identity(),
            schema(),
            complete,
            cap,
            token.clone(),
            if attack == "deadline" {
                Instant::now()
            } else {
                until()
            },
        );
        if let Ok(mut p) = stage {
            let mut b = batch();
            if attack == "schema" {
                b = RecordBatch::try_new(
                    ade_exec::physical::arrow_schema(&vec![Field::new("other", Type::I64, false)]),
                    vec![b.column(0).clone()],
                )
                .unwrap();
            }
            if attack == "null" {
                b = RecordBatch::try_new(
                    ade_exec::physical::arrow_schema(&vec![Field::new("v", Type::I64, true)]),
                    vec![Arc::new(Int64Array::from(vec![None, Some(4), Some(7)]))],
                )
                .unwrap();
            }
            if attack == "cancel" {
                token.cancel();
            }
            let _ = p.push(&b);
            assert!(p.publish().is_err(), "{attack}");
        }
        assert!(!root.join("COMMITTED.json").exists(), "{attack}");
    }
}
#[test]
fn marker_collision_mutation_truncation_reference_escape_and_foreign_identity_refuse() {
    for attack in [
        "mutate", "truncate", "path", "foreign", "marker", "symlink", "link",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        let id = identity();
        let mut p = stage(&root, id.clone(), limits());
        p.push(&batch()).unwrap();
        if attack == "marker" {
            fs::write(root.join("COMMITTED.json"), b"foreign").unwrap();
            assert!(p.publish().is_err());
            assert_eq!(fs::read(root.join("COMMITTED.json")).unwrap(), b"foreign");
            continue;
        }
        let manifest = p.publish().unwrap();
        let file = root.join(&manifest.file);
        match attack {
            "mutate" => {
                let mut data = fs::read(&file).unwrap();
                data[10] ^= 1;
                fs::write(&file, data).unwrap();
            }
            "truncate" => fs::OpenOptions::new()
                .write(true)
                .open(&file)
                .unwrap()
                .set_len(8)
                .unwrap(),
            "path" => {
                let mut changed = manifest.clone();
                changed.file = "../outside.arrow".into();
                fs::write(
                    root.join("COMMITTED.json"),
                    serde_json::to_vec(&changed).unwrap(),
                )
                .unwrap();
            }
            "foreign" => {
                let mut foreign = id.clone();
                foreign.instance = Uuid::new_v4();
                assert!(
                    publication::verify(
                        &root,
                        &foreign,
                        &schema(),
                        limits(),
                        &ade_exec::CancellationToken::default(),
                        until()
                    )
                    .is_err()
                );
                continue;
            }
            "symlink" => {
                fs::rename(&file, root.join("other")).unwrap();
                symlink(root.join("other"), &file).unwrap();
            }
            "link" => fs::hard_link(&file, root.join("extra-link")).unwrap(),
            _ => unreachable!(),
        }
        assert!(
            publication::verify(
                &root,
                &id,
                &schema(),
                limits(),
                &ade_exec::CancellationToken::default(),
                until()
            )
            .is_err(),
            "{attack}"
        );
    }
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o755)).unwrap();
    assert!(
        Publisher::stage(
            &root,
            identity(),
            schema(),
            completion(3),
            limits(),
            ade_exec::CancellationToken::default(),
            until()
        )
        .is_err()
    );
}
#[test]
fn empty_output_and_complete_set_are_valid_without_fabricating_a_job_success() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    let id = identity();
    let manifest = Publisher::stage(
        &root,
        id.clone(),
        schema(),
        completion(0),
        Limits {
            rows: 0,
            bytes: 4096,
        },
        ade_exec::CancellationToken::default(),
        until(),
    )
    .unwrap()
    .publish()
    .unwrap();
    assert_eq!(manifest.rows, 0);
    publication::verify(
        &root,
        &id,
        &schema(),
        Limits {
            rows: 0,
            bytes: 4096,
        },
        &ade_exec::CancellationToken::default(),
        until(),
    )
    .unwrap();
}
