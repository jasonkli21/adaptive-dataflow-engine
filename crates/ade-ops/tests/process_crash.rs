#![cfg(feature = "fault-injection")]
mod support;
use ade_ops::{JobState, Limits, Store};
use std::{
    fs,
    io::{BufRead, BufReader},
    os::unix::{fs::PermissionsExt, process::ExitStatusExt},
    path::Path,
    process::{Command, Stdio},
    sync::mpsc,
    time::Duration,
};
fn kill_at(root: &Path, command: &[&str], stage: &str) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_ade-batch"))
        .arg("--state")
        .arg(root)
        .args(command)
        .env("ADE_OPS_FAULT", stage)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stderr = child.stderr.take().unwrap();
    let (tx, rx) = mpsc::sync_channel(1);
    let expected = format!("ADE_OPS_REACHED {stage}");
    let witness = std::thread::spawn(move || {
        let mut diagnostics = vec![];
        for line in BufReader::new(stderr).lines() {
            let line = line.unwrap();
            diagnostics.push(line.clone());
            if line == expected {
                tx.send(line).unwrap();
                return diagnostics;
            }
        }
        diagnostics
    });
    let reached = rx.recv_timeout(Duration::from_secs(15));
    // Child::kill is SIGKILL on the supported Unix platforms. Reap every child,
    // including a fixture failure, so tests never leave an owner lock behind.
    let owner_refused = reached.is_ok()
        && (command[0] == "exercise-publication"
            || matches!(Store::open(root), Err(ade_ops::Error::Owned)));
    let _ = child.kill();
    let status = child.wait().unwrap();
    let diagnostics = witness.join().unwrap();
    assert!(
        reached.is_ok(),
        "did not reach {stage}: {status}; {diagnostics:?}"
    );
    assert!(owner_refused, "concurrent owner must refuse");
    assert_eq!(status.signal(), Some(9));
    eprintln!("phase12 fixture witness: {stage}; SIGKILL=9");
}
#[test]
fn real_owner_kill_at_durable_claim_never_replays_ambiguous_attempt() {
    let (dir, recipe) = support::fixture(false);
    let root = support::state_path(&dir);
    let mut store = Store::init(&root, Limits::default()).unwrap();
    let job = store.submit("crash", recipe).unwrap();
    drop(store);
    kill_at(&root, &["exercise-claim", &job.id.to_string()], "claimed");
    for first in [true, false, false] {
        let mut store = support::open_store(&root);
        let before = store.status(job.id).unwrap();
        if first {
            assert_eq!(before.state, JobState::Claimed);
            assert_eq!(before.attempt, 1);
            assert!(before.instance.is_some());
        }
        store.recover().unwrap();
        let after = store.status(job.id).unwrap();
        assert_eq!(after.state, JobState::NeedsOperator);
        assert_eq!(after.attempt, 1);
        assert_eq!(after.instance, before.instance);
        assert!(store.run(job.id).is_err());
        assert!(!root.join("COMMITTED.json").exists());
    }
}
#[test]
fn snapshot_kill_before_and_after_atomic_replacement_preserves_valid_generation() {
    for stage in ["snapshot_synced", "snapshot_renamed"] {
        let dir = tempfile::tempdir().unwrap();
        let root = support::state_path(&dir);
        let store = Store::init(&root, Limits::default()).unwrap();
        let generation = store.generation();
        drop(store);
        kill_at(&root, &["exercise-owner"], stage);
        let raw: serde_json::Value =
            serde_json::from_slice(&fs::read(root.join("state.json")).unwrap()).unwrap();
        assert_eq!(
            raw["data"]["generation"].as_u64().unwrap(),
            generation + u64::from(stage == "snapshot_renamed")
        );
        let mut store = Store::open(&root).unwrap();
        assert!(store.list(None, 32).unwrap().is_empty());
        assert!(!store.cleanup().unwrap());
    }
}
#[test]
fn publication_kill_windows_expose_zero_or_one_verified_marker() {
    use ade_ops::publication::{self, Identity, Limits};
    use ade_physical::{Field, Type};
    use uuid::Uuid;
    for stage in [
        "output_synced",
        "manifest_synced",
        "marker_published",
        "marker_durable",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        kill_at(&root, &["exercise-publication"], stage);
        let visible = root.join("COMMITTED.json").exists();
        assert_eq!(visible, stage.starts_with("marker_"));
        if visible {
            let id = Identity {
                store: Uuid::from_u128(1),
                job: Uuid::from_u128(2),
                instance: Uuid::from_u128(3),
                owner: 1,
                engine_job: 1,
                plan_hash: "a".repeat(64),
                source_hash: "b".repeat(64),
                config_hash: "c".repeat(64),
                workers_hash: "d".repeat(64),
            };
            for _ in 0..3 {
                publication::verify(
                    &root,
                    &id,
                    &vec![Field::new("value", Type::I64, false)],
                    Limits {
                        rows: 0,
                        bytes: 4096,
                    },
                    &ade_exec::CancellationToken::default(),
                    std::time::Instant::now() + Duration::from_secs(5),
                )
                .unwrap();
            }
            let digest = blake3::hash(&fs::read(root.join("COMMITTED.json")).unwrap())
                .to_hex()
                .to_string();
            let output = Command::new(env!("CARGO_BIN_EXE_ade-batch"))
                .arg("--state")
                .arg(&root)
                .arg("exercise-publication")
                .output()
                .unwrap();
            assert!(!output.status.success());
            assert_eq!(
                digest,
                blake3::hash(&fs::read(root.join("COMMITTED.json")).unwrap())
                    .to_hex()
                    .to_string()
            );
            eprintln!("phase12 marker verified: {stage}; digest={digest}");
        }
    }
}
