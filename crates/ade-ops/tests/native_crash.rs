#![cfg(feature = "fault-injection")]
mod support;
use ade_ops::{JobState, Limits, Store, runner};
use std::{
    fs,
    io::{BufRead, BufReader},
    os::unix::process::ExitStatusExt,
    path::Path,
    process::{Child, Command, Stdio},
    sync::mpsc,
    time::Duration,
};
fn coordinator(root: &Path, id: uuid::Uuid, fault: Option<&str>) -> Child {
    let mut command = Command::new(env!("CARGO_BIN_EXE_ade-batch"));
    command
        .arg("--state")
        .arg(root)
        .arg("run")
        .arg(id.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    if let Some(fault) = fault {
        command.env("ADE_OPS_FAULT", fault);
    }
    command.spawn().unwrap()
}
fn killed_at(child: &mut Child, stage: &str) {
    let stderr = child.stderr.take().unwrap();
    let (tx, rx) = mpsc::sync_channel(1);
    let expected = format!("ADE_OPS_REACHED {stage}");
    let reader = std::thread::spawn(move || {
        let mut diagnostics = vec![];
        for line in BufReader::new(stderr).lines() {
            let line = line.unwrap();
            diagnostics.push(line.clone());
            if line == expected {
                tx.send(()).unwrap();
                return diagnostics;
            }
        }
        diagnostics
    });
    let reached = rx.recv_timeout(Duration::from_secs(15));
    let _ = child.kill();
    let status = child.wait().unwrap();
    let diagnostics = reader.join().unwrap();
    assert!(reached.is_ok(), "missing native {stage}: {diagnostics:?}");
    assert_eq!(status.signal(), Some(9));
    eprintln!("ADE_NATIVE_KILL_WITNESS {stage} SIGKILL=9");
}
#[test]
fn coordinator_kills_recover_exactly_one_native_result_across_both_shapes() {
    for join in [false, true] {
        for stage in [
            "claimed_native",
            "prepared",
            "gather",
            "staged",
            "prepublish",
            "marker_published",
            "published_before_success",
        ] {
            let (dir, mut recipe) = support::fixture(join);
            let root = support::state_path(&dir);
            let mut store = Store::init(&root, Limits::default()).unwrap();
            let (_workers, workers) =
                support::managed_workers(&dir.path().canonicalize().unwrap(), store.id(), 2);
            recipe.workers = workers;
            recipe.max_attempts = 3;
            let job = store.submit("crash", recipe).unwrap();
            drop(store);
            let mut child = coordinator(&root, job.id, Some(stage));
            killed_at(&mut child, stage);
            let marker = root
                .join("jobs")
                .join(job.id.to_string())
                .join("COMMITTED.json");
            let published = marker.exists();
            assert_eq!(
                published,
                matches!(stage, "marker_published" | "published_before_success")
            );
            let succeeded = runner::run(&root, job.id).unwrap();
            assert_eq!(
                succeeded.state,
                JobState::Succeeded,
                "{join} {stage}: {:?}",
                succeeded.attempts
            );
            let result = runner::verify_output(&root, job.id).unwrap();
            assert_eq!(result.rows, if join { 5 } else { 2 });
            support::assert_recovered_fixture(&root, job.id, join, stage);
            let identity = fs::read(&marker).unwrap();
            for _ in 0..2 {
                runner::recover(&root).unwrap();
                assert_eq!(
                    runner::run(&root, job.id).unwrap().state,
                    JobState::Succeeded
                );
                assert_eq!(fs::read(&marker).unwrap(), identity);
            }
            let ids: std::collections::BTreeSet<_> =
                succeeded.attempts.iter().map(|a| a.engine_job).collect();
            let instances: std::collections::BTreeSet<_> =
                succeeded.attempts.iter().map(|a| a.instance).collect();
            assert_eq!(ids.len(), succeeded.attempts.len());
            assert_eq!(instances.len(), succeeded.attempts.len());
            runner::gc(&root, job.id, true).unwrap();
            for attempt in &succeeded.attempts {
                assert!(
                    !root
                        .join("scratch")
                        .join(attempt.instance.to_string())
                        .exists()
                );
            }
            runner::verify_output(&root, job.id).unwrap();
        }
    }
}
#[test]
fn real_producer_prefix_kill_drains_original_tasks_before_fresh_instance() {
    for stage in ["producer", "gather"] {
        let (dir, mut recipe) = support::fixture(false);
        let root = support::state_path(&dir);
        let mut store = Store::init(&root, Limits::default()).unwrap();
        let (workers, endpoints) = support::managed_workers_with(
            &dir.path().canonicalize().unwrap(),
            store.id(),
            2,
            &[("ADE_MANAGED_PAUSE", stage)],
        );
        recipe.workers = endpoints;
        recipe.max_attempts = 3;
        let job = store.submit("prefix", recipe).unwrap();
        drop(store);
        let mut child = coordinator(&root, job.id, None);
        let witness = workers
            .witnesses
            .recv_timeout(Duration::from_secs(15))
            .unwrap();
        assert_eq!(witness, format!("ADE_MANAGED_REACHED {stage}"));
        assert!(matches!(
            runner::run(&root, job.id),
            Err(ade_ops::Error::Owned)
        ));
        child.kill().unwrap();
        assert_eq!(child.wait().unwrap().signal(), Some(9));
        let succeeded = runner::run(&root, job.id).unwrap();
        assert_eq!(
            succeeded.state,
            JobState::Succeeded,
            "{:?}",
            succeeded.attempts
        );
        assert_eq!(succeeded.attempt, 2);
        runner::verify_output(&root, job.id).unwrap();
        eprintln!(
            "ADE_NATIVE_KILL_WITNESS real_{stage}_prefix; distinct instances and drain confirmed"
        );
    }
}
#[test]
fn durable_cancel_running_and_staged_never_publishes_and_late_cancel_preserves_success() {
    let (dir, mut recipe) = support::fixture(false);
    let root = support::state_path(&dir);
    let mut store = Store::init(&root, Limits::default()).unwrap();
    let (workers, endpoints) = support::managed_workers_with(
        &dir.path().canonicalize().unwrap(),
        store.id(),
        2,
        &[("ADE_MANAGED_PAUSE", "producer")],
    );
    recipe.workers = endpoints;
    let job = store.submit("cancel-running", recipe).unwrap();
    drop(store);
    let mut child = coordinator(&root, job.id, None);
    assert_eq!(
        workers
            .witnesses
            .recv_timeout(Duration::from_secs(15))
            .unwrap(),
        "ADE_MANAGED_REACHED producer"
    );
    assert!(runner::cancel(&root, job.id).unwrap().cancel_requested);
    let status = child.wait().unwrap();
    assert!(!status.success());
    assert_eq!(
        support::open_store(&root).status(job.id).unwrap().state,
        JobState::Cancelled
    );
    assert!(
        !root
            .join("jobs")
            .join(job.id.to_string())
            .join("COMMITTED.json")
            .exists()
    );
    for stage in ["staged", "marker_published"] {
        let (dir, mut recipe) = support::fixture(false);
        let root = support::state_path(&dir);
        let mut store = Store::init(&root, Limits::default()).unwrap();
        let (_workers, endpoints) =
            support::managed_workers(&dir.path().canonicalize().unwrap(), store.id(), 2);
        recipe.workers = endpoints;
        let job = store.submit("cancel-race", recipe).unwrap();
        drop(store);
        let mut child = coordinator(&root, job.id, Some(stage));
        // Wait for the exact point while keeping the owner alive to submit intent
        // for STAGED (outside metadata lock). For the published case kill first.
        let stderr = child.stderr.take().unwrap();
        let (tx, rx) = mpsc::sync_channel(1);
        let expected = format!("ADE_OPS_REACHED {stage}");
        let reader = std::thread::spawn(move || {
            for line in BufReader::new(stderr).lines() {
                if line.unwrap() == expected {
                    tx.send(()).unwrap();
                    return;
                }
            }
        });
        rx.recv_timeout(Duration::from_secs(15)).unwrap();
        if stage == "staged" {
            runner::cancel(&root, job.id).unwrap();
        }
        child.kill().unwrap();
        child.wait().unwrap();
        reader.join().unwrap();
        let cancelled = runner::cancel(&root, job.id).unwrap();
        let final_job = runner::run(&root, job.id).unwrap();
        assert_eq!(
            final_job.state,
            if stage == "staged" {
                JobState::Cancelled
            } else {
                JobState::Succeeded
            }
        );
        if stage == "marker_published" {
            assert_eq!(cancelled.state, JobState::Succeeded);
            runner::verify_output(&root, job.id).unwrap();
        }
    }
}
#[test]
fn finite_transient_retry_backoff_and_restart_use_new_engine_identity() {
    for restart in [false, true] {
        let (dir, mut recipe) = support::fixture(false);
        let root = support::state_path(&dir);
        let mut store = Store::init(&root, Limits::default()).unwrap();
        let (_workers, endpoints) = support::managed_workers_with(
            &dir.path().canonicalize().unwrap(),
            store.id(),
            2,
            &[("ADE_MANAGED_FAIL_ONCE", "1")],
        );
        recipe.workers = endpoints;
        recipe.max_attempts = 3;
        let job = store.submit("retry", recipe).unwrap();
        drop(store);
        if restart {
            let mut child = coordinator(&root, job.id, Some("retry_wait"));
            killed_at(&mut child, "retry_wait");
        }
        let succeeded = runner::run(&root, job.id).unwrap();
        assert_eq!(
            succeeded.state,
            JobState::Succeeded,
            "{:?}",
            succeeded.attempts
        );
        assert_eq!(succeeded.attempt, 2);
        assert_ne!(
            succeeded.attempts[0].engine_job,
            succeeded.attempts[1].engine_job
        );
        assert_ne!(
            succeeded.attempts[0].instance,
            succeeded.attempts[1].instance
        );
        assert_eq!(succeeded.expires_ms, job.expires_ms);
        runner::verify_output(&root, job.id).unwrap();
    }
}

#[test]
fn injected_output_io_failures_are_atomic_and_ambiguous_publication_reconciles_once() {
    for stage in [
        "output_batch",
        "data_sync",
        "manifest_sync",
        "link",
        "exdev",
        "marker_sync",
        "success_write",
    ] {
        let (dir, mut recipe) = support::fixture(false);
        let root = support::state_path(&dir);
        let mut store = Store::init(&root, Limits::default()).unwrap();
        let (_workers, endpoints) =
            support::managed_workers(&dir.path().canonicalize().unwrap(), store.id(), 2);
        recipe.workers = endpoints;
        let job = store.submit("io-fault", recipe).unwrap();
        drop(store);
        let output = Command::new(env!("CARGO_BIN_EXE_ade-batch"))
            .arg("--state")
            .arg(&root)
            .arg("run")
            .arg(job.id.to_string())
            .env("ADE_OPS_FAIL_IO", stage)
            .output()
            .unwrap();
        assert!(
            !output.status.success(),
            "{stage}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(output.stdout, Vec::<u8>::new());
        let diagnostics = String::from_utf8_lossy(&output.stderr);
        assert!(
            diagnostics.contains(&format!("ADE_OPS_IO_WITNESS {stage}")),
            "{diagnostics}"
        );
        let marker = root
            .join("jobs")
            .join(job.id.to_string())
            .join("COMMITTED.json");
        if matches!(stage, "marker_sync" | "success_write") {
            assert!(marker.exists());
            let before = fs::read(&marker).unwrap();
            let recovered = runner::recover(&root).unwrap();
            assert_eq!(recovered[0].state, JobState::Succeeded);
            assert_eq!(recovered[0].attempt, 1);
            runner::verify_output(&root, job.id).unwrap();
            assert_eq!(before, fs::read(marker).unwrap());
        } else {
            assert!(!marker.exists());
            let failed = support::open_store(&root).status(job.id).unwrap();
            assert_eq!(
                failed.state,
                JobState::Failed,
                "{stage}: {:?}",
                failed.attempts
            );
            let dry = runner::gc(&root, job.id, false).unwrap();
            assert_ne!(dry.files.len(), 0);
            let apply = runner::gc(&root, job.id, true).unwrap();
            assert_eq!(apply.files, dry.files);
            assert_eq!(runner::gc(&root, job.id, true).unwrap().files.len(), 0);
        }
    }
}
#[test]
fn corrupt_staged_and_foreign_markers_freeze_without_replay_or_overwrite() {
    for attack in ["data", "candidate", "foreign-marker", "source"] {
        let (dir, mut recipe) = support::fixture(false);
        let root = support::state_path(&dir);
        let mut store = Store::init(&root, Limits::default()).unwrap();
        let (_workers, endpoints) =
            support::managed_workers(&dir.path().canonicalize().unwrap(), store.id(), 2);
        recipe.workers = endpoints;
        let job = store.submit("corrupt-stage", recipe.clone()).unwrap();
        drop(store);
        let mut child = coordinator(&root, job.id, Some("staged"));
        killed_at(&mut child, "staged");
        let staged = support::open_store(&root).status(job.id).unwrap();
        let manifest = staged.output.as_ref().unwrap();
        let directory = root.join("jobs").join(job.id.to_string());
        let marker = directory.join("COMMITTED.json");
        match attack {
            "data" => {
                let path = directory.join(&manifest.file);
                let mut bytes = fs::read(&path).unwrap();
                bytes[12] ^= 1;
                fs::write(path, bytes).unwrap();
            }
            "candidate" => {
                fs::write(
                    directory.join(format!("manifest-{}.json", manifest.identity.instance)),
                    b"{}",
                )
                .unwrap();
            }
            "foreign-marker" => {
                fs::write(&marker, b"foreign").unwrap();
            }
            _ => {
                let path = recipe.inputs[0].root.join("a.parquet");
                let mut bytes = fs::read(&path).unwrap();
                bytes[10] ^= 1;
                fs::write(path, bytes).unwrap();
            }
        }
        let outcome = runner::run(&root, job.id);
        if attack == "foreign-marker" {
            assert!(outcome.is_err());
            assert_eq!(fs::read(&marker).unwrap(), b"foreign");
        } else {
            assert_eq!(
                outcome.unwrap().state,
                if attack == "source" {
                    JobState::Failed
                } else {
                    JobState::NeedsOperator
                }
            );
            assert!(!marker.exists());
        }
        let frozen = support::open_store(&root).status(job.id).unwrap();
        assert_eq!(frozen.attempt, 1);
        assert!(runner::verify_output(&root, job.id).is_err());
    }
}
#[test]
fn worker_loss_boot_registry_change_and_poisoned_quota_never_trigger_blind_retry() {
    for attack in ["dead", "boot", "poison"] {
        let (dir, mut recipe) = support::fixture(attack == "poison");
        let root = support::state_path(&dir);
        let mut store = Store::init(&root, Limits::default()).unwrap();
        let (mut workers, endpoints) =
            support::managed_workers(&dir.path().canonicalize().unwrap(), store.id(), 2);
        recipe.workers = endpoints;
        recipe.max_attempts = 3;
        if attack == "poison" {
            recipe.runtime_bytes = 1;
        }
        let job = store.submit("unsafe-worker", recipe).unwrap();
        drop(store);
        if attack == "dead" {
            workers.children[0].kill().unwrap();
            workers.children[0].wait().unwrap();
        }
        if attack == "boot" {
            let mut child = coordinator(&root, job.id, Some("prepared"));
            killed_at(&mut child, "prepared");
            workers.children[0].kill().unwrap();
            workers.children[0].wait().unwrap();
            let store = support::open_store(&root);
            let pinned = store.status(job.id).unwrap();
            let store_id = store.id();
            drop(store);
            let w = &pinned.recipe.workers[0];
            let binary = std::path::PathBuf::from(env!("CARGO_BIN_EXE_ade-batch"))
                .with_file_name("ade-worker");
            let mut replacement = Command::new(binary)
                .args([
                    "--managed",
                    "0",
                    w.scratch_root.to_str().unwrap(),
                    &store_id.to_string(),
                    &w.control.to_string(),
                    &w.exchange.to_string(),
                ])
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap();
            let ready: ade_distributed::managed::Ready = ade_distributed::managed::read_message(
                &mut BufReader::new(replacement.stdout.take().unwrap()),
            )
            .unwrap();
            assert_ne!(ready.boot.instance, w.instance.to_string());
            workers.children[0] = replacement;
        }
        let stopped = runner::run(&root, job.id).unwrap();
        assert_eq!(
            stopped.state,
            JobState::NeedsOperator,
            "{attack}: {:?}",
            stopped.attempts
        );
        assert_eq!(stopped.attempt, 1);
        assert!(
            !root
                .join("jobs")
                .join(job.id.to_string())
                .join("COMMITTED.json")
                .exists()
        );
        assert_eq!(runner::run(&root, job.id).unwrap().attempt, 1);
    }
}
#[test]
fn retry_limit_and_durable_backoff_cancellation_do_not_extend_deadline_or_publish() {
    for cancel in [false, true] {
        let (dir, mut recipe) = support::fixture(false);
        let root = support::state_path(&dir);
        let mut store = Store::init(&root, Limits::default()).unwrap();
        let (_workers, endpoints) = support::managed_workers_with(
            &dir.path().canonicalize().unwrap(),
            store.id(),
            2,
            &[("ADE_MANAGED_FAIL_ONCE", "1")],
        );
        recipe.workers = endpoints;
        recipe.max_attempts = if cancel { 3 } else { 1 };
        let job = store.submit("limit", recipe).unwrap();
        drop(store);
        if cancel {
            let mut child = coordinator(&root, job.id, Some("retry_wait"));
            killed_at(&mut child, "retry_wait");
            runner::cancel(&root, job.id).unwrap();
        }
        let result = runner::run(&root, job.id).unwrap();
        assert_eq!(
            result.state,
            if cancel {
                JobState::Cancelled
            } else {
                JobState::Failed
            }
        );
        assert_eq!(result.attempt, 1);
        assert_eq!(result.expires_ms, job.expires_ms);
        assert!(
            !root
                .join("jobs")
                .join(job.id.to_string())
                .join("COMMITTED.json")
                .exists()
        );
    }
}

#[test]
fn deadline_during_real_hash_reads_and_postflight_leaves_no_visible_result() {
    for stage in ["preflight", "postflight"] {
        let (dir, mut recipe) = support::fixture(false);
        let root = support::state_path(&dir);
        let mut store = Store::init(&root, Limits::default()).unwrap();
        let (_workers, endpoints) =
            support::managed_workers(&dir.path().canonicalize().unwrap(), store.id(), 2);
        recipe.workers = endpoints;
        recipe.timeout_ms = 2000;
        let job = store.submit("deadline", recipe).unwrap();
        drop(store);
        let output = Command::new(env!("CARGO_BIN_EXE_ade-batch"))
            .arg("--state")
            .arg(&root)
            .arg("run")
            .arg(job.id.to_string())
            .env("ADE_OPS_HASH_STAGE", stage)
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(
            String::from_utf8_lossy(&output.stderr)
                .contains(&format!("ADE_OPS_HASH_WITNESS {stage}"))
        );
        let failed = support::open_store(&root).status(job.id).unwrap();
        assert_eq!(failed.state, JobState::Failed);
        assert_eq!(failed.expires_ms, job.expires_ms);
        assert!(
            !root
                .join("jobs")
                .join(job.id.to_string())
                .join("COMMITTED.json")
                .exists()
        );
    }
}

#[test]
fn durable_cancel_interrupts_real_preflight_and_postflight_hash_reads() {
    for stage in ["preflight", "postflight"] {
        let (dir, mut recipe) = support::fixture(false);
        let root = support::state_path(&dir);
        let mut store = Store::init(&root, Limits::default()).unwrap();
        let (_workers, endpoints) =
            support::managed_workers(&dir.path().canonicalize().unwrap(), store.id(), 2);
        recipe.workers = endpoints;
        let job = store.submit("hash-cancel", recipe).unwrap();
        drop(store);
        let mut child = Command::new(env!("CARGO_BIN_EXE_ade-batch"))
            .arg("--state")
            .arg(&root)
            .arg("run")
            .arg(job.id.to_string())
            .env("ADE_OPS_HASH_STAGE", stage)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let stderr = child.stderr.take().unwrap();
        let (tx, rx) = mpsc::sync_channel(1);
        let expected = format!("ADE_OPS_HASH_WITNESS {stage}");
        let reader = std::thread::spawn(move || {
            for line in BufReader::new(stderr).lines() {
                if line.unwrap() == expected {
                    tx.send(()).unwrap();
                }
            }
        });
        rx.recv_timeout(Duration::from_secs(10)).unwrap();
        runner::cancel(&root, job.id).unwrap();
        runner::cancel(&root, job.id).unwrap();
        assert!(!child.wait().unwrap().success());
        reader.join().unwrap();
        let result = runner::run(&root, job.id).unwrap();
        assert_eq!(result.state, JobState::Cancelled);
        assert!(
            !root
                .join("jobs")
                .join(job.id.to_string())
                .join("COMMITTED.json")
                .exists()
        );
        eprintln!("ADE_NATIVE_CANCEL_HASH_WITNESS {stage}");
    }
}
