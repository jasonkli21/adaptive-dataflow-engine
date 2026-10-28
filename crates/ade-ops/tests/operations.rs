mod support;
use ade_ops::{Error, JobState, Limits, Store};
use std::{
    fs,
    os::unix::fs::{PermissionsExt, symlink},
};

#[test]
fn both_recipes_are_durable_idempotent_and_execution_refuses_without_contacts() {
    for join in [false, true] {
        let (dir, mut recipe) = support::fixture(join);
        // Real listening endpoints prove run never dispatches worker RPCs.
        let listeners: Vec<_> = (0..4)
            .map(|_| {
                let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
                l.set_nonblocking(true).unwrap();
                l
            })
            .collect();
        for (i, w) in recipe.workers.iter_mut().enumerate() {
            w.control = listeners[2 * i].local_addr().unwrap();
            w.exchange = listeners[2 * i + 1].local_addr().unwrap();
        }
        let path = support::state_path(&dir);
        let mut store = Store::init(&path, Limits::default()).unwrap();
        let id = store.id();
        let generation = store.generation();
        let job = store.submit("request-1", recipe.clone()).unwrap();
        assert!(matches!(Store::open(&path), Err(Error::Owned)));
        let duplicate = store.submit("request-1", recipe.clone()).unwrap();
        assert_eq!(duplicate.id, job.id);
        assert_eq!(duplicate.expires_ms, job.expires_ms);
        let mut changed = recipe.clone();
        changed.staging_bytes += 1;
        assert!(store.submit("request-1", changed).is_err());
        store.verify_sources(job.id).unwrap();
        assert!(matches!(store.run(job.id), Err(Error::Unsupported(_))));
        for l in listeners {
            assert_eq!(
                l.accept().unwrap_err().kind(),
                std::io::ErrorKind::WouldBlock
            );
        }
        assert_eq!(store.status(job.id).unwrap().state, JobState::Pending);
        assert!(!path.join("COMMITTED.json").exists());
        drop(store);
        let mut store = Store::open(&path).unwrap();
        assert_eq!(id, store.id());
        assert_eq!(store.generation(), generation);
        assert_eq!(store.list(None, 1).unwrap()[0].id, job.id);
        assert!(store.list(Some(job.id), 32).unwrap().is_empty());
        assert!(store.list(None, 0).is_err());
        assert!(store.list(None, 33).is_err());
        let cancelled = store.cancel(job.id).unwrap();
        assert_eq!(cancelled.state, JobState::Cancelled);
        assert_eq!(cancelled.sequence, store.cancel(job.id).unwrap().sequence);
        assert!(store.run(job.id).is_err());
        assert!(store.recover().unwrap().is_empty());
        assert_eq!(
            store.submit("request-1", recipe).unwrap().state,
            JobState::Cancelled
        );
    }
}
#[test]
fn queue_history_event_and_recipe_limits_fail_closed() {
    let (dir, recipe) = support::fixture(false);
    let path = support::state_path(&dir);
    assert!(
        Store::init(
            &path,
            Limits {
                jobs: 1,
                queued: 2,
                events_per_job: 4
            }
        )
        .is_err()
    );
    let mut store = Store::init(
        &path,
        Limits {
            jobs: 2,
            queued: 1,
            events_per_job: 4,
        },
    )
    .unwrap();
    let a = store.submit("a", recipe.clone()).unwrap();
    assert!(matches!(
        store.submit("b", recipe.clone()),
        Err(Error::Quota("queued jobs"))
    ));
    for _ in 0..8 {
        assert!(store.run(a.id).is_err());
    }
    let status = store.status(a.id).unwrap();
    assert_eq!(status.events.len(), 4);
    assert_eq!(status.sequence, 9);
    store.cancel(a.id).unwrap();
    let b = store.submit("b", recipe.clone()).unwrap();
    store.cancel(b.id).unwrap();
    assert!(matches!(
        store.submit("c", recipe.clone()),
        Err(Error::Quota("retained jobs"))
    ));
    assert!(store.submit(&"x".repeat(129), recipe.clone()).is_err());
    assert!(store.submit("../escape", recipe.clone()).is_err());
    let mut malformed = recipe.clone();
    malformed.max_attempts = 4;
    assert!(malformed.validate().is_err());
    malformed = recipe.clone();
    malformed.timeout_ms = 120_001;
    assert!(malformed.validate().is_err());
    malformed = recipe.clone();
    malformed.workers[0].control = "192.0.2.1:19".parse().unwrap();
    assert!(malformed.validate().is_err());
    malformed = recipe.clone();
    malformed.plan.version = 2;
    assert!(malformed.validate().is_err());
    malformed = recipe;
    malformed.staging_bytes = 64 * 1024 * 1024 + 1;
    assert!(malformed.validate().is_err());
    assert!(!path.join("COMMITTED.json").exists());
}
#[test]
fn input_drift_rename_manifest_and_symlink_refuse() {
    for attack in ["mutate", "rename", "symlink", "manifest"] {
        let (dir, recipe) = support::fixture(false);
        let path = support::state_path(&dir);
        let data = recipe.inputs[0].root.clone();
        let mut store = Store::init(&path, Limits::default()).unwrap();
        let job = store.submit("pinned", recipe.clone()).unwrap();
        let input = data.join("a.parquet");
        match attack {
            "mutate" => {
                let mut bytes = fs::read(&input).unwrap();
                bytes[12] ^= 1;
                fs::write(&input, bytes).unwrap();
            }
            "rename" => {
                let bytes = fs::read(&input).unwrap();
                fs::write(data.join("replacement"), bytes).unwrap();
                fs::rename(data.join("replacement"), &input).unwrap();
            }
            "symlink" => {
                fs::rename(&input, data.join("original")).unwrap();
                symlink(data.join("original"), &input).unwrap();
            }
            _ => {
                fs::write(data.join("manifest.json"), b"{}").unwrap();
            }
        }
        assert!(store.verify_sources(job.id).is_err(), "{attack}");
        if attack != "rename" {
            assert!(store.submit("new", recipe).is_err(), "{attack}");
        }
        assert!(!path.join("COMMITTED.json").exists());
    }
}
#[test]
fn corrupt_unknown_version_missing_store_and_unsafe_paths_never_initialize() {
    for attack in [
        "corrupt",
        "version",
        "checksum",
        "unknown",
        "missing",
        "permissions",
        "symlink",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let path = support::state_path(&dir);
        drop(Store::init(&path, Limits::default()).unwrap());
        let state = path.join("state.json");
        let original = fs::read(&state).unwrap();
        match attack {
            "corrupt" => fs::write(&state, b"{").unwrap(),
            "missing" => fs::remove_file(&state).unwrap(),
            "permissions" => {
                fs::set_permissions(&state, fs::Permissions::from_mode(0o644)).unwrap();
            }
            "symlink" => {
                fs::rename(&state, path.join("other")).unwrap();
                symlink(path.join("other"), &state).unwrap();
            }
            _ => {
                let mut value: serde_json::Value = serde_json::from_slice(&original).unwrap();
                match attack {
                    "version" => value["version"] = 99.into(),
                    "checksum" => value["checksum"] = "a".repeat(64).into(),
                    _ => value["unexpected"] = true.into(),
                }
                fs::write(&state, serde_json::to_vec(&value).unwrap()).unwrap();
            }
        }
        assert!(Store::open(&path).is_err(), "{attack}");
        assert!(Store::init(&path, Limits::default()).is_err());
    }
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let path = root.join("state");
    drop(Store::init(&path, Limits::default()).unwrap());
    symlink(&path, root.join("alias")).unwrap();
    assert!(Store::open(&root.join("alias")).is_err());
}
#[test]
fn cleanup_deletes_only_abandoned_transaction_and_preserves_every_other_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = support::state_path(&dir);
    let mut store = Store::init(&path, Limits::default()).unwrap();
    fs::write(path.join("unknown"), b"preserve").unwrap();
    fs::write(path.join("state.next"), b"incomplete").unwrap();
    fs::set_permissions(path.join("state.next"), fs::Permissions::from_mode(0o600)).unwrap();
    assert!(store.cleanup().unwrap());
    assert!(!store.cleanup().unwrap());
    assert_eq!(fs::read(path.join("unknown")).unwrap(), b"preserve");
    symlink(path.join("unknown"), path.join("state.next")).unwrap();
    assert!(store.cleanup().is_err());
}
