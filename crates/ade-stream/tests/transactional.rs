#![cfg(feature = "transactional-state")]
use ade_exec::CancellationToken;
use ade_stream::{
    EventV1, RawRecord, Security, Start, StreamConfigV1,
    state::{self, store::CHARGE},
    transactional::{
        Config, Limits, Transaction,
        epoch::{self, Operation},
        savepoint, sink,
    },
};
use std::{fs, os::unix::fs::PermissionsExt, path::PathBuf};
use uuid::Uuid;
struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        let p = std::env::temp_dir()
            .canonicalize()
            .unwrap()
            .join(format!("ade17-{}", Uuid::new_v4()));
        mkdir(&p);
        Self(p)
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn mkdir(p: &std::path::Path) {
    fs::create_dir(p).unwrap();
    fs::set_permissions(p, fs::Permissions::from_mode(0o700)).unwrap();
}
fn config(t: &Temp) -> Config {
    for d in ["work", "check", "exports"] {
        mkdir(&t.0.join(d));
    }
    Config {
        version: 3,
        codec: 1,
        trusted_history: "test-immutable-incarnation".into(),
        limits: Limits {
            root_bytes: 32 * 1024 * 1024,
            stage_bytes: 1024 * 1024,
            output_bytes: 262_144,
            history_bytes: 1024 * 1024,
            epochs: 64,
            diagnostic_bytes: 1_048_576,
            savepoint_bytes: 64 * 1024 * 1024,
            savepoints: 8,
        },
        state: state::Config {
            version: 1,
            job_id: Uuid::new_v4(),
            source: StreamConfigV1 {
                version: 1,
                bootstrap: "127.0.0.1:1".into(),
                expected_cluster_id: "test".into(),
                topic: "test".into(),
                starts: vec![
                    Start {
                        partition: 0,
                        offset: 0,
                    },
                    Start {
                        partition: 1,
                        offset: 0,
                    },
                ],
                security: Security::LoopbackDev {},
                key_less_than: 1000,
                allowance_ms: 10,
                payload_bytes: 4096,
                client_queue_kib: 1,
                sink_stall_ms: 1000,
                timers: vec![],
            },
            topic_partitions: vec![0, 1],
            operator: "tumbling_sum_count_v1".into(),
            window_ms: 100,
            late_policy: "receipt_drop_closed_v1".into(),
            hash_version: 1,
            owners: 1,
            hot_bytes: CHARGE * 4,
            cold_bytes: 1024 * 1024,
            checkpoint_bytes: 16 * 1024 * 1024,
            max_generations: 64,
            checkpoint_every_records: 100,
            checkpoint_every_ms: 1000,
            working_root: t.0.join("work"),
            checkpoint_root: t.0.join("check"),
        },
    }
}
fn open(c: &Config, resume: bool) -> Transaction {
    Transaction::open(c.clone(), resume, CancellationToken::default()).unwrap()
}
fn event(txn: &mut Transaction, p: i32, off: i64, key: u64, value: i64, time: i64) {
    txn.accept(RawRecord {
        session: txn.engine.progress.session,
        partition: p,
        offset: off,
        payload: serde_json::to_vec(&EventV1 {
            version: 1,
            key,
            value_i64: value,
            event_time_ms: time,
        })
        .unwrap(),
    })
    .unwrap();
}
fn seed(c: &Config) -> epoch::Pointer {
    let mut tx = open(c, false);
    for i in 0..40 {
        event(
            &mut tx,
            i % 2,
            i64::from(i / 2),
            u64::try_from(i).unwrap(),
            1,
            -50,
        );
    }
    tx.commit(Operation::Commit, &mut |_| Ok(())).unwrap()
}
fn close(tx: &mut Transaction) {
    event(tx, 0, 20, 9999, 0, 200);
    event(tx, 1, 20, 9999, 0, 200);
}
const POINTS: &[&str] = &[
    "before_stage",
    "stage_created",
    "before_output_write",
    "output_half_written",
    "before_output_sync",
    "output_synced",
    "state_written",
    "state_synced",
    "before_manifest",
    "manifest_synced",
    "epoch_durable",
    "epoch_verified",
    "before_selector",
    "selector_half_written",
    "selector_synced",
    "selector_renamed",
    "selector_durable",
    "before_telemetry",
];
#[test]
fn every_failure_selects_whole_old_or_new_and_poison() {
    for point in POINTS {
        let temp = Temp::new();
        let c = config(&temp);
        let old = seed(&c);
        let mut txn = open(&c, true);
        close(&mut txn);
        assert_eq!(sink::read(&c).unwrap(), []);
        assert!(
            txn.commit(Operation::Paused, &mut |p| if p == *point {
                Err(ade_stream::Error::State("injected"))
            } else {
                Ok(())
            })
            .is_err()
        );
        assert!(txn.commit(Operation::Commit, &mut |_| Ok(())).is_err());
        drop(txn);
        let selected = epoch::selection(&c.state.checkpoint_root).unwrap().unwrap();
        let after = ["selector_renamed", "selector_durable", "before_telemetry"].contains(point);
        assert_eq!(selected == old, !after, "{point}");
        let rows = sink::read(&c).unwrap();
        assert_eq!(rows.len(), if after { 40 } else { 0 }, "{point}");
        let txn = open(&c, true);
        assert_eq!(
            txn.engine.progress.forwarded_records,
            if after { 42 } else { 40 }
        );
    }
}
#[test]
fn savepoint_codec_and_owners_round_trip_preserves_full_state_output_and_progress() {
    let temp = Temp::new();
    let mut c = config(&temp);
    seed(&c);
    let (path, digest) =
        savepoint::export(&c, &temp.0.join("exports"), "safe", &mut |_| Ok(())).unwrap();
    assert!(savepoint::export(&c, &temp.0.join("exports"), "safe", &mut |_| Ok(())).is_err());
    assert!(savepoint::export(&c, &temp.0.join("exports"), "../escape", &mut |_| Ok(())).is_err());
    let old = epoch::selection(&c.state.checkpoint_root).unwrap().unwrap();
    assert!(
        Transaction::transition(c.clone(), 2, 2, &mut |p| if p == "epoch_verified" {
            Err(ade_stream::Error::State("failed migration"))
        } else {
            Ok(())
        })
        .is_err()
    );
    assert_eq!(
        epoch::selection(&c.state.checkpoint_root).unwrap().unwrap(),
        old
    );
    for (codec, owners) in [(2, 2), (2, 4), (2, 1)] {
        Transaction::transition(c.clone(), codec, owners, &mut |_| Ok(())).unwrap();
        let old_config = c.clone();
        c.codec = codec;
        c.state.owners = owners;
        if old_config.codec < codec {
            assert!(sink::read(&old_config).is_err());
        }
        let tx = open(&c, true);
        assert_eq!(tx.engine.progress.forwarded_records, 40);
        assert_eq!(tx.engine.state.as_ref().unwrap().store.metrics.windows, 40);
        assert!(tx.engine.state.as_ref().unwrap().store.disk_bytes() > 0);
        drop(tx);
    }
    let mut txn = open(&c, true);
    close(&mut txn);
    txn.commit(Operation::Paused, &mut |_| Ok(())).unwrap();
    drop(txn);
    assert_eq!(sink::read(&c).unwrap().len(), 40);
    // Import an old savepoint only to a new independent root; no rollback of current.
    mkdir(&temp.0.join("import"));
    let mut imported = c.clone();
    imported.codec = 1;
    imported.state.owners = 1;
    imported.state.checkpoint_root = temp.0.join("import");
    savepoint::import(&imported, &path, &digest, &mut |_| Ok(())).unwrap();
    let tx = open(&imported, true);
    assert_eq!(tx.engine.progress.forwarded_records, 40);
    drop(tx);
    assert!(savepoint::import(&c, &path, &digest, &mut |_| Ok(())).is_err());
    fs::write(path.join("SAVEPOINT3"), b"{}").unwrap();
    assert!(savepoint::verify(&imported, &path, &digest).is_err());
}
#[test]
fn competing_writer_paths_hash_consistent_semantic_corruption_and_resource_refusals() {
    let temp = Temp::new();
    let mut c = config(&temp);
    let tx = open(&c, false);
    assert!(Transaction::open(c.clone(), false, CancellationToken::default()).is_err());
    drop(tx);
    let p = seed(&c);
    let mut foreign = c.clone();
    foreign.trusted_history = "changed".into();
    assert!(Transaction::open(foreign, true, CancellationToken::default()).is_err());
    c.limits.epochs = 2;
    let mut tx = open(&c, true);
    close(&mut tx);
    tx.commit(Operation::Commit, &mut |_| Ok(())).unwrap();
    assert!(tx.commit(Operation::Commit, &mut |_| Ok(())).is_err());
    drop(tx);
    c.limits.epochs = 64;
    let selected = epoch::selection(&c.state.checkpoint_root).unwrap().unwrap();
    let path = c
        .state
        .checkpoint_root
        .join(selected.id.to_string())
        .join("epoch.json");
    let mut m: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    m["progress"]["frontier"] = serde_json::json!(999);
    let b = serde_json::to_vec(&m).unwrap();
    fs::write(&path, &b).unwrap();
    let mut forged = selected;
    forged.blake3 = epoch::hash(&b);
    fs::write(
        c.state.checkpoint_root.join("CURRENT3"),
        epoch::encode(&forged).unwrap(),
    )
    .unwrap();
    assert!(sink::read(&c).is_err());
    assert_ne!(p, forged);
}
#[test]
fn no_id_only_dedup_and_overflow_poison() {
    let temp = Temp::new();
    let c = config(&temp);
    let mut tx = open(&c, false);
    event(&mut tx, 0, 0, 1, i64::MAX, 0);
    assert!(
        tx.accept(RawRecord {
            session: tx.engine.progress.session,
            partition: 0,
            offset: 1,
            payload: serde_json::to_vec(&EventV1 {
                version: 1,
                key: 1,
                value_i64: 1,
                event_time_ms: 0
            })
            .unwrap()
        })
        .is_err()
    );
    assert!(tx.commit(Operation::Commit, &mut |_| Ok(())).is_err());
    drop(tx);
    assert_eq!(sink::read(&c).unwrap(), []);
    let o = sink::Output {
        output_version: 1,
        job_id: c.state.job_id,
        key: 1,
        window_start: 0,
        window_end: 100,
        sum_i64: 1,
        count_u64: 1,
        output_id: format!("{}:1:0:100:1", c.state.job_id),
        payload_fingerprint: "window-payload-v1:1:1".into(),
    };
    let mut map = std::collections::BTreeMap::new();
    sink::unique(&mut map, &o).unwrap();
    let mut changed = o;
    changed.sum_i64 = 2;
    changed.payload_fingerprint = "window-payload-v1:2:1".into();
    assert!(sink::unique(&mut map, &changed).is_err());
}

#[test]
fn legacy_modes_stale_inode_and_cloned_roots_are_fenced() {
    let temp = Temp::new();
    let c = config(&temp);
    seed(&c);
    assert!(
        state::State::open(
            c.state.clone(),
            Uuid::new_v4(),
            CancellationToken::default(),
            false
        )
        .is_err()
    );
    let mut tx = open(&c, true);
    assert!(
        tx.engine
            .state
            .as_mut()
            .unwrap()
            .checkpoint(&tx.engine.progress, 0, &mut |_| Ok(()))
            .is_err()
    );
    let lock = c.state.checkpoint_root.join("WRITER3");
    fs::remove_file(&lock).unwrap();
    let f = state::store::create_file(&lock).unwrap();
    drop(f);
    assert!(tx.commit(Operation::Commit, &mut |_| Ok(())).is_err());
    drop(tx);
    mkdir(&temp.0.join("clone"));
    fs::copy(
        c.state.checkpoint_root.join("ROOT3"),
        temp.0.join("clone/ROOT3"),
    )
    .unwrap();
    let mut cloned = c.clone();
    cloned.state.checkpoint_root = temp.0.join("clone");
    assert!(Transaction::open(cloned, false, CancellationToken::default()).is_err());
}
#[test]
fn crash_orphans_savepoint_tamper_hardlinks_and_strict_configs_refuse() {
    let temp = Temp::new();
    let mut c = config(&temp);
    seed(&c);
    let mut bad = serde_json::to_value(&c).unwrap();
    bad["new_unknown_field"] = serde_json::json!(true);
    assert!(Config::parse(&serde_json::to_vec(&bad).unwrap()).is_err());
    assert!(Config::parse(&vec![b' '; 32769]).is_err());
    let (path, digest) =
        savepoint::export(&c, &temp.0.join("exports"), "first", &mut |_| Ok(())).unwrap();
    let mut top: savepoint::Export =
        epoch::json(&fs::read(path.join("SAVEPOINT3")).unwrap()).unwrap();
    top.files[0].path = "../../outside".into();
    let data = epoch::encode(&top).unwrap();
    fs::write(path.join("SAVEPOINT3"), &data).unwrap();
    assert!(savepoint::verify(&c, &path, &epoch::hash(&data)).is_err());
    assert!(savepoint::verify(&c, &path, &digest).is_err());
    let orphan = c.state.checkpoint_root.join(Uuid::new_v4().to_string());
    mkdir(&orphan);
    let file = state::store::create_file(&orphan.join("partial.bin")).unwrap();
    file.set_len(131_072).unwrap();
    c.limits.root_bytes = 131_072;
    c.limits.stage_bytes = 65_536;
    c.limits.output_bytes = 2048;
    assert!(Transaction::open(c.clone(), true, CancellationToken::default()).is_err());
    fs::remove_dir_all(orphan).unwrap();
    c.limits.root_bytes = 32 * 1024 * 1024;
    c.limits.stage_bytes = 1024 * 1024;
    let p = epoch::selection(&c.state.checkpoint_root).unwrap().unwrap();
    let file = c
        .state
        .checkpoint_root
        .join(p.id.to_string())
        .join("output.jsonl");
    fs::hard_link(&file, c.state.checkpoint_root.join("extra")).unwrap();
    assert!(sink::read(&c).is_err());
}
#[test]
fn clean_pause_replay_is_idempotent_and_partial_migration_never_activates() {
    let temp = Temp::new();
    let c = config(&temp);
    let mut tx = open(&c, false);
    let first = tx.commit(Operation::Paused, &mut |_| Ok(())).unwrap();
    drop(tx);
    let mut tx = open(&c, true);
    assert_eq!(
        tx.commit(Operation::Paused, &mut |_| Ok(())).unwrap(),
        first
    );
    drop(tx);
    assert!(Transaction::transition(c.clone(), 1, 1, &mut |_| Ok(())).is_err());
    assert_eq!(
        epoch::selection(&c.state.checkpoint_root).unwrap().unwrap(),
        first
    );
    let mut bytes = Vec::new();
    ade_stream::transactional::migration::write(
        &mut bytes,
        state::Entry {
            start: 0,
            key: 1,
            sum: 4,
            count: 1,
        },
        2,
        100,
    )
    .unwrap();
    bytes[32..40].copy_from_slice(&101i64.to_le_bytes());
    let checksum = blake3::hash(&bytes[..48]);
    bytes[48..].copy_from_slice(checksum.as_bytes());
    assert!(ade_stream::transactional::migration::read(&mut bytes.as_slice(), 2, 100).is_err());
}
