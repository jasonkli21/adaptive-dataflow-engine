#![cfg(feature = "stateful")]
#![allow(
    clippy::many_single_char_names,
    reason = "Compact independent test fixtures"
)]
use ade_exec::CancellationToken;
use ade_stream::{
    Engine, Error, EventV1, RawRecord, Security, Start, StreamConfigV1,
    state::{
        Config, Entry, State, checkpoint,
        store::{CHARGE, SHARDS},
        window,
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
            .join(format!("ade15-{}", Uuid::new_v4()));
        fs::create_dir(&p).unwrap();
        fs::set_permissions(&p, fs::Permissions::from_mode(0o700)).unwrap();
        Self(p)
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn config(t: &Temp) -> Config {
    for name in ["work", "check"] {
        fs::create_dir(t.0.join(name)).unwrap();
        fs::set_permissions(t.0.join(name), fs::Permissions::from_mode(0o700)).unwrap();
    }
    Config {
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
            key_less_than: u64::MAX,
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
    }
}
fn prepare(
    e: &mut Engine,
    p: i32,
    off: i64,
    key: u64,
    value: i64,
    time: i64,
) -> ade_stream::Result<()> {
    e.prepare(RawRecord {
        session: e.progress.session,
        partition: p,
        offset: off,
        payload: serde_json::to_vec(&EventV1 {
            version: 1,
            key,
            value_i64: value,
            event_time_ms: time,
        })
        .unwrap(),
    })?;
    Ok(())
}
fn deliver(e: &mut Engine, p: i32, off: i64, key: u64, value: i64, time: i64) {
    prepare(e, p, off, key, value, time).unwrap();
    e.acknowledge().unwrap();
}
fn drain(e: &mut Engine) -> Vec<serde_json::Value> {
    let mut v = vec![];
    let s = e.state.as_mut().unwrap();
    while let Some(r) = s.next_close(e.progress.frontier).unwrap() {
        v.push(serde_json::to_value(r).unwrap());
        s.confirm_close().unwrap();
    }
    v
}
fn cut(e: &mut Engine) -> ade_stream::Result<checkpoint::Pointer> {
    e.state
        .as_mut()
        .unwrap()
        .checkpoint(&e.progress, 0, &mut |_| Ok(()))
}
fn entries(e: &Engine) -> Vec<Entry> {
    let mut v = vec![];
    for s in 0..SHARDS {
        e.state
            .as_ref()
            .unwrap()
            .store
            .visit_shard(s, |e| {
                v.push(e);
                Ok(())
            })
            .unwrap();
    }
    v.sort_by_key(|e| e.id());
    v
}
#[test]
fn signed_boundaries_strict_contract_and_identity() {
    assert_eq!(window(-1, 100).unwrap(), (-100, 0));
    assert_eq!(window(-100, 100).unwrap(), (-100, 0));
    assert_eq!(window(0, 100).unwrap(), (0, 100));
    assert!(window(i64::MAX, 100).is_err());
    assert!(window(i64::MIN, 100).is_err());
    let t = Temp::new();
    let c = config(&t);
    let mut v = serde_json::to_value(&c).unwrap();
    v["unknown"] = 1.into();
    assert!(Config::parse(&serde_json::to_vec(&v).unwrap()).is_err());
    let mut d = c.clone();
    d.source.starts[0].offset = 99;
    d.owners = 4;
    assert_eq!(c.fingerprint().unwrap(), d.fingerprint().unwrap());
    d.window_ms += 1;
    assert_ne!(c.fingerprint().unwrap(), d.fingerprint().unwrap());
    for owners in [0, 3, 5] {
        d.owners = owners;
        assert!(d.validate().is_err());
    }
}
#[test]
fn cross_partition_windows_unknown_frontier_late_and_overflow() {
    let t = Temp::new();
    let c = config(&t);
    let mut e = Engine::stateful(c, Uuid::new_v4(), CancellationToken::default(), false).unwrap();
    deliver(&mut e, 0, 0, 1, 5, -1);
    deliver(&mut e, 0, 2, 1, 7, -5);
    assert_eq!(drain(&mut e).len(), 0);
    deliver(&mut e, 1, 0, 1, -2, -2);
    assert_eq!(drain(&mut e).len(), 0);
    deliver(&mut e, 0, 3, 99, 0, 110);
    deliver(&mut e, 1, 4, 99, 0, 110);
    let v = drain(&mut e);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0]["sum_i64"], 10);
    assert_eq!(v[0]["count_u64"], 3);
    deliver(&mut e, 0, 5, 1, 999, -1);
    assert!(e.state.as_ref().unwrap().last_closed_late);
    assert_eq!(drain(&mut e).len(), 0);
    deliver(&mut e, 0, 6, 2, i64::MAX, 150);
    assert!(matches!(
        prepare(&mut e, 0, 7, 2, 1, 150),
        Err(Error::Overflow(_))
    ));
    assert!(cut(&mut e).is_err());
}
#[test]
fn tiny_tier_matches_control_reloads_skew_and_disk_quota() {
    let t = Temp::new();
    let c = config(&t);
    let u = Temp::new();
    let mut d = config(&u);
    d.job_id = c.job_id;
    d.hot_bytes = CHARGE * 1000;
    let mut small = Engine::stateful(
        c.clone(),
        Uuid::new_v4(),
        CancellationToken::default(),
        false,
    )
    .unwrap();
    let mut large =
        Engine::stateful(d, Uuid::new_v4(), CancellationToken::default(), false).unwrap();
    for i in 0..400 {
        for e in [&mut small, &mut large] {
            deliver(e, 0, i, u64::try_from(i).unwrap() % 67, i % 19 - 9, i % 13);
        }
    }
    assert_eq!(entries(&small), entries(&large));
    let metrics = &small.state.as_ref().unwrap().store.metrics;
    assert!(metrics.evictions > 200 && metrics.cold_hits > 200);
    assert!(metrics.hot_peak_bytes <= c.hot_bytes && metrics.disk_peak_bytes <= c.cold_bytes);
    let v = Temp::new();
    let mut q = config(&v);
    q.cold_bytes = 128;
    q.hot_bytes = CHARGE;
    let mut e = Engine::stateful(q, Uuid::new_v4(), CancellationToken::default(), false).unwrap();
    let mut failed = false;
    for i in 0..100 {
        if prepare(&mut e, 0, i, u64::try_from(i).unwrap(), 1, 0).is_err() {
            failed = true;
            break;
        }
        e.acknowledge().unwrap();
    }
    assert!(failed);
    assert!(cut(&mut e).is_err());
}
#[test]
fn cuts_refuse_input_and_pending_closure_then_restore_1_2_4_1() {
    let t = Temp::new();
    let mut c = config(&t);
    let mut e = Engine::stateful(
        c.clone(),
        Uuid::new_v4(),
        CancellationToken::default(),
        false,
    )
    .unwrap();
    for i in 0..80 {
        deliver(&mut e, 0, i, u64::try_from(i).unwrap(), 1, 0);
    }
    prepare(&mut e, 1, 0, 1, 4, 0).unwrap();
    assert!(cut(&mut e).is_err());
    e.acknowledge().unwrap();
    let original = entries(&e);
    let session = e.progress.session;
    cut(&mut e).unwrap();
    drop(e);
    for owners in [2, 4, 1] {
        c.owners = owners;
        let mut r = Engine::stateful(
            c.clone(),
            Uuid::new_v4(),
            CancellationToken::default(),
            true,
        )
        .unwrap();
        assert_ne!(r.progress.session, session);
        assert_eq!(entries(&r), original);
        for s in 0..SHARDS {
            assert!(
                r.state
                    .as_ref()
                    .unwrap()
                    .store
                    .path(s)
                    .parent()
                    .unwrap()
                    .ends_with(format!("owner-{}", s % owners))
            );
        }
        cut(&mut r).unwrap();
        drop(r);
    }
    let mut r = Engine::stateful(c, Uuid::new_v4(), CancellationToken::default(), true).unwrap();
    deliver(&mut r, 0, 90, 100, 0, 110);
    deliver(&mut r, 1, 5, 100, 0, 110);
    let state = r.state.as_mut().unwrap();
    assert!(state.next_close(r.progress.frontier).unwrap().is_some());
    assert!(state.checkpoint(&r.progress, 0, &mut |_| Ok(())).is_err());
    state.confirm_close().unwrap();
    let v = drain(&mut r);
    assert_eq!(v.len(), 79);
    cut(&mut r).unwrap();
}
#[test]
fn every_publication_fault_keeps_prior_and_selects_only_whole_generations() {
    let points = [
        "stage_created",
        "shard_write",
        "shard_checksum",
        "shard_synced",
        "manifest_write",
        "manifest_checksum",
        "manifest_synced",
        "staging_verified",
        "stage_synced",
        "generation_renamed",
        "generation_synced",
        "pointer_write",
        "pointer_synced",
        "pointer_renamed",
        "pointer_durable",
        "cleanup",
    ];
    for point in points {
        let t = Temp::new();
        let c = config(&t);
        let token = CancellationToken::default();
        let mut e = Engine::stateful(c.clone(), Uuid::new_v4(), token.clone(), false).unwrap();
        deliver(&mut e, 0, 0, 1, 2, 0);
        let prior = cut(&mut e).unwrap();
        let prior_bytes = fs::read(
            c.checkpoint_root
                .join(prior.generation.to_string())
                .join("manifest.json"),
        )
        .unwrap();
        deliver(&mut e, 0, 1, 2, 3, 0);
        let result = e
            .state
            .as_mut()
            .unwrap()
            .checkpoint(&e.progress, 0, &mut |p| {
                if p == point {
                    Err(Error::State("injected"))
                } else {
                    Ok(())
                }
            });
        assert!(result.is_err(), "{point}");
        let (selected, m) = checkpoint::load(&c, &token).unwrap();
        assert_eq!(
            m.windows,
            if ["pointer_renamed", "pointer_durable", "cleanup"].contains(&point) {
                2
            } else {
                1
            }
        );
        assert_eq!(
            fs::read(
                c.checkpoint_root
                    .join(prior.generation.to_string())
                    .join("manifest.json")
            )
            .unwrap(),
            prior_bytes
        );
        assert!(
            !c.checkpoint_root
                .join(selected.generation.to_string())
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("stage-")
        );
    }
}
fn rewrite_manifest(c: &Config, change: impl FnOnce(&mut serde_json::Value)) {
    let mut p: serde_json::Value =
        serde_json::from_slice(&fs::read(c.checkpoint_root.join("CURRENT")).unwrap()).unwrap();
    let path = c
        .checkpoint_root
        .join(p["generation"].as_str().unwrap())
        .join("manifest.json");
    let mut m: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    change(&mut m);
    let b = serde_json::to_vec(&m).unwrap();
    fs::write(path, &b).unwrap();
    p["manifest_blake3"] = blake3::hash(&b).to_hex().to_string().into();
    fs::write(
        c.checkpoint_root.join("CURRENT"),
        serde_json::to_vec(&p).unwrap(),
    )
    .unwrap();
}
#[test]
fn checksum_consistent_metadata_corruption_and_identity_refusals() {
    for attack in 0..12 {
        let t = Temp::new();
        let mut c = config(&t);
        let token = CancellationToken::default();
        let mut e = Engine::stateful(c.clone(), Uuid::new_v4(), token.clone(), false).unwrap();
        deliver(&mut e, 0, 0, 1, 2, 0);
        cut(&mut e).unwrap();
        drop(e);
        match attack {
            0 => rewrite_manifest(&c, |m| m["version"] = 2.into()),
            1 => rewrite_manifest(&c, |m| m["shards"][1]["shard"] = 0.into()),
            2 => rewrite_manifest(&c, |m| m["shards"][1]["owner"] = 3.into()),
            3 => rewrite_manifest(&c, |m| m["progress"]["partitions"]["0"]["seen"] = 10.into()),
            4 => rewrite_manifest(&c, |m| m["progress"]["frontier"] = 100.into()),
            5 => rewrite_manifest(&c, |m| m["pending_closures"] = 1.into()),
            6 => rewrite_manifest(&c, |m| m["shards"][0]["count"] = u64::MAX.into()),
            7 => {
                let mut p: serde_json::Value =
                    serde_json::from_slice(&fs::read(c.checkpoint_root.join("CURRENT")).unwrap())
                        .unwrap();
                p["generation"] = "../../escape".into();
                fs::write(
                    c.checkpoint_root.join("CURRENT"),
                    serde_json::to_vec(&p).unwrap(),
                )
                .unwrap();
            }
            8 => c.source.expected_cluster_id = "other".into(),
            9 => c.source.key_less_than = 2,
            10 => c.window_ms = 99,
            _ => rewrite_manifest(&c, |m| m["foreign"] = "field".into()),
        }
        assert!(checkpoint::load(&c, &token).is_err(), "attack {attack}");
        assert!(State::open(c, Uuid::new_v4(), token, true).is_err());
    }
}
#[test]
fn corrupt_partial_working_disk_and_cancel_fail_closed() {
    for partial in [true, false] {
        let t = Temp::new();
        let c = config(&t);
        let token = CancellationToken::default();
        let mut e = Engine::stateful(c, Uuid::new_v4(), token.clone(), false).unwrap();
        for i in 0..20 {
            deliver(&mut e, 0, i, u64::try_from(i).unwrap(), 1, 0);
        }
        let store = &e.state.as_ref().unwrap().store;
        let path = (0..SHARDS)
            .map(|s| store.path(s))
            .find(|p| fs::metadata(p).unwrap().len() > 0)
            .unwrap();
        let mut b = fs::read(&path).unwrap();
        if partial {
            b.pop();
        } else {
            b[0] ^= 1;
        }
        fs::write(path, b).unwrap();
        assert!(cut(&mut e).is_err());
        assert!(
            !e.state
                .as_ref()
                .unwrap()
                .config
                .checkpoint_root
                .join("CURRENT")
                .exists()
        );
        token.cancel();
        assert!(
            e.state
                .as_ref()
                .unwrap()
                .store
                .first_closed(Some(100), 100)
                .is_err()
        );
    }
}
#[test]
fn symlink_permissions_excessive_metadata_and_checkpoint_quota_refuse() {
    let t = Temp::new();
    let mut c = config(&t);
    let token = CancellationToken::default();
    let mut e = Engine::stateful(c.clone(), Uuid::new_v4(), token.clone(), false).unwrap();
    deliver(&mut e, 0, 0, 1, 2, 0);
    cut(&mut e).unwrap();
    drop(e);
    let current = fs::read(c.checkpoint_root.join("CURRENT")).unwrap();
    fs::remove_file(c.checkpoint_root.join("CURRENT")).unwrap();
    std::os::unix::fs::symlink(t.0.join("other"), c.checkpoint_root.join("CURRENT")).unwrap();
    assert!(checkpoint::load(&c, &token).is_err());
    fs::remove_file(c.checkpoint_root.join("CURRENT")).unwrap();
    fs::write(c.checkpoint_root.join("CURRENT"), &current).unwrap();
    fs::set_permissions(
        c.checkpoint_root.join("CURRENT"),
        fs::Permissions::from_mode(0o600),
    )
    .unwrap();
    fs::write(c.checkpoint_root.join("CURRENT"), vec![b' '; 513]).unwrap();
    assert!(checkpoint::load(&c, &token).is_err());
    fs::write(c.checkpoint_root.join("CURRENT"), current).unwrap();
    c.max_generations = 2;
    let mut e = Engine::stateful(c.clone(), Uuid::new_v4(), token.clone(), true).unwrap();
    cut(&mut e).unwrap();
    assert!(cut(&mut e).is_err());
    assert!(checkpoint::load(&c, &token).is_ok());
}

#[test]
fn partial_input_partial_close_and_failed_diagnostics_never_publish_a_cut() {
    use ade_stream::{
        MonotonicClock, Poll, Progress, Result, Source,
        runtime::{Metrics, Sink, Stop, run},
    };
    struct One(Option<RawRecord>);
    impl Source for One {
        fn poll(&mut self) -> Result<Poll> {
            Ok(self.0.take().map_or(Poll::Stopped, Poll::Data))
        }
        fn pause(&mut self) -> Result<()> {
            Ok(())
        }
        fn resume(&mut self) -> Result<()> {
            Ok(())
        }
        fn verify(&mut self, _: &Progress) -> Result<()> {
            Ok(())
        }
    }
    struct Partial {
        calls: usize,
        fail: bool,
    }
    impl Sink for Partial {
        fn try_write(&mut self, b: &[u8]) -> Result<usize> {
            self.calls += 1;
            if self.fail {
                if self.calls == 1 {
                    Ok(b.len() / 2)
                } else {
                    Err(Error::Sink)
                }
            } else {
                Ok(b.len())
            }
        }
    }
    for mode in [0, 1, 2] {
        let t = Temp::new();
        let c = config(&t);
        let mut e = Engine::stateful(
            c.clone(),
            Uuid::new_v4(),
            CancellationToken::default(),
            false,
        )
        .unwrap();
        deliver(&mut e, 0, 0, 1, 2, -1);
        cut(&mut e).unwrap();
        let old = fs::read(c.checkpoint_root.join("CURRENT")).unwrap();
        let raw = RawRecord {
            session: e.progress.session,
            partition: 1,
            offset: 0,
            payload: serde_json::to_vec(&EventV1 {
                version: 1,
                key: 1,
                value_i64: 3,
                event_time_ms: 0,
            })
            .unwrap(),
        };
        if mode == 1 {
            deliver(&mut e, 0, 1, 99, 0, 110);
            deliver(&mut e, 1, 0, 99, 0, 110);
        }
        let mut source = One(if mode == 1 { None } else { Some(raw) });
        let result = run(
            &mut source,
            &mut e,
            &mut Partial {
                calls: 0,
                fail: mode != 2,
            },
            &mut Partial {
                calls: 0,
                fail: mode == 2,
            },
            &MonotonicClock::default(),
            &CancellationToken::default(),
            &Stop::default(),
            1000,
            &mut Metrics::default(),
        );
        assert!(result.is_err());
        assert!(cut(&mut e).is_err());
        assert_eq!(fs::read(c.checkpoint_root.join("CURRENT")).unwrap(), old);
        if mode == 0 {
            assert_eq!(e.progress.partitions[&1].forwarded, None);
        } else if mode == 2 {
            assert_eq!(e.progress.partitions[&1].forwarded, Some(0));
        }
    }
}
#[test]
fn independent_signed_window_property_and_count_overflow() {
    for width in [1, 3, 10, 97, 10000] {
        for n in -1000i64..1000 {
            let time = n * 99991;
            let t = i128::from(time);
            let w = i128::from(width);
            let floor = t - ((t % w + w) % w);
            assert_eq!(
                window(time, width).unwrap(),
                (
                    i64::try_from(floor).unwrap(),
                    i64::try_from(floor + w).unwrap()
                )
            );
        }
    }
    let t = Temp::new();
    let c = config(&t);
    let mut e = Engine::stateful(c, Uuid::new_v4(), CancellationToken::default(), false).unwrap();
    deliver(&mut e, 0, 0, 1, 1, 0);
    e.state
        .as_mut()
        .unwrap()
        .store
        .put(
            Entry {
                start: 0,
                key: 1,
                sum: 0,
                count: u64::MAX,
            },
            false,
        )
        .unwrap();
    assert!(matches!(
        prepare(&mut e, 0, 1, 1, 0, 0),
        Err(Error::Overflow("window count"))
    ));
    assert!(cut(&mut e).is_err());
}
#[test]
fn cold_partial_write_no_space_and_parent_symlink_poison_attempt() {
    for after in [0, 1, 31, 63] {
        let t = Temp::new();
        let mut c = config(&t);
        c.hot_bytes = CHARGE;
        let mut e = Engine::stateful(
            c.clone(),
            Uuid::new_v4(),
            CancellationToken::default(),
            false,
        )
        .unwrap();
        deliver(&mut e, 0, 0, 1, 2, 0);
        let pointer = cut(&mut e).unwrap();
        e.state
            .as_mut()
            .unwrap()
            .store
            .inject_write_failure_after(after);
        assert!(prepare(&mut e, 0, 1, 2, 3, 0).is_err());
        assert!(cut(&mut e).is_err());
        let (saved, _) = checkpoint::load(&c, &CancellationToken::default()).unwrap();
        assert_eq!(pointer, saved);
    }
    let t = Temp::new();
    let c = config(&t);
    let mut e = Engine::stateful(c, Uuid::new_v4(), CancellationToken::default(), false).unwrap();
    deliver(&mut e, 0, 0, 1, 2, 0);
    let parent = e
        .state
        .as_ref()
        .unwrap()
        .store
        .path(0)
        .parent()
        .unwrap()
        .to_path_buf();
    let replacement = t.0.join("renamed-owner");
    fs::rename(&parent, &replacement).unwrap();
    std::os::unix::fs::symlink(&replacement, &parent).unwrap();
    assert!(cut(&mut e).is_err());
}
#[test]
fn orphan_working_state_refuses_before_a_new_allocation() {
    let t = Temp::new();
    let c = config(&t);
    fs::create_dir(c.working_root.join(Uuid::new_v4().to_string())).unwrap();
    assert!(matches!(
        Engine::stateful(c, Uuid::new_v4(), CancellationToken::default(), false),
        Err(Error::State(
            "working root not empty; prove prior attempt dead and remove orphan state"
        ))
    ));
}
