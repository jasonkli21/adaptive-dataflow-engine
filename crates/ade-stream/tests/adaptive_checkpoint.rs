#![cfg(feature = "adaptive-checkpoint")]
#![allow(
    clippy::many_single_char_names,
    reason = "Compact independent test fixtures"
)]
use ade_exec::CancellationToken;
use ade_stream::{
    Engine, Error, EventV1, RawRecord, Security, Start, StreamConfigV1,
    state::{
        Config, Entry, checkpoint,
        store::{CHARGE, SHARDS},
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

fn policy() -> ade_stream::adaptive::Policy {
    serde_json::from_value(serde_json::json!({
        "version":1,"mode":"adaptive","min_records":1,"max_records":10,"min_ms":100,"max_ms":1000,"max_depth":2,"min_samples":2,"cooldown_ms":0,"hysteresis_nano":0,
        "placements":["local"],"static_placement":"local",
        "constraints":{"min_copies":1,"require_remote":false,"max_loss_ppm":1_000_000,"local_loss_ppm":100_000,"remote_loss_ppm":100,"both_loss_ppm":1,"recovery_slo_ms":86_400_000,"failure_ppm":1000,"retention_ms":1000},
        "prices":{"local_write_byte":1,"local_read_byte":0,"remote_read_byte":0,"remote_write_byte":5,"remote_request":3,"capacity_byte_ms":0,"compute_ms":1,"replay_record":1},
        "limits":{"object_bytes":1_048_576,"remote_objects":8192,"remote_write_bytes":268_435_456,"requests":65_536,"response_bytes":68_719_476_736u64,"io_timeout_ms":1000,"trace_bytes":67_108_864},"remote":null,
        "recovery":{"original_starts":[{"partition":0,"offset":0},{"partition":1,"offset":0}],"trusted_immutable_history":true,"allow_replay":false,"accept_duplicate_and_conflicting_outputs":false,"replay_certificate_blake3":null,"replay_estimated_ms":1000,"restore_estimated_ms":100}
    })).unwrap()
}
fn adaptive(c: Config, p: ade_stream::adaptive::Policy, recover: bool) -> Engine {
    let (mut engine, recovery) = Engine::adaptive_stateful(
        c,
        p,
        Uuid::new_v4(),
        CancellationToken::default(),
        recover,
        ade_stream::adaptive::policy::Placement::Local,
        None,
    )
    .unwrap();
    if recovery == "recovery_pending" {
        engine
            .finish_adaptive_recovery(test_recovery_sources(true, true))
            .unwrap();
    }
    engine
}
fn test_recovery_sources(
    replay_eligible: bool,
    restore_eligible: bool,
) -> ade_stream::adaptive::policy::RecoverySources {
    use ade_stream::adaptive::policy::{CandidateEligibility, RecoverySources};
    RecoverySources {
        cluster_id: "test".into(),
        partition_inventory: vec![0, 1],
        replay: CandidateEligibility {
            eligible: replay_eligible,
            reason: (!replay_eligible).then_some("origin_retention"),
            ranges: Vec::new(),
        },
        restore: CandidateEligibility {
            eligible: restore_eligible,
            reason: (!restore_eligible).then_some("saved_offset_retention"),
            ranges: Vec::new(),
        },
    }
}
fn calibrated(e: &mut Engine) {
    use ade_stream::adaptive::policy::Observation;
    e.state
        .as_mut()
        .unwrap()
        .adaptive
        .as_mut()
        .unwrap()
        .observation = Observation {
        samples: 2,
        scan_bytes_per_second: 1000,
        local_bytes_per_second: 1000,
        remote_bytes_per_second: 1000,
        replay_ns_per_record: 10000,
        ..Observation::default()
    };
}
#[test]
fn strict_policy_cost_constraints_cooldown_and_checked_math() {
    use ade_stream::adaptive::{
        Policy,
        policy::{self, Action, Format, Mode, Observation, Placement},
    };
    let mut p = policy();
    p.validate().unwrap();
    let encoded = serde_json::to_vec(&p).unwrap();
    assert!(Policy::parse(&encoded).is_ok());
    let unknown = String::from_utf8(encoded.clone())
        .unwrap()
        .replacen('{', "{\"secret\":1,", 1);
    assert!(Policy::parse(unknown.as_bytes()).is_err());
    let duplicate = String::from_utf8(encoded)
        .unwrap()
        .replacen('{', "{\"version\":1,", 1);
    assert!(Policy::parse(duplicate.as_bytes()).is_err());
    let mut o = Observation {
        samples: 2,
        logical_bytes: 6400,
        changed_bytes: 64,
        changed_shards: 1,
        scan_bytes_per_second: 1000,
        local_bytes_per_second: 1000,
        remote_bytes_per_second: 1000,
        replay_ns_per_record: 10000,
        depth: 0,
        local_available_bytes: 1_000_000_000,
        remote_available_bytes: 1_000_000_000,
        remote_available_objects: 8192,
        remaining_requests: 65536,
        remaining_response_bytes: 68_719_476_736,
    };
    let d = policy::decide(&p, &o, None, 0).unwrap();
    assert_eq!(d.chosen.format, Format::Incremental);
    o.samples = 0;
    assert!(
        policy::decide(&p, &o, None, 0)
            .unwrap()
            .reason
            .contains("fallback")
    );
    o.samples = 2;
    let full = Action {
        format: Format::Full,
        placement: Placement::Local,
        records: 1,
        ms: 100,
    };
    p.cooldown_ms = 1000;
    assert_eq!(
        policy::decide(&p, &o, Some((&full, 0)), 10).unwrap().chosen,
        full
    );
    p.mode = Mode::Static;
    assert_eq!(
        policy::decide(&p, &o, None, 0).unwrap().chosen.format,
        Format::Full
    );
    p.constraints.max_loss_ppm = 10;
    assert!(p.validate().is_err());
    p.constraints.max_loss_ppm = 1_000_000;
    o.logical_bytes = u64::MAX;
    o.local_available_bytes = u64::MAX;
    p.constraints.recovery_slo_ms = u64::MAX;
    assert!(policy::evaluate(&p, &o, full).is_err());
    p.constraints.recovery_slo_ms = 86_400_000;
    p.recovery.allow_replay = true;
    assert!(p.validate().is_err());
    p.recovery.accept_duplicate_and_conflicting_outputs = true;
    p.recovery.replay_certificate_blake3 = Some("a".repeat(64));
    assert!(p.validate().is_ok());
    p.recovery.trusted_immutable_history = false;
    assert!(p.validate().is_err());
}
#[test]
fn actual_incremental_bytes_complete_restore_and_periodic_full() {
    use ade_stream::adaptive::snapshot;
    let t = Temp::new();
    let c = config(&t);
    let p = policy();
    let mut e = adaptive(c.clone(), p.clone(), false);
    assert!(!e.state.as_ref().unwrap().due(&e.progress, 100_000));
    for i in 0..100 {
        deliver(&mut e, 0, i, u64::try_from(i).unwrap(), 1, 0);
    }
    cut(&mut e).unwrap();
    calibrated(&mut e);
    for round in 0..3 {
        deliver(&mut e, 0, 100 + round, 1, 2, 0);
        let all = entries(&e);
        cut(&mut e).unwrap();
        let a = e.state.as_ref().unwrap().adaptive.as_ref().unwrap();
        if round < 2 {
            assert!(a.last_metrics.physical_shard_bytes < a.last_metrics.canonical_scanned_bytes);
            assert!(a.last_metrics.physical_shard_objects < 32);
        } else {
            assert_eq!(a.last_metrics.physical_shard_objects, 32);
        }
        let cert = a.current.clone().unwrap();
        let mut m: snapshot::Manifest = serde_json::from_slice(
            &fs::read(
                c.checkpoint_root
                    .join(cert.manifest.generation.to_string())
                    .join("manifest.json"),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(m.shards.len(), 32);
        assert_eq!(
            m.depth,
            if round < 2 {
                u64::try_from(round + 1).unwrap()
            } else {
                0
            }
        );
        // Logical owner redistribution rehydrates separate real stores.
        drop(e);
        let mut new = c.clone();
        new.owners = [2, 4, 1][usize::try_from(round).unwrap()];
        e = adaptive(new, p.clone(), true);
        assert_eq!(entries(&e), all);
        calibrated(&mut e);
        m.progress.session = e.progress.session;
        assert_eq!(m.progress.forwarded_records, e.progress.forwarded_records);
    }
    assert!(!e.state.as_ref().unwrap().due(&e.progress, 100_000));
    drop(e);
    assert!(Engine::stateful(c, Uuid::new_v4(), CancellationToken::default(), false).is_err());
}
#[test]
fn pending_failed_and_unchanged_cuts_refuse_and_fake_clock_cadence() {
    let t = Temp::new();
    let c = config(&t);
    let mut e = adaptive(c, policy(), false);
    assert!(cut(&mut e).is_err());
    drop(e);
    // Failed empty attempt is not reused.
    let t = Temp::new();
    let c = config(&t);
    let mut e = adaptive(c, policy(), false);
    prepare(&mut e, 0, 0, 1, 1, 0).unwrap();
    assert!(cut(&mut e).is_err());
    e.acknowledge().unwrap();
    cut(&mut e).unwrap();
    assert!(!e.state.as_ref().unwrap().due(&e.progress, u64::MAX));
    deliver(&mut e, 0, 1, 1, 1, 0);
    assert!(e.state.as_ref().unwrap().due(&e.progress, 0));
}
#[test]
fn every_v2_local_failpoint_selects_exact_old_or_new_and_poison() {
    use ade_stream::adaptive::snapshot;
    let points = [
        "policy_decided",
        "stage_created",
        "shard_write",
        "shard_synced",
        "manifest_write",
        "manifest_synced",
        "stage_synced",
        "generation_renamed",
        "generation_synced",
        "copies_verified",
        "certificate_synced",
        "before_selector",
        "selector_synced",
        "selector_renamed",
        "selector_durable",
    ];
    for point in points {
        let t = Temp::new();
        let c = config(&t);
        let p = policy();
        let mut e = adaptive(c.clone(), p.clone(), false);
        deliver(&mut e, 0, 0, 1, 1, 0);
        cut(&mut e).unwrap();
        let prior = snapshot::selection(&c).unwrap();
        deliver(&mut e, 0, 1, 2, 2, 0);
        assert!(
            e.state
                .as_mut()
                .unwrap()
                .checkpoint(&e.progress, 100, &mut |n| if n == point {
                    Err(Error::State("injected boundary"))
                } else {
                    Ok(())
                })
                .is_err(),
            "{point}"
        );
        assert!(cut(&mut e).is_err(), "poisoned {point}");
        let selected = snapshot::selection(&c).unwrap();
        let new = ["selector_renamed", "selector_durable"].contains(&point);
        assert_eq!(
            selected.certificate_blake3 == prior.certificate_blake3,
            !new,
            "{point}"
        );
        drop(e);
        let e = adaptive(c, p, true);
        assert_eq!(
            e.progress.forwarded_records,
            if new { 2 } else { 1 },
            "{point}"
        );
    }
}
#[test]
fn corrupt_ancestor_and_semantic_checksum_consistent_forgeries_refuse() {
    use ade_stream::adaptive::snapshot::{self, Selector};
    for attack in [
        "missing",
        "byte",
        "owner",
        "count",
        "depth",
        "cycle",
        "duplicate",
        "codec",
        "frontier",
        "hardlink",
        "symlink",
        "oversize",
    ] {
        let t = Temp::new();
        let c = config(&t);
        let p = policy();
        let mut e = adaptive(c.clone(), p.clone(), false);
        for i in 0..20 {
            deliver(&mut e, 0, i, u64::try_from(i).unwrap(), 1, 0);
        }
        cut(&mut e).unwrap();
        calibrated(&mut e);
        deliver(&mut e, 0, 20, 1, 1, 0);
        cut(&mut e).unwrap();
        drop(e);
        let mut selector = snapshot::selection(&c).unwrap();
        let path = c
            .checkpoint_root
            .join(selector.certificate.manifest.generation.to_string())
            .join("manifest.json");
        let mut m: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        let ancestor = m["prior"]["generation"].as_str().unwrap().to_string();
        let shard_path = c.checkpoint_root.join(ancestor).join("shard-00.bin");
        match attack {
            "missing" => {
                fs::remove_file(shard_path).unwrap();
            }
            "byte" => {
                fs::write(shard_path, b"corrupt").unwrap();
            }
            "hardlink" => {
                fs::hard_link(&shard_path, t.0.join("linked")).unwrap();
            }
            "symlink" => {
                fs::remove_file(&shard_path).unwrap();
                std::os::unix::fs::symlink("/etc/passwd", shard_path).unwrap();
            }
            "oversize" => {
                fs::write(&path, vec![0; 32769]).unwrap();
            }
            _ => {
                match attack {
                    "owner" => m["shards"][0]["owner"] = 7.into(),
                    "count" => m["windows"] = 999.into(),
                    "depth" => m["depth"] = 5.into(),
                    "cycle" => m["prior"]["generation"] = m["generation"].clone(),
                    "duplicate" => m["shards"][1] = m["shards"][0].clone(),
                    "codec" => m["version"] = 1.into(),
                    "frontier" => m["progress"]["frontier"] = 200.into(),
                    _ => unreachable!(),
                }
                let b = serde_json::to_vec(&m).unwrap();
                fs::write(&path, &b).unwrap();
                selector.certificate.manifest.blake3 = snapshot::hash(&b);
                selector.certificate.manifest.bytes = b.len() as u64;
                selector.certificate_blake3 =
                    snapshot::hash(&snapshot::encode(&selector.certificate).unwrap());
                fs::write(
                    c.checkpoint_root.join("CURRENT2"),
                    snapshot::encode(&selector).unwrap(),
                )
                .unwrap();
            }
        }
        let result = Engine::adaptive_stateful(
            c,
            p,
            Uuid::new_v4(),
            CancellationToken::default(),
            true,
            ade_stream::adaptive::policy::Placement::Local,
            None::<Selector>,
        );
        assert!(result.is_err(), "{attack}");
    }
}
#[test]
fn executed_replay_requires_exact_selected_consent_and_empty_state() {
    use ade_stream::adaptive::snapshot;
    let t = Temp::new();
    let c = config(&t);
    let mut p = policy();
    let mut e = adaptive(c.clone(), p.clone(), false);
    deliver(&mut e, 0, 0, 1, 9, 0);
    cut(&mut e).unwrap();
    drop(e);
    let selected = snapshot::selection(&c).unwrap();
    p.recovery.allow_replay = true;
    p.recovery.accept_duplicate_and_conflicting_outputs = true;
    p.recovery.replay_certificate_blake3 = Some(selected.certificate_blake3);
    p.recovery.replay_estimated_ms = 1;
    p.recovery.restore_estimated_ms = 100;
    let (mut e, recovery) = Engine::adaptive_stateful(
        c.clone(),
        p.clone(),
        Uuid::new_v4(),
        CancellationToken::default(),
        true,
        ade_stream::adaptive::policy::Placement::Local,
        None,
    )
    .unwrap();
    assert_eq!(recovery, "recovery_pending");
    let reason = e
        .finish_adaptive_recovery(test_recovery_sources(true, true))
        .unwrap();
    assert!(reason.contains("replay"));
    assert_eq!(e.progress.forwarded_records, 0);
    assert_eq!(entries(&e), []);
    // Exceed the old cut's scalar record count before the replay's first cut.
    // Replay history is trace-relative, so ancestry from the older state is not
    // a valid substitute even when one shard happens to match byte-for-byte.
    deliver(&mut e, 0, 0, 1, 9, 0);
    deliver(&mut e, 0, 1, 2, 4, 0);
    cut(&mut e).unwrap();
    let adaptive = e.state.as_ref().unwrap().adaptive.as_ref().unwrap();
    assert_eq!(adaptive.last_metrics.physical_shard_objects, 32);
    let generation = adaptive.current.as_ref().unwrap().manifest.generation;
    let manifest: snapshot::Manifest = serde_json::from_slice(
        &fs::read(
            c.checkpoint_root
                .join(generation.to_string())
                .join("manifest.json"),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(manifest.depth, 0);
    assert!(manifest.prior.is_none());
    drop(e);
    p.recovery.replay_certificate_blake3 = Some("a".repeat(64));
    let (mut e, recovery) = Engine::adaptive_stateful(
        c,
        p,
        Uuid::new_v4(),
        CancellationToken::default(),
        true,
        ade_stream::adaptive::policy::Placement::Local,
        None,
    )
    .unwrap();
    assert_eq!(recovery, "recovery_pending");
    assert!(
        e.finish_adaptive_recovery(test_recovery_sources(true, true))
            .is_err()
    );
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "One regression covers eligible, fallback and total-refusal recovery paths"
)]
fn recovery_costs_only_eligible_candidates_and_restores_at_retained_cut() {
    use ade_stream::adaptive::policy::{
        CandidateEligibility, ObservedOffsetRange, RecoverySources, recovery,
    };
    let mut p = policy();
    let certificate = "c".repeat(64);
    p.recovery.allow_replay = true;
    p.recovery.accept_duplicate_and_conflicting_outputs = true;
    p.recovery.replay_certificate_blake3 = Some(certificate.clone());
    p.recovery.replay_estimated_ms = 1;
    p.recovery.restore_estimated_ms = 10_000;

    // deleteRecords before 20 removes the replay origin but leaves the
    // selected checkpoint's exact 20/20 vector at each low watermark.
    let sources = RecoverySources {
        cluster_id: "fixture-cluster".into(),
        partition_inventory: vec![0, 1],
        replay: CandidateEligibility {
            eligible: false,
            reason: Some("origin_retention"),
            ranges: vec![
                ObservedOffsetRange {
                    partition: 0,
                    requested_offset: 0,
                    low: 20,
                    high: 40,
                },
                ObservedOffsetRange {
                    partition: 1,
                    requested_offset: 0,
                    low: 0,
                    high: 40,
                },
            ],
        },
        restore: CandidateEligibility {
            eligible: true,
            reason: None,
            ranges: vec![
                ObservedOffsetRange {
                    partition: 0,
                    requested_offset: 20,
                    low: 20,
                    high: 40,
                },
                ObservedOffsetRange {
                    partition: 1,
                    requested_offset: 20,
                    low: 0,
                    high: 40,
                },
            ],
        },
    };
    let decision = recovery(
        &p,
        &certificate,
        6_400,
        40,
        ade_stream::adaptive::policy::Placement::Local,
        sources,
    )
    .unwrap();
    assert_eq!(decision.chosen, "checkpoint_restore");
    assert_eq!(decision.chosen_reason, "replay_ineligible_origin_retention");
    assert!(!decision.replay_eligible);
    assert!(decision.restore_eligible);
    assert_eq!(decision.replay_cost_nano, None);

    let replay_only = RecoverySources {
        cluster_id: "fixture-cluster".into(),
        partition_inventory: vec![0, 1],
        replay: CandidateEligibility {
            eligible: true,
            reason: None,
            ranges: Vec::new(),
        },
        restore: CandidateEligibility {
            eligible: false,
            reason: Some("saved_offset_retention"),
            ranges: Vec::new(),
        },
    };
    let decision = recovery(
        &p,
        &certificate,
        6_400,
        40,
        ade_stream::adaptive::policy::Placement::Local,
        replay_only,
    )
    .unwrap();
    assert_eq!(decision.chosen, "full_replay_explicit_conflict_consent");
    assert_eq!(
        decision.chosen_reason,
        "restore_ineligible_saved_offset_retention"
    );

    let neither = RecoverySources {
        cluster_id: "fixture-cluster".into(),
        partition_inventory: vec![0, 1],
        replay: CandidateEligibility {
            eligible: false,
            reason: Some("origin_retention"),
            ranges: Vec::new(),
        },
        restore: CandidateEligibility {
            eligible: false,
            reason: Some("saved_offset_retention"),
            ranges: Vec::new(),
        },
    };
    assert!(matches!(
        recovery(
            &p,
            &certificate,
            6_400,
            40,
            ade_stream::adaptive::policy::Placement::Local,
            neither,
        ),
        Err(Error::RecoveryUnavailable { .. })
    ));
}

#[test]
fn policy_changes_cadence_placement_replication_and_preserves_floors() {
    use ade_stream::adaptive::policy::{self, Observation, Placement};
    let mut p = policy();
    let observation = Observation {
        samples: 2,
        logical_bytes: 6400,
        changed_bytes: 64,
        changed_shards: 1,
        scan_bytes_per_second: 1000,
        local_bytes_per_second: 1000,
        remote_bytes_per_second: 1000,
        replay_ns_per_record: 10000,
        depth: 0,
        local_available_bytes: 1_000_000_000,
        remote_available_bytes: 1_000_000_000,
        remote_available_objects: 8192,
        remaining_requests: 65536,
        remaining_response_bytes: 68_719_476_736,
    };
    p.constraints.failure_ppm = 0;
    assert_eq!(
        policy::decide(&p, &observation, None, 0)
            .unwrap()
            .chosen
            .records,
        10
    );
    p.constraints.failure_ppm = 1_000_000;
    p.prices.replay_record = 10_000_000;
    assert_eq!(
        policy::decide(&p, &observation, None, 0)
            .unwrap()
            .chosen
            .records,
        1
    );
    #[cfg(feature = "remote-checkpoint")]
    {
        p.remote = Some(ade_stream::adaptive::Remote {
            endpoint: "http://127.0.0.1:1".into(),
            bucket: "fixture".into(),
            region: "us-east-1".into(),
            prefix: "private".into(),
            namespace: Uuid::new_v4(),
            dev_allow_http: true,
        });
        p.placements = vec![Placement::Local, Placement::Remote, Placement::Both];
        p.static_placement = Placement::Both;
        p.prices.local_write_byte = 1_000_000;
        p.prices.remote_write_byte = 0;
        p.prices.remote_request = 0;
        assert_eq!(
            policy::decide(&p, &observation, None, 0)
                .unwrap()
                .chosen
                .placement,
            Placement::Remote
        );
        p.constraints.min_copies = 2;
        let decision = policy::decide(&p, &observation, None, 0).unwrap();
        assert_eq!(decision.chosen.placement, Placement::Both);
        assert!(
            decision
                .candidates
                .iter()
                .filter(|c| c.action.placement != Placement::Both)
                .all(|c| c.rejection.as_deref() == Some("durability_floor"))
        );
        p.constraints.recovery_slo_ms = 1;
        assert!(policy::decide(&p, &observation, None, 0).is_err());
        for endpoint in [
            "http://localhost:9000",
            "http://192.0.2.1:9000",
            "https://u:p@example.org",
            "https://example.org/?secret=bad",
            "https://example.org/path",
        ] {
            p.remote.as_mut().unwrap().endpoint = endpoint.into();
            assert!(p.validate().is_err());
        }
    }
}
#[test]
#[cfg(feature = "remote-checkpoint")]
fn forced_full_placement_transition_is_ranked_before_remote_capacity_effects() {
    use ade_stream::adaptive::policy::{self, Action, Format, Observation, Placement};
    let mut p = policy();
    p.placements = vec![Placement::Local, Placement::Remote];
    p.static_placement = Placement::Local;
    p.remote = Some(ade_stream::adaptive::Remote {
        endpoint: "http://127.0.0.1:1".into(),
        bucket: "fixture".into(),
        region: "us-east-1".into(),
        prefix: "private".into(),
        namespace: Uuid::new_v4(),
        dev_allow_http: true,
    });
    p.prices.local_write_byte = 100_000_000;
    p.prices.remote_write_byte = 0;
    p.prices.remote_request = 0;
    let observation = Observation {
        samples: 2,
        logical_bytes: 6_400,
        changed_bytes: 64,
        changed_shards: 1,
        scan_bytes_per_second: 1_000,
        local_bytes_per_second: 1_000,
        remote_bytes_per_second: 1_000,
        replay_ns_per_record: 10_000,
        depth: 1,
        local_available_bytes: 10_000,
        // Incremental remote fits (64 + fixed reservation); full remote does not.
        remote_available_bytes: 66_000,
        remote_available_objects: 8_192,
        remaining_requests: 65_536,
        remaining_response_bytes: 68_719_476_736,
    };
    // The verified prior was `both`; neither local nor remote can extend its
    // ancestry. Full/local remains executable while full/remote exceeds quota.
    let previous = Action {
        format: Format::Incremental,
        placement: Placement::Both,
        records: 1,
        ms: 100,
    };
    let decision = policy::decide_with_incremental_placements(
        &p,
        &observation,
        Some((&previous, 0)),
        1_000,
        &[],
    )
    .unwrap();
    assert_eq!(decision.chosen.format, Format::Full);
    assert_eq!(decision.chosen.placement, Placement::Local);
    assert!(
        decision
            .candidates
            .iter()
            .filter(|c| {
                c.action.format == Format::Incremental && p.placements.contains(&c.action.placement)
            })
            .all(|c| c.rejection.as_deref() == Some("self_contained_required"))
    );
    assert!(decision.candidates.iter().any(|c| {
        c.action.format == Format::Full
            && c.action.placement == Placement::Remote
            && c.rejection.as_deref() == Some("capacity_floor")
    }));
    assert!(decision.candidates.iter().any(|c| {
        c.action.format == Format::Full
            && c.action.placement == Placement::Local
            && c.rejection.is_none()
    }));
}

#[test]
fn v2_quota_cancel_duplicate_progress_and_policy_floor_attacks() {
    use ade_stream::adaptive::snapshot;
    let t = Temp::new();
    let mut c = config(&t);
    c.max_generations = 2;
    let p = policy();
    let mut e = adaptive(c.clone(), p.clone(), false);
    deliver(&mut e, 0, 0, 1, 1, 0);
    cut(&mut e).unwrap();
    assert!(cut(&mut e).is_err());
    deliver(&mut e, 0, 1, 1, 1, 0);
    cut(&mut e).unwrap();
    deliver(&mut e, 0, 2, 1, 1, 0);
    assert!(cut(&mut e).is_err());
    drop(e);
    let mut changed = p.clone();
    changed.constraints.max_loss_ppm = 1_000_000;
    let mut selected = snapshot::selection(&c).unwrap();
    selected.certificate.max_loss_ppm = 1;
    selected.certificate_blake3 = snapshot::hash(&snapshot::encode(&selected.certificate).unwrap());
    assert!(
        Engine::adaptive_stateful(
            c.clone(),
            changed,
            Uuid::new_v4(),
            CancellationToken::default(),
            true,
            ade_stream::adaptive::policy::Placement::Local,
            Some(selected)
        )
        .is_err()
    );
    assert!(snapshot::json::<serde_json::Value>(br#"{"progress":{"0":1,"0":2}}"#).is_err());
    let t = Temp::new();
    let c = config(&t);
    let token = CancellationToken::default();
    let mut e = Engine::adaptive_stateful(
        c,
        policy(),
        Uuid::new_v4(),
        token.clone(),
        false,
        ade_stream::adaptive::policy::Placement::Local,
        None,
    )
    .unwrap()
    .0;
    deliver(&mut e, 0, 0, 1, 1, 0);
    token.cancel();
    assert!(cut(&mut e).is_err());
}

#[test]
fn each_of_32_local_object_publication_boundaries_retains_prior() {
    use ade_stream::adaptive::{policy::Mode, snapshot};
    for boundary in ["shard_write", "shard_synced"] {
        for target in 0..32 {
            let t = Temp::new();
            let c = config(&t);
            let mut p = policy();
            p.mode = Mode::Static;
            let mut e = adaptive(c.clone(), p.clone(), false);
            deliver(&mut e, 0, 0, 1, 1, 0);
            cut(&mut e).unwrap();
            let old = snapshot::selection(&c).unwrap();
            deliver(&mut e, 0, 1, 2, 2, 0);
            let mut seen = 0;
            assert!(
                e.state
                    .as_mut()
                    .unwrap()
                    .checkpoint(&e.progress, 0, &mut |point| {
                        if point == boundary {
                            let fail = seen == target;
                            seen += 1;
                            if fail {
                                return Err(Error::State("per-object boundary"));
                            }
                        }
                        Ok(())
                    })
                    .is_err()
            );
            assert_eq!(
                snapshot::selection(&c).unwrap().certificate_blake3,
                old.certificate_blake3
            );
            drop(e);
            assert_eq!(adaptive(c, p, true).progress.forwarded_records, 1);
        }
    }
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "One ordered trace audits both complete physical mechanisms"
)]
fn identical_ordered_trace_full_incremental_cuts_and_window_outputs_match() {
    use ade_stream::adaptive::{policy::Mode, snapshot};
    let left = Temp::new();
    let right = Temp::new();
    let full_config = config(&left);
    let mut incremental_config = config(&right);
    incremental_config.job_id = full_config.job_id;
    let mut control_policy = policy();
    control_policy.mode = Mode::Static;
    let mut full = adaptive(full_config.clone(), control_policy, false);
    let mut incremental = adaptive(incremental_config.clone(), policy(), false);
    for i in 0..100 {
        for e in [&mut full, &mut incremental] {
            deliver(
                e,
                i32::try_from(i % 2).unwrap(),
                i / 2,
                u64::try_from(i).unwrap(),
                1,
                0,
            );
        }
    }
    let compare = |a: &Engine, b: &Engine| {
        assert_eq!(entries(a), entries(b));
        let mut ap = serde_json::to_value(&a.progress).unwrap();
        let mut bp = serde_json::to_value(&b.progress).unwrap();
        ap.as_object_mut().unwrap().remove("session");
        bp.as_object_mut().unwrap().remove("session");
        assert_eq!(ap, bp);
        let am = a
            .state
            .as_ref()
            .unwrap()
            .adaptive
            .as_ref()
            .unwrap()
            .current
            .as_ref()
            .unwrap();
        let bm = b
            .state
            .as_ref()
            .unwrap()
            .adaptive
            .as_ref()
            .unwrap()
            .current
            .as_ref()
            .unwrap();
        let manifest = |config: &Config, generation: Uuid| {
            snapshot::json::<snapshot::Manifest>(
                &fs::read(
                    config
                        .checkpoint_root
                        .join(generation.to_string())
                        .join("manifest.json"),
                )
                .unwrap(),
            )
            .unwrap()
        };
        let am = manifest(&full_config, am.manifest.generation);
        let bm = manifest(&incremental_config, bm.manifest.generation);
        assert_eq!(
            am.shards
                .iter()
                .map(|s| (s.shard, s.owner, s.count, s.bytes, &s.blake3))
                .collect::<Vec<_>>(),
            bm.shards
                .iter()
                .map(|s| (s.shard, s.owner, s.count, s.bytes, &s.blake3))
                .collect::<Vec<_>>()
        );
    };
    cut(&mut full).unwrap();
    cut(&mut incremental).unwrap();
    compare(&full, &incremental);
    calibrated(&mut incremental);
    for off in 50..53 {
        for e in [&mut full, &mut incremental] {
            deliver(e, 0, off, 1, 3, 0);
            cut(e).unwrap();
        }
        compare(&full, &incremental);
    }
    for (partition, offset) in [(0, 53), (1, 50)] {
        for e in [&mut full, &mut incremental] {
            deliver(e, partition, offset, u64::MAX, 0, 200);
        }
    }
    let mut outputs = [vec![], vec![]];
    for (index, e) in [&mut full, &mut incremental].into_iter().enumerate() {
        let state = e.state.as_mut().unwrap();
        while let Some(output) = state.next_close(e.progress.frontier).unwrap() {
            outputs[index].push(serde_json::to_value(output).unwrap());
            state.confirm_close().unwrap();
        }
        cut(e).unwrap();
    }
    assert_eq!(outputs[0].len(), 100);
    assert_eq!(outputs[0], outputs[1]);
    compare(&full, &incremental);
}
