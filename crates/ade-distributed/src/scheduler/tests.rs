use super::*;
fn ledger(n: usize, workers: usize) -> Ledger {
    let config = Config {
        policy: Policy::LeasedV1,
        recovery_enabled: false,
        audit_capacity: 1,
        ..Config::default()
    };
    ledger_with(n, workers, config)
}
fn ledger_with(n: usize, workers: usize, config: Config) -> Ledger {
    let source = SourceSnapshot {
        version: 1,
        manifest_fingerprint: "manifest".into(),
        schema_fingerprint: "schema".into(),
        logical_fingerprint: "logical".into(),
        replayable: false,
        morsels: (0..n)
            .map(|i| SourceMorsel {
                id: MorselId(i as u64),
                path: format!("/{i}.parquet").into(),
                fingerprint: "a".repeat(64),
                bytes: (i as u64 + 1) * 100,
            })
            .collect(),
    };
    let fence = Fence {
        job: JobId(1),
        execution: "scope".into(),
        epoch: 1,
        digest: "d".repeat(64),
    };
    let mut l = Ledger::new(fence, source, config, workers, 100_000).unwrap();
    for w in 0..workers {
        rpc(&mut l, w, Op::Ready, 0).unwrap();
    }
    l
}
fn rpc(l: &mut Ledger, w: usize, op: Op, now: u64) -> Result<Response> {
    l.handle(
        Rpc {
            fence: l.metrics.fence.clone(),
            worker: WorkerId(w as u64),
            op,
        },
        now,
    )
}
fn next(l: &mut Ledger, w: usize, now: u64) -> Lease {
    match rpc(l, w, Op::Acquire, now).unwrap() {
        Response::Lease(lease) => lease,
        r => panic!("unexpected {r:?}"),
    }
}
fn ack(lease: Lease) -> Completion {
    Completion {
        lease,
        input_rows: 10,
        output_rows: 3,
        work_ns: 1000,
    }
}
#[test]
fn malformed_control_aborts_service_and_dead_service_refuses_clients() {
    use std::io::Write;
    let fixture = ledger(2, 2);
    let mut task = TaskConfig {
        scheduler: fixture.metrics.config.clone(),
        ..TaskConfig::default()
    };
    task.exchange.mode = crate::ExchangeMode::Spill;
    let workers = (0..2)
        .map(|w| WorkerEndpoint {
            managed: None,
            id: WorkerId(w),
            control: format!("127.0.0.1:{}", 10000 + w * 2).parse().unwrap(),
            exchange: format!("127.0.0.1:{}", 10001 + w * 2).parse().unwrap(),
        })
        .collect::<Vec<_>>();
    let service = Service::start(
        JobId(1),
        "scope",
        &workers,
        &task,
        fixture.metrics.source.clone(),
        Duration::from_secs(2),
    )
    .unwrap();
    let spec = service.spec.clone();
    let mut socket = protocol::connect(spec.address, Duration::from_secs(1)).unwrap();
    socket.write_all(b"ADETCP11").unwrap();
    drop(socket);
    let deadline = Instant::now() + Duration::from_secs(1);
    while service.ledger.lock().unwrap().failed.is_none() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(2));
    }
    assert!(service.finish().is_err());
    drop(service);
    assert!(
        Client::start(
            spec,
            WorkerId(0),
            &task.scheduler,
            CancellationToken::default()
        )
        .is_err()
    );
}
#[test]
fn stalled_control_does_not_monopolize_minimum_lease_renewals() {
    use std::io::Write;
    let fixture = ledger(2, 2);
    let mut task = TaskConfig {
        scheduler: Config {
            lease_ms: 80,
            heartbeat_ms: 20,
            ..fixture.metrics.config.clone()
        },
        ..TaskConfig::default()
    };
    task.exchange.mode = crate::ExchangeMode::Spill;
    let workers = (0..2)
        .map(|w| WorkerEndpoint {
            managed: None,
            id: WorkerId(w),
            control: format!("127.0.0.1:{}", 10000 + w * 2).parse().unwrap(),
            exchange: format!("127.0.0.1:{}", 10001 + w * 2).parse().unwrap(),
        })
        .collect::<Vec<_>>();
    let service = Service::start(
        JobId(1),
        "scope",
        &workers,
        &task,
        fixture.metrics.source.clone(),
        Duration::from_secs(2),
    )
    .unwrap();
    let first = Client::start(
        service.spec.clone(),
        WorkerId(0),
        &task.scheduler,
        CancellationToken::default(),
    )
    .unwrap();
    let _second = Client::start(
        service.spec.clone(),
        WorkerId(1),
        &task.scheduler,
        CancellationToken::default(),
    )
    .unwrap();
    let lease = first.next().unwrap().unwrap();
    let mut stalled = protocol::connect(service.spec.address, Duration::from_secs(1)).unwrap();
    stalled.write_all(b"A").unwrap();
    // Longer than an entire minimum lease, shorter than the bounded peer timeout.
    thread::sleep(Duration::from_millis(180));
    {
        let state = service.ledger.lock().unwrap();
        assert!(state.failed.is_none(), "{:?}", state.failed);
        assert!(
            state.metrics.renewals >= 3,
            "healthy renewals blocked by partial peer"
        );
    }
    first.complete(ack(lease)).unwrap();
    drop(stalled);
    let until = Instant::now() + Duration::from_secs(1);
    while service.ledger.lock().unwrap().failed.is_none() && Instant::now() < until {
        thread::sleep(Duration::from_millis(2));
    }
    // A truncated peer still poisons the job; isolation never weakens safety.
    assert!(service.finish().is_err());
    assert!(first.next().is_err());
}
#[test]
fn persistent_idle_main_session_retains_independent_renewals_and_closure() {
    let fixture = ledger(2, 2);
    let task = TaskConfig {
        scheduler: Config {
            lease_ms: 80,
            heartbeat_ms: 20,
            ..fixture.metrics.config.clone()
        },
        ..TaskConfig::default()
    };
    let workers = (0..2)
        .map(|w| WorkerEndpoint {
            managed: None,
            id: WorkerId(w),
            control: format!("127.0.0.1:{}", 10000 + w * 2).parse().unwrap(),
            exchange: format!("127.0.0.1:{}", 10001 + w * 2).parse().unwrap(),
        })
        .collect::<Vec<_>>();
    let service = Service::start(
        JobId(1),
        "scope",
        &workers,
        &task,
        fixture.metrics.source.clone(),
        Duration::from_secs(3),
    )
    .unwrap();
    let first = Client::start(
        service.spec.clone(),
        WorkerId(0),
        &task.scheduler,
        CancellationToken::default(),
    )
    .unwrap();
    let second = Client::start(
        service.spec.clone(),
        WorkerId(1),
        &task.scheduler,
        CancellationToken::default(),
    )
    .unwrap();
    let a = first.next().unwrap().unwrap();
    let b = second.next().unwrap().unwrap();
    // Both main sockets are idle beyond the per-frame limit while separate
    // persistent renewal sessions keep the minimum 80ms leases alive.
    thread::sleep(Duration::from_millis(650));
    assert!(service.ledger.lock().unwrap().failed.is_none());
    assert!(service.ledger.lock().unwrap().metrics.renewals >= 20);
    first.complete(ack(a)).unwrap();
    second.complete(ack(b)).unwrap();
    assert!(first.next().unwrap().is_none());
    assert!(second.next().unwrap().is_none());
    first.closed().unwrap();
    second.closed().unwrap();
    drop(first);
    drop(second);
    thread::sleep(Duration::from_millis(5));
    service.finish().unwrap();
}

#[test]
fn coverage_steal_duplicate_ack_and_bounded_log() {
    let mut l = ledger(16, 2);
    let first = next(&mut l, 0, 1);
    // Worker0 stays busy while worker1 consumes its own queue then pending donor work.
    while l.metrics.entries.iter().any(|e| e.lease.is_none()) {
        let lease = next(&mut l, 1, 2);
        let c = ack(lease);
        rpc(&mut l, 1, Op::Complete(c.clone()), 3).unwrap();
        rpc(&mut l, 1, Op::Complete(c), 3).unwrap();
    }
    assert!(
        l.metrics
            .entries
            .iter()
            .any(|e| e.preferred == WorkerId(0) && e.lease.as_ref().unwrap().worker == WorkerId(1))
    );
    rpc(&mut l, 0, Op::Complete(ack(first)), 4).unwrap();
    rpc(&mut l, 0, Op::Closed, 5).unwrap();
    rpc(&mut l, 1, Op::Closed, 5).unwrap();
    let m = l.finish(6).unwrap();
    assert_eq!(m.entries.len(), 16);
    assert_eq!(m.decisions.len(), 1);
    assert_eq!(m.dropped_decisions, 15);
    assert!(m.peak_outstanding <= 2);
}
#[test]
fn fencing_owner_nonce_source_conflicts_and_expiry() {
    for change in 0..5 {
        let mut l = ledger(2, 2);
        let lease = next(&mut l, 0, 1);
        let mut c = ack(lease);
        match change {
            0 => c.lease.nonce += 1,
            1 => c.lease.worker = WorkerId(1),
            2 => c.lease.morsel.fingerprint = "b".repeat(64),
            3 => c.lease.morsel.path = "/forged".into(),
            _ => c.output_rows = 11,
        }
        assert!(rpc(&mut l, 0, Op::Complete(c), 2).is_err());
    }
    let mut l = ledger(2, 2);
    let lease = next(&mut l, 0, 1);
    let c = ack(lease.clone());
    rpc(&mut l, 0, Op::Complete(c.clone()), 2).unwrap();
    let mut bad = c;
    bad.work_ns += 1;
    assert!(rpc(&mut l, 0, Op::Complete(bad), 3).is_err());
    assert!(rpc(&mut l, 0, Op::Renew(lease), 3).is_err());
    for kind in 0..4 {
        let mut l = ledger(2, 2);
        let mut f = l.metrics.fence.clone();
        match kind {
            0 => f.epoch += 1,
            1 => f.job = JobId(9),
            2 => f.execution = "foreign".into(),
            _ => f.digest = "a".repeat(64),
        }
        assert!(
            l.handle(
                Rpc {
                    fence: f,
                    worker: WorkerId(0),
                    op: Op::Acquire
                },
                1
            )
            .is_err()
        );
    }
    let mut l = ledger(2, 2);
    let lease = next(&mut l, 0, 1);
    rpc(&mut l, 0, Op::Renew(lease.clone()), 9000).unwrap();
    assert!(rpc(&mut l, 0, Op::Acquire, 9001).is_err());
    assert!(rpc(&mut l, 0, Op::Complete(ack(lease)), 19000).is_err());
    assert_eq!(
        l.metrics
            .entries
            .iter()
            .filter(|e| e.lease.is_some())
            .count(),
        1
    );
}
#[test]
fn participation_drain_has_cutoff_and_spot_deadline() {
    let config = Config {
        policy: Policy::LeasedV1,
        recovery_enabled: false,
        participation: vec![Participation {
            after_completed: 1,
            worker: WorkerId(2),
            action: Action::Activate,
        }],
        ..Config::default()
    };
    let mut l = ledger_with(8, 4, config);
    l.metrics.workers[2] = WorkerState::Parked;
    l.metrics.workers[3] = WorkerState::Parked;
    assert!(matches!(
        rpc(&mut l, 2, Op::Acquire, 1).unwrap(),
        Response::Wait
    ));
    let lease = next(&mut l, 0, 1);
    let other = next(&mut l, 1, 2);
    rpc(&mut l, 1, Op::Complete(ack(other)), 3).unwrap();
    let activated = next(&mut l, 2, 4);
    l.transition(0, Action::SpotNotice { grace_ms: 100 }, 5)
        .unwrap();
    l.transition(0, Action::Drain, 6).unwrap();
    assert!(
        l.metrics
            .events
            .iter()
            .any(|e| e.in_flight == Some(lease.morsel.id))
    );
    rpc(&mut l, 0, Op::Complete(ack(lease)), 7).unwrap();
    assert!(matches!(
        rpc(&mut l, 0, Op::Acquire, 8).unwrap(),
        Response::Finish
    ));
    rpc(&mut l, 0, Op::Closed, 9).unwrap();
    rpc(&mut l, 2, Op::Complete(ack(activated)), 10).unwrap();
    l.check(104).unwrap();
    let mut l = ledger(4, 2);
    let _ = next(&mut l, 0, 1);
    l.transition(0, Action::SpotNotice { grace_ms: 1 }, 2)
        .unwrap();
    assert!(l.check(3).is_err());
    l.metrics.workers[1] = WorkerState::Drained;
    assert!(l.transition(0, Action::Drain, 2).is_ok());
}
#[test]
fn no_steal_empty_producer_deadlines_and_bounds() {
    let mut l = ledger(1, 4);
    l.metrics.config.policy = Policy::LeasedNoSteal;
    assert!(matches!(
        rpc(&mut l, 1, Op::Acquire, 1).unwrap(),
        Response::Finish
    ));
    assert!(l.finish(2).is_err());
    assert!(l.check(100_000).is_err());
    let mut l = ledger(1, 2);
    l.metrics.config.max_control_messages = 0;
    assert!(rpc(&mut l, 0, Op::Acquire, 1).is_err());
    for c in [
        Config {
            lease_ms: 1,
            ..Config::default()
        },
        Config {
            capacities: vec![0, 1],
            ..Config::default()
        },
        Config {
            max_morsels: 1025,
            ..Config::default()
        },
        Config {
            audit_capacity: 513,
            ..Config::default()
        },
        Config {
            initial_active: 5,
            ..Config::default()
        },
    ] {
        assert!(c.validate(2).is_err());
    }
    let mut l = ledger(1, 2);
    l.metrics.config.audit_capacity = 1;
    let _ = next(&mut l, 0, 1);
    assert!(rpc(&mut l, 0, Op::Closed, 2).is_err());
}
#[test]
fn pure_capacity_locality_critical_path_and_overflow() {
    let mut o = Observation {
        pending: vec![(MorselId(1), 1, 100, true), (MorselId(2), 2, 100, false)],
        queued: vec![0, 100, 1000],
        capacities: vec![1, 1, 1],
        active_workers: vec![true, true, true],
        requester: 0,
        critical: false,
        steal: true,
        min_steal_bytes: 1,
    };
    assert_eq!(decide(&o).unwrap().unwrap().morsel, MorselId(1));
    o.critical = true;
    assert_eq!(decide(&o).unwrap().unwrap().morsel, MorselId(2));
    o.capacities[2] = 100;
    assert_eq!(decide(&o).unwrap().unwrap().morsel, MorselId(1));
    o.steal = false;
    assert!(decide(&o).unwrap().is_none());
    o.capacities[0] = 0;
    assert!(decide(&o).is_err());
    o.capacities[0] = 1;
    o.queued[2] = u64::MAX;
    o.capacities[2] = 1;
    o.steal = true;
    assert!(decide(&o).is_err());
    let mut o = Observation {
        pending: vec![(MorselId(0), 0, 100, false), (MorselId(1), 1, 200, false)],
        queued: vec![100, 1000],
        capacities: vec![10, 1],
        active_workers: vec![true, true],
        requester: 0,
        critical: true,
        steal: true,
        min_steal_bytes: 1,
    };
    assert_eq!(decide(&o).unwrap().unwrap().preferred, WorkerId(1));
    o.capacities[0] = 1;
    o.capacities[1] = 100;
    assert_eq!(decide(&o).unwrap().unwrap().preferred, WorkerId(0));
}

#[test]
fn minimum_steal_waits_for_active_owner_and_falls_back_for_inactive_owners() {
    let mut observation = Observation {
        pending: vec![(MorselId(1), 1, 100, false)],
        queued: vec![0, 100],
        capacities: vec![1, 1],
        active_workers: vec![true, true],
        requester: 0,
        critical: false,
        steal: true,
        min_steal_bytes: 1_000,
    };
    assert!(decide(&observation).unwrap().is_none());

    observation.active_workers[1] = false;
    let decision = decide(&observation).unwrap().unwrap();
    assert_eq!(decision.morsel, MorselId(1));
    assert_eq!(decision.reason, "steal_pending_for_progress");

    observation.steal = false;
    assert!(decide(&observation).unwrap().is_none());
}

#[test]
fn parked_producer_waits_for_scheduled_activation_then_closes() {
    let config = Config {
        policy: Policy::LeasedV1,
        recovery_enabled: false,
        initial_active: 2,
        participation: vec![Participation {
            after_completed: 3,
            worker: WorkerId(2),
            action: Action::Activate,
        }],
        ..Config::default()
    };
    let mut l = ledger_with(4, 4, config);

    let first = next(&mut l, 0, 1);
    rpc(&mut l, 0, Op::Complete(ack(first)), 2).unwrap();
    let second = next(&mut l, 1, 3);
    rpc(&mut l, 1, Op::Complete(ack(second)), 4).unwrap();
    let third = next(&mut l, 0, 5);
    let fourth = next(&mut l, 1, 6);

    assert!(matches!(
        rpc(&mut l, 2, Op::Acquire, 7).unwrap(),
        Response::Wait
    ));
    rpc(&mut l, 0, Op::Complete(ack(third)), 8).unwrap();
    assert_eq!(l.metrics.workers[2], WorkerState::Active);
    assert!(matches!(
        rpc(&mut l, 2, Op::Acquire, 9).unwrap(),
        Response::Finish
    ));
    rpc(&mut l, 2, Op::Closed, 10).unwrap();

    rpc(&mut l, 1, Op::Complete(ack(fourth)), 11).unwrap();
    for worker in [0, 1, 3] {
        assert!(matches!(
            rpc(&mut l, worker, Op::Acquire, 12).unwrap(),
            Response::Finish
        ));
        rpc(&mut l, worker, Op::Closed, 13).unwrap();
    }
    assert_eq!(l.finish(14).unwrap().events.len(), 1);
}

#[test]
fn parked_without_activation_closes_and_small_pending_work_makes_progress() {
    let config = Config {
        policy: Policy::LeasedV1,
        recovery_enabled: false,
        initial_active: 1,
        min_steal_bytes: 1_000_000,
        ..Config::default()
    };
    let mut l = ledger_with(2, 2, config.clone());
    let first = next(&mut l, 0, 1);
    rpc(&mut l, 0, Op::Complete(ack(first)), 2).unwrap();
    let stolen = next(&mut l, 0, 3);
    assert_eq!(stolen.morsel.id, MorselId(1));
    assert!(
        l.metrics
            .decisions
            .iter()
            .any(|decision| decision.reason == "steal_pending_for_progress")
    );
    rpc(&mut l, 0, Op::Complete(ack(stolen)), 4).unwrap();
    for worker in 0..2 {
        assert!(matches!(
            rpc(&mut l, worker, Op::Acquire, 5).unwrap(),
            Response::Finish
        ));
        rpc(&mut l, worker, Op::Closed, 6).unwrap();
    }
    l.finish(7).unwrap();

    let mut empty_parked = ledger_with(1, 2, config);
    assert!(matches!(
        rpc(&mut empty_parked, 1, Op::Acquire, 1).unwrap(),
        Response::Finish
    ));
    let only = next(&mut empty_parked, 0, 1);
    rpc(&mut empty_parked, 0, Op::Complete(ack(only)), 2).unwrap();
    rpc(&mut empty_parked, 0, Op::Closed, 3).unwrap();
    rpc(&mut empty_parked, 1, Op::Closed, 3).unwrap();
    empty_parked.finish(4).unwrap();
}

#[test]
fn source_catalog_bounds_participation_and_rejects_last_active_drain() {
    let impossible_count = Config {
        policy: Policy::LeasedV1,
        recovery_enabled: false,
        participation: vec![Participation {
            after_completed: 2,
            worker: WorkerId(1),
            action: Action::Activate,
        }],
        ..Config::default()
    };
    assert!(impossible_count.validate(2).is_ok());
    assert!(impossible_count.validate_for_source(2, 1).is_err());

    let last_drain = Config {
        policy: Policy::LeasedV1,
        recovery_enabled: false,
        initial_active: 1,
        participation: vec![Participation {
            after_completed: 1,
            worker: WorkerId(0),
            action: Action::Drain,
        }],
        ..Config::default()
    };
    assert!(last_drain.validate_for_source(2, 2).is_err());

    let final_activation = Config {
        policy: Policy::LeasedV1,
        recovery_enabled: false,
        initial_active: 1,
        participation: vec![Participation {
            after_completed: 1,
            worker: WorkerId(1),
            action: Action::Activate,
        }],
        ..Config::default()
    };
    let mut l = ledger_with(1, 2, final_activation);
    assert!(matches!(
        rpc(&mut l, 1, Op::Acquire, 1).unwrap(),
        Response::Wait
    ));
    let only = next(&mut l, 0, 1);
    rpc(&mut l, 0, Op::Complete(ack(only)), 2).unwrap();
    assert_eq!(l.metrics.workers[1], WorkerState::Active);
    assert!(matches!(
        rpc(&mut l, 1, Op::Acquire, 3).unwrap(),
        Response::Finish
    ));
    rpc(&mut l, 1, Op::Closed, 4).unwrap();
    assert!(matches!(
        rpc(&mut l, 0, Op::Acquire, 4).unwrap(),
        Response::Finish
    ));
    rpc(&mut l, 0, Op::Closed, 5).unwrap();
    l.finish(6).unwrap();
}

#[test]
fn source_verification_checks_deadline_cancellation_and_drift() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("source");
    std::fs::write(&path, b"immutable").unwrap();
    let morsel = SourceMorsel {
        id: MorselId(0),
        path: path.clone(),
        fingerprint: crate::lineage::fingerprint(&path).unwrap(),
        bytes: 9,
    };
    verify_sources(
        std::slice::from_ref(&morsel),
        &CancellationToken::default(),
        Instant::now() + Duration::from_secs(1),
    )
    .unwrap();
    assert!(
        verify_sources(
            std::slice::from_ref(&morsel),
            &CancellationToken::default(),
            Instant::now()
        )
        .is_err()
    );
    let cancelled = CancellationToken::default();
    cancelled.cancel();
    assert!(
        verify_sources(
            std::slice::from_ref(&morsel),
            &cancelled,
            Instant::now() + Duration::from_secs(1)
        )
        .is_err()
    );
    std::fs::write(&path, b"different").unwrap();
    assert!(
        verify_sources(
            &[morsel],
            &CancellationToken::default(),
            Instant::now() + Duration::from_secs(1)
        )
        .is_err()
    );
}

#[test]
fn source_replacement_and_symlink_drift_are_rechecked_after_a_verified_pass() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("source");
    let replacement = root.path().join("replacement");
    let original = vec![42u8; 1024 * 1024 + 7];
    std::fs::write(&path, &original).unwrap();
    std::fs::write(&replacement, vec![43u8; original.len()]).unwrap();
    let morsel = SourceMorsel {
        id: MorselId(0),
        path: path.clone(),
        fingerprint: crate::lineage::fingerprint(&path).unwrap(),
        bytes: original.len() as u64,
    };
    let verify = || {
        verify_sources(
            std::slice::from_ref(&morsel),
            &CancellationToken::default(),
            Instant::now() + Duration::from_secs(1),
        )
    };
    verify().unwrap();
    // Same size and same path, different bytes: metadata cannot authorize an ACK.
    std::fs::rename(&replacement, &path).unwrap();
    assert!(verify().is_err());
    #[cfg(unix)]
    {
        std::fs::write(&replacement, &original).unwrap();
        std::fs::remove_file(&path).unwrap();
        std::os::unix::fs::symlink(&replacement, &path).unwrap();
        verify().unwrap();
        std::fs::write(&replacement, vec![44u8; original.len()]).unwrap();
        assert!(verify().is_err());
    }
}

#[test]
fn control_admission_saturation_aborts_and_closes_bounded_peer_set() {
    use std::io::{Read, Write};
    let fixture = ledger(2, 2);
    let task = TaskConfig {
        scheduler: fixture.metrics.config.clone(),
        ..TaskConfig::default()
    };
    let workers = (0..2)
        .map(|w| WorkerEndpoint {
            managed: None,
            id: WorkerId(w),
            control: format!("127.0.0.1:{}", 10000 + w * 2).parse().unwrap(),
            exchange: format!("127.0.0.1:{}", 10001 + w * 2).parse().unwrap(),
        })
        .collect::<Vec<_>>();
    let service = Service::start(
        JobId(1),
        "scope",
        &workers,
        &task,
        fixture.metrics.source.clone(),
        Duration::from_secs(2),
    )
    .unwrap();
    let mut peers = Vec::new();
    for _ in 0..6 {
        let mut socket = protocol::connect(service.spec.address, Duration::from_secs(1)).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        socket.write_all(b"A").unwrap();
        peers.push(socket);
    }
    let until = Instant::now() + Duration::from_millis(400);
    while service.ledger.lock().unwrap().failed.is_none() && Instant::now() < until {
        thread::sleep(Duration::from_millis(2));
    }
    let failure = service.ledger.lock().unwrap().failed.clone().unwrap();
    assert!(failure.contains("control admission cap"), "{failure}");
    assert!(service.finish().is_err());
    drop(service);
    for mut peer in peers {
        // Closing with unread input may yield reset rather than EOF.
        match peer.read(&mut [0]) {
            Ok(n) => assert_eq!(n, 0),
            Err(e) => assert!(
                matches!(
                    e.kind(),
                    std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::ConnectionAborted
                        | std::io::ErrorKind::BrokenPipe
                        | std::io::ErrorKind::NotConnected
                ),
                "{e}"
            ),
        }
    }
}
