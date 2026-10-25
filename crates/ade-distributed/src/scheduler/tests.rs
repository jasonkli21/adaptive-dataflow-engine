use super::*;
fn ledger(n: usize, workers: usize) -> Ledger {
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
    let c = Config {
        policy: Policy::LeasedV1,
        recovery_enabled: false,
        audit_capacity: 1,
        ..Config::default()
    };
    let mut l = Ledger::new(fence, source, c, workers, 100_000).unwrap();
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
    let mut l = ledger(8, 4);
    l.metrics.workers[2] = WorkerState::Parked;
    l.metrics.workers[3] = WorkerState::Parked;
    assert!(matches!(
        rpc(&mut l, 2, Op::Acquire, 1).unwrap(),
        Response::Wait
    ));
    let lease = next(&mut l, 0, 1);
    l.transition(2, Action::Activate, 2).unwrap();
    let _ = next(&mut l, 2, 2);
    l.transition(0, Action::SpotNotice { grace_ms: 100 }, 3)
        .unwrap();
    l.transition(0, Action::Drain, 4).unwrap();
    assert!(
        l.metrics
            .events
            .iter()
            .any(|e| e.in_flight == Some(lease.morsel.id))
    );
    rpc(&mut l, 0, Op::Complete(ack(lease)), 5).unwrap();
    assert!(matches!(
        rpc(&mut l, 0, Op::Acquire, 6).unwrap(),
        Response::Finish
    ));
    rpc(&mut l, 0, Op::Closed, 7).unwrap();
    rpc(&mut l, 0, Op::Closed, 8).unwrap();
    l.check(104).unwrap();
    let mut l = ledger(4, 2);
    let _ = next(&mut l, 0, 1);
    l.transition(0, Action::SpotNotice { grace_ms: 1 }, 2)
        .unwrap();
    assert!(l.check(3).is_err());
    l.metrics.workers[1] = WorkerState::Drained;
    assert!(l.transition(0, Action::Drain, 2).is_err());
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
