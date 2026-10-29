use ade_stream::{
    Engine, Error, EventV1, RawRecord, Security, Start, StreamConfigV1,
    timers::{Fired, Kind, Registration, Timers},
};
use uuid::Uuid;
fn config() -> StreamConfigV1 {
    StreamConfigV1 {
        version: 1,
        bootstrap: "127.0.0.1:19092".into(),
        expected_cluster_id: "test".into(),
        topic: "test".into(),
        starts: vec![
            Start {
                partition: 0,
                offset: 5,
            },
            Start {
                partition: 1,
                offset: 0,
            },
        ],
        security: Security::LoopbackDev {},
        key_less_than: 10,
        allowance_ms: 10,
        payload_bytes: 4096,
        client_queue_kib: 16,
        sink_stall_ms: 60000,
        timers: vec![],
    }
}
fn record(e: &Engine, p: i32, offset: i64, key: u64, time: i64) -> RawRecord {
    RawRecord {
        session: e.progress.session,
        partition: p,
        offset,
        payload: serde_json::to_vec(&EventV1 {
            version: 1,
            key,
            value_i64: -12,
            event_time_ms: time,
        })
        .unwrap(),
    }
}
fn deliver(e: &mut Engine, p: i32, offset: i64, key: u64, time: i64) {
    let r = record(e, p, offset, key, time);
    e.prepare(r).unwrap();
    e.acknowledge().unwrap();
}
#[test]
fn offsets_holes_frontier_and_late_records_follow_sink_ack() {
    let mut e = Engine::new(config(), Uuid::new_v4()).unwrap();
    let receipt = e.prepare(record(&e, 0, 5, 3, 100)).unwrap();
    assert_eq!(receipt.projected.unwrap().value_i64, -12);
    assert_eq!(e.progress.partitions[&0].seen, Some(5));
    assert_eq!(e.progress.partitions[&0].forwarded, None);
    assert_eq!(e.progress.frontier, None);
    assert!(e.prepare(record(&e, 0, 8, 0, 101)).is_err());
    e.acknowledge().unwrap();
    assert_eq!(e.progress.partitions[&0].watermark, Some(90));
    assert_eq!(e.progress.frontier, None); // quiet partition remains unknown
    deliver(&mut e, 1, 0, 3, 70);
    assert_eq!(e.progress.frontier, Some(60));
    deliver(&mut e, 0, 8, 11, 80); // legal hole and an explicit filtered receipt
    assert_eq!(e.progress.partitions[&0].watermark, Some(90));
    assert_eq!(e.progress.late_records, 1);
    assert_eq!(e.progress.partitions[&0].next_offset_suggestion, 9);
    deliver(&mut e, 1, 4, 0, 200);
    assert_eq!(e.progress.frontier, Some(90));
    assert!(e.prepare(record(&e, 0, 8, 0, 200)).is_err());
    assert!(e.acknowledge().is_err());
}
#[test]
fn negative_times_checked_overflow_and_malformed_fail_without_ack() {
    let mut e = Engine::new(config(), Uuid::new_v4()).unwrap();
    deliver(&mut e, 0, 5, 0, -100);
    assert_eq!(e.progress.partitions[&0].watermark, Some(-110));
    let bad = record(&e, 1, 0, 0, i64::MIN);
    assert!(matches!(e.prepare(bad), Err(Error::Overflow(_))));
    assert_eq!(e.progress.partitions[&1].seen, Some(0));
    assert_eq!(e.progress.partitions[&1].forwarded, None);
    let mut bad = record(&e, 1, 1, 0, 10);
    bad.payload = b"{\"version\":1,\"extra\":0}".to_vec();
    assert!(matches!(e.prepare(bad), Err(Error::Event)));
    let mut bad = record(&e, 1, 2, 0, 10);
    bad.payload = vec![b' '; 4097];
    assert!(e.prepare(bad).is_err());
    let bad = record(&e, 1, i64::MAX, 0, 1);
    assert!(e.prepare(bad).is_err());
    assert_eq!(e.progress.partitions[&1].next_offset_suggestion, 0);
}
#[test]
fn session_partition_start_fencing_and_cancellation() {
    let mut e = Engine::new(config(), Uuid::new_v4()).unwrap();
    let mut r = record(&e, 0, 5, 1, 50);
    r.session = Uuid::new_v4();
    assert!(e.prepare(r).is_err());
    assert!(e.prepare(record(&e, 3, 5, 1, 50)).is_err());
    assert!(e.prepare(record(&e, 0, 4, 1, 50)).is_err());
    e.prepare(record(&e, 0, 5, 1, 50)).unwrap();
    e.cancel();
    assert!(e.acknowledge().is_err());
    assert_eq!(e.progress.partitions[&0].forwarded, None);
    let restart = Engine::new(config(), Uuid::new_v4()).unwrap();
    assert_ne!(e.progress.session, restart.progress.session);
}
#[test]
fn strict_admission_security_and_event_schema() {
    let c = config();
    let bytes = serde_json::to_vec(&c).unwrap();
    assert!(StreamConfigV1::parse(&bytes).is_ok());
    let mut v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    v["unknown"] = true.into();
    assert!(StreamConfigV1::parse(&serde_json::to_vec(&v).unwrap()).is_err());
    let mut nested = serde_json::to_value(config()).unwrap();
    nested["security"]["unexpected"] = true.into();
    assert!(StreamConfigV1::parse(&serde_json::to_vec(&nested).unwrap()).is_err());
    let mut c = config();
    c.bootstrap = "192.0.2.1:9092".into();
    assert!(c.validate().is_err());
    let mut c = config();
    c.starts[1].partition = 0;
    assert!(c.validate().is_err());
    let mut c = config();
    c.allowance_ms = -1;
    assert!(c.validate().is_err());
    let mut c = config();
    c.payload_bytes = 4097;
    assert!(c.validate().is_err());
    assert!(StreamConfigV1::parse(&vec![b' '; 16385]).is_err());
    let mut e = Engine::new(config(), Uuid::new_v4()).unwrap();
    let mut r = record(&e, 0, 5, 1, 1);
    r.payload =
        br#"{"version":1,"key":1,"value_i64":0,"event_time_ms":0,"secret":"never log"}"#.to_vec();
    assert!(matches!(e.prepare(r), Err(Error::Event)));
    let mut r = record(&e, 0, 6, 1, 1);
    r.payload = br#"{"version":1,"key":1,"key":2,"value_i64":0,"event_time_ms":0}"#.to_vec();
    assert!(e.prepare(r).is_err());
}
#[test]
fn timers_deadline_kind_tie_order_caps_and_no_refire() {
    let mut timers = Timers::new(3, 384, 4).unwrap();
    let event_id = timers
        .register(Registration {
            kind: Kind::Event,
            deadline_ms: -5,
        })
        .unwrap();
    let first_processing_id = timers
        .register(Registration {
            kind: Kind::Processing,
            deadline_ms: 10,
        })
        .unwrap();
    let second_processing_id = timers
        .register(Registration {
            kind: Kind::Processing,
            deadline_ms: 10,
        })
        .unwrap();
    assert!(
        timers
            .register(Registration {
                kind: Kind::Event,
                deadline_ms: 0
            })
            .is_err()
    );
    assert_eq!(timers.fire(9, None), Vec::<Fired>::new());
    assert_eq!(
        timers.fire(10, None),
        vec![
            Fired {
                id: first_processing_id,
                kind: Kind::Processing,
                deadline_ms: 10
            },
            Fired {
                id: second_processing_id,
                kind: Kind::Processing,
                deadline_ms: 10
            }
        ]
    );
    assert_eq!(timers.fire(10, Some(-6)), Vec::<Fired>::new());
    assert_eq!(
        timers.fire(10, Some(-5)),
        vec![Fired {
            id: event_id,
            kind: Kind::Event,
            deadline_ms: -5
        }]
    );
    assert_eq!(timers.fire(u64::MAX, Some(i64::MAX)), Vec::<Fired>::new());
    let cancelled_id = timers
        .register(Registration {
            kind: Kind::Event,
            deadline_ms: 0,
        })
        .unwrap();
    assert!(timers.cancel(cancelled_id));
    assert!(
        timers
            .register(Registration {
                kind: Kind::Event,
                deadline_ms: 0
            })
            .is_err()
    ); // lifetime cap
    assert_eq!(timers.charged_bytes(), 0);
    let mut timers = Timers::new(2, 128, 2).unwrap();
    assert!(
        timers
            .register(Registration {
                kind: Kind::Processing,
                deadline_ms: -1
            })
            .is_err()
    );
    timers
        .register(Registration {
            kind: Kind::Event,
            deadline_ms: 0,
        })
        .unwrap();
    assert!(
        timers
            .register(Registration {
                kind: Kind::Event,
                deadline_ms: 0
            })
            .is_err()
    );
    timers.clear();
    assert_eq!(timers.fire(10, Some(10)), Vec::<Fired>::new());
}
#[test]
fn native_filter_projection_exact_extremes_and_partition_order() {
    let mut c = config();
    c.allowance_ms = 0;
    let mut e = Engine::new(c, Uuid::new_v4()).unwrap();
    for (offset, key, value, time) in [
        (5, 0, i64::MIN, -1),
        (6, 9, i64::MAX, i64::MAX),
        (7, 10, 0, 0),
        (11, u64::MAX, -1, 3),
    ] {
        let r = RawRecord {
            session: e.progress.session,
            partition: 0,
            offset,
            payload: serde_json::to_vec(&EventV1 {
                version: 1,
                key,
                value_i64: value,
                event_time_ms: time,
            })
            .unwrap(),
        };
        let receipt = e.prepare(r).unwrap();
        assert_eq!(
            receipt
                .projected
                .map(|p| (p.key, p.value_i64, p.event_time_ms)),
            (key < 10).then_some((key, value, time))
        );
        e.acknowledge().unwrap();
    }
    assert_eq!(e.progress.partitions[&0].seen, Some(11));
    assert!(e.arrow_peak_bytes <= 8192);
}

#[test]
fn four_partition_frontier_requires_every_partition_and_config_refuses_five() {
    let mut c = config();
    c.starts = (0..4)
        .map(|partition| Start {
            partition,
            offset: 0,
        })
        .collect();
    let mut e = Engine::new(c.clone(), Uuid::new_v4()).unwrap();
    for (partition, time) in [(0, 100), (3, 1000), (2, -10)] {
        deliver(&mut e, partition, 0, 1, time);
        assert_eq!(e.progress.frontier, None);
    }
    deliver(&mut e, 1, 0, 1, 500);
    assert_eq!(e.progress.frontier, Some(-20));
    c.starts.push(Start {
        partition: 4,
        offset: 0,
    });
    assert!(c.validate().is_err());
}
