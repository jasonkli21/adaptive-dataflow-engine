#![cfg(all(feature = "unified-planner", feature = "transactional-state"))]
use ade_logical::{Computation, Kafka, KafkaMode, LogicalPlanV1, Offset, Sink};
use ade_stream::unified::Admission;
fn fixture(hybrid: bool, committed: bool) -> (LogicalPlanV1, ade_stream::transactional::Config) {
    let mut c: ade_stream::transactional::Config = serde_json::from_slice(include_bytes!(
        "../../../benchmarks/examples/phase17-config3.json"
    ))
    .unwrap();
    let src = &c.state.source;
    let p = LogicalPlanV1 {
        version: 1,
        sink: if committed {
            Sink::CommittedLocalV3
        } else {
            Sink::Receipts
        },
        computation: Computation::Kafka {
            source: Kafka {
                cluster: src.expected_cluster_id.clone(),
                topic: src.topic.clone(),
                topic_partitions: c.state.topic_partitions.clone(),
                original_starts: src
                    .starts
                    .iter()
                    .map(|s| Offset {
                        partition: s.partition,
                        offset: s.offset,
                    })
                    .collect(),
                immutable_history: c.trusted_history.clone(),
                mode: if hybrid {
                    KafkaMode::HistoryThenLive {
                        cut: vec![
                            Offset {
                                partition: 0,
                                offset: 2,
                            },
                            Offset {
                                partition: 1,
                                offset: 3,
                            },
                        ],
                    }
                } else {
                    KafkaMode::Assigned
                },
            },
            job: c.state.job_id.to_string(),
            key_less_than: src.key_less_than,
            allowance_ms: src.allowance_ms,
            projection: vec!["value_i64".into(), "key".into(), "event_time_ms".into()],
            window_ms: Some(c.state.window_ms),
            late_policy: Some(c.state.late_policy.clone()),
            hash_version: 1,
            timers: vec![],
        },
    };
    if !committed {
        c.state.job_id = uuid::Uuid::from_bytes(p.bound_job_bytes().unwrap());
    }
    (p, c)
}
fn admit(p: &LogicalPlanV1) -> ade_stream::Result<Admission> {
    Admission::compile(&serde_json::to_vec(p).unwrap())
}
#[test]
fn hybrid_identity_offset_gaps_and_all_native_constraints() {
    let (p, c) = fixture(true, false);
    let a = admit(&p).unwrap();
    a.validate_state(&c.state, false).unwrap();
    assert_eq!(a.record(0, 0)["phase"], "historical");
    assert_eq!(a.record(0, 2)["phase"], "live");
    assert_eq!(a.record(1, 8)["phase"], "live");
    let mut q = p.clone();
    if let Computation::Kafka { source, .. } = &mut q.computation
        && let KafkaMode::HistoryThenLive { cut } = &mut source.mode
    {
        cut[0].offset += 1;
    }
    assert!(admit(&q).unwrap().validate_state(&c.state, false).is_err());
    let mut q = p;
    if let Computation::Kafka { source, .. } = &mut q.computation {
        source.immutable_history.push('x');
    }
    assert!(admit(&q).unwrap().validate_state(&c.state, false).is_err());
    let mut changed = c.state.clone();
    changed.source.key_less_than += 1;
    assert!(a.validate_state(&changed, false).is_err());
    changed = c.state.clone();
    changed.window_ms += 1;
    assert!(a.validate_state(&changed, false).is_err());
    changed = c.state.clone();
    changed.source.starts[0].offset += 1;
    assert!(a.validate_state(&changed, false).is_err());
    assert!(a.validate_state(&c.state, true).is_err());
    assert!(a.validate_stateless(&c.state.source).is_err());
}
#[test]
fn transactional_hybrid_refuses_and_live_retains_delivery() {
    let (p, _) = fixture(true, true);
    assert!(admit(&p).is_err());
    let (p, c) = fixture(false, true);
    let a = admit(&p).unwrap();
    a.validate_state(&c.state, true).unwrap();
    assert!(a.validate_state(&c.state, false).is_err());
    let mut q = p;
    q.sink = Sink::Receipts;
    assert!(admit(&q).unwrap().validate_state(&c.state, true).is_err());
}

#[test]
fn pure_live_origin_changes_cannot_launder_state_lineage() {
    let (plan, config) = fixture(false, false);
    admit(&plan)
        .unwrap()
        .validate_state(&config.state, false)
        .unwrap();
    let mut changed = plan;
    if let Computation::Kafka { source, .. } = &mut changed.computation {
        source.original_starts[0].offset += 1;
    }
    let mut native = config.state;
    native.source.starts[0].offset += 1;
    assert!(
        admit(&changed)
            .unwrap()
            .validate_state(&native, false)
            .is_err()
    );
}
