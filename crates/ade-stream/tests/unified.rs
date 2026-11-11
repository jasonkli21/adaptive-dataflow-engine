#![cfg(all(feature = "unified-planner", feature = "transactional-state"))]
use ade_logical::{Computation, Kafka, KafkaMode, LogicalPlanV1, Offset, Sink};
use ade_stream::unified::Admission;
use ade_stream::{Engine, EventV1, RawRecord};
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

fn stateless_fixture() -> (LogicalPlanV1, ade_stream::StreamConfigV1) {
    let (mut plan, config) = fixture(false, false);
    plan.sink = Sink::Receipts;
    if let Computation::Kafka {
        job,
        window_ms,
        late_policy,
        timers,
        ..
    } = &mut plan.computation
    {
        *job = "unified-stateless-test".into();
        *window_ms = None;
        *late_policy = None;
        timers.clear();
    }
    (plan, config.state.source)
}

#[test]
fn compiled_native_program_drives_filter_time_timers_and_projection() {
    let (plan, config) = stateless_fixture();
    let admitted = admit(&plan).unwrap();
    let mut stale = config.clone();
    stale.key_less_than += 1;
    assert!(admitted.bind_stateless(&stale).is_err());

    let old_threshold = config.key_less_than;
    let mut changed_plan = plan.clone();
    if let Computation::Kafka {
        key_less_than,
        allowance_ms,
        timers,
        ..
    } = &mut changed_plan.computation
    {
        *key_less_than += 1;
        *allowance_ms += 10;
        timers.push(ade_logical::Timer {
            kind: ade_logical::TimerKind::Processing,
            deadline_ms: 0,
        });
    }
    let mut matching_config = config.clone();
    matching_config.key_less_than += 1;
    matching_config.allowance_ms += 10;
    matching_config
        .timers
        .push(ade_stream::timers::Registration {
            kind: ade_stream::timers::Kind::Processing,
            deadline_ms: 0,
        });
    let changed = admit(&changed_plan).unwrap();
    assert_ne!(admitted.native_spec_hash(), changed.native_spec_hash());
    let session = uuid::Uuid::new_v4();
    let mut engine = Engine::new_unified(&matching_config, session, &changed).unwrap();
    let receipt = engine
        .prepare(RawRecord {
            session,
            partition: 0,
            offset: 0,
            payload: serde_json::to_vec(&EventV1 {
                version: 1,
                key: old_threshold,
                value_i64: 7,
                event_time_ms: 100,
            })
            .unwrap(),
        })
        .unwrap();
    assert!(
        receipt.projected.is_some(),
        "the changed logical threshold is executed"
    );
    engine.acknowledge().unwrap();
    assert_eq!(engine.progress.partitions[&0].watermark, Some(80));
    assert_eq!(engine.timers.fire(0, None).len(), 1);
    assert_eq!(receipt.projected.unwrap().value_i64, 7);

    let mut unsupported_projection = changed_plan;
    if let Computation::Kafka { projection, .. } = &mut unsupported_projection.computation {
        projection.swap(0, 1);
    }
    assert!(admit(&unsupported_projection).is_err());
}

#[test]
fn native_spec_lowers_source_and_state_semantics_and_binds_identity() {
    let (plan, config) = stateless_fixture();
    let mut changed = plan.clone();
    let mut matching = config.clone();
    if let Computation::Kafka { source, .. } = &mut changed.computation {
        source.cluster = "pinned-cluster-2".into();
        source.topic = "another-owned-topic".into();
        source.original_starts[0].offset += 5;
    }
    matching.expected_cluster_id = "pinned-cluster-2".into();
    matching.topic = "another-owned-topic".into();
    matching.starts[0].offset += 5;
    let lowered = admit(&changed).unwrap().bind_source(&matching).unwrap();
    assert_eq!(lowered.expected_cluster_id, "pinned-cluster-2");
    assert_eq!(lowered.topic, "another-owned-topic");
    assert_eq!(lowered.starts[0].offset, 5);

    let (mut state_plan, state_config) = fixture(false, false);
    if let Computation::Kafka { window_ms, .. } = &mut state_plan.computation {
        *window_ms = Some(200);
    }
    let mut matching_state = state_config.state.clone();
    matching_state.window_ms = 200;
    matching_state.job_id = uuid::Uuid::from_bytes(state_plan.bound_job_bytes().unwrap());
    let state_admission = admit(&state_plan).unwrap();
    let lowered_state = state_admission.bind_state(&matching_state, false).unwrap();
    assert_eq!(lowered_state.window_ms, 200);
    assert_eq!(lowered_state.job_id, matching_state.job_id);
    assert!(
        state_admission
            .bind_state(&state_config.state, false)
            .is_err()
    );
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
    let bound = a.bind_transactional(&c).unwrap();
    assert_eq!(bound.state.job_id, c.state.job_id);
    assert_eq!(bound.trusted_history, c.trusted_history);
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
