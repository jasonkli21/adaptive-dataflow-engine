#![allow(
    clippy::many_single_char_names,
    reason = "Compact independent adversarial plan fixtures"
)]
use ade_logical::*;
use ade_physical::{Field, Operator, PlanV1, Source, SourceFile, Target, Type};
fn batch(rows: u64) -> LogicalPlanV1 {
    let mut p = PlanV1::new(
        Target::Local,
        vec![Source {
            id: 0,
            table: "input".into(),
            manifest: "a".repeat(64),
            logical: "b".repeat(64),
            schema_identity: "c".repeat(64),
            schema: vec![
                Field::new("key", Type::U64, false),
                Field::new("value_i64", Type::I64, false),
            ],
            files: vec![SourceFile {
                name: "a.parquet".into(),
                bytes: 1,
                rows,
                fingerprint: "d".repeat(64),
            }],
            replayable: true,
        }],
    );
    p.push(Operator::Scan { source: 0 }).unwrap();
    p.push(Operator::GroupSumCount {
        input: 0,
        key: 0,
        value: 1,
    })
    .unwrap();
    LogicalPlanV1::from_physical(&p).unwrap()
}
#[test]
fn candidate_legality_static_forced_digest_and_search_bounds() {
    for rows in [0, 1, 9_999, 10_000, 10_001, 1_000_000] {
        let p = batch(rows);
        let c = Controls {
            distributed_available: true,
            ..Controls::default()
        };
        let d = optimize(&p, &c).unwrap();
        assert_eq!(d.candidates.len(), 3);
        assert_eq!(
            d.chosen,
            if rows > 10000 {
                Choice::DistributedAggregate
            } else {
                Choice::Local
            }
        );
        assert!(d.candidates[2].rejection.is_some());
        for _ in 0..4 {
            assert_eq!(
                serde_json::to_vec(&d).unwrap(),
                serde_json::to_vec(&optimize(&p, &c).unwrap()).unwrap()
            );
        }
        for choice in [Choice::Local, Choice::DistributedAggregate] {
            let controls = Controls {
                forced: Some(choice),
                ..c.clone()
            };
            let (native, d) = p.lower_batch(&controls).unwrap();
            assert_eq!(native.target, choice.target().unwrap());
            assert_eq!(p.digest().unwrap(), d.semantic_digest);
        }
        assert_eq!(
            optimize(
                &p,
                &Controls {
                    enabled: false,
                    ..c.clone()
                }
            )
            .unwrap()
            .chosen,
            Choice::Local
        );
        assert!(
            optimize(
                &p,
                &Controls {
                    forced: Some(Choice::DistributedJoin),
                    ..c.clone()
                }
            )
            .is_err()
        );
        assert!(
            optimize(
                &p,
                &Controls {
                    forced: Some(Choice::DistributedAggregate),
                    workers: 5,
                    ..c
                }
            )
            .is_err()
        );
    }
}
fn kafka() -> LogicalPlanV1 {
    LogicalPlanV1 {
        version: 1,
        sink: Sink::Receipts,
        computation: Computation::Kafka {
            source: Kafka {
                cluster: "cluster".into(),
                topic: "topic".into(),
                topic_partitions: vec![0, 1],
                original_starts: vec![
                    Offset {
                        partition: 0,
                        offset: 0,
                    },
                    Offset {
                        partition: 1,
                        offset: 0,
                    },
                ],
                immutable_history: "pinned-history".into(),
                mode: KafkaMode::HistoryThenLive {
                    cut: vec![
                        Offset {
                            partition: 0,
                            offset: 8,
                        },
                        Offset {
                            partition: 1,
                            offset: 5,
                        },
                    ],
                },
            },
            job: "job".into(),
            key_less_than: 100,
            allowance_ms: 10,
            projection: vec!["value_i64".into(), "key".into(), "event_time_ms".into()],
            window_ms: Some(100),
            late_policy: Some("receipt_drop_closed_v1".into()),
            hash_version: 1,
            timers: vec![],
        },
    }
}
#[test]
fn stream_lattice_cut_and_delivery_refusals() {
    let p = kafka();
    let props = p.properties().unwrap();
    assert_eq!(props.termination, Termination::NonTerminating);
    assert!(props.stateful);
    assert_eq!(props.phase, "history_then_live");
    let mut q = p.clone();
    q.sink = Sink::Collect;
    assert!(q.properties().is_err());
    q.sink = Sink::CommittedLocalV3;
    assert!(q.properties().is_err());
    if let Computation::Kafka { source, .. } = &mut q.computation {
        source.mode = KafkaMode::Assigned;
    }
    assert!(q.properties().is_ok());
    let digest = p.digest().unwrap();
    let mut q = p.clone();
    if let Computation::Kafka { source, .. } = &mut q.computation
        && let KafkaMode::HistoryThenLive { cut } = &mut source.mode
    {
        cut[0].offset += 1;
    }
    assert_ne!(q.digest().unwrap(), digest);
    assert_ne!(q.bound_job_bytes().unwrap(), p.bound_job_bytes().unwrap());
    if let Computation::Kafka { source, .. } = &mut q.computation {
        source.original_starts[0].offset = 100;
    }
    assert!(q.properties().is_err());
}
#[test]
fn strict_json_and_malicious_graphs() {
    let p = batch(8);
    let b = serde_json::to_vec(&p).unwrap();
    assert_eq!(p, LogicalPlanV1::from_json(&b).unwrap());
    let mut v = serde_json::to_value(&p).unwrap();
    v["properties"] = serde_json::json!({"termination":"finite"});
    assert!(LogicalPlanV1::from_json(&serde_json::to_vec(&v).unwrap()).is_err());
    let s =
        String::from_utf8(b)
            .unwrap()
            .replacen("\"version\":1", "\"version\":1,\"version\":1", 1);
    assert!(LogicalPlanV1::from_json(s.as_bytes()).is_err());
    assert!(LogicalPlanV1::from_json(&vec![b' '; MAX_BYTES + 1]).is_err());
    let mut q = p.clone();
    if let Computation::ParquetManifest { operators, .. } = &mut q.computation {
        operators[1] = Operator::GroupSumCount {
            input: 1,
            key: 0,
            value: 1,
        };
    }
    assert!(q.properties().is_err());
    let mut q = p.clone();
    if let Computation::ParquetManifest { operators, .. } = &mut q.computation {
        operators.extend(vec![Operator::Scan { source: 0 }; 64]);
    }
    assert!(q.properties().is_err());
    let mut q = p;
    if let Computation::ParquetManifest { sources, .. } = &mut q.computation {
        sources[0].files[0].name = "../escape.parquet".into();
    }
    assert!(q.properties().is_err());
}

#[test]
fn output_resource_constraints_do_not_change_semantic_identity() {
    let plan = batch(8);
    let mut constrained = plan.clone();
    if let Computation::ParquetManifest {
        max_output_rows,
        max_output_bytes,
        ..
    } = &mut constrained.computation
    {
        *max_output_rows = 100;
        *max_output_bytes = 1024;
    }
    assert_eq!(plan.digest().unwrap(), constrained.digest().unwrap());
    assert_ne!(
        serde_json::to_vec(&plan).unwrap(),
        serde_json::to_vec(&constrained).unwrap()
    );
}

#[test]
fn assigned_subset_does_not_make_unassigned_partitions_hold_progress() {
    let mut plan = kafka();
    if let Computation::Kafka { source, .. } = &mut plan.computation {
        source.original_starts.retain(|s| s.partition == 1);
        if let KafkaMode::HistoryThenLive { cut } = &mut source.mode {
            cut.retain(|s| s.partition == 1);
        }
    }
    assert!(plan.properties().is_ok());
    if let Computation::Kafka { source, .. } = &mut plan.computation {
        source.original_starts.push(Offset {
            partition: 1,
            offset: 0,
        });
    }
    assert!(plan.properties().is_err());
}

#[test]
fn programmatic_metadata_is_refused_before_derived_copies() {
    let mut plan = kafka();
    if let Computation::Kafka { source, .. } = &mut plan.computation {
        source.original_starts = vec![
            Offset {
                partition: 0,
                offset: 0
            };
            1024
        ];
    }
    assert!(plan.properties().is_err());
    let mut plan = batch(8);
    if let Computation::ParquetManifest { operators, .. } = &mut plan.computation {
        operators.push(Operator::Project {
            input: 1,
            columns: vec![ade_physical::Selection {
                column: 0,
                alias: "x".repeat(129),
            }],
        });
    }
    assert!(plan.properties().is_err());
}
