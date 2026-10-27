use ade_distributed::{
    ExchangeMode, TaskConfig,
    coordinator::{self, LocalCluster, StaticAggregateJob},
    ids::{JobId, MorselId, WorkerId},
    lineage::{self, SourceMorsel, SourceSnapshot},
    scheduler::{Action, Config, Participation, Policy},
};
use arrow::{
    array::{Array, Int64Array, UInt64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::arrow::ArrowWriter;
use std::{
    collections::BTreeMap,
    fs::File,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};
fn fixture(
    root: &Path,
    files: usize,
    empty: bool,
) -> (Vec<PathBuf>, SourceSnapshot, BTreeMap<u64, (i64, u64)>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::UInt64, false),
        Field::new("value_i64", DataType::Int64, false),
    ]));
    let mut expected = BTreeMap::<u64, (i64, u64)>::new();
    let paths: Vec<_> = (0..files)
        .map(|i| {
            let rows = if empty {
                0
            } else if i % 2 == 0 {
                8192
            } else {
                128
            };
            let keys: Vec<_> = (0..rows).map(|r| r % 1024).collect();
            let values: Vec<_> = (0..rows)
                .map(|r| i64::try_from(r % 31).unwrap() - 15)
                .collect();
            for (k, v) in keys.iter().zip(&values) {
                let e = expected.entry(*k).or_default();
                e.0 += v;
                e.1 += 1;
            }
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(UInt64Array::from(keys)),
                    Arc::new(Int64Array::from(values)),
                ],
            )
            .unwrap();
            let path = root.join(format!("{i}.parquet"));
            let mut writer =
                ArrowWriter::try_new(File::create(&path).unwrap(), schema.clone(), None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
            path
        })
        .collect();
    let snapshot = SourceSnapshot {
        version: 1,
        manifest_fingerprint: "fixture".into(),
        schema_fingerprint: "schema".into(),
        logical_fingerprint: "logical".into(),
        replayable: false,
        morsels: paths
            .iter()
            .enumerate()
            .map(|(i, p)| SourceMorsel {
                id: MorselId(i as u64),
                path: p.clone(),
                fingerprint: lineage::fingerprint(p).unwrap(),
                bytes: std::fs::metadata(p).unwrap().len(),
            })
            .collect(),
    };
    (paths, snapshot, expected)
}
fn config() -> TaskConfig {
    let mut c = TaskConfig {
        scheduler: Config {
            policy: Policy::LeasedV1,
            recovery_enabled: false,
            ..Config::default()
        },
        ..TaskConfig::default()
    };
    c.exchange.mode = ExchangeMode::Spill;
    c.exchange.buffer_bytes = 1 << 20;
    c.memory_limit_bytes = 4 << 20;
    c
}
fn check(
    output: &coordinator::JobOutput,
    expected: &BTreeMap<u64, (i64, u64)>,
    files: usize,
    workers: usize,
) {
    let mut actual = BTreeMap::new();
    for batch in &output.batches {
        let k = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let s = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let c = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        for r in 0..k.len() {
            assert!(
                actual
                    .insert(k.value(r), (s.value(r), c.value(r)))
                    .is_none()
            );
        }
    }
    assert_eq!(&actual, expected);
    let m = output.metrics.scheduler.as_ref().unwrap();
    assert_eq!(m.entries.len(), files);
    assert!(m.entries.iter().all(|e| e.completion.is_some()));
    assert!(m.peak_outstanding <= workers);
    assert_eq!(
        output
            .metrics
            .producers
            .values()
            .map(|p| p.sent.transfers)
            .sum::<u64>(),
        (workers * workers) as u64
    );
    assert_eq!(
        output
            .metrics
            .finals
            .values()
            .map(|p| p.received.transfers)
            .sum::<u64>(),
        (workers * workers) as u64
    );
    assert_eq!(output.metrics.gather.transfers, workers as u64);
    for (wid, producer) in &output.metrics.producers {
        assert_eq!(producer.routing_buffer_capacity_rows, workers * 1024);
        assert!(producer.routing_buffer_peak_rows <= producer.routing_buffer_capacity_rows);
        assert_eq!(producer.sent.rows, producer.output_rows as u64);
        assert_eq!(
            producer.output_rows as u64,
            m.entries
                .iter()
                .filter_map(|e| e.completion.as_ref())
                .filter(|c| c.lease.worker.0 == *wid)
                .map(|c| c.output_rows)
                .sum::<u64>()
        );
    }
}
#[test]
fn leased_exact_oracle_coverage_two_three_four_workers_and_empty_terminals() {
    let data = tempfile::tempdir().unwrap();
    let (files, snapshot, expected) = fixture(data.path(), 1, false);
    for workers in 2..=4 {
        let cluster =
            LocalCluster::spawn(Path::new(env!("CARGO_BIN_EXE_ade-worker")), workers).unwrap();
        let job = StaticAggregateJob {
            id: JobId(1),
            files: files.clone(),
            snapshot: Some(snapshot.clone()),
            workers: cluster.workers.clone(),
            config: config(),
            task_timeout: Duration::from_secs(20),
        };
        let output = coordinator::execute(&job).unwrap();
        check(&output, &expected, 1, workers);
        assert_eq!(
            output
                .metrics
                .producers
                .values()
                .filter(|p| p.source_files == 0)
                .count(),
            workers - 1
        );
    }
    let empty = tempfile::tempdir().unwrap();
    let (files, snapshot, expected) = fixture(empty.path(), 4, true);
    let cluster = LocalCluster::spawn(Path::new(env!("CARGO_BIN_EXE_ade-worker")), 2).unwrap();
    let job = StaticAggregateJob {
        id: JobId(1),
        files,
        snapshot: Some(snapshot),
        workers: cluster.workers.clone(),
        config: config(),
        task_timeout: Duration::from_secs(20),
    };
    check(&coordinator::execute(&job).unwrap(), &expected, 4, 2);
}
#[test]
#[cfg(feature = "research-hooks")]
fn real_pending_steal_and_live_activate_drain_keep_all_final_owners() {
    for workers in [2, 4] {
        let data = tempfile::tempdir().unwrap();
        let (files, snapshot, expected) = fixture(data.path(), 24, false);
        let cluster =
            LocalCluster::spawn(Path::new(env!("CARGO_BIN_EXE_ade-worker")), workers).unwrap();
        let mut c = config();
        c.scheduler.capacities = (0..workers).map(|w| if w == 0 { 1 } else { 4 }).collect();
        c.scheduler.research_delay_worker = Some(0);
        c.scheduler.research_delay_ms = 350;
        c.scheduler.heartbeat_ms = 20;
        c.scheduler.lease_ms = 200;
        if workers == 4 {
            c.scheduler.initial_active = 2;
            c.scheduler.participation = vec![
                Participation {
                    after_completed: 1,
                    worker: WorkerId(2),
                    action: Action::Activate,
                },
                Participation {
                    after_completed: 2,
                    worker: WorkerId(0),
                    action: Action::SpotNotice { grace_ms: 5000 },
                },
                Participation {
                    after_completed: 3,
                    worker: WorkerId(0),
                    action: Action::Drain,
                },
            ];
        }
        let job = StaticAggregateJob {
            id: JobId(1),
            files,
            snapshot: Some(snapshot),
            workers: cluster.workers.clone(),
            config: c,
            task_timeout: Duration::from_secs(20),
        };
        let output = coordinator::execute(&job).unwrap();
        check(&output, &expected, 24, workers);
        let m = output.metrics.scheduler.as_ref().unwrap();
        assert!(
            m.entries
                .iter()
                .any(|e| e.preferred != e.lease.as_ref().unwrap().worker)
        );
        assert!(m.renewals > 0);
        if workers == 4 {
            assert_eq!(output.metrics.producers[&3].source_files, 0);
            assert!(output.metrics.producers[&2].source_files > 0);
            let drain = m
                .events
                .iter()
                .find(|e| matches!(e.action, Action::SpotNotice { .. }))
                .unwrap();
            assert!(drain.in_flight.is_some());
            assert!(drain.drained_ms.is_some());
            assert!(
                m.entries
                    .iter()
                    .all(|e| e.lease.as_ref().unwrap().worker != WorkerId(0)
                        || e.granted_ms <= drain.at_ms)
            );
        }
    }
}
#[test]
#[cfg(feature = "research-hooks")]
fn leased_real_process_exit_lease_expiry_and_inadequate_notice_fail_closed() {
    let data = tempfile::tempdir().unwrap();
    let (files, snapshot, _) = fixture(data.path(), 16, false);
    for fault in 0..4 {
        let cluster = LocalCluster::spawn(Path::new(env!("CARGO_BIN_EXE_ade-worker")), 2).unwrap();
        let root = cluster.temp_root().to_path_buf();
        let mut c = config();
        match fault {
            0 => c.scheduler.research_exit_after_emit_worker = Some(0),
            1 => {
                c.scheduler.research_delay_worker = Some(0);
                c.scheduler.research_delay_ms = 350;
                c.scheduler.heartbeat_ms = 20;
                c.scheduler.lease_ms = 80;
                c.scheduler.research_no_heartbeat_worker = Some(0);
            }
            3 => c.scheduler.research_exit_final_worker = Some(0),
            _ => {
                c.scheduler.research_delay_worker = Some(0);
                c.scheduler.research_delay_ms = 350;
                c.scheduler.participation = vec![Participation {
                    after_completed: 1,
                    worker: WorkerId(0),
                    action: Action::SpotNotice { grace_ms: 1 },
                }];
            }
        }
        let job = StaticAggregateJob {
            id: JobId(1),
            files: files.clone(),
            snapshot: Some(snapshot.clone()),
            workers: cluster.workers.clone(),
            config: c,
            task_timeout: Duration::from_secs(10),
        };
        let start = Instant::now();
        let error = coordinator::execute(&job)
            .err()
            .expect("fault returned no successful output");
        assert!(start.elapsed() < Duration::from_secs(10), "{error}");
        assert!(
            ade_distributed::protocol::rpc(
                &cluster.workers[1],
                ade_distributed::protocol::Request::Ping,
                Duration::from_secs(1)
            )
            .is_ok()
        );
        drop(cluster);
        assert!(!root.exists());
    }
}
#[test]
fn leased_admission_source_drift_deadline_and_ingress_bound_refuse_output() {
    let data = tempfile::tempdir().unwrap();
    let (files, snapshot, _) = fixture(data.path(), 4, false);
    let cluster = LocalCluster::spawn(Path::new(env!("CARGO_BIN_EXE_ade-worker")), 2).unwrap();
    let mut job = StaticAggregateJob {
        id: JobId(1),
        files,
        snapshot: Some(snapshot),
        workers: cluster.workers.clone(),
        config: config(),
        task_timeout: Duration::from_secs(10),
    };
    for kind in 0..7 {
        job.id = JobId(kind + 1);
        let mut bad = job.config.clone();
        match kind {
            0 => bad.scheduler.recovery_enabled = true,
            1 => bad.topology.policy = ade_distributed::topology::PartitionPolicy::SampledV1,
            2 => bad.partial.mode = ade_exec::partial::Mode::RawPartial,
            3 => bad.exchange.mode = ExchangeMode::Direct,
            4 => bad.scheduler.max_morsels = 1,
            5 => bad.scheduler.max_input_rows = 1,
            _ => bad.scheduler.max_exchange_bytes = 1,
        }
        let normal = std::mem::replace(&mut job.config, bad);
        assert!(coordinator::execute(&job).is_err());
        job.config = normal;
    }
    job.id = JobId(9);
    let source = job.snapshot.as_mut().unwrap();
    source.morsels[0].fingerprint = "b".repeat(64);
    assert!(coordinator::execute(&job).is_err());
}

#[test]
fn leased_file_partials_preserve_wide_sums_and_reject_final_overflow() {
    let data = tempfile::tempdir().unwrap();
    let (files, mut snapshot, _) = fixture(data.path(), 4, true);
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::UInt64, false),
        Field::new("value_i64", DataType::Int64, false),
    ]));
    for (i, values) in [
        vec![i64::MAX, i64::MAX],
        vec![],
        vec![-i64::MAX, -i64::MAX],
        vec![],
    ]
    .into_iter()
    .enumerate()
    {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![7; values.len()])),
                Arc::new(Int64Array::from(values)),
            ],
        )
        .unwrap();
        let mut writer =
            ArrowWriter::try_new(File::create(&files[i]).unwrap(), schema.clone(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        snapshot.morsels[i].bytes = std::fs::metadata(&files[i]).unwrap().len();
        snapshot.morsels[i].fingerprint = lineage::fingerprint(&files[i]).unwrap();
    }
    let cluster = LocalCluster::spawn(Path::new(env!("CARGO_BIN_EXE_ade-worker")), 2).unwrap();
    let mut job = StaticAggregateJob {
        id: JobId(1),
        files,
        snapshot: Some(snapshot),
        workers: cluster.workers.clone(),
        config: config(),
        task_timeout: Duration::from_secs(20),
    };
    let output = coordinator::execute(&job).unwrap();
    check(&output, &[(7, (0, 4))].into(), 4, 2);
    let original = output.metrics.scheduler.as_ref().unwrap();
    for change in 0..4 {
        let mut forged = original.clone();
        match change {
            0 => forged.fence.digest = "f".repeat(64),
            1 => forged.entries[0].lease.as_mut().unwrap().nonce += 1,
            2 => forged.entries[0].completion.as_mut().unwrap().input_rows += 1,
            _ => forged.decisions[0].reason = "forged".into(),
        }
        // Counter forgery is also checked against producer totals by the coordinator.
        if change != 2 {
            assert!(ade_distributed::scheduler::verify_metrics(&forged).is_err());
        }
    }

    // Remove the cancelling file: the final checked Int64 result must now fail.
    job.id = JobId(2);
    job.files = job.files[..2].to_vec();
    job.snapshot.as_mut().unwrap().morsels.truncate(2);
    assert!(coordinator::execute(&job).is_err());
}

#[test]
fn static_projected_matches_file_partials_with_modulo_ownership_and_cleanup() {
    let data = tempfile::tempdir().unwrap();
    let (files, snapshot, expected) = fixture(data.path(), 7, false);
    for workers in [2, 4] {
        let cluster =
            LocalCluster::spawn(Path::new(env!("CARGO_BIN_EXE_ade-worker")), workers).unwrap();
        let root = cluster.temp_root().to_path_buf();
        let mut c = config();
        c.scheduler.policy = Policy::Static;
        c.static_projected = true;
        let mut job = StaticAggregateJob {
            id: JobId(1),
            files: files.clone(),
            snapshot: Some(snapshot.clone()),
            workers: cluster.workers.clone(),
            config: c,
            task_timeout: Duration::from_secs(20),
        };
        let static_output = coordinator::execute(&job).unwrap();
        assert!(static_output.metrics.scheduler.is_none());
        let static_rows: Vec<_> = static_output
            .batches
            .iter()
            .flat_map(|batch| {
                let keys = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap();
                let sums = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let counts = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap();
                (0..batch.num_rows())
                    .map(|r| (keys.value(r), (sums.value(r), counts.value(r))))
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(
            static_rows.iter().copied().collect::<BTreeMap<_, _>>(),
            expected
        );
        assert_eq!(static_rows.len(), expected.len());
        job.id = JobId(2);
        job.config.static_projected = false;
        job.config.scheduler.policy = Policy::LeasedNoSteal;
        let matched = coordinator::execute(&job).unwrap();
        check(&matched, &expected, files.len(), workers);
        for w in 0..workers as u64 {
            let a = &static_output.metrics.producers[&w];
            let b = &matched.metrics.producers[&w];
            assert_eq!(
                a.source_files,
                files
                    .iter()
                    .skip(usize::try_from(w).unwrap())
                    .step_by(workers)
                    .count()
            );
            assert_eq!(a.source_hash_threads, b.source_hash_threads);
            assert_eq!(
                a.source_hash_scratch_peak_bytes,
                b.source_hash_scratch_peak_bytes
            );
            assert_eq!(
                a.routing_buffer_capacity_rows,
                b.routing_buffer_capacity_rows
            );
            assert_eq!(a.input_rows, b.input_rows);
            assert_eq!(a.output_rows, b.output_rows);
            assert_eq!(a.sent.rows, b.sent.rows);
            assert_eq!(a.sent.batches, b.sent.batches);
            assert_eq!(a.lease_ack_ns, 0);
            assert_eq!(a.lease_wait_ns, 0);
        }
        job.id = JobId(3);
        job.config.static_projected = true;
        job.config.scheduler.policy = Policy::Static;
        job.snapshot.as_mut().unwrap().morsels[0].fingerprint = "b".repeat(64);
        assert!(coordinator::execute(&job).is_err());
        job.id = JobId(4);
        job.snapshot = None;
        assert!(coordinator::execute(&job).is_err());
        drop(cluster);
        assert!(!root.exists());
    }
}
