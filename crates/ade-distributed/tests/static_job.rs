use ade_distributed::{
    ExchangeMode, TaskConfig,
    coordinator::{self, JobState, LocalCluster, StaticAggregateJob},
    ids::JobId,
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
fn fixture(root: &Path) -> Vec<PathBuf> {
    fixture_keys(root, 4096)
}
fn fixture_keys(root: &Path, keys: u64) -> Vec<PathBuf> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::UInt64, false),
        Field::new("value_i64", DataType::Int64, false),
    ]));
    (0..4)
        .map(|file| {
            let path = root.join(format!("{file}.parquet"));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(UInt64Array::from_iter_values(0..keys)),
                    Arc::new(Int64Array::from_iter_values(
                        (0..keys).map(|key| file - i64::try_from(key).unwrap()),
                    )),
                ],
            )
            .unwrap();
            let mut writer =
                ArrowWriter::try_new(File::create(&path).unwrap(), schema.clone(), None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
            path
        })
        .collect()
}
fn failure_cleanup(mut cluster: LocalCluster, job: &mut StaticAggregateJob) {
    job.id = JobId(50);
    // Install state on the survivor before losing its peer, so failure must clean it.
    let spec = ade_distributed::protocol::JobSpec {
        job: job.id,
        workers: cluster.workers.clone(),
        config: job.config.clone(),
        snapshot: None,
    };
    ade_distributed::protocol::rpc(
        &cluster.workers[0],
        ade_distributed::protocol::Request::PrepareJob(spec),
        Duration::from_secs(5),
    )
    .unwrap();
    cluster.terminate(1).unwrap();
    let start = Instant::now();
    assert!(coordinator::execute(job).is_err());
    assert!(start.elapsed() < Duration::from_secs(20));
    // The surviving worker was cancelled, rather than left holding the failed job.
    let reply = ade_distributed::protocol::rpc(
        &cluster.workers[0],
        ade_distributed::protocol::Request::Ping,
        Duration::from_secs(5),
    )
    .unwrap();
    assert!(matches!(reply, ade_distributed::protocol::Reply::Pong(_)));
    let spec = ade_distributed::protocol::JobSpec {
        job: JobId(51),
        workers: cluster.workers.clone(),
        config: job.config.clone(),
        snapshot: None,
    };
    let ready = ade_distributed::protocol::rpc(
        &cluster.workers[0],
        ade_distributed::protocol::Request::PrepareJob(spec),
        Duration::from_secs(5),
    )
    .unwrap();
    assert!(matches!(ready, ade_distributed::protocol::Reply::Ready));
    let root = cluster.temp_root().to_path_buf();
    drop(cluster);
    assert!(!root.exists());
}
#[test]
fn two_worker_process_graph_and_failure_cleanup() {
    let data = tempfile::tempdir().unwrap();
    let files = fixture(data.path());
    let cluster = LocalCluster::spawn(Path::new(env!("CARGO_BIN_EXE_ade-worker")), 2).unwrap();
    let mut job = StaticAggregateJob {
        id: JobId(42),
        files,
        workers: cluster.workers.clone(),
        config: TaskConfig {
            batch_size: 7,
            ..Default::default()
        },
        task_timeout: Duration::from_secs(20),
        snapshot: None,
    };
    for (i, mode) in [
        ExchangeMode::LocalMaterialized,
        ExchangeMode::Direct,
        ExchangeMode::Buffered,
        ExchangeMode::Spill,
        ExchangeMode::ObjectStoreMaterialized,
    ]
    .into_iter()
    .enumerate()
    {
        job.id = JobId(42 + i as u64);
        job.config.exchange.mode = mode;
        job.config.exchange.buffer_bytes = if mode == ExchangeMode::Spill {
            1
        } else {
            1024 * 1024
        };
        let output = coordinator::execute(&job).unwrap();
        verify_output(output, mode);
    }
    // Every mode reused the same workers with monotonically increasing job IDs.
    // Producer failure must cancel already-waiting final sources without a partial result.
    let files = job.files.clone();
    job.id = JobId(47);
    job.files = vec![data.path().join("missing.parquet")];
    let start = Instant::now();
    assert!(coordinator::execute(&job).is_err());
    assert!(start.elapsed() < Duration::from_secs(10));
    job.id = JobId(48);
    job.files = files;
    assert!(coordinator::execute(&job).is_ok());
    failure_cleanup(cluster, &mut job);
}

fn verify_output(output: coordinator::JobOutput, mode: ExchangeMode) {
    verify_output_keys(output, mode, 4096);
}
fn verify_output_keys(output: coordinator::JobOutput, mode: ExchangeMode, keys: u64) {
    assert_eq!(output.metrics.exchange_mode, mode);
    assert_eq!(output.state, JobState::Succeeded);
    let mut actual = BTreeMap::new();
    for batch in output.batches {
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
        for row in 0..keys.len() {
            assert!(
                actual
                    .insert(keys.value(row), (sums.value(row), counts.value(row)))
                    .is_none()
            );
        }
    }
    let expected: BTreeMap<_, _> = (0..keys)
        .map(|key| (key, (6 - 4 * i64::try_from(key).unwrap(), 4)))
        .collect();
    assert_eq!(actual, expected);
    assert_eq!(output.metrics.worker_count, 2);
    for worker in 0..2 {
        let producer = &output.metrics.producers[&worker];
        let final_task = &output.metrics.finals[&worker];
        assert_eq!(producer.source_files, 2);
        assert_eq!(producer.input_rows as u64, keys * 2);
        assert_eq!(producer.output_rows as u64, keys);
        assert_eq!(producer.sent.transfers, 2);
        assert!(producer.sent.payload_bytes > 0);
        assert_eq!(producer.destinations.len(), 2);
        assert!(final_task.input_rows > 0);
        assert!(final_task.output_rows > 0);
        assert_eq!(output.metrics.received[&worker].transfers, 2);
    }
    assert_eq!(output.metrics.gather.rows, keys);
    assert!(output.metrics.gather.batches > 0);
    assert!(output.metrics.gather.payload_bytes > 0);
    assert!(output.metrics.elapsed_ms > 0);
    for (partition, task) in &output.metrics.finals {
        let m = &task.retention;
        if output.metrics.recovery.committed_attempts[partition]
            == ade_distributed::ids::AttemptId(1)
        {
            assert_eq!(m.local_materialized_files, 2);
            continue;
        }
        match mode {
            ExchangeMode::Direct | ExchangeMode::Buffered => {
                assert!(m.peak_buffer_bytes > 0 && m.peak_buffer_bytes <= 1024 * 1024);
                assert_eq!(
                    m.spill_write_bytes + m.local_materialized_bytes + m.object_write_bytes,
                    0
                );
            }
            ExchangeMode::Spill => {
                assert!(m.spill_write_bytes > 0);
                assert_eq!(m.spill_write_bytes, m.spill_read_bytes);
                assert_eq!(m.peak_buffer_bytes, 0);
            }
            ExchangeMode::LocalMaterialized => {
                assert_eq!(m.local_materialized_files, 2);
                assert!(m.local_materialized_bytes > 0);
            }
            ExchangeMode::Adaptive => {
                assert!(m.adaptive.extents <= 4);
                assert_eq!(
                    m.adaptive.segments_by_tier.iter().sum::<u64>(),
                    task.received.batches
                );
            }
            ExchangeMode::ObjectStoreMaterialized => {
                assert!(m.object_count > 2);
                assert_eq!(m.object_write_bytes, m.object_read_bytes);
                assert!(m.object_write_bytes > 0);
                assert_eq!(m.object_store_backend.as_deref(), Some("local_filesystem"));
                assert_eq!(
                    m.object_store_durability,
                    Some(ade_distributed::ObjectStoreDurability::Ephemeral)
                );
            }
        }
    }
}

#[test]
#[cfg(feature = "research-hooks")]
fn adaptive_process_oracle_divergence_and_failure() {
    let data = tempfile::tempdir().unwrap();
    let files = fixture_keys(data.path(), 32768);
    let cluster = LocalCluster::spawn(Path::new(env!("CARGO_BIN_EXE_ade-worker")), 2).unwrap();
    let mut config = TaskConfig::default();
    config.exchange.mode = ExchangeMode::Adaptive;
    config.exchange.buffer_bytes = 600_000;
    config.exchange.adaptive.buffer_high_pct = 100;
    config.exchange.adaptive.remote_after_bytes = 32768;
    config.exchange.pressure = ade_distributed::PressureConfig {
        target_partition: Some(0),
        after_segments: 1,
        delay_ms: 2000,
    };
    let mut job = StaticAggregateJob {
        id: JobId(1),
        files,
        workers: cluster.workers.clone(),
        config,
        task_timeout: Duration::from_secs(20),
        snapshot: None,
    };
    let output = coordinator::execute(&job).unwrap();
    for (partition, task) in &output.metrics.finals {
        eprintln!(
            "partition {partition}: adaptive segments {:?}",
            task.retention.adaptive.segments_by_tier
        );
    }
    let slow = &output.metrics.finals[&0].retention;
    assert!(
        slow.adaptive
            .segments_by_tier
            .iter()
            .filter(|n| **n > 0)
            .count()
            >= 2
    );
    assert!(
        slow.adaptive
            .transitions
            .iter()
            .any(|t| t.new == ade_distributed::adaptive::AdaptiveTier::Spill)
    );
    assert!(slow.adaptive.transitions.len() <= 3);
    assert!(
        output.metrics.finals[&1]
            .retention
            .adaptive
            .transitions
            .len()
            < slow.adaptive.transitions.len(),
        "normal partition must remain at a lower tier"
    );
    for task in output.metrics.finals.values() {
        assert_eq!(task.retention.current_buffer_bytes, 0);
        assert_eq!(task.retention.current_spill_backlog_bytes, 0);
        assert_eq!(
            task.retention.adaptive.bytes_by_tier.iter().sum::<u64>(),
            task.received.payload_bytes
        );
    }
    verify_output_keys(output, ExchangeMode::Adaptive, 32768);
    job.id = JobId(2);
    job.files = vec![data.path().join("missing.parquet")];
    assert!(coordinator::execute(&job).is_err());
    failure_cleanup(cluster, &mut job);
}

#[cfg(feature = "research-hooks")]
fn source_snapshot(files: &[PathBuf]) -> ade_distributed::lineage::SourceSnapshot {
    use ade_distributed::{
        ids::MorselId,
        lineage::{SourceMorsel, SourceSnapshot, fingerprint},
    };
    SourceSnapshot {
        version: 1,
        manifest_fingerprint: "process-fixture-v1".into(),
        schema_fingerprint: "fixture-schema-v1".into(),
        logical_fingerprint: "four-files-4096-keys".into(),
        replayable: true,
        morsels: files
            .iter()
            .enumerate()
            .map(|(i, path)| SourceMorsel {
                id: MorselId(i as u64),
                path: path.clone(),
                bytes: std::fs::metadata(path).unwrap().len(),
                fingerprint: fingerprint(path).unwrap(),
            })
            .collect(),
    }
}
#[cfg(feature = "research-hooks")]
#[test]
fn real_final_worker_loss_replays_only_missing_partition_with_partial_gather() {
    let root = tempfile::tempdir().unwrap();
    let files = fixture(root.path());
    let cluster = LocalCluster::spawn(Path::new(env!("CARGO_BIN_EXE_ade-worker")), 2).unwrap();
    let mut job = StaticAggregateJob {
        id: JobId(100),
        snapshot: Some(source_snapshot(&files)),
        files,
        workers: cluster.workers.clone(),
        config: TaskConfig::default(),
        task_timeout: Duration::from_secs(20),
    };
    let normal = coordinator::execute(&job).unwrap();
    assert_eq!(normal.metrics.recovery.recovery_attempts, 0);
    verify_output(normal, ExchangeMode::LocalMaterialized);
    job.id = JobId(101);
    job.config.research_exit_final_partition = Some(0);
    let output = coordinator::execute(&job).unwrap();
    let recovery = &output.metrics.recovery;
    assert!(output.metrics.finals[&0].sent.batches > 1);
    assert_eq!(recovery.worker_failures_detected, 1);
    assert_eq!(recovery.recovery_attempts, 1);
    assert_eq!(recovery.replayed_producer_tasks, 2);
    assert_eq!(recovery.replayed_morsels, 4);
    assert_eq!(recovery.lost_segments, recovery.recomputed_segments);
    assert_eq!(
        recovery.lost_payload_bytes,
        recovery.recomputed_payload_bytes
    );
    assert!(recovery.full_job_restart_avoided);
    assert_eq!(
        recovery.committed_attempts[&0],
        ade_distributed::ids::AttemptId(1)
    );
    assert_eq!(
        recovery.committed_attempts[&1],
        ade_distributed::ids::AttemptId(0)
    );
    let plan = recovery.plan.as_ref().unwrap();
    assert_eq!(plan.job, JobId(101));
    assert_eq!(plan.host, ade_distributed::ids::WorkerId(1));
    assert_eq!(plan.reused_partitions, [1]);
    assert!(plan.ranges.iter().all(|r| r.transfer.partition.0 == 0));
    for manifest in &recovery.replay_manifests {
        output.metrics.producers[&manifest.logical.producer.0].manifests[&0]
            .validate_replay(manifest)
            .unwrap();
    }
    assert!(
        ade_distributed::protocol::rpc(
            &cluster.workers[0],
            ade_distributed::protocol::Request::Ping,
            Duration::from_millis(250)
        )
        .is_err()
    );
    verify_output(output, ExchangeMode::LocalMaterialized);
}
#[cfg(feature = "research-hooks")]
#[test]
fn real_worker_loss_without_replayable_lineage_fails_and_cleans_survivor() {
    let root = tempfile::tempdir().unwrap();
    let files = fixture(root.path());
    let cluster = LocalCluster::spawn(Path::new(env!("CARGO_BIN_EXE_ade-worker")), 2).unwrap();
    let mut snapshot = source_snapshot(&files);
    snapshot.replayable = false;
    let job = StaticAggregateJob {
        id: JobId(200),
        snapshot: Some(snapshot),
        files,
        workers: cluster.workers.clone(),
        config: TaskConfig {
            research_exit_final_partition: Some(0),
            ..Default::default()
        },
        task_timeout: Duration::from_secs(20),
    };
    let error = coordinator::execute(&job).err().unwrap();
    assert!(matches!(error, ade_distributed::Error::Unrecoverable(_)));
    assert!(error.to_string().contains("non-replayable"));
    // Cleanup permits a new job on the surviving worker; no recovery loop.
    let response = ade_distributed::protocol::rpc(
        &cluster.workers[1],
        ade_distributed::protocol::Request::PrepareJob(ade_distributed::protocol::JobSpec {
            job: JobId(201),
            workers: cluster.workers.clone(),
            config: TaskConfig::default(),
            snapshot: None,
        }),
        Duration::from_secs(5),
    )
    .unwrap();
    assert!(matches!(response, ade_distributed::protocol::Reply::Ready));
}

#[cfg(feature = "research-hooks")]
fn files_below(root: &Path) -> usize {
    if !root.exists() {
        return 0;
    }
    std::fs::read_dir(root)
        .unwrap()
        .map(|e| {
            let p = e.unwrap().path();
            if p.is_dir() { files_below(&p) } else { 1 }
        })
        .sum()
}
#[cfg(feature = "research-hooks")]
#[test]
fn expected_cost_worker_loss_restores_or_recomputes_and_parent_cleans() {
    use ade_distributed::{
        ObjectStoreConfig, ObjectStoreDurability,
        cost::{CostConfig, MaterializationMode},
    };
    let root = tempfile::tempdir().unwrap();
    let files = fixture(root.path());
    for (mode, read_price, restored) in [
        (MaterializationMode::Expected, 0, true),
        (MaterializationMode::Always, 1_000_000, false),
        (MaterializationMode::Never, 0, false),
    ] {
        let cluster = LocalCluster::spawn_with_materialization(
            Path::new(env!("CARGO_BIN_EXE_ade-worker")),
            2,
        )
        .unwrap();
        let mut config = TaskConfig::default();
        config.exchange.mode = ExchangeMode::Adaptive;
        config.exchange.adaptive.remote_after_bytes = 0;
        config.exchange.object_store = ObjectStoreConfig {
            backend: "shared_local_fixture".into(),
            durability: ObjectStoreDurability::WorkerProcessSurviving,
        };
        config.exchange.cost = Some(Box::new(CostConfig {
            mode,
            failure_probability_ppm: 1_000_000,
            source_read_nano_usd_per_byte: 1000,
            object_read_nano_usd_per_byte: read_price,
            ..Default::default()
        }));
        let mut job = StaticAggregateJob {
            id: JobId(300),
            snapshot: Some(source_snapshot(&files)),
            files: files.clone(),
            workers: cluster.workers.clone(),
            config,
            task_timeout: Duration::from_secs(20),
        };
        let normal = coordinator::execute(&job).unwrap();
        for producer in normal.metrics.producers.values() {
            assert_eq!(producer.materializations.len(), 2);
            for fact in producer.materializations.values() {
                assert_eq!(fact.receipt.is_some(), mode != MaterializationMode::Never);
                assert_eq!(
                    fact.staging_write_bytes,
                    fact.decision.observation.output_bytes
                );
            }
        }
        verify_output(normal, ExchangeMode::Adaptive);
        let complete_root = cluster.temp_root().join("shared-objects/complete-v1");
        assert_eq!(files_below(&complete_root), 0);
        job.id = JobId(301);
        job.config.research_exit_final_partition = Some(0);
        let recovered = coordinator::execute(&job).unwrap();
        let r = &recovered.metrics.recovery;
        assert_eq!(r.worker_failures_detected, 1);
        assert_eq!(r.restored_segments > 0, restored);
        assert_eq!(r.recomputed_segments > 0, !restored);
        assert_eq!(r.replayed_morsels, if restored { 0 } else { 4 });
        assert_eq!(r.committed_attempts[&0], ade_distributed::ids::AttemptId(1));
        assert_eq!(files_below(&complete_root), 0); // Cleanup after real writer loss.
        verify_output(recovered, ExchangeMode::Adaptive);
        let parent = cluster.temp_root().to_owned();
        drop(cluster);
        assert!(!parent.exists());
    }
}
