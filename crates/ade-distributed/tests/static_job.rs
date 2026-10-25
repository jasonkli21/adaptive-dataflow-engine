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
        schedule: None,
        job: job.id,
        execution_namespace: "8abade70-a479-4c7e-9bd8-989ed8f1a1d7".into(),
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
        schedule: None,
        job: JobId(51),
        execution_namespace: "7b9b1bbf-929e-4b5e-a86e-8ed9bb5ed5b4".into(),
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
            schedule: None,
            job: JobId(201),
            execution_namespace: "715e945f-bc9f-4ea3-a274-7f3163dc89fb".into(),
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

fn topology_fixture(root: &Path, keys: &[u64]) -> Vec<PathBuf> {
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
                    Arc::new(UInt64Array::from(keys.to_vec())),
                    Arc::new(Int64Array::from_iter_values(
                        keys.iter()
                            .map(|key| file - i64::try_from(key % 1000).unwrap()),
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
fn assert_topology_output(output: &coordinator::JobOutput, keys: &[u64], workers: usize) {
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
        for i in 0..k.len() {
            assert!(
                actual
                    .insert(k.value(i), (s.value(i), c.value(i)))
                    .is_none()
            );
        }
    }
    let expected: BTreeMap<_, _> = keys
        .iter()
        .map(|key| (*key, (6 - 4 * i64::try_from(key % 1000).unwrap(), 4)))
        .collect();
    assert_eq!(actual, expected);
    let decision = output.metrics.topology.as_ref().unwrap();
    assert_eq!(
        output
            .metrics
            .finals
            .keys()
            .copied()
            .collect::<std::collections::BTreeSet<_>>(),
        decision.layout.active()
    );
    assert_eq!(
        output.metrics.finals.len() + output.metrics.inactive.len(),
        workers
    );
    assert_eq!(
        output.metrics.gather.transfers,
        output.metrics.finals.len() as u64
    );
    for task in output.metrics.inactive.values() {
        assert!(!task.executed_final);
        assert_eq!(task.received.transfers, workers as u64);
        assert_eq!(
            task.received.rows + task.received.batches + task.received.payload_bytes,
            0
        );
    }
    for task in output
        .metrics
        .producers
        .values()
        .chain(output.metrics.finals.values())
    {
        assert_eq!(task.layout_digest.as_ref(), Some(&decision.layout.digest));
    }
}
#[test]
fn runtime_split_and_real_coalesce_across_two_three_four_workers_and_retention() {
    use ade_distributed::topology::{PartitionPolicy, TopologyConfig};
    for n in 2..=4 {
        let cluster = LocalCluster::spawn(Path::new(env!("CARGO_BIN_EXE_ade-worker")), n).unwrap();
        let mut id = 0;
        for tiny in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let keys: Vec<_> = if tiny {
                vec![0, 1, 2, 3]
            } else {
                (0..)
                    .filter(|key| ade_distributed::partition::destination(*key, n).unwrap() == 0)
                    .take(2048)
                    .collect()
            };
            let files = topology_fixture(root.path(), &keys);
            for mode in [
                ExchangeMode::Direct,
                ExchangeMode::Buffered,
                ExchangeMode::Spill,
                ExchangeMode::LocalMaterialized,
                ExchangeMode::ObjectStoreMaterialized,
                ExchangeMode::Adaptive,
            ] {
                id += 1;
                let mut config = TaskConfig {
                    topology: TopologyConfig {
                        policy: PartitionPolicy::SampledV1,
                        ..Default::default()
                    },
                    ..Default::default()
                };
                config.exchange.mode = mode;
                config.memory_limit_bytes = 1024 * 1024;
                config.exchange.buffer_bytes = if mode == ExchangeMode::Spill {
                    1
                } else {
                    1024 * 1024
                };
                let output = coordinator::execute(&StaticAggregateJob {
                    id: JobId(id),
                    snapshot: None,
                    files: files.clone(),
                    workers: cluster.workers.clone(),
                    config,
                    task_timeout: Duration::from_secs(20),
                })
                .unwrap();
                assert_topology_output(&output, &keys, n);
                let d = output.metrics.topology.as_ref().unwrap();
                if tiny {
                    assert_eq!(d.reason, "coalesce_tiny");
                    assert_eq!(output.metrics.finals.len(), 1);
                } else {
                    assert_eq!(d.reason, "split_oversized_bucket");
                    assert_eq!(output.metrics.finals.len(), n);
                    assert!(d.moved_shards > 0);
                }
            }
        }
    }
}

#[test]
fn complete_tiny_source_coalesces_single_key_empty_and_multi_key_inputs() {
    use ade_distributed::topology::{PartitionPolicy, TopologyConfig};
    let cluster = LocalCluster::spawn(Path::new(env!("CARGO_BIN_EXE_ade-worker")), 2).unwrap();
    for (id, keys) in [(40, vec![0]), (41, vec![]), (42, vec![0, 1, 2, 3])] {
        let root = tempfile::tempdir().unwrap();
        let files = topology_fixture(root.path(), &keys);
        let output = coordinator::execute(&StaticAggregateJob {
            id: JobId(id),
            snapshot: None,
            files,
            workers: cluster.workers.clone(),
            config: TaskConfig {
                topology: TopologyConfig {
                    policy: PartitionPolicy::SampledV1,
                    ..Default::default()
                },
                ..Default::default()
            },
            task_timeout: Duration::from_secs(20),
        })
        .unwrap();
        assert_topology_output(&output, &keys, 2);
        let decision = output.metrics.topology.as_ref().unwrap();
        assert_eq!(decision.reason, "coalesce_tiny");
        assert!(
            decision
                .observations
                .iter()
                .all(|observation| observation.source_complete)
        );
        assert_eq!(output.metrics.finals.len(), 1);
        assert_eq!(output.metrics.inactive.len(), 1);
        assert_eq!(output.metrics.gather.transfers, 1);
    }
}

#[test]
fn many_distinct_keys_in_one_virtual_shard_keep_v1_ownership_for_two_to_four_workers() {
    use ade_distributed::topology::{Layout, PartitionPolicy, TopologyConfig, shard};
    let keys: Vec<_> = (0..50_000)
        .filter(|key| shard(*key) == 0)
        .take(1000)
        .collect();
    assert_eq!(keys.len(), 1000);
    for workers in 2..=4 {
        let root = tempfile::tempdir().unwrap();
        let files = topology_fixture(root.path(), &keys);
        let cluster =
            LocalCluster::spawn(Path::new(env!("CARGO_BIN_EXE_ade-worker")), workers).unwrap();
        let output = coordinator::execute(&StaticAggregateJob {
            id: JobId(44 + workers as u64),
            snapshot: None,
            files,
            workers: cluster.workers.clone(),
            config: TaskConfig {
                topology: TopologyConfig {
                    policy: PartitionPolicy::SampledV1,
                    ..Default::default()
                },
                ..Default::default()
            },
            task_timeout: Duration::from_secs(20),
        })
        .unwrap();
        assert_topology_output(&output, &keys, workers);
        assert!(output.metrics.producers.values().all(|p| p.prefix_rows > 0
            && p.prefix_rows <= TaskConfig::default().batch_size
            && p.preparation_ns > 0));
        let decision = output.metrics.topology.as_ref().unwrap();
        assert_eq!(decision.reason, "one_occupied_shard");
        assert_eq!(decision.layout, Layout::static_layout(workers).unwrap());
        assert_eq!(output.metrics.finals.len(), workers);
        assert_eq!(output.metrics.inactive.len(), 0);
        assert_eq!(output.metrics.gather.transfers, workers as u64);
        assert_eq!(
            output
                .metrics
                .finals
                .values()
                .filter(|task| task.received.rows > 0)
                .count(),
            1
        );
    }
}

#[test]
fn a_tiny_prefix_does_not_coalesce_a_large_unobserved_suffix() {
    use ade_distributed::topology::{Layout, PartitionPolicy, TopologyConfig};
    let root = tempfile::tempdir().unwrap();
    let keys = [vec![0, 2], (3..=1024).collect()].concat();
    let files = topology_fixture(root.path(), &keys);
    let cluster = LocalCluster::spawn(Path::new(env!("CARGO_BIN_EXE_ade-worker")), 2).unwrap();
    let output = coordinator::execute(&StaticAggregateJob {
        id: JobId(43),
        snapshot: None,
        files,
        workers: cluster.workers.clone(),
        config: TaskConfig {
            topology: TopologyConfig {
                policy: PartitionPolicy::SampledV1,
                sample_rows: 2,
                rows_per_file: 2,
                ..Default::default()
            },
            ..Default::default()
        },
        task_timeout: Duration::from_secs(20),
    })
    .unwrap();
    assert_topology_output(&output, &keys, 2);
    let decision = output.metrics.topology.as_ref().unwrap();
    assert_eq!(decision.reason, "insufficient_sample_coverage");
    assert!(
        decision
            .observations
            .iter()
            .all(|observation| !observation.source_complete)
    );
    assert_eq!(decision.layout, Layout::static_layout(2).unwrap());
    assert_eq!(output.metrics.finals.len(), 2);
    assert_eq!(output.metrics.gather.transfers, 2);
    assert!(
        output
            .metrics
            .finals
            .values()
            .all(|task| task.received.rows > 0)
    );
}

#[cfg(feature = "research-hooks")]
#[test]
fn runtime_layout_survives_real_worker_loss_by_replay_or_complete_copy() {
    use ade_distributed::{
        ObjectStoreConfig, ObjectStoreDurability,
        cost::{CostConfig, MaterializationMode},
        topology::{PartitionPolicy, TopologyConfig},
    };
    for (tiny, cost) in [(false, false), (true, false), (false, true), (true, true)] {
        let root = tempfile::tempdir().unwrap();
        let keys: Vec<_> = if tiny {
            vec![0, 1, 2, 3]
        } else {
            (0..)
                .filter(|key| ade_distributed::partition::destination(*key, 2).unwrap() == 0)
                .take(4096)
                .collect()
        };
        let files = topology_fixture(root.path(), &keys);
        let cluster = if cost {
            LocalCluster::spawn_with_materialization(Path::new(env!("CARGO_BIN_EXE_ade-worker")), 2)
        } else {
            LocalCluster::spawn(Path::new(env!("CARGO_BIN_EXE_ade-worker")), 2)
        }
        .unwrap();
        let mut config = TaskConfig {
            topology: TopologyConfig {
                policy: PartitionPolicy::SampledV1,
                ..Default::default()
            },
            research_exit_final_partition: Some(0),
            ..Default::default()
        };
        if cost {
            config.exchange.mode = ExchangeMode::Adaptive;
            config.exchange.adaptive.remote_after_bytes = 0;
            config.exchange.object_store = ObjectStoreConfig {
                backend: "shared_local_fixture".into(),
                durability: ObjectStoreDurability::WorkerProcessSurviving,
            };
            config.exchange.cost = Some(Box::new(CostConfig {
                mode: MaterializationMode::Always,
                source_read_nano_usd_per_byte: 1000,
                ..Default::default()
            }));
        }
        let output = coordinator::execute(&StaticAggregateJob {
            id: JobId(1),
            snapshot: Some(source_snapshot(&files)),
            files,
            workers: cluster.workers.clone(),
            config,
            task_timeout: Duration::from_secs(30),
        })
        .unwrap();
        assert_topology_output(&output, &keys, 2);
        let recovery = &output.metrics.recovery;
        assert_eq!(recovery.recovered_partitions, 1);
        assert_eq!(
            recovery.committed_attempts[&0],
            ade_distributed::ids::AttemptId(1)
        );
        assert_eq!(
            recovery.plan.as_ref().unwrap().layout_digest.as_ref(),
            Some(&output.metrics.topology.as_ref().unwrap().layout.digest)
        );
        if cost {
            assert!(recovery.restored_segments > 0);
            assert_eq!(files_below(&cluster.temp_root().join("shared-objects")), 0);
        } else {
            assert!(recovery.recomputed_segments > 0);
        }
    }
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "One state-machine fixture covers admission, fencing and cleanup."
)]
fn sampled_protocol_fences_early_tasks_duplicate_commit_and_bounded_probe_failure() {
    use ade_distributed::{
        ids::{AttemptId, TaskId},
        protocol::{self, JobSpec, Reply, Request},
        topology::{Layout, PartitionPolicy, TopologyConfig},
    };
    let root = tempfile::tempdir().unwrap();
    let files = topology_fixture(root.path(), &[0, 1, 2, 3]);
    let cluster = LocalCluster::spawn(Path::new(env!("CARGO_BIN_EXE_ade-worker")), 2).unwrap();
    let config = TaskConfig {
        topology: TopologyConfig {
            policy: PartitionPolicy::SampledV1,
            ..Default::default()
        },
        ..Default::default()
    };
    let endpoint = &cluster.workers[0];
    let rpc = |request| protocol::rpc(endpoint, request, Duration::from_secs(5));
    let spec = JobSpec {
        schedule: None,
        job: JobId(1),
        execution_namespace: "8abade70-a479-4c7e-9bd8-989ed8f1a1d7".into(),
        workers: cluster.workers.clone(),
        config: config.clone(),
        snapshot: None,
    };
    rpc(Request::PrepareJob(spec)).unwrap();
    assert!(
        rpc(Request::RunProducerTask {
            job: JobId(1),
            task: TaskId(0),
            attempt: AttemptId(0),
            files: files.clone()
        })
        .is_err()
    );
    assert!(
        rpc(Request::CommitLayout {
            job: JobId(1),
            layout: Layout::static_layout(2).unwrap()
        })
        .is_err()
    );
    assert!(
        ade_distributed::exchange::Sender::new(
            endpoint.exchange,
            ade_distributed::ids::transfer(JobId(1), 0, 0, 0),
            ade_exec::CancellationToken::default()
        )
        .unwrap()
        .finish()
        .is_err()
    );
    assert!(matches!(
        rpc(Request::Probe {
            timeout_ms: 5000,
            job: JobId(1),
            files: files.clone()
        })
        .unwrap(),
        Reply::Probe(_)
    ));
    assert!(
        rpc(Request::Probe {
            timeout_ms: 5000,
            job: JobId(1),
            files: files.clone()
        })
        .is_err()
    );
    let mut invalid = Layout::static_layout(2).unwrap();
    invalid.digest.clear();
    assert!(
        rpc(Request::CommitLayout {
            job: JobId(1),
            layout: invalid
        })
        .is_err()
    );
    rpc(Request::CommitLayout {
        job: JobId(1),
        layout: Layout::static_layout(2).unwrap(),
    })
    .unwrap();
    assert!(
        rpc(Request::CommitLayout {
            job: JobId(1),
            layout: Layout::static_layout(2).unwrap()
        })
        .is_err()
    );
    rpc(Request::CancelJob { job: JobId(1) }).unwrap();
    let mut bounded = config.clone();
    bounded.topology.sample_bytes = 1;
    let mut job = StaticAggregateJob {
        id: JobId(2),
        files,
        workers: cluster.workers.clone(),
        config: bounded,
        task_timeout: Duration::from_secs(5),
        snapshot: None,
    };
    let start = Instant::now();
    assert!(coordinator::execute(&job).is_err());
    assert!(start.elapsed() < Duration::from_secs(10));
    job.id = JobId(3);
    job.config = config;
    assert!(coordinator::execute(&job).is_ok());
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "One partial-commit fixture proves source and cross-job fencing."
)]
fn biased_probe_and_partial_layout_commit_preserve_correctness_and_cleanup() {
    use ade_distributed::{
        protocol::{self, JobSpec, Request},
        topology::{Layout, PartitionPolicy, TopologyConfig},
    };
    let root = tempfile::tempdir().unwrap();
    let keys: Vec<_> = (0..2048).collect();
    let files = topology_fixture(root.path(), &keys);
    let cluster = LocalCluster::spawn(Path::new(env!("CARGO_BIN_EXE_ade-worker")), 2).unwrap();
    let config = TaskConfig {
        topology: TopologyConfig {
            policy: PartitionPolicy::SampledV1,
            sample_rows: 1,
            rows_per_file: 1,
            ..Default::default()
        },
        ..Default::default()
    };
    let mut job = StaticAggregateJob {
        id: JobId(1),
        files: files.clone(),
        workers: cluster.workers.clone(),
        config: config.clone(),
        snapshot: Some(source_snapshot(&files)),
        task_timeout: Duration::from_secs(20),
    };
    let output = coordinator::execute(&job).unwrap();
    assert_topology_output(&output, &keys, 2);
    assert_eq!(
        output.metrics.topology.as_ref().unwrap().reason,
        "insufficient_sample_coverage"
    );
    for endpoint in &cluster.workers {
        protocol::rpc(
            endpoint,
            Request::PrepareJob(JobSpec {
                schedule: None,
                job: JobId(2),
                execution_namespace: "8abade70-a479-4c7e-9bd8-989ed8f1a1d7".into(),
                workers: cluster.workers.clone(),
                config: config.clone(),
                snapshot: job.snapshot.clone(),
            }),
            Duration::from_secs(5),
        )
        .unwrap();
    }
    let endpoint = &cluster.workers[0];
    assert!(
        protocol::rpc(
            endpoint,
            Request::Probe {
                job: JobId(2),
                files: files.clone(),
                timeout_ms: 5000
            },
            Duration::from_secs(5)
        )
        .is_err()
    );
    protocol::rpc(
        endpoint,
        Request::Probe {
            job: JobId(2),
            files: vec![files[0].clone(), files[2].clone()],
            timeout_ms: 5000,
        },
        Duration::from_secs(5),
    )
    .unwrap();
    protocol::rpc(
        endpoint,
        Request::CommitLayout {
            job: JobId(2),
            layout: Layout::static_layout(2).unwrap(),
        },
        Duration::from_secs(5),
    )
    .unwrap();
    // Only endpoint 0 committed. Endpoint 1 cannot produce under the partial plan.
    assert!(
        protocol::rpc(
            &cluster.workers[1],
            Request::RunProducerTask {
                job: JobId(2),
                task: ade_distributed::ids::TaskId(1),
                attempt: ade_distributed::ids::AttemptId(0),
                files: vec![files[1].clone(), files[3].clone()]
            },
            Duration::from_secs(5)
        )
        .is_err()
    );
    for endpoint in &cluster.workers {
        protocol::rpc(
            endpoint,
            Request::CancelJob { job: JobId(2) },
            Duration::from_secs(5),
        )
        .unwrap();
    }
    job.id = JobId(3);
    assert_topology_output(&coordinator::execute(&job).unwrap(), &keys, 2);
    assert!(
        protocol::rpc(
            endpoint,
            Request::CommitLayout {
                job: JobId(2),
                layout: Layout::static_layout(2).unwrap()
            },
            Duration::from_secs(5)
        )
        .is_err()
    );
}

#[cfg(feature = "research-hooks")]
#[test]
fn dynamic_unsupported_loss_and_source_drift_fail_closed() {
    use ade_distributed::topology::{PartitionPolicy, TopologyConfig};
    for (tiny, snapshot) in [(true, true), (false, false), (false, true)] {
        let root = tempfile::tempdir().unwrap();
        let keys: Vec<_> = if tiny {
            vec![0, 1, 2, 3]
        } else {
            (0..4096).collect()
        };
        let files = topology_fixture(root.path(), &keys);
        let cluster = LocalCluster::spawn(Path::new(env!("CARGO_BIN_EXE_ade-worker")), 2).unwrap();
        let config = TaskConfig {
            topology: TopologyConfig {
                policy: PartitionPolicy::SampledV1,
                ..Default::default()
            },
            research_exit_final_partition: Some(u64::from(tiny)),
            ..Default::default()
        };
        let job = StaticAggregateJob {
            id: JobId(1),
            snapshot: snapshot.then(|| source_snapshot(&files)),
            files: files.clone(),
            workers: cluster.workers.clone(),
            config,
            task_timeout: Duration::from_secs(20),
        };
        if snapshot && !tiny {
            use std::io::Write;
            File::options()
                .append(true)
                .open(&files[0])
                .unwrap()
                .write_all(b"drift")
                .unwrap();
        }
        let error = coordinator::execute(&job).err().unwrap().to_string();
        if tiny {
            assert!(error.contains("inactive"), "{error}");
        } else if !snapshot {
            assert!(error.contains("source snapshot"), "{error}");
        } else {
            assert!(error.contains("source size mismatch"), "{error}");
        }
    }
}

#[test]
fn static_packed_is_an_explicit_static_diagnostic() {
    let root = tempfile::tempdir().unwrap();
    let files = fixture(root.path());
    let cluster = LocalCluster::spawn(Path::new(env!("CARGO_BIN_EXE_ade-worker")), 2).unwrap();
    let mut config = TaskConfig::default();
    config.topology.policy = ade_distributed::topology::PartitionPolicy::StaticPacked;
    let output = coordinator::execute(&StaticAggregateJob {
        id: JobId(1),
        files,
        workers: cluster.workers.clone(),
        config,
        task_timeout: Duration::from_secs(20),
        snapshot: None,
    })
    .unwrap();
    assert!(output.metrics.topology.is_none());
    assert!(
        output
            .metrics
            .producers
            .values()
            .all(|p| p.layout_digest.is_none()
                && p.routing_buffer_peak_rows <= p.routing_buffer_capacity_rows
                && p.routing_buffer_capacity_rows == 2 * ade_distributed::exchange::MAX_BATCH_ROWS)
    );
    verify_output(output, ExchangeMode::LocalMaterialized);
}

fn assert_partial_result(output: &coordinator::JobOutput, keys: &[u64]) {
    let mut actual = BTreeMap::new();
    for b in &output.batches {
        let k = b.column(0).as_any().downcast_ref::<UInt64Array>().unwrap();
        let s = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        let c = b.column(2).as_any().downcast_ref::<UInt64Array>().unwrap();
        for row in 0..b.num_rows() {
            assert!(
                actual
                    .insert(k.value(row), (s.value(row), c.value(row)))
                    .is_none()
            );
        }
    }
    let expected: BTreeMap<_, _> = keys
        .iter()
        .map(|key| (*key, (6 - 4 * i64::try_from(key % 1000).unwrap(), 4)))
        .collect();
    assert_eq!(actual, expected);
}
#[test]
fn partial_two_worker_modes_preserve_contributions_and_refuse_replay() {
    use ade_distributed::topology::PartitionPolicy;
    use ade_exec::partial::{Config, Mode};
    let root = tempfile::tempdir().unwrap();
    let keys: Vec<_> = (0..100_000)
        .filter(|k| ade_distributed::partition::hash_v1(*k).is_multiple_of(4))
        .take(3000)
        .collect();
    let files = topology_fixture(root.path(), &keys);
    let cluster = LocalCluster::spawn(Path::new(env!("CARGO_BIN_EXE_ade-worker")), 2).unwrap();
    let mut id = 100;
    for policy in [Mode::AdaptiveV1, Mode::RawPartial] {
        for mode in [
            ExchangeMode::Direct,
            ExchangeMode::Buffered,
            ExchangeMode::Spill,
            ExchangeMode::LocalMaterialized,
            ExchangeMode::ObjectStoreMaterialized,
            ExchangeMode::Adaptive,
        ] {
            id += 1;
            let mut config = TaskConfig {
                partial: Config {
                    mode: policy,
                    sample_rows: 65,
                    ..Default::default()
                },
                ..Default::default()
            };
            config.exchange.mode = mode;
            config.exchange.buffer_bytes = 1024 * 1024;
            if mode == ExchangeMode::Spill || mode == ExchangeMode::ObjectStoreMaterialized {
                config.topology.policy = PartitionPolicy::SampledV1;
            }
            let job = StaticAggregateJob {
                id: JobId(id),
                files: files.clone(),
                workers: cluster.workers.clone(),
                config,
                task_timeout: Duration::from_secs(20),
                snapshot: None,
            };
            let output = coordinator::execute(&job).unwrap();
            assert_partial_result(&output, &keys);
            for producer in output.metrics.producers.values() {
                let p = producer.partial.as_ref().unwrap();
                assert!(p.state_released_before_tail);
                if policy == Mode::AdaptiveV1 {
                    assert!(p.hash_to_raw);
                    assert_eq!(p.flushed_count, 65);
                    assert_eq!(p.raw_rows, 6000 - 65);
                } else {
                    assert!(!p.hash_to_raw);
                    assert_eq!(p.raw_rows, 6000);
                }
                assert_eq!(producer.sent.rows, 6000);
            }
            if job.config.topology.policy == PartitionPolicy::SampledV1 {
                assert_eq!(
                    output.metrics.topology.as_ref().unwrap().reason,
                    "split_oversized_bucket"
                );
            }
        }
    }
    let job = StaticAggregateJob {
        id: JobId(id + 1),
        files: files.clone(),
        workers: cluster.workers.clone(),
        config: TaskConfig {
            partial: Config {
                mode: Mode::AdaptiveV1,
                ..Default::default()
            },
            ..Default::default()
        },
        task_timeout: Duration::from_secs(20),
        snapshot: Some(source_snapshot(&files)),
    };
    assert!(
        coordinator::execute(&job)
            .err()
            .expect("non-fixed replay must refuse")
            .to_string()
            .contains("not admitted")
    );
    #[cfg(feature = "research-hooks")]
    {
        let mut job = job;
        job.snapshot = None;
        job.config.research_exit_final_partition = Some(0);
        assert!(coordinator::execute(&job).is_err());
    }
}
#[test]
fn partial_three_four_worker_pinned_layout_and_tiny_coalesce() {
    use ade_distributed::topology::{PartitionPolicy, TopologyConfig};
    use ade_exec::partial::{Config, Mode};
    for workers in [3, 4] {
        for policy in [Mode::AdaptiveV1, Mode::RawPartial] {
            let root = tempfile::tempdir().unwrap();
            let keys = vec![0, 1, 2, 3];
            let files = topology_fixture(root.path(), &keys);
            let cluster =
                LocalCluster::spawn(Path::new(env!("CARGO_BIN_EXE_ade-worker")), workers).unwrap();
            let job = StaticAggregateJob {
                id: JobId(1),
                files,
                workers: cluster.workers.clone(),
                config: TaskConfig {
                    partial: Config {
                        mode: policy,
                        ..Default::default()
                    },
                    topology: TopologyConfig {
                        policy: PartitionPolicy::SampledV1,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                task_timeout: Duration::from_secs(20),
                snapshot: None,
            };
            let output = coordinator::execute(&job).unwrap();
            assert_partial_result(&output, &keys);
            assert_eq!(output.metrics.finals.len(), 1);
            assert_eq!(output.metrics.inactive.len(), workers - 1);
            assert_eq!(output.metrics.gather.transfers, 1);
        }
    }
}
