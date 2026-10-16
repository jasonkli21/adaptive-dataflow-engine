use ade_distributed::{
    TaskConfig,
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
                    Arc::new(UInt64Array::from_iter_values(0..32)),
                    Arc::new(Int64Array::from_iter_values((0..32).map(|key| file - key))),
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
#[test]
fn two_worker_process_graph_and_failure_cleanup() {
    let data = tempfile::tempdir().unwrap();
    let files = fixture(data.path());
    let mut cluster = LocalCluster::spawn(Path::new(env!("CARGO_BIN_EXE_ade-worker")), 2).unwrap();
    let mut job = StaticAggregateJob {
        id: JobId(42),
        files,
        workers: cluster.workers.clone(),
        config: TaskConfig {
            batch_size: 7,
            ..Default::default()
        },
        task_timeout: Duration::from_secs(20),
    };
    let output = coordinator::execute(&job).unwrap();
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
    let expected: BTreeMap<_, _> = (0..32)
        .map(|key| (key, (6 - 4 * i64::try_from(key).unwrap(), 4)))
        .collect();
    assert_eq!(actual, expected);
    assert_eq!(output.metrics.worker_count, 2);
    for worker in 0..2 {
        let producer = &output.metrics.producers[&worker];
        let final_task = &output.metrics.finals[&worker];
        assert_eq!(producer.source_files, 2);
        assert_eq!(producer.input_rows, 64);
        assert_eq!(producer.output_rows, 32);
        assert_eq!(producer.sent.transfers, 2);
        assert!(producer.sent.payload_bytes > 0);
        assert_eq!(producer.destinations.len(), 2);
        assert!(final_task.input_rows > 0);
        assert!(final_task.output_rows > 0);
        assert_eq!(output.metrics.received[&worker].transfers, 2);
    }
    assert_eq!(output.metrics.gather.rows, 32);
    assert!(output.metrics.gather.payload_bytes > 0);
    // Successful cleanup permits a second job on the same static topology.
    job.id = JobId(43);
    assert!(coordinator::execute(&job).is_ok());
    job.id = JobId(44);
    // Install state on the survivor before losing its peer, so failure must clean it.
    let spec = ade_distributed::protocol::JobSpec {
        job: job.id,
        workers: cluster.workers.clone(),
        config: job.config.clone(),
    };
    ade_distributed::protocol::rpc(
        &cluster.workers[0],
        ade_distributed::protocol::Request::PrepareJob(spec),
        Duration::from_secs(5),
    )
    .unwrap();
    cluster.terminate(1).unwrap();
    let start = Instant::now();
    assert!(coordinator::execute(&job).is_err());
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
        job: JobId(45),
        workers: cluster.workers.clone(),
        config: job.config.clone(),
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
