use ade_distributed::{
    WorkerEndpoint,
    managed::{self, Fence},
    physical,
};
use ade_exec::{
    CancellationToken,
    physical::{Binding, Catalog, arrow_schema},
};
use ade_physical::{Field, Operator, PlanV1, Source, SourceFile, Target, Type, fingerprint};
use arrow::{
    array::{Int64Array, UInt64Array},
    record_batch::RecordBatch,
};
use std::{
    fs::{self, File},
    io::BufReader,
    os::unix::fs::PermissionsExt,
    process::{Child, Command, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};
struct Workers(Vec<Child>);
impl Drop for Workers {
    fn drop(&mut self) {
        for child in &mut self.0 {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
#[test]
#[allow(clippy::too_many_lines)] // One end-to-end two-shape protocol scenario.
fn managed_real_workers_stream_both_shapes_and_prove_drain_and_fences() {
    for target in [Target::DistributedAggregate, Target::DistributedJoin] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();
        let store = uuid::Uuid::new_v4().to_string();
        let instance = uuid::Uuid::new_v4().to_string();
        let mut children = Workers(vec![]);
        let mut workers: Vec<WorkerEndpoint> = vec![];
        for id in 0..2 {
            let parent = root.join(format!("worker-{id}"));
            fs::create_dir(&parent).unwrap();
            fs::set_permissions(&parent, fs::Permissions::from_mode(0o700)).unwrap();
            let mut child = Command::new(env!("CARGO_BIN_EXE_ade-worker"))
                .args([
                    "--managed",
                    &id.to_string(),
                    parent.to_str().unwrap(),
                    &store,
                    "127.0.0.1:0",
                    "127.0.0.1:0",
                ])
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap();
            let ready: managed::Ready =
                managed::read_message(&mut BufReader::new(child.stdout.take().unwrap())).unwrap();
            let mut endpoint = ready.endpoint;
            endpoint.managed = Some(Fence {
                version: 1,
                boot: ready.boot.instance,
                store: store.clone(),
                owner: 1,
                instance: instance.clone(),
                job: 1,
                disk_bytes: managed::DISK_BYTES,
                scratch: parent.to_string_lossy().into_owned(),
            });
            workers.push(endpoint);
            children.0.push(child);
        }
        let fields = vec![
            Field::new("key", Type::U64, false),
            Field::new("value_i64", Type::I64, false),
        ];
        let schema = arrow_schema(&fields);
        let mut sources = vec![];
        let mut bindings = vec![];
        for (id, table) in if target == Target::DistributedAggregate {
            vec![(0, "input")]
        } else {
            vec![(0, "ade_left"), (1, "ade_right")]
        } {
            let path = root.join(format!("source-{id}.parquet"));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(UInt64Array::from(vec![1, 1, 2])),
                    Arc::new(Int64Array::from(vec![-9, 4, 7])),
                ],
            )
            .unwrap();
            let mut writer = parquet::arrow::ArrowWriter::try_new(
                File::create(&path).unwrap(),
                schema.clone(),
                None,
            )
            .unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
            let bytes = fs::read(&path).unwrap();
            let source = Source {
                id,
                table: table.into(),
                manifest: "a".repeat(64),
                logical: "b".repeat(64),
                schema_identity: "c".repeat(64),
                schema: fields.clone(),
                files: vec![SourceFile {
                    name: format!("source-{id}.parquet"),
                    bytes: bytes.len() as u64,
                    rows: 3,
                    fingerprint: fingerprint(&bytes),
                }],
                replayable: target == Target::DistributedAggregate,
            };
            sources.push(source.clone());
            bindings.push(Binding {
                source,
                files: vec![path],
            });
        }
        let mut plan = PlanV1::new(target, sources);
        plan.push(Operator::Scan { source: 0 }).unwrap();
        if target == Target::DistributedAggregate {
            plan.push(Operator::GroupSumCount {
                input: 0,
                key: 0,
                value: 1,
            })
            .unwrap();
        } else {
            plan.push(Operator::Scan { source: 1 }).unwrap();
            plan.push(Operator::InnerJoin {
                left: 0,
                right: 1,
                left_key: 0,
                right_key: 0,
                left_value: 1,
                right_value: 1,
            })
            .unwrap();
        }
        let until = Instant::now() + Duration::from_secs(15);
        managed::claim(&workers, until).unwrap();
        let mut options = physical::Options::default();
        options.aggregate.spill_enabled = false;
        options.aggregate.exchange.mode = ade_distributed::ExchangeMode::Buffered;
        options.join.local.spill_enabled = false;
        options.join.local.algorithm = ade_exec::join::Algorithm::HashOnly;
        let mut batches = vec![];
        let proof = managed::execute_to_sink(
            physical::Request {
                plan: &plan,
                catalog: &Catalog { bindings },
                workers: workers.clone(),
                job: ade_distributed::ids::JobId(1),
                options,
            },
            &root,
            &CancellationToken::default(),
            |batch| {
                batches.push(batch);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(proof.terminals().len(), 2);
        let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(
            rows,
            if target == Target::DistributedAggregate {
                2
            } else {
                5
            }
        );
        let drained = managed::drain(&workers, until).unwrap();
        assert_eq!(drained.len(), 2);
        assert!(
            ade_distributed::protocol::rpc(
                &workers[0],
                ade_distributed::protocol::Request::Ping,
                Duration::from_secs(1)
            )
            .is_err()
        );
        let next_instance = uuid::Uuid::new_v4().to_string();
        for worker in &mut workers {
            let fence = worker.managed.as_mut().unwrap();
            fence.owner = 2;
            fence.job = 2;
            fence.instance = next_instance.clone();
        }
        managed::claim(&workers, until).unwrap();
        managed::drain(&workers, until).unwrap();
    }
}
