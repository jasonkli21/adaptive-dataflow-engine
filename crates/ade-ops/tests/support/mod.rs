#![allow(
    dead_code,
    reason = "Shared fixtures are used by independent integration test targets."
)]
use ade_ops::{Input, Recipe, Worker};
use ade_physical::{Field, Operator, Target, Type, fingerprint};
use arrow::{
    array::{
        ArrayRef, Float64Array, Int64Array, StringArray, TimestampMillisecondArray, UInt32Array,
        UInt64Array,
    },
    datatypes::Schema,
    record_batch::RecordBatch,
};
use std::{
    fs::{self, File},
    path::{Path, PathBuf},
    sync::Arc,
};
use uuid::Uuid;
#[allow(
    clippy::too_many_lines,
    reason = "Builds both exact manifest schemas for the durable admission tests."
)]
pub fn fixture(join: bool) -> (tempfile::TempDir, Recipe) {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let data = root.join("data");
    fs::create_dir(&data).unwrap();
    let key: ArrayRef = Arc::new(UInt64Array::from(vec![1, 1, 2]));
    let val: ArrayRef = Arc::new(Int64Array::from(vec![-9, 4, 7]));
    let mut files = vec![];
    let required_schema;
    let schema_fingerprint;
    if join {
        required_schema = "synthetic-join-v1";
        schema_fingerprint = fingerprint(
            b"left-v1:key=u64,value_i64=i64;right-v1:key=u64,value_i64=i64;all_nonnull",
        );
        let fields = vec![
            Field::new("key", Type::U64, false),
            Field::new("value_i64", Type::I64, false),
        ];
        for side in ["left", "right"] {
            fs::create_dir(data.join(side)).unwrap();
            let name = format!("{side}/a.parquet");
            write(&data.join(&name), &fields, vec![key.clone(), val.clone()]);
            files.push(file_json(&data, &name));
        }
    } else {
        required_schema = "synthetic-events-v1";
        schema_fingerprint = fingerprint(b"synthetic-events-v1:row_id=u64,key=u64,secondary=u64,value_i64=i64,value_f64=f64,category=u32,payload=utf8,event_time=timestamp_ms;all_nonnull");
        let fields = vec![
            Field::new("row_id", Type::U64, false),
            Field::new("key", Type::U64, false),
            Field::new("secondary", Type::U64, false),
            Field::new("value_i64", Type::I64, false),
            Field::new("value_f64", Type::F64, false),
            Field::new("category", Type::U32, false),
            Field::new("payload", Type::Utf8, false),
            Field::new("event_time", Type::TimestampMs, false),
        ];
        write(
            &data.join("a.parquet"),
            &fields,
            vec![
                Arc::new(UInt64Array::from(vec![0, 1, 2])),
                key,
                key_array(),
                val,
                Arc::new(Float64Array::from(vec![1., 2., 3.])),
                Arc::new(UInt32Array::from(vec![0, 0, 0])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
                Arc::new(TimestampMillisecondArray::from(vec![0, 1, 2])),
            ],
        );
        files.push(file_json(&data, "a.parquet"));
    }
    let manifest = data.join("manifest.json");
    fs::write(&manifest, serde_json::to_vec(&serde_json::json!({"schema_version":1,"dataset_version":1,"required_schema":required_schema,"schema_fingerprint":schema_fingerprint,"logical_fingerprint":fingerprint(b"fixture logical"),"format":"parquet","row_count":if join {6} else {3},"files":files})).unwrap()).unwrap();
    let mut catalog = ade_frontend::Catalog::default();
    let tables = if join {
        vec!["ade_left", "ade_right"]
    } else {
        vec!["input"]
    };
    for table in &tables {
        catalog.register_manifest(table, &data, &manifest).unwrap();
    }
    let (mut plan, _) = catalog
        .plan(
            if join {
                Target::DistributedJoin
            } else {
                Target::DistributedAggregate
            },
            &tables,
        )
        .unwrap();
    plan.push(Operator::Scan { source: 0 }).unwrap();
    if join {
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
    } else {
        plan.push(Operator::GroupSumCount {
            input: 0,
            key: 1,
            value: 3,
        })
        .unwrap();
    }
    plan.validate().unwrap();
    let recipe = Recipe {
        version: 1,
        plan,
        inputs: tables
            .into_iter()
            .map(|table| Input {
                table: table.into(),
                root: data.clone(),
                manifest: manifest.clone(),
            })
            .collect(),
        workers: (0..2)
            .map(|id| Worker {
                id,
                control: format!("127.0.0.1:{}", 19100 + 2 * id).parse().unwrap(),
                exchange: format!("127.0.0.1:{}", 19101 + 2 * id).parse().unwrap(),
                instance: Uuid::new_v4(),
                scratch_root: root.join(format!("worker-{id}")),
            })
            .collect(),
        timeout_ms: 120_000,
        max_attempts: 1,
        runtime_bytes: ade_distributed::managed::DISK_BYTES,
        staging_bytes: 65536,
    };
    (dir, recipe)
}
fn key_array() -> ArrayRef {
    Arc::new(UInt64Array::from(vec![1, 1, 2]))
}
fn write(path: &Path, fields: &ade_physical::Schema, arrays: Vec<ArrayRef>) {
    let schema: Arc<Schema> = ade_exec::physical::arrow_schema(fields);
    let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}
fn file_json(root: &Path, name: &str) -> serde_json::Value {
    let bytes = fs::read(root.join(name)).unwrap();
    serde_json::json!({"relative_path":name,"bytes":bytes.len(),"rows":3,"fingerprint":fingerprint(&bytes)})
}
pub fn state_path(dir: &tempfile::TempDir) -> PathBuf {
    dir.path().canonicalize().unwrap().join("state")
}

pub struct ManagedWorkers {
    pub children: Vec<std::process::Child>,
    pub witnesses: std::sync::mpsc::Receiver<String>,
    readers: Vec<std::thread::JoinHandle<()>>,
}
impl Drop for ManagedWorkers {
    fn drop(&mut self) {
        for child in &mut self.children {
            let _ = child.kill();
            let _ = child.wait();
        }
        for reader in self.readers.drain(..) {
            let _ = reader.join();
        }
    }
}
pub fn managed_workers(root: &Path, store: Uuid, n: usize) -> (ManagedWorkers, Vec<Worker>) {
    managed_workers_with(root, store, n, &[])
}
pub fn managed_workers_with(
    root: &Path,
    store: Uuid,
    n: usize,
    environment: &[(&str, &str)],
) -> (ManagedWorkers, Vec<Worker>) {
    use std::{
        io::{BufRead, BufReader},
        os::unix::fs::PermissionsExt,
        process::{Command, Stdio},
        sync::mpsc,
    };
    let worker = PathBuf::from(env!("CARGO_BIN_EXE_ade-batch")).with_file_name("ade-worker");
    assert!(
        worker.exists(),
        "build matching ade-worker --features research-hooks first"
    );
    let (sender, witnesses) = mpsc::sync_channel(32);
    let mut cluster = ManagedWorkers {
        children: vec![],
        witnesses,
        readers: vec![],
    };
    let mut workers = vec![];
    for id in 0..n {
        let scratch = root.join(format!("managed-worker-{id}"));
        fs::create_dir(&scratch).unwrap();
        fs::set_permissions(&scratch, fs::Permissions::from_mode(0o700)).unwrap();
        let mut command = Command::new(&worker);
        command
            .args([
                "--managed",
                &id.to_string(),
                scratch.to_str().unwrap(),
                &store.to_string(),
                "127.0.0.1:0",
                "127.0.0.1:0",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if id == 0 {
            for (key, value) in environment {
                command.env(key, value);
            }
        }
        let mut child = command.spawn().unwrap();
        let stderr = child.stderr.take().unwrap();
        let notify = sender.clone();
        cluster.readers.push(std::thread::spawn(move || {
            for line in BufReader::new(stderr).lines() {
                let line = line.unwrap();
                eprintln!("worker{id}: {line}");
                let _ = notify.try_send(line);
            }
        }));
        let ready: ade_distributed::managed::Ready = ade_distributed::managed::read_message(
            &mut BufReader::new(child.stdout.take().unwrap()),
        )
        .unwrap();
        workers.push(Worker {
            id: id as u64,
            control: ready.endpoint.control,
            exchange: ready.endpoint.exchange,
            instance: Uuid::parse_str(&ready.boot.instance).unwrap(),
            scratch_root: scratch,
        });
        cluster.children.push(child);
    }
    (cluster, workers)
}

pub fn rewrite(recipe: &mut Recipe, left: &[(u64, i64)], right: &[(u64, i64)]) {
    let manifest = recipe.inputs[0].manifest.clone();
    let mut metadata: serde_json::Value =
        serde_json::from_slice(&fs::read(&manifest).unwrap()).unwrap();
    for source in &mut recipe.plan.sources {
        let rows = if source.id == 0 { left } else { right };
        let n = rows.len();
        let path = recipe.inputs[source.id as usize]
            .root
            .join(&source.files[0].name);
        let keys: ArrayRef = Arc::new(UInt64Array::from(
            rows.iter().map(|r| r.0).collect::<Vec<_>>(),
        ));
        let values: ArrayRef = Arc::new(Int64Array::from(
            rows.iter().map(|r| r.1).collect::<Vec<_>>(),
        ));
        let arrays: Vec<ArrayRef> = if source.table == "input" {
            vec![
                Arc::new(UInt64Array::from((0..n as u64).collect::<Vec<_>>())),
                keys,
                Arc::new(UInt64Array::from(vec![0; n])),
                values,
                Arc::new(Float64Array::from(vec![0.; n])),
                Arc::new(UInt32Array::from(vec![0; n])),
                Arc::new(StringArray::from(vec!["x"; n])),
                Arc::new(TimestampMillisecondArray::from(vec![0; n])),
            ]
        } else {
            vec![keys, values]
        };
        let schema = ade_exec::physical::arrow_schema(&source.schema);
        let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
        let props = parquet::file::properties::WriterProperties::builder()
            .set_max_row_group_row_count(Some(8192))
            .build();
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(File::create(&path).unwrap(), schema, Some(props))
                .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let data = fs::read(&path).unwrap();
        let f = &mut source.files[0];
        f.rows = n as u64;
        f.bytes = data.len() as u64;
        f.fingerprint = fingerprint(&data);
        let entry = metadata["files"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|entry| entry["relative_path"] == f.name)
            .unwrap();
        entry["rows"] = f.rows.into();
        entry["bytes"] = f.bytes.into();
        entry["fingerprint"] = f.fingerprint.clone().into();
    }
    metadata["row_count"] = metadata["files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["rows"].as_u64().unwrap())
        .sum::<u64>()
        .into();
    metadata["logical_fingerprint"] =
        fingerprint(&serde_json::to_vec(&(left, right)).unwrap()).into();
    let bytes = serde_json::to_vec(&metadata).unwrap();
    fs::write(&manifest, &bytes).unwrap();
    for source in &mut recipe.plan.sources {
        source.manifest = fingerprint(&bytes);
        source.logical = metadata["logical_fingerprint"].as_str().unwrap().into();
    }
}
pub fn pad_source(recipe: &mut Recipe, bytes: u64) {
    use std::io::{Seek, SeekFrom, Write};
    let source = &mut recipe.plan.sources[0];
    let f = &mut source.files[0];
    let path = recipe.inputs[0].root.join(&f.name);
    let original = fs::read(&path).unwrap();
    let footer = 8 + u32::from_le_bytes(
        original[original.len() - 8..original.len() - 4]
            .try_into()
            .unwrap(),
    ) as usize;
    let mut file = fs::OpenOptions::new().write(true).open(&path).unwrap();
    file.seek(SeekFrom::Start(bytes - footer as u64)).unwrap();
    file.write_all(&original[original.len() - footer..])
        .unwrap();
    file.set_len(bytes).unwrap();
    drop(file);
    f.bytes = bytes;
    f.fingerprint = ade_distributed::lineage::fingerprint(&path).unwrap();
    let manifest = &recipe.inputs[0].manifest;
    let mut metadata: serde_json::Value =
        serde_json::from_slice(&fs::read(manifest).unwrap()).unwrap();
    metadata["files"][0]["bytes"] = bytes.into();
    metadata["files"][0]["fingerprint"] = f.fingerprint.clone().into();
    let data = serde_json::to_vec(&metadata).unwrap();
    fs::write(manifest, &data).unwrap();
    source.manifest = fingerprint(&data);
}

// Same bounded metadata-lock acquisition as the public runner. A live cancel
// monitor or another parallel test's process-spawn window may briefly own it.
pub fn open_store(root: &Path) -> ade_ops::Store {
    let until = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        match ade_ops::Store::open(root) {
            Ok(store) => return store,
            Err(ade_ops::Error::Owned) if std::time::Instant::now() < until => {
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
            Err(error) => panic!("metadata read refused: {error}"),
        }
    }
}

/// Engine-independent canonical result of this fixture, including join bags.
pub fn assert_recovered_fixture(root: &Path, id: Uuid, join: bool, stage: &str) {
    let manifest = ade_ops::runner::verify_output(root, id).unwrap();
    let path = root.join("jobs").join(id.to_string()).join(&manifest.file);
    let reader =
        arrow::ipc::reader::StreamReader::try_new(File::open(path).unwrap(), None).unwrap();
    assert_eq!(
        reader.schema(),
        ade_exec::physical::arrow_schema(&manifest.schema)
    );
    let mut actual = vec![];
    for batch in reader {
        let batch = batch.unwrap();
        for row in 0..batch.num_rows() {
            actual.push(
                (0..batch.num_columns())
                    .map(|c| {
                        arrow::util::display::array_value_to_string(batch.column(c), row).unwrap()
                    })
                    .collect::<Vec<_>>(),
            );
        }
    }
    actual.sort();
    let mut expected = if join {
        vec![
            vec!["1", "-9", "-9"],
            vec!["1", "-9", "4"],
            vec!["1", "4", "-9"],
            vec!["1", "4", "4"],
            vec!["2", "7", "7"],
        ]
    } else {
        vec![vec!["1", "-5", "2"], vec!["2", "7", "1"]]
    }
    .into_iter()
    .map(|row| row.into_iter().map(String::from).collect::<Vec<_>>())
    .collect::<Vec<_>>();
    expected.sort();
    assert_eq!(actual, expected, "recovered {join} {stage}");
    eprintln!(
        "ADE_NATIVE_RECOVERY_ORACLE join={join} stage={stage} canonical={} ipc={}",
        blake3::hash(&serde_json::to_vec(&actual).unwrap()).to_hex(),
        manifest.blake3
    );
}
