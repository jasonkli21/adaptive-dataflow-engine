#![cfg(feature = "remote-s3")]
use ade_distributed::{
    ids::JobId,
    remote::{self, Event},
};
use ade_physical::{Field, Operator, PlanV1, Source, SourceFile, Target, Type, fingerprint};
use arrow::{
    array::{
        ArrayRef, Float64Array, Int64Array, StringArray, TimestampMillisecondArray, UInt32Array,
        UInt64Array,
    },
    record_batch::RecordBatch,
};
use std::{
    collections::BTreeMap,
    fs::{self, File},
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    sync::Arc,
};
#[allow(
    clippy::too_many_lines,
    reason = "One manifest-backed native Parquet fixture includes an independently accumulated scalar oracle."
)]
fn fixture(n: usize, rows: usize) -> (tempfile::TempDir, remote::Run, BTreeMap<u64, (i64, i64)>) {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
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
    let schema = ade_exec::physical::arrow_schema(&fields);
    let mut files = vec![];
    let mut manifest_files = vec![];
    let mut source_files = vec![];
    let mut oracle = BTreeMap::new();
    for f in 0..4 {
        let keys: Vec<_> = (0..rows)
            .map(|i| if i % 3 == 0 { 0 } else { (i % 17) as u64 })
            .collect();
        let values: Vec<_> = (0..rows)
            .map(|i| i64::try_from(i % 13).unwrap() - 6)
            .collect();
        for (k, v) in keys.iter().zip(&values) {
            let a = oracle.entry(*k).or_insert((0i64, 0i64));
            a.0 += v;
            a.1 += 1;
        }
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(UInt64Array::from(
                (0..rows).map(|i| (f * rows + i) as u64).collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(keys)),
            Arc::new(UInt64Array::from(vec![0; rows])),
            Arc::new(Int64Array::from(values)),
            Arc::new(Float64Array::from(vec![0.; rows])),
            Arc::new(UInt32Array::from(vec![0; rows])),
            Arc::new(StringArray::from(vec!["x"; rows])),
            Arc::new(TimestampMillisecondArray::from(vec![0; rows])),
        ];
        let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
        let name = format!("part-{f}.parquet");
        let path = root.join(&name);
        let mut writer = parquet::arrow::ArrowWriter::try_new(
            File::create(&path).unwrap(),
            schema.clone(),
            None,
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let data = fs::read(&path).unwrap();
        let h = fingerprint(&data);
        source_files.push(SourceFile {
            name: name.clone(),
            bytes: data.len() as u64,
            rows: rows as u64,
            fingerprint: h.clone(),
        });
        manifest_files.push(serde_json::json!({"relative_path":name,"bytes":data.len(),"rows":rows,"fingerprint":h}));
        files.push(path);
    }
    let identity = fingerprint(b"synthetic-events-v1:row_id=u64,key=u64,secondary=u64,value_i64=i64,value_f64=f64,category=u32,payload=utf8,event_time=timestamp_ms;all_nonnull");
    let logical = fingerprint(b"independent phase13 fixture v1");
    let data = serde_json::to_vec(&serde_json::json!({"schema_version":1,"dataset_version":1,"required_schema":"synthetic-events-v1","format":"parquet","schema_fingerprint":identity,"logical_fingerprint":logical,"row_count":rows*4,"files":manifest_files})).unwrap();
    let manifest = root.join("manifest.json");
    fs::write(&manifest, &data).unwrap();
    let source = Source {
        id: 0,
        table: "input".into(),
        manifest: fingerprint(&data),
        logical,
        schema_identity: identity,
        schema: fields,
        files: source_files,
        replayable: true,
    };
    let mut plan = PlanV1::new(Target::DistributedAggregate, vec![source]);
    plan.push(Operator::Scan { source: 0 }).unwrap();
    plan.push(Operator::GroupSumCount {
        input: 0,
        key: 1,
        value: 3,
    })
    .unwrap();
    let job = remote::Job {
        version: 1,
        job: JobId(1),
        plan,
        files,
        manifest,
        workers: n,
        readers: n,
        store: remote::S3Config {
            endpoint: std::env::var("ADE_REMOTE_ENDPOINT").expect("separate S3 fixture required"),
            bucket: "ade-phase13-test".into(),
            prefix: "acceptance".into(),
            region: "us-east-1".into(),
            dev_allow_http: true,
        },
        limits: remote::Limits::default(),
        rates: remote::Rates {
            worker_placement: "loopback-compute".into(),
            store_placement: "loopback-store".into(),
            ..Default::default()
        },
    };
    let worker_binary = PathBuf::from(env!("CARGO_BIN_EXE_ade-worker"));
    (
        dir,
        remote::Run {
            job,
            worker_binary,
            scratch_parent: root,
        },
        oracle,
    )
}
#[test]
#[ignore = "requires separate S3-compatible fixture and feature-matched worker"]
fn genuine_remote_static_turnover_and_scoped_cleanup() {
    for (n, readers, rows) in [
        (2, 2, 1031),
        (4, 4, 1031),
        (4, 2, 1031),
        (2, 1, 0),
        (4, 2, 1),
    ] {
        let (_dir, mut run, oracle) = fixture(n, rows);
        run.job.readers = readers;
        run.job.rates.delete_nano_usd = 7;
        run.job.rates.storage_byte_ms_nano_usd = 19;
        run.job.rates.retention_ms = 23;
        let mut exited = false;
        let mut result =
            remote::execute_observed(&run, ade_exec::CancellationToken::default(), |event| {
                match event {
                    Event::ProducersExited { boots, .. } => {
                        assert_eq!(boots.len(), n);
                        assert!(boots.iter().all(|b| !b.scratch.exists()));
                        exited = true;
                    }
                    Event::ReadersStarted { boots, .. } => {
                        assert!(exited);
                        assert_eq!(boots.len(), readers);
                    }
                    _ => {}
                }
                Ok(())
            })
            .unwrap();
        let mut actual = BTreeMap::new();
        for batch in &result.batches {
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
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                assert!(
                    actual
                        .insert(k.value(i), (s.value(i), c.value(i)))
                        .is_none()
                );
            }
        }
        assert_eq!(actual, oracle);
        let dry = result.cleanup(false).unwrap();
        assert!(!dry.applied);
        assert!(dry.post_cleanup_lifecycle_proxy_nano_usd.is_none());
        assert!(dry.verified_retained_bytes > 0);
        assert_ne!(dry.keys, Vec::<String>::new());
        assert!(dry.keys.iter().all(|k| k.starts_with(&format!(
            "acceptance/remote-v1/{}/attempt-0/",
            result.receipt.context.execution
        ))));
        let applied = result.cleanup(true).unwrap();
        assert_eq!(applied.keys, dry.keys);
        assert_eq!(applied.counters.delete, dry.keys.len() as u64);
        let mut lifecycle_counters = result.receipt.counters.clone();
        lifecycle_counters.add(&applied.counters).unwrap();
        assert_eq!(
            applied.post_cleanup_lifecycle_proxy_nano_usd,
            Some(
                result
                    .receipt
                    .effective
                    .rates
                    .estimate_lifecycle_nano_usd(
                        &lifecycle_counters,
                        applied.verified_retained_bytes,
                    )
                    .unwrap()
            )
        );
        assert!(
            applied.post_cleanup_lifecycle_proxy_nano_usd.unwrap()
                >= result.receipt.estimated_nano_usd
                    + applied.counters.delete * run.job.rates.delete_nano_usd
        );
        eprintln!(
            "ADE_REMOTE_ORACLE workers={n} readers={readers} rows={} canonical={} receipt={}",
            rows * 4,
            fingerprint(&serde_json::to_vec(&actual).unwrap()),
            serde_json::to_string(&result.receipt).unwrap()
        );
    }
}

fn raw_store(run: &remote::Run) -> object_store::aws::AmazonS3 {
    object_store::aws::AmazonS3Builder::new()
        .with_endpoint(&run.job.store.endpoint)
        .with_bucket_name(&run.job.store.bucket)
        .with_region("us-east-1")
        .with_allow_http(true)
        .with_access_key_id("ade-fixture")
        .with_secret_access_key("ade-fixture-secret")
        .with_retry(object_store::RetryConfig {
            max_retries: 0,
            ..Default::default()
        })
        .build()
        .unwrap()
}
fn mutate(run: &remote::Run, key: &str, bytes: Option<Vec<u8>>) {
    use object_store::ObjectStoreExt;
    let store = raw_store(run);
    let path = object_store::path::Path::from(key);
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            if let Some(bytes) = bytes {
                store.put(&path, bytes.into()).await.unwrap();
            } else {
                store.delete(&path).await.unwrap();
            }
        });
}
#[test]
#[ignore = "requires separate S3-compatible fixture and feature-matched worker"]
fn real_remote_corruption_missing_old_partial_sources_and_reader_death_never_return_output() {
    for attack in [
        "missing-manifest",
        "truncated-manifest",
        "old-manifest",
        "missing-terminal",
        "corrupt-segment",
        "truncated-segment",
        "source-drift",
        "reader-death",
        "cancel-readers",
    ] {
        let (_dir, run, _) = fixture(2, 1031);
        let token = ade_exec::CancellationToken::default();
        let stop = token.clone();
        let mut reached = false;
        let result = remote::execute_observed(&run, token, |event| {
            match event {
                Event::ManifestCommitted { context, .. } => {
                    reached = true;
                    let base = format!(
                        "{}/remote-v1/{}/attempt-0",
                        run.job.store.prefix, context.execution
                    );
                    match attack {
                        "missing-manifest" => mutate(&run, &format!("{base}/MANIFEST.json"), None),
                        "truncated-manifest" => {
                            mutate(&run, &format!("{base}/MANIFEST.json"), Some(b"{".to_vec()));
                        }
                        "old-manifest" => mutate(
                            &run,
                            &format!("{base}/MANIFEST.json"),
                            Some(b"{\"attempt\":1}".to_vec()),
                        ),
                        "missing-terminal" => {
                            mutate(&run, &format!("{base}/p-0/from-0/TERMINAL.json"), None);
                        }
                        "corrupt-segment" => mutate(
                            &run,
                            &format!("{base}/p-0/from-0/segment-0.arrow"),
                            Some(vec![0; 1120]),
                        ),
                        "truncated-segment" => mutate(
                            &run,
                            &format!("{base}/p-0/from-0/segment-0.arrow"),
                            Some(vec![0]),
                        ),
                        "source-drift" => {
                            let mut bytes = fs::read(&run.job.files[0]).unwrap();
                            bytes[10] ^= 1;
                            fs::write(&run.job.files[0], bytes).unwrap();
                        }
                        _ => {}
                    }
                }
                Event::ReadersStarted { boots, .. } if attack == "reader-death" => {
                    assert!(
                        std::process::Command::new("kill")
                            .args(["-KILL", &boots[0].pid.to_string()])
                            .status()
                            .unwrap()
                            .success()
                    );
                }
                Event::ReadersStarted { .. } if attack == "cancel-readers" => stop.cancel(),
                _ => {}
            }
            Ok(())
        });
        assert!(reached, "{attack}");
        assert!(result.is_err(), "accepted {attack}");
        eprintln!(
            "ADE_REMOTE_NEGATIVE stage=committed_manifest attack={attack} no_output=true error={}",
            result.err().unwrap()
        );
    }
}
#[test]
#[ignore = "requires separate S3-compatible fixture and feature-matched worker"]
fn real_remote_caps_and_producer_death_cancel_fail_without_result() {
    for attack in [
        "output-rows",
        "output-bytes",
        "segments",
        "objects",
        "requests",
        "write-bytes",
        "missing-bucket",
        "deadline",
        "producer-death",
        "cancel-producers",
    ] {
        let (_dir, mut run, _) = fixture(2, 1031);
        match attack {
            "output-rows" => run.job.plan.max_output_rows = 1,
            "output-bytes" => run.job.plan.max_output_bytes = 1,
            "segments" => run.job.limits.segments = 1,
            "objects" => run.job.limits.objects = 3,
            "requests" => run.job.limits.requests = 5,
            "write-bytes" => run.job.limits.write_bytes = 1,
            "missing-bucket" => run.job.store.bucket = "nonexistent-private".into(),
            "deadline" => run.job.limits.timeout_ms = 1,
            _ => {}
        }
        let token = ade_exec::CancellationToken::default();
        let stop = token.clone();
        let mut started = false;
        let result = remote::execute_observed(&run, token, |event| {
            if let Event::ProducersStarted { boots, .. } = event {
                started = true;
                if attack == "producer-death" {
                    assert!(
                        std::process::Command::new("kill")
                            .args(["-KILL", &boots[0].pid.to_string()])
                            .status()
                            .unwrap()
                            .success()
                    );
                }
                if attack == "cancel-producers" {
                    stop.cancel();
                }
            }
            Ok(())
        });
        assert!(result.is_err(), "accepted {attack}");
        if ["missing-bucket", "write-bytes", "producer-death"].contains(&attack) {
            let error = match &result {
                Err(error) => error.to_string(),
                Ok(_) => unreachable!("failure was asserted above"),
            };
            let code = match attack {
                "missing-bucket" => "provider",
                "write-bytes" => "limit",
                "producer-death" => "child_crash",
                _ => unreachable!(),
            };
            assert!(error.contains(code), "{attack}: {error}");
            assert!(!error.contains("ade-fixture-secret"), "{attack}: {error}");
        }
        if ["producer-death", "cancel-producers"].contains(&attack) {
            assert!(started);
        }
        eprintln!(
            "ADE_REMOTE_NEGATIVE attack={attack} producers_started={started} no_output=true error={}",
            result.err().unwrap()
        );
    }
}

fn proxy_control(value: &serde_json::Value) {
    use std::io::{Read, Write};
    let endpoint = std::env::var("ADE_REMOTE_ENDPOINT").unwrap();
    let u = url::Url::parse(&endpoint).unwrap();
    let mut s = std::net::TcpStream::connect((u.host_str().unwrap(), u.port().unwrap())).unwrap();
    s.set_read_timeout(Some(std::time::Duration::from_secs(2)))
        .unwrap();
    let data = serde_json::to_vec(value).unwrap();
    write!(
        s,
        "POST /__control HTTP/1.0\r\nHost: fixture\r\nContent-Length: {}\r\n\r\n",
        data.len()
    )
    .unwrap();
    s.write_all(&data).unwrap();
    let mut reply = vec![];
    s.take(4096).read_to_end(&mut reply).unwrap();
    assert!(String::from_utf8_lossy(&reply).contains("200 OK"));
}
#[test]
#[ignore = "requires fault proxy in front of separate S3 service"]
#[allow(
    clippy::too_many_lines,
    reason = "Each bounded service fault is independently reached, reset and checked against the same oracle."
)]
fn real_remote_transient_errors_outage_poison_and_late_put_are_bounded() {
    use object_store::ObjectStoreExt;
    for attack in [
        "transient-get",
        "truncated-get",
        "corrupt-get",
        "redirect",
        "oversize-error",
        "outage",
        "late-put",
    ] {
        let (_dir, mut run, oracle) = fixture(2, 1031);
        if attack == "late-put" {
            run.job.limits.io_timeout_ms = 100;
        }
        let mut execution = String::new();
        let mut reached = false;
        let result = remote::execute_observed(
            &run,
            ade_exec::CancellationToken::default(),
            |event| {
                match event {
                    Event::Admitted { context } if attack == "late-put" => {
                        execution = context.execution.clone();
                        reached = true;
                        proxy_control(
                            &serde_json::json!({"action":"delay","method":"PUT","contains":"/p-0/from-0/segment-0.arrow","remaining":1,"delay_ms":600}),
                        );
                    }
                    Event::ManifestCommitted { context, .. } if attack != "late-put" => {
                        execution = context.execution.clone();
                        reached = true;
                        proxy_control(&match attack {
                            "transient-get" => {
                                serde_json::json!({"action":"500","method":"GET","contains":"segment-0.arrow","remaining":1})
                            }
                            "truncated-get" => {
                                serde_json::json!({"action":"truncate","method":"GET","contains":"segment-0.arrow","remaining":1})
                            }
                            "redirect" => {
                                serde_json::json!({"action":"redirect","method":"GET","contains":"segment-0.arrow","remaining":64})
                            }
                            "oversize-error" => {
                                serde_json::json!({"action":"oversize-error","method":"GET","contains":"segment-0.arrow","remaining":64})
                            }
                            "corrupt-get" => {
                                serde_json::json!({"action":"corrupt","method":"GET","contains":"segment-0.arrow","remaining":1})
                            }
                            _ => {
                                serde_json::json!({"action":"503","contains":context.execution,"remaining":64})
                            }
                        });
                    }
                    _ => {}
                }
                Ok(())
            },
        );
        proxy_control(&serde_json::Value::Null);
        assert!(reached, "{attack}");
        if ["transient-get", "truncated-get"].contains(&attack) {
            let mut output = result.unwrap();
            assert_eq!(output.receipt.counters.retries, 1);
            let mut actual = BTreeMap::new();
            for b in &output.batches {
                let k = b.column(0).as_any().downcast_ref::<UInt64Array>().unwrap();
                let v = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
                let c = b.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
                for i in 0..b.num_rows() {
                    actual.insert(k.value(i), (v.value(i), c.value(i)));
                }
            }
            assert_eq!(actual, oracle);
            output.cleanup(true).unwrap();
            eprintln!("ADE_REMOTE_FAULT attack={attack} exact_output=true retries=1");
        } else {
            assert!(result.is_err(), "{attack}");
            if attack == "late-put" {
                std::thread::sleep(std::time::Duration::from_millis(800));
                let mut direct = run.clone();
                direct.job.store.endpoint = std::env::var("ADE_REMOTE_BACKEND").unwrap();
                let store = raw_store(&direct);
                let base = format!("{}/remote-v1/{execution}/attempt-0", run.job.store.prefix);
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                assert!(
                    rt.block_on(store.head(&object_store::path::Path::from(format!(
                        "{base}/p-0/from-0/segment-0.arrow"
                    ))))
                    .is_ok()
                );
                assert!(
                    rt.block_on(store.head(&object_store::path::Path::from(format!(
                        "{base}/MANIFEST.json"
                    ))))
                    .is_err()
                );
                eprintln!(
                    "ADE_REMOTE_LATE_PUT orphan_observed=true global_manifest_absent=true execution={execution}"
                );
            }
            eprintln!(
                "ADE_REMOTE_NEGATIVE stage=remote_request attack={attack} no_output=true error={}",
                result.err().unwrap()
            );
        }
    }
}

#[test]
#[ignore = "requires feature-matched real worker; service credentials are fixture only"]
fn fresh_boot_stale_epoch_attempt_backend_duplicate_and_foreign_grants_refuse_before_io() {
    use std::io::{Read, Write};
    use std::process::{Command, Stdio};
    fn write_frame(out: &mut impl Write, v: &serde_json::Value) {
        let data = serde_json::to_vec(v).unwrap();
        out.write_all(b"ADEOBJ02").unwrap();
        out.write_all(&u32::try_from(data.len()).unwrap().to_le_bytes())
            .unwrap();
        out.write_all(&data).unwrap();
    }
    fn read_frame(input: &mut impl Read) -> serde_json::Value {
        let mut m = [0; 8];
        input.read_exact(&mut m).unwrap();
        assert_eq!(&m, b"ADEOBJ02");
        let mut n = [0; 4];
        input.read_exact(&mut n).unwrap();
        let n = u32::from_le_bytes(n) as usize;
        assert!(n <= remote::MAX_JSON);
        let mut data = vec![0; n];
        input.read_exact(&mut data).unwrap();
        serde_json::from_slice(&data).unwrap()
    }
    for attack in [
        "boot",
        "epoch",
        "attempt",
        "backend",
        "unknown",
        "duplicate",
        "credentials",
    ] {
        let (_dir, run, _) = fixture(2, 1);
        let mut command = Command::new(&run.worker_binary);
        if attack == "credentials" {
            command
                .env_remove("AWS_ACCESS_KEY_ID")
                .env_remove("AWS_SECRET_ACCESS_KEY")
                .env_remove("AWS_SESSION_TOKEN");
        }
        let mut child = command
            .arg("--remote-stage")
            .arg(&run.scratch_parent)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let ready = read_frame(child.stdout.as_mut().unwrap());
        let boot = &ready["Ready"];
        let execution = uuid::Uuid::new_v4().to_string();
        let mut grant = serde_json::json!({"version":2,"boot":boot["instance"],"context":{"version":1,"execution":execution,"attempt":0,"job":run.job.job,"workers":2,"plan":run.job.plan.digest().unwrap(),"source":fingerprint(&serde_json::to_vec(&run.job.plan.sources).unwrap()),"config":fingerprint(&serde_json::to_vec(&run.job).unwrap()),"store":run.job.store.digest().unwrap(),"topology":fingerprint(&serde_json::to_vec(&("static-hash1",2usize)).unwrap())},"job":run.job,"role":{"Read":{"worker":0,"width":2,"epoch":1,"manifest_bytes":1,"manifest_hash":"a".repeat(64),"gather":"127.0.0.1:1"}}});
        match attack {
            "boot" => grant["boot"] = uuid::Uuid::new_v4().to_string().into(),
            "epoch" => grant["role"]["Read"]["epoch"] = 0.into(),
            "attempt" => grant["context"]["attempt"] = 1.into(),
            "backend" => grant["context"]["store"] = "d".repeat(64).into(),
            "unknown" => grant["foreign_field"] = 1.into(),
            _ => {}
        }
        let mut input = child.stdin.take().unwrap();
        write_frame(&mut input, &grant);
        if attack == "duplicate" {
            write_frame(&mut input, &grant);
        }
        drop(input);
        let until = std::time::Instant::now() + std::time::Duration::from_secs(3);
        while child.try_wait().unwrap().is_none() {
            if std::time::Instant::now() >= until {
                child.kill().unwrap();
                panic!("stale grant stalled {attack}");
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        let output = child.wait_with_output().unwrap();
        assert!(!output.status.success(), "{attack}");
        let refusal = read_frame(&mut output.stdout.as_slice());
        let expected_class = if attack == "credentials" {
            "credentials"
        } else {
            "admission"
        };
        assert_eq!(refusal["Refused"]["class"], expected_class, "{attack}");
        assert!(refusal.get("Done").is_none(), "unexpected result {attack}");
        assert!(!PathBuf::from(boot["scratch"].as_str().unwrap()).exists());
        eprintln!(
            "ADE_REMOTE_GRANT_REFUSED attack={attack} boot={} no_output=true",
            boot["instance"]
        );
    }
}

#[test]
#[ignore = "requires separate S3-compatible fixture and feature-matched worker"]
fn immutable_cleanup_capability_cannot_delete_another_live_namespace() {
    let (_a, run_a, _) = fixture(2, 1);
    let (_b, run_b, _) = fixture(2, 1);
    let mut first = remote::execute(&run_a, ade_exec::CancellationToken::default()).unwrap();
    let first_context = first.receipt.context.clone();
    let mut deleted = false;
    let mut second =
        remote::execute_observed(&run_b, ade_exec::CancellationToken::default(), |event| {
            if let Event::ManifestCommitted { context, .. } = event {
                assert_ne!(context.execution, first_context.execution);
                let gc = first.cleanup(true).unwrap();
                assert!(gc.keys.iter().all(|k| k.contains(&first_context.execution)));
                deleted = true;
            }
            Ok(())
        })
        .unwrap();
    assert!(deleted);
    let original = second.receipt.context.execution.clone();
    // Serialized/display metadata is not an authority to retarget deletion.
    second.receipt.context = first_context;
    second.receipt.effective.store.prefix = "foreign".into();
    assert!(
        second
            .cleanup(false)
            .unwrap()
            .keys
            .iter()
            .all(|k| k.contains(&original))
    );
    second.cleanup(true).unwrap();
    eprintln!("ADE_REMOTE_GC immutable_authority=true other_live_namespace_preserved=true");
}

#[test]
#[ignore = "requires separate S3-compatible fixture and feature-matched worker"]
fn failed_delete_reports_partial_cleanup_without_lifecycle_estimate() {
    let (_dir, run, _) = fixture(2, 1031);
    let mut output = remote::execute(&run, ade_exec::CancellationToken::default()).unwrap();
    let expected_keys = output.cleanup(false).unwrap().keys.len();
    proxy_control(&serde_json::json!({
        "action": "500",
        "method": "POST",
        "contains": "?delete",
        "remaining": 1
    }));
    let error = output.cleanup(true).unwrap_err().to_string();
    assert!(error.contains("cleanup_incomplete"), "{error}");
    assert!(error.contains("successful deletes=0"), "{error}");
    assert!(error.contains("failed delete attempts=1"), "{error}");
    assert!(error.contains("lifecycle estimate unavailable"), "{error}");
    assert!(!error.contains("ade-fixture-secret"), "{error}");

    // The exact same in-memory capability can retry after the transient failure.
    let completed = output.cleanup(true).unwrap();
    assert!(completed.applied);
    assert_eq!(completed.counters.delete, expected_keys as u64);
    assert!(completed.post_cleanup_lifecycle_proxy_nano_usd.is_some());
}

#[test]
#[ignore = "destructive only to the explicitly identified private fixture; run last with ADE_REMOTE_SERVICE_PID and ADE_MOTO_BIN"]
fn zz_real_service_restart_refuses_lost_intermediates() {
    use std::{
        process::{Child, Command, Stdio},
        time::{Duration, Instant},
    };
    struct Service(Option<Child>);
    impl Drop for Service {
        fn drop(&mut self) {
            if let Some(c) = &mut self.0 {
                let _ = c.kill();
                let _ = c.wait();
            }
        }
    }
    let (_dir, run, _) = fixture(2, 1031);
    let pid = std::env::var("ADE_REMOTE_SERVICE_PID").expect("explicit owned fixture PID required");
    let binary = std::env::var("ADE_MOTO_BIN").expect("pinned fixture executable required");
    let backend = url::Url::parse(&std::env::var("ADE_REMOTE_BACKEND").unwrap()).unwrap();
    assert_eq!(backend.host_str(), Some("127.0.0.1"));
    let address = ("127.0.0.1", backend.port().unwrap());
    let mut replacement = Service(None);
    let mut reached = false;
    let result = remote::execute_observed(&run, ade_exec::CancellationToken::default(), |event| {
        if let Event::ManifestCommitted { .. } = event {
            reached = true;
            assert!(
                Command::new("kill")
                    .args(["-TERM", &pid])
                    .status()
                    .unwrap()
                    .success()
            );
            let until = Instant::now() + Duration::from_secs(5);
            while std::net::TcpStream::connect(address).is_ok() {
                assert!(Instant::now() < until);
                std::thread::sleep(Duration::from_millis(5));
            }
            replacement.0 = Some(
                Command::new(&binary)
                    .args(["-H", "127.0.0.1", "-p", &address.1.to_string()])
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .spawn()
                    .unwrap(),
            );
            while std::net::TcpStream::connect(address).is_err() {
                assert!(Instant::now() < until);
                std::thread::sleep(Duration::from_millis(5));
            }
        }
        Ok(())
    });
    assert!(reached);
    assert!(result.is_err());
    eprintln!(
        "ADE_REMOTE_SERVICE_RESTART real_process_replaced=true prior_manifest_committed=true lost_store_state_refused=true no_output=true"
    );
}
