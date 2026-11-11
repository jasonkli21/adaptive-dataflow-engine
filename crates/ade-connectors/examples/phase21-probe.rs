//! Acceptance process for public connector APIs.
use ade_connectors::{
    Catalog, CatalogEntryV1, Context, Error, ResolvedSnapshotV1, Source, Stage,
    batch::sink::{LocalSink, PendingSink, Sink},
    delta, flight, iceberg, kafka,
    local::Local,
    postgres, s3,
};
use ade_frontend::{
    Catalog as NativeCatalog, Target,
    execution::{Options, Output},
};
use ade_stream::{
    MonotonicClock, StreamConfigV1,
    runtime::{self, Metrics, Stop},
};
use arrow::array::Int64Array;
use serde_json::{Value, json};
use std::{
    env, fs,
    io::Write,
    net::Ipv4Addr,
    path::Path,
    time::{Duration, Instant},
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn stage(
    source: &dyn Source,
    entry: CatalogEntryV1,
    expected: &str,
    parent: &Path,
    cx: &Context,
) -> Result<Stage> {
    let mut catalog = Catalog::default();
    let name = entry.name.clone();
    catalog.register(entry)?;
    Ok(catalog.stage(&name, source, expected, parent, cx)?)
}

fn text<'a>(request: &'a Value, key: &str) -> Result<&'a str> {
    request[key]
        .as_str()
        .ok_or_else(|| "missing string request field".into())
}

async fn batch(request: &Value, cx: &Context) -> Result<Output> {
    let mode = text(request, "mode")?;
    let parent = Path::new(text(request, "stage")?);
    let sql = text(request, "sql")?;
    let source = match mode {
        "local" | "s3" | "baseline" => {
            let snapshot: ResolvedSnapshotV1 = serde_json::from_value(request["snapshot"].clone())?;
            let expected = text(request, "expected")?;
            match mode {
                "local" => stage(
                    &Local::new(Path::new(text(request, "root")?), snapshot.clone())?,
                    snapshot.entry,
                    expected,
                    parent,
                    cx,
                )?,
                "s3" => stage(
                    &s3::S3::new(
                        s3::Endpoint {
                            url: text(request, "url")?.parse()?,
                            address: Ipv4Addr::LOCALHOST,
                            bucket: "ade-connectors".into(),
                            region: "us-east-1".into(),
                            fixture_http: true,
                        },
                        snapshot.clone(),
                    )?,
                    snapshot.entry,
                    expected,
                    parent,
                    cx,
                )?,
                _ => return baseline(request, snapshot, sql, cx).await,
            }
        }
        "delta" => {
            let entry: CatalogEntryV1 = serde_json::from_value(request["entry"].clone())?;
            let source = delta::Delta::new(
                Path::new(text(request, "root")?),
                entry.clone(),
                serde_json::from_value(request["pin"].clone())?,
            )?;
            stage(&source, entry, text(request, "expected")?, parent, cx)?
        }
        "iceberg" => {
            let entry: CatalogEntryV1 = serde_json::from_value(request["entry"].clone())?;
            let source = iceberg::Iceberg::new(
                Path::new(text(request, "root")?),
                entry.clone(),
                serde_json::from_value(request["pin"].clone())?,
            )?;
            let mut catalog = Catalog::default();
            catalog.register(entry)?;
            source.stage(&catalog, text(request, "expected")?, parent, cx)?
        }
        "postgres" | "flight" => capture(request, mode, parent, cx).await?,
        _ => return Err("unknown mode".into()),
    };
    Ok(source.query(sql, cx).await?)
}

async fn capture(request: &Value, mode: &str, parent: &Path, cx: &Context) -> Result<Stage> {
    let entry = serde_json::from_value(request["entry"].clone())?;
    let port = u16::try_from(request["port"].as_u64().ok_or("missing port")?)?;
    let ca = request["ca"].as_str().map(fs::read).transpose()?;
    let host = if ca.is_some() {
        "localhost".to_owned()
    } else {
        Ipv4Addr::LOCALHOST.to_string()
    };
    if mode == "postgres" {
        return Ok(postgres::capture(
            &postgres::Endpoint {
                host,
                address: Ipv4Addr::LOCALHOST,
                port,
                database: "postgres".into(),
                user: text(request, "user")?.into(),
                fixture_plaintext: ca.is_none(),
                fixture_ca_pem: ca,
            },
            &postgres::Table {
                schema: "public".into(),
                name: text(request, "table")?.into(),
            },
            entry,
            parent,
            cx,
        )
        .await?);
    }
    Ok(flight::capture(
        &flight::Endpoint {
            host,
            address: Ipv4Addr::LOCALHOST,
            port,
            fixture_plaintext: ca.is_none(),
            fixture_ca_pem: ca,
        },
        text(request, "ticket")?.as_bytes(),
        entry,
        parent,
        cx,
    )
    .await?)
}

async fn baseline(
    request: &Value,
    snapshot: ResolvedSnapshotV1,
    sql: &str,
    cx: &Context,
) -> Result<Output> {
    let digest = snapshot.digest()?;
    if digest != text(request, "expected")? {
        return Err(Error::Identity("baseline expected snapshot").into());
    }
    let root = Path::new(text(request, "root")?);
    let mut catalog = NativeCatalog::default();
    let files = snapshot.splits.iter().map(|s| root.join(&s.key)).collect();
    let source = ade_physical::Source {
        id: 0,
        table: "input".into(),
        manifest: digest.clone(),
        logical: digest,
        schema_identity: ade_physical::fingerprint(&serde_json::to_vec(&snapshot.entry.schema)?),
        schema: snapshot.entry.schema,
        replayable: true,
        files: snapshot
            .splits
            .into_iter()
            .map(|s| ade_physical::SourceFile {
                name: s.key,
                bytes: s.bytes,
                rows: s.rows,
                fingerprint: s.blake3,
            })
            .collect(),
    };
    catalog.register_staged_input(
        ade_exec::physical::Binding { source, files },
        cx.deadline(),
        &ade_exec::CancellationToken::default(),
    )?;
    let plan = catalog.plan_sql(sql, Target::Local).await?;
    Ok(catalog.execute_until(&plan, Options::default(), cx.deadline())?)
}

#[derive(Default)]
struct Frames {
    data: Vec<u8>,
    signal_after: Option<usize>,
    count: usize,
}

impl runtime::Sink for Frames {
    fn try_write(&mut self, remaining: &[u8]) -> ade_stream::Result<usize> {
        if self.data.len() + remaining.len() > 1024 * 1024 {
            return Err(ade_stream::Error::Sink);
        }
        self.data.extend_from_slice(remaining);
        if remaining.ends_with(b"\n") {
            self.count += 1;
            if self.signal_after == Some(self.count) {
                eprintln!("HISTORY_RECEIPTS");
            }
        }
        Ok(remaining.len())
    }
}

fn kafka_run(request: &Value) -> Result<Value> {
    let logical = serde_json::to_vec(&request["logical"])?;
    let config = StreamConfigV1::parse(&serde_json::to_vec(&request["config"])?)?;
    let hash = ade_stream::unified::Admission::compile(&logical)?
        .native_spec_hash()
        .to_owned();
    let entry = kafka::KafkaEntryV1 {
        version: 1,
        name: "source".into(),
        native_spec_blake3: if request["wrong_pin"] == true {
            "0".repeat(64)
        } else {
            hash
        },
    };
    let capabilities = entry.capabilities();
    let bound = entry.bind(&logical, &config)?;
    let (mut source, mut engine) = bound.connect(uuid::Uuid::new_v4())?;
    let mut frames = Frames {
        signal_after: request["signal_after"]
            .as_u64()
            .and_then(|n| usize::try_from(n).ok()),
        ..Frames::default()
    };
    let mut trace = Frames::default();
    let mut metrics = Metrics::default();
    let stop = Stop {
        after_records: request["stop_records"].as_u64(),
        after_ms: Some(5000),
    };
    let result = runtime::run(
        &mut source,
        &mut engine,
        &mut frames,
        &mut trace,
        &MonotonicClock::default(),
        &ade_exec::CancellationToken::default(),
        &stop,
        config.sink_stall_ms,
        &mut metrics,
    );
    engine.cancel();
    source.shutdown()?;
    let reason = result?;
    let decode = |bytes: Vec<u8>| -> Result<Vec<Value>> {
        String::from_utf8(bytes)?
            .lines()
            .map(|line| Ok(serde_json::from_str(line)?))
            .collect()
    };
    Ok(
        json!({"receipts": decode(frames.data)?, "trace": decode(trace.data)?, "stop": reason,
        "metrics": metrics, "progress": engine.progress, "capabilities": capabilities}),
    )
}

fn publish(request: &Value, output: &Output, cx: &Context) -> Result<Value> {
    let root = Path::new(text(request, "sink")?);
    let sink = LocalSink::new(root)?;
    let key = text(request, "key")?;
    let plan = output.diagnostics["plan_hash"]
        .as_str()
        .ok_or("plan digest")?;
    let schema = output.batches().first().ok_or("no batch")?.schema();
    let mut pending = sink.prepare(key, plan, &schema, cx)?;
    for batch in output.batches() {
        pending.write(batch, cx)?;
    }
    if request["abort"] == true {
        pending.abort();
        return Ok(json!({"aborted":true}));
    }
    if request["pause"] == "prepared" {
        barrier();
    }
    let receipt = pending.commit(cx)?;
    if request["pause"] == "committed" {
        barrier();
    }
    let published = sink.reopen(key, cx)?;
    if receipt != published.receipt {
        return Err("reopened receipt mismatch".into());
    }
    Ok(serde_json::to_value(receipt)?)
}

fn barrier() {
    eprintln!("REACHED");
    let _ = std::io::stderr().flush();
    std::thread::sleep(Duration::from_secs(30));
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    match execute().await {
        Ok(output) => println!("{output}"),
        Err(error) => {
            if let Some(connector) = error.downcast_ref::<Error>() {
                eprintln!("ADE_ERROR_CODE={}", connector.code());
            }
            eprintln!("{error}");
            std::process::exit(1);
        }
    }
}

async fn execute() -> Result<Value> {
    let path = env::args_os()
        .nth(1)
        .ok_or("expected one fixture request file")?;
    let request: Value = serde_json::from_slice(&fs::read(path)?)?;
    if request["mode"] == "kafka" {
        return kafka_run(&request);
    }
    let cx = Context::new(Duration::from_millis(
        request["timeout_ms"].as_u64().unwrap_or(15_000),
    ))?;
    let _cancel = request["cancel_ms"].as_u64().map(|delay| {
        let cx = cx.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(delay)).await;
            cx.cancel();
        })
    });
    let started = Instant::now();
    let output = batch(&request, &cx).await?;
    let mut rows = output
        .batches()
        .iter()
        .map(|b| {
            let values = b
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or("expected i64 output")?;
            Ok(values.values().to_vec())
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    let row_digest = if request["summarize"] == true {
        let mut hasher = blake3::Hasher::new();
        for row in &rows {
            hasher.update(&row.to_le_bytes());
        }
        rows.truncate(3);
        Some(hasher.finalize().to_hex().to_string())
    } else {
        None
    };
    let receipt = if request.get("sink").is_some() {
        publish(&request, &output, &cx)?
    } else {
        Value::Null
    };
    Ok(
        json!({"rows": rows, "row_digest": row_digest, "receipt":receipt,"diagnostics":output.diagnostics,"elapsed_ns":started.elapsed().as_nanos()}),
    )
}
