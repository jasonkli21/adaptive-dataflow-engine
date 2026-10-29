//! One owned native producer or final-reader stage per fresh worker process.
use super::{
    Context, Job, Limits, MAX_JSON, Manifest, Segment, Terminal, TerminalRef, VERSION, add, digest,
    json,
    store::Client,
    terminal_hash, transfer, transfer_hash,
    wire::{self, Boot, Done, Final, Grant, Reply, Role},
};
use crate::{Error, Result, exchange, require};
use ade_exec::{CancellationToken, LocalPlan};
use arrow::{array::UInt64Array, ipc::writer::StreamWriter, record_batch::RecordBatch};
use std::{
    io::{Read, Write},
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

pub(super) fn terminal_key(p: usize, w: usize) -> String {
    format!("p-{p}/from-{w}/TERMINAL.json")
}
pub(super) fn segment_key(p: usize, w: usize, s: u64) -> String {
    format!("p-{p}/from-{w}/segment-{s}.arrow")
}
#[allow(
    clippy::needless_pass_by_value,
    reason = "Owned map_err adapter between engine error types."
)]
fn local(error: Error) -> ade_exec::Error {
    ade_exec::Error::Invalid(format!("remote exchange: {error}"))
}
fn execution() -> ade_exec::ExecutionConfig {
    ade_exec::ExecutionConfig {
        memory_limit_bytes: super::TASK_MEMORY,
        spill_enabled: false,
        prefetch_batches: 0,
        output_batch_rows: exchange::MAX_BATCH_ROWS,
    }
}
fn check(token: &CancellationToken, until: Instant) -> Result<()> {
    token.check()?;
    require(Instant::now() < until, "remote stage deadline")
}
fn verified_job(grant: &Grant) -> Result<()> {
    grant.job.validate()?;
    grant.context.validate()?;
    require(
        grant.version == VERSION
            && grant.context.job == grant.job.job
            && grant.context.workers == grant.job.workers
            && grant.context.plan
                == grant
                    .job
                    .plan
                    .digest()
                    .map_err(|e| Error::Invalid(e.to_string()))?
            && grant.context.source == digest(&grant.job.plan.sources)?
            && grant.context.config == digest(&grant.job)?
            && grant.context.store == grant.job.store.digest()?,
        "remote grant config/source/plan fence",
    )
}
/// Fresh one-shot worker. The caller owns/reaps this process; no listener is opened.
/// # Errors
/// Rejects stale/malformed grants, credentials, sources, data, quotas and deadline failures.
#[allow(
    clippy::too_many_lines,
    reason = "One one-shot grant owns the producer or sequential final-reader lifecycle."
)]
pub fn serve(parent: &Path, input: &mut impl Read, output: &mut impl Write) -> Result<()> {
    super::coordinator::private_root(parent)?;
    let scratch = tempfile::Builder::new()
        .prefix("remote-worker-")
        .tempdir_in(parent)?;
    let boot = Boot {
        version: VERSION,
        instance: uuid::Uuid::new_v4().to_string(),
        pid: std::process::id(),
        runtime: crate::managed::RUNTIME_ID.into(),
        scratch: scratch.path().canonicalize()?,
    };
    wire::write(output, &Reply::Ready(boot.clone()))?;
    let grant: Grant = wire::read(input)?;
    let mut tail = [0];
    require(
        input.read(&mut tail)? == 0,
        "remote worker accepts exactly one grant",
    )?;
    require(grant.boot == boot.instance, "remote stale boot grant")?;
    verified_job(&grant)?;
    let token = CancellationToken::default();
    let until = Instant::now() + Duration::from_millis(grant.job.limits.timeout_ms);
    let mut client = Client::new(
        &grant.job.store,
        &grant.context,
        &grant.job.limits,
        token.clone(),
        until,
    )?;
    let (terminals, finals) = match grant.role {
        Role::Produce { worker } => {
            require(worker < grant.context.workers, "remote producer slot")?;
            (
                produce(
                    &grant.job,
                    &grant.context,
                    worker,
                    &mut client,
                    &token,
                    until,
                )?,
                vec![],
            )
        }
        Role::Read {
            worker,
            width,
            epoch,
            manifest_bytes,
            ref manifest_hash,
            gather,
        } => {
            require(
                epoch == 1
                    && width == grant.job.readers
                    && worker < width
                    && width <= grant.context.workers
                    && gather.ip().is_loopback()
                    && gather.port() != 0,
                "remote handoff epoch/reader/gather",
            )?;
            let terminals = load(
                &mut client,
                &grant.context,
                &grant.job.limits,
                manifest_bytes,
                manifest_hash,
            )?;
            let shared = Arc::new(Mutex::new(client));
            let mut finals = vec![];
            for p in (worker..grant.context.workers).step_by(width) {
                check(&token, until)?;
                let selected: Vec<_> = terminals
                    .iter()
                    .filter(|t| t.partition == p)
                    .cloned()
                    .collect();
                let source_rows = Arc::new(AtomicU64::new(0));
                let source = Source::new(selected, shared.clone(), source_rows.clone())?;
                let mut sender = None;
                ade_exec::execute_merge_source_to_sink(
                    Box::new(source),
                    execution(),
                    token.clone(),
                    |batch| {
                        check(&token, until).map_err(local)?;
                        if sender.is_none() {
                            sender = Some(
                                exchange::Sender::new(
                                    gather,
                                    crate::ids::transfer(
                                        grant.context.job,
                                        1,
                                        0,
                                        (grant.context.workers + p) as u64,
                                    ),
                                    token.clone(),
                                )
                                .map_err(local)?,
                            );
                        }
                        sender
                            .as_mut()
                            .ok_or_else(|| {
                                ade_exec::Error::Invalid("remote sender missing".into())
                            })?
                            .batch(&batch)
                            .map_err(local)
                    },
                )?;
                let sender = match sender {
                    Some(s) => s,
                    None => exchange::Sender::new(
                        gather,
                        crate::ids::transfer(
                            grant.context.job,
                            1,
                            0,
                            (grant.context.workers + p) as u64,
                        ),
                        token.clone(),
                    )?,
                };
                let (sent, terminal) = sender.finish_manifest()?;
                finals.push(Final {
                    partition: p,
                    sent,
                    terminal,
                    input_rows: source_rows.load(Ordering::Relaxed),
                });
            }
            client = Arc::try_unwrap(shared)
                .map_err(|_| Error::Invalid("remote outstanding reader I/O".into()))?
                .into_inner()
                .map_err(|_| Error::Invalid("remote reader poisoned lock".into()))?;
            (vec![], finals)
        }
    };
    check(&token, until)?;
    let done = Done {
        boot,
        context: grant.context,
        terminals,
        finals,
        counters: client.counters.clone(),
    };
    // Remove the worker-owned scratch BEFORE reporting a successful terminal reply.
    scratch.close()?;
    wire::write(output, &Reply::Done(Box::new(done)))
}
#[allow(
    clippy::too_many_lines,
    reason = "Producer counters, hash chains and immutable terminal publication share one native sink lifetime."
)]
fn produce(
    job: &Job,
    c: &Context,
    w: usize,
    client: &mut Client,
    token: &CancellationToken,
    until: Instant,
) -> Result<Vec<TerminalRef>> {
    job.verify(token, until, true)?;
    let files: Vec<_> = job
        .files
        .iter()
        .skip(w)
        .step_by(c.workers)
        .cloned()
        .collect();
    let mut terminals: Vec<_> = (0..c.workers)
        .map(|p| Terminal {
            context: c.clone(),
            producer: w,
            partition: p,
            segments: vec![],
            terminal: crate::lineage::TransferManifest {
                logical: transfer(c, p, w).logical(),
                attempt: crate::ids::AttemptId(0),
                segments: 0,
                rows: 0,
                payload_bytes: 0,
                fingerprint: String::new(),
            },
        })
        .collect();
    let mut hashes = (0..c.workers)
        .map(|p| transfer_hash(c, p, w))
        .collect::<Result<Vec<_>>>()?;
    let mut count = 0;
    let mut represented = 0u64;
    if !files.is_empty() {
        let plan = LocalPlan::HashAggregate {
            input: Box::new(LocalPlan::Project {
                input: Box::new(LocalPlan::ParquetScan {
                    files,
                    batch_size: 8192,
                }),
                columns: vec!["key".into(), "value_i64".into()],
            }),
            key: "key".into(),
            value: "value_i64".into(),
        };
        ade_exec::execute_to_sink(plan, execution(), token.clone(), |batch| {
            check(token, until).map_err(local)?;
            let counts = batch
                .column(2)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| ade_exec::Error::Invalid("remote partial count schema".into()))?;
            for v in counts.values() {
                represented = add(represented, *v).map_err(local)?;
            }
            for p in 0..c.workers {
                let selected = crate::partition::select(&batch, p, c.workers).map_err(local)?;
                if selected.num_rows() == 0 {
                    continue;
                }
                count = add(count, 1).map_err(local)?;
                require(count <= job.limits.segments, "remote producer segment cap")
                    .map_err(local)?;
                let mut encoded = exchange::BoundedBuffer::new(exchange::MAX_FRAME_BYTES);
                {
                    let mut ipc = StreamWriter::try_new(&mut encoded, &exchange::schema())?;
                    ipc.write(&selected)?;
                    ipc.finish()?;
                }
                // Validate the same IPC/schema envelope used by TCP exchange before external effects.
                exchange::decode(&encoded.0).map_err(local)?;
                let t = &mut terminals[p];
                let sequence = t.segments.len() as u64;
                let n = encoded.0.len() as u64;
                let checksum = blake3::hash(&encoded.0).to_hex().to_string();
                hashes[p].update(&sequence.to_le_bytes());
                hashes[p].update(&n.to_le_bytes());
                hashes[p].update(&encoded.0);
                client
                    .put(&client.key(&segment_key(p, w, sequence)), encoded.0)
                    .map_err(local)?;
                t.segments.push(Segment {
                    sequence,
                    rows: selected.num_rows() as u64,
                    bytes: n,
                    blake3: checksum,
                });
                t.terminal.rows =
                    add(t.terminal.rows, selected.num_rows() as u64).map_err(local)?;
                t.terminal.payload_bytes = add(t.terminal.payload_bytes, n).map_err(local)?;
            }
            Ok(())
        })?;
    }
    let expected = job.plan.sources[0]
        .files
        .iter()
        .skip(w)
        .step_by(c.workers)
        .try_fold(0, |a, f| add(a, f.rows))?;
    require(
        represented == expected,
        "remote source/partial count conservation",
    )?;
    job.verify(token, until, false)?;
    let mut refs = vec![];
    for (mut t, h) in terminals.into_iter().zip(hashes) {
        t.terminal.segments = t.segments.len() as u64;
        t.terminal.fingerprint = terminal_hash(h, t.terminal.segments);
        t.validate(c, &job.limits)?;
        let bytes = json(&t)?;
        require(bytes.len() <= 65536, "remote terminal metadata cap")?;
        let r = TerminalRef {
            producer: w,
            partition: t.partition,
            bytes: bytes.len() as u64,
            blake3: blake3::hash(&bytes).to_hex().to_string(),
        };
        client.put(&client.key(&terminal_key(t.partition, w)), bytes)?;
        refs.push(r);
    }
    Ok(refs)
}
pub(super) fn load(
    client: &mut Client,
    c: &Context,
    limits: &Limits,
    bytes: u64,
    checksum: &str,
) -> Result<Vec<Terminal>> {
    let data = client.fetch(&client.key("MANIFEST.json"), bytes, checksum)?;
    let manifest: Manifest = serde_json::from_slice(&data)?;
    manifest.validate(c)?;
    load_terminals(client, c, limits, &manifest)
}
pub(super) fn load_terminals(
    client: &mut Client,
    c: &Context,
    limits: &Limits,
    manifest: &Manifest,
) -> Result<Vec<Terminal>> {
    manifest.validate(c)?;
    let mut terms = vec![];
    let mut segments = vec![0; c.workers];
    let mut encoded = vec![0; c.workers];
    let mut total_json = 0;
    for r in &manifest.terminals {
        require(r.bytes <= 65536, "remote terminal cap")?;
        total_json = add(total_json, r.bytes)?;
        require(
            total_json <= MAX_JSON as u64,
            "remote total terminal metadata cap",
        )?;
        let data = client.fetch(
            &client.key(&terminal_key(r.partition, r.producer)),
            r.bytes,
            &r.blake3,
        )?;
        let t: Terminal = serde_json::from_slice(&data)?;
        t.validate(c, limits)?;
        require(
            t.partition == r.partition && t.producer == r.producer,
            "remote terminal reference fence",
        )?;
        segments[t.producer] = add(segments[t.producer], t.terminal.segments)?;
        encoded[t.producer] = add(encoded[t.producer], add(t.terminal.payload_bytes, r.bytes)?)?;
        require(
            segments[t.producer] <= limits.segments
                && encoded[t.producer] <= limits.write_bytes
                && add(segments[t.producer], c.workers as u64)? <= limits.objects,
            "remote complete producer budget",
        )?;
        terms.push(t);
    }
    Ok(terms)
}
struct Source {
    terms: std::vec::IntoIter<Terminal>,
    current: Option<Terminal>,
    sequence: usize,
    chain: Option<blake3::Hasher>,
    client: Arc<Mutex<Client>>,
    rows: Arc<AtomicU64>,
}
impl std::fmt::Debug for Source {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RemoteVerifiedSource")
    }
}
impl Source {
    fn new(terms: Vec<Terminal>, client: Arc<Mutex<Client>>, rows: Arc<AtomicU64>) -> Result<Self> {
        require(!terms.is_empty(), "remote source terminal set")?;
        Ok(Self {
            terms: terms.into_iter(),
            current: None,
            sequence: 0,
            chain: None,
            client,
            rows,
        })
    }
}
impl ade_exec::BatchSource for Source {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        exchange::schema()
    }
    fn next_batch(&mut self, token: &CancellationToken) -> ade_exec::Result<Option<RecordBatch>> {
        loop {
            token.check()?;
            if self.current.is_none() {
                let Some(t) = self.terms.next() else {
                    return Ok(None);
                };
                self.chain =
                    Some(transfer_hash(&t.context, t.partition, t.producer).map_err(local)?);
                self.sequence = 0;
                self.current = Some(t);
            }
            let t = self.current.as_ref().expect("installed");
            if self.sequence == t.segments.len() {
                require(
                    terminal_hash(self.chain.take().expect("chain"), t.segments.len() as u64)
                        == t.terminal.fingerprint,
                    "remote terminal hash chain mismatch",
                )
                .map_err(local)?;
                self.current = None;
                continue;
            }
            let s = &t.segments[self.sequence];
            let mut client = self
                .client
                .lock()
                .map_err(|_| ade_exec::Error::Invalid("remote client lock".into()))?;
            let key = client.key(&segment_key(t.partition, t.producer, s.sequence));
            let bytes = client.fetch(&key, s.bytes, &s.blake3).map_err(local)?;
            drop(client);
            let batch = exchange::decode(&bytes).map_err(local)?;
            require(
                batch.num_rows() as u64 == s.rows,
                "remote segment row mismatch",
            )
            .map_err(local)?;
            let counts = batch
                .column(2)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| ade_exec::Error::Invalid("remote count schema".into()))?;
            require(
                counts.values().iter().all(|v| *v > 0),
                "remote partial counts must be positive",
            )
            .map_err(local)?;
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| ade_exec::Error::Invalid("remote key schema".into()))?;
            for key in keys.values() {
                require(
                    crate::partition::destination(*key, t.context.workers).map_err(local)?
                        == t.partition,
                    "remote hash partition owner",
                )
                .map_err(local)?;
            }
            let chain = self.chain.as_mut().expect("chain");
            chain.update(&s.sequence.to_le_bytes());
            chain.update(&s.bytes.to_le_bytes());
            chain.update(&bytes);
            self.rows.fetch_add(s.rows, Ordering::Relaxed);
            self.sequence += 1;
            return Ok(Some(batch));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ade_exec::BatchSource;
    use object_store::{ObjectStoreExt, path::Path as ObjectPath};
    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "One independent fixture exercises each decoder/ownership/terminal rejection boundary."
    )]
    fn independently_pinned_bytes_still_require_schema_rows_owner_and_terminal_chain() {
        for attack in ["schema", "rows", "owner", "chain", "zero-count"] {
            let config = super::super::S3Config {
                endpoint: "https://fixture.invalid".into(),
                bucket: "test-bucket".into(),
                prefix: "private".into(),
                region: "us-east-1".into(),
                dev_allow_http: false,
            };
            let c = Context {
                version: 1,
                execution: uuid::Uuid::new_v4().to_string(),
                attempt: 0,
                job: crate::ids::JobId(1),
                workers: 2,
                plan: "a".repeat(64),
                source: "b".repeat(64),
                config: "c".repeat(64),
                store: config.digest().unwrap(),
                topology: digest(&("static-hash1", 2usize)).unwrap(),
            };
            let owner = crate::partition::destination(0, 2).unwrap();
            let partition = if attack == "owner" { 1 - owner } else { owner };
            let batch = if attack == "schema" {
                RecordBatch::try_from_iter([(
                    "foreign",
                    Arc::new(UInt64Array::from(vec![0])) as arrow::array::ArrayRef,
                )])
                .unwrap()
            } else {
                RecordBatch::try_new(
                    exchange::schema(),
                    vec![
                        Arc::new(UInt64Array::from(vec![0])),
                        Arc::new(arrow::array::Int64Array::from(vec![7])),
                        Arc::new(UInt64Array::from(vec![u64::from(attack != "zero-count")])),
                    ],
                )
                .unwrap()
            };
            let mut data = Vec::new();
            {
                let mut w = StreamWriter::try_new(&mut data, &batch.schema()).unwrap();
                w.write(&batch).unwrap();
                w.finish().unwrap();
            }
            let mut chain = transfer_hash(&c, partition, 0).unwrap();
            chain.update(&0u64.to_le_bytes());
            chain.update(&(data.len() as u64).to_le_bytes());
            chain.update(&data);
            let rows = if attack == "rows" { 2 } else { 1 };
            let t = Terminal {
                context: c.clone(),
                producer: 0,
                partition,
                segments: vec![Segment {
                    sequence: 0,
                    rows,
                    bytes: data.len() as u64,
                    blake3: blake3::hash(&data).to_hex().to_string(),
                }],
                terminal: crate::lineage::TransferManifest {
                    logical: transfer(&c, partition, 0).logical(),
                    attempt: crate::ids::AttemptId(0),
                    segments: 1,
                    rows,
                    payload_bytes: data.len() as u64,
                    fingerprint: if attack == "chain" {
                        "e".repeat(64)
                    } else {
                        terminal_hash(chain, 1)
                    },
                },
            };
            let backend: Arc<dyn object_store::ObjectStore> =
                Arc::new(object_store::memory::InMemory::new());
            let client = Client::with_backend(
                backend.clone(),
                &config,
                &c,
                &Limits::default(),
                CancellationToken::default(),
                Instant::now() + Duration::from_secs(1),
            )
            .unwrap();
            let key = client.key(&segment_key(partition, 0, 0));
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(backend.put(&ObjectPath::from(key), data.into()))
                .unwrap();
            let mut source = Source::new(
                vec![t],
                Arc::new(Mutex::new(client)),
                Arc::new(AtomicU64::new(0)),
            )
            .unwrap();
            let result = source.next_batch(&CancellationToken::default());
            if attack == "chain" {
                assert!(result.unwrap().is_some());
                assert!(source.next_batch(&CancellationToken::default()).is_err());
            } else {
                assert!(result.is_err(), "{attack}");
            }
        }
    }
}
