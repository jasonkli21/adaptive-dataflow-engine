//! Bounded stage coordinator. Reaped producer processes precede fresh reader grants.
use super::{
    Context, Counters, Job, Manifest, VERSION, add, json,
    store::Client,
    wire::{self, Boot, Done, FailureClass, Grant, Reply, Role},
    worker::{load_terminals, segment_key, terminal_key},
};
use crate::{Error, Result, bounded_output::OutputLimits, gather::Gather, ids::AttemptId, require};
use ade_exec::CancellationToken;
use arrow::{array::Int64Array, record_batch::RecordBatch};
use serde::{Deserialize, Serialize};
use std::{
    fs,
    io::Read,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{Arc, mpsc},
    thread,
    time::{Duration, Instant},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Run {
    pub job: Job,
    pub worker_binary: PathBuf,
    pub scratch_parent: PathBuf,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Receipt {
    pub version: u32,
    pub handoff_epoch: u64,
    pub context: Context,
    pub effective: Job,
    pub producers: Vec<Boot>,
    pub readers: Vec<Boot>,
    pub manifest_bytes: u64,
    pub manifest_hash: String,
    pub counters: Counters,
    pub production_ns: u64,
    pub consumption_ns: u64,
    pub wall_ns: u64,
    pub rate_provenance: String,
    pub observation_provenance: String,
    pub estimated_nano_usd: u64,
}
pub struct Output {
    pub batches: Vec<RecordBatch>,
    pub receipt: Receipt,
    quiesced: Quiesced,
}
struct Quiesced {
    context: Context,
    store: super::S3Config,
    limits: super::Limits,
    keys: Vec<String>,
    rates: super::Rates,
    execution_counters: Counters,
    retained_bytes: u64,
    completed_cleanup: Option<Cleanup>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cleanup {
    pub keys: Vec<String>,
    pub applied: bool,
    pub counters: Counters,
    pub verified_retained_bytes: u64,
    pub assumed_retention_ms: u64,
    pub post_cleanup_lifecycle_proxy_nano_usd: Option<u64>,
}
impl Output {
    /// Dry-run by default at the CLI. Only this post-reap output owns apply authority.
    /// # Errors
    /// Refuses service errors, forged namespaces and limits; it never discovers/deletes by LIST.
    pub fn cleanup(&mut self, apply: bool) -> Result<Cleanup> {
        if let Some(completed) = &self.quiesced.completed_cleanup {
            if apply {
                return Ok(completed.clone());
            }
            return Ok(Cleanup {
                keys: vec![],
                applied: false,
                counters: Counters::default(),
                verified_retained_bytes: self.quiesced.retained_bytes,
                assumed_retention_ms: self.quiesced.rates.retention_ms,
                post_cleanup_lifecycle_proxy_nano_usd: None,
            });
        }
        let mut result = Cleanup {
            keys: self.quiesced.keys.clone(),
            applied: apply,
            counters: Counters::default(),
            verified_retained_bytes: self.quiesced.retained_bytes,
            assumed_retention_ms: self.quiesced.rates.retention_ms,
            post_cleanup_lifecycle_proxy_nano_usd: None,
        };
        if apply {
            let limits = &self.quiesced.limits;
            require(
                result.keys.len() as u64 <= limits.requests,
                "remote GC request cap before deletion",
            )?;
            let mut client = Client::new(
                &self.quiesced.store,
                &self.quiesced.context,
                limits,
                CancellationToken::default(),
                Instant::now() + Duration::from_millis(limits.timeout_ms),
            )?;
            for (index, key) in result.keys.iter().enumerate() {
                if client.delete(key).is_err() {
                    let unattempted = result.keys.len() - index - 1;
                    return Err(Error::Remote {
                        code: "cleanup_incomplete".into(),
                        message: format!(
                            "exact-key cleanup incomplete: successful deletes={index}, failed delete attempts=1, unattempted keys={unattempted}; lifecycle estimate unavailable"
                        ),
                    });
                }
            }
            result.counters = client.counters;
            let mut lifecycle_counters = self.quiesced.execution_counters.clone();
            lifecycle_counters.add(&result.counters)?;
            result.post_cleanup_lifecycle_proxy_nano_usd =
                Some(self.quiesced.rates.estimate_lifecycle_nano_usd(
                    &lifecycle_counters,
                    self.quiesced.retained_bytes,
                )?);
            self.quiesced.completed_cleanup = Some(result.clone());
        }
        Ok(result)
    }
}
pub enum Event<'a> {
    Admitted {
        context: &'a Context,
    },
    ProducersStarted {
        context: &'a Context,
        boots: &'a [Boot],
    },
    ProducersExited {
        context: &'a Context,
        boots: &'a [Boot],
    },
    ManifestCommitted {
        context: &'a Context,
        manifest: &'a Manifest,
        bytes: u64,
        hash: &'a str,
    },
    ReadersStarted {
        context: &'a Context,
        boots: &'a [Boot],
    },
}
fn ns(start: Instant) -> Result<u64> {
    u64::try_from(start.elapsed().as_nanos())
        .map_err(|_| Error::Invalid("remote observation overflow".into()))
}
pub(super) fn private_root(root: &Path) -> Result<()> {
    require(
        root.is_absolute()
            && root.canonicalize()? == root
            && fs::metadata(root)?.is_dir()
            && fs::metadata(root)?.permissions().mode() & 0o777 == 0o700,
        "remote scratch must be private canonical directory",
    )?;
    for p in root.ancestors() {
        require(
            !fs::symlink_metadata(p)?.file_type().is_symlink(),
            "remote scratch symlink",
        )?;
    }
    Ok(())
}
fn check(token: &CancellationToken, until: Instant) -> Result<()> {
    token.check()?;
    require(Instant::now() < until, "remote coordinator deadline")
}
struct Actor {
    child: Child,
    events: mpsc::Receiver<Result<Reply>>,
    reader: Option<thread::JoinHandle<()>>,
    writer: Option<thread::JoinHandle<()>>,
    status: Option<std::process::ExitStatus>,
    boot: Option<Boot>,
}
impl Actor {
    fn child_crash() -> Error {
        Error::Remote {
            code: FailureClass::ChildCrash.code().into(),
            message: FailureClass::ChildCrash.message().into(),
        }
    }
    fn spawn(run: &Run) -> Result<Self> {
        let mut child = Command::new(&run.worker_binary)
            .arg("--remote-stage")
            .arg(&run.scratch_parent)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()?;
        let mut output = child
            .stdout
            .take()
            .ok_or_else(|| Error::Invalid("remote worker pipe".into()))?;
        let (tx, events) = mpsc::sync_channel(2);
        let reader = thread::spawn(move || {
            let first = wire::read(&mut output);
            let healthy = matches!(&first, Ok(Reply::Ready(_)));
            if tx.send(first).is_err() || !healthy {
                return;
            }
            let done = wire::read(&mut output).and_then(|reply| {
                let mut tail = [0];
                require(output.read(&mut tail)? == 0, "remote worker extra reply")?;
                Ok(reply)
            });
            let _ = tx.send(done);
        });
        Ok(Self {
            child,
            events,
            reader: Some(reader),
            writer: None,
            status: None,
            boot: None,
        })
    }
    fn event(&mut self, token: &CancellationToken, until: Instant) -> Result<Reply> {
        loop {
            check(token, until)?;
            match self.events.recv_timeout(Duration::from_millis(5)) {
                Ok(Ok(reply)) => return Ok(reply),
                Ok(Err(error)) => {
                    if self.status.is_some_and(|status| !status.success()) {
                        return Err(Self::child_crash());
                    }
                    return Err(error);
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(_) => {
                    if self.status.is_some_and(|status| !status.success()) {
                        return Err(Self::child_crash());
                    }
                    return Err(Error::Invalid("remote worker reply lost".into()));
                }
            }
        }
    }
    fn ready(&mut self, token: &CancellationToken, until: Instant) -> Result<Boot> {
        let boot = match self.event(token, until)? {
            Reply::Ready(boot) => boot,
            Reply::Refused { class } => {
                return Err(Error::Remote {
                    code: class.code().into(),
                    message: class.message().into(),
                });
            }
            Reply::Done(_) => return Err(Error::Invalid("remote ready expected".into())),
        };
        boot.validate()?;
        require(boot.pid == self.child.id(), "remote PID fence")?;
        self.boot = Some(boot.clone());
        Ok(boot)
    }
    fn grant(&mut self, grant: &Grant, token: &CancellationToken, until: Instant) -> Result<()> {
        let mut pipe = self
            .child
            .stdin
            .take()
            .ok_or_else(|| Error::Invalid("remote grant already used".into()))?;
        let grant = grant.clone();
        let (tx, rx) = mpsc::sync_channel(1);
        self.writer = Some(thread::spawn(move || {
            let _ = tx.send(wire::write(&mut pipe, &grant));
        }));
        loop {
            check(token, until)?;
            match rx.recv_timeout(Duration::from_millis(5)) {
                Ok(result) => {
                    result?;
                    break;
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(_) => return Err(Error::Invalid("remote grant pipe lost".into())),
            }
        }
        if let Some(writer) = self.writer.take() {
            writer
                .join()
                .map_err(|_| Error::Invalid("remote grant writer panicked".into()))?;
        }
        Ok(())
    }
    fn done(
        &mut self,
        context: &Context,
        token: &CancellationToken,
        until: Instant,
    ) -> Result<Done> {
        let done = match self.event(token, until)? {
            Reply::Done(done) => {
                if !self.status.is_some_and(|status| status.success()) {
                    return Err(Self::child_crash());
                }
                done
            }
            Reply::Refused { class } => {
                return Err(Error::Remote {
                    code: class.code().into(),
                    message: class.message().into(),
                });
            }
            Reply::Ready(_) => return Err(Error::Invalid("remote done expected".into())),
        };
        require(
            self.boot.as_ref() == Some(&done.boot)
                && &done.context == context
                && !done.boot.scratch.try_exists()?,
            "remote done boot/context/scratch fence",
        )?;
        if let Some(reader) = self.reader.take() {
            reader
                .join()
                .map_err(|_| Error::Invalid("remote pipe reader panicked".into()))?;
        }
        Ok(*done)
    }
}
impl Drop for Actor {
    fn drop(&mut self) {
        if self.status.is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        self.child.stdin.take();
        if let Some(writer) = self.writer.take() {
            let _ = writer.join();
        }
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
    }
}
fn reap(actors: &mut [Actor], token: &CancellationToken, until: Instant) -> Result<()> {
    loop {
        check(token, until)?;
        let mut remaining = false;
        for a in &mut *actors {
            if a.status.is_none() {
                if let Some(status) = a.child.try_wait()? {
                    a.status = Some(status);
                } else {
                    remaining = true;
                }
            }
        }
        if !remaining {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(2));
    }
}
fn start(
    run: &Run,
    context: &Context,
    roles: Vec<Role>,
    token: &CancellationToken,
    until: Instant,
) -> Result<(Vec<Actor>, Vec<Boot>)> {
    let mut actors = Vec::new();
    for _ in &roles {
        check(token, until)?;
        actors.push(Actor::spawn(run)?);
    }
    let mut boots = Vec::new();
    for (actor, role) in actors.iter_mut().zip(roles) {
        let boot = actor.ready(token, until)?;
        require(
            boot.scratch.starts_with(&run.scratch_parent)
                && !boots.iter().any(|b: &Boot| b.instance == boot.instance),
            "remote private fresh boot",
        )?;
        actor.grant(
            &Grant {
                version: wire::WIRE_VERSION,
                boot: boot.instance.clone(),
                context: context.clone(),
                job: run.job.clone(),
                role,
            },
            token,
            until,
        )?;
        boots.push(boot);
    }
    Ok((actors, boots))
}
/// Execute real native static aggregation through the remote provider and fresh worker stages.
/// # Errors
/// Fails closed on source/data/fence/worker/store/resource errors, returning no batches.
pub fn execute(run: &Run, token: CancellationToken) -> Result<Output> {
    execute_observed(run, token, |_| Ok(()))
}
/// Same lifecycle with bounded synchronous stage observations for supervision/fault witnesses.
/// # Errors
/// Observer errors cancel the run; observers cannot bypass terminal/gather checks.
#[allow(
    clippy::too_many_lines,
    clippy::needless_pass_by_value,
    reason = "One owned cancellation token spans both stage cohorts, terminal promotion and gather commitment."
)]
pub fn execute_observed(
    run: &Run,
    token: CancellationToken,
    mut observe: impl FnMut(Event<'_>) -> Result<()>,
) -> Result<Output> {
    let started = Instant::now();
    run.job.limits.validate()?;
    let until = started + Duration::from_millis(run.job.limits.timeout_ms);
    run.job.validate()?;
    private_root(&run.scratch_parent)?;
    require(
        run.worker_binary.is_absolute()
            && fs::metadata(&run.worker_binary)?.is_file()
            && fs::metadata(&run.worker_binary)?.nlink() == 1,
        "remote worker executable identity",
    )?;
    check(&token, until)?;
    let context = Context::new(&run.job)?;
    observe(Event::Admitted { context: &context })?;
    run.job.verify(&token, until, true)?;
    let mut client = Client::new(
        &run.job.store,
        &context,
        &run.job.limits,
        token.clone(),
        until,
    )?;
    client.probe()?;
    let scratch = tempfile::Builder::new()
        .prefix("remote-coordinator-")
        .tempdir_in(&run.scratch_parent)?;
    let production = Instant::now();
    let (mut producers, producer_boots) = start(
        run,
        &context,
        (0..run.job.workers)
            .map(|worker| Role::Produce { worker })
            .collect(),
        &token,
        until,
    )?;
    observe(Event::ProducersStarted {
        context: &context,
        boots: &producer_boots,
    })?;
    reap(&mut producers, &token, until)?;
    let mut terminals = Vec::new();
    let mut counters = Counters::default();
    for actor in &mut producers {
        let done = actor.done(&context, &token, until)?;
        require(
            done.finals.is_empty() && done.terminals.len() == context.workers,
            "remote producer reply coverage",
        )?;
        counters.add(&done.counters)?;
        terminals.extend(done.terminals);
    }
    drop(producers); // Every producer process has exited and scratch is gone before handoff.
    observe(Event::ProducersExited {
        context: &context,
        boots: &producer_boots,
    })?;
    run.job.verify(&token, until, false)?;
    let manifest = Manifest {
        context: context.clone(),
        terminals,
    };
    manifest.validate(&context)?;
    // Complete terminal set and aggregate producer budgets precede global eligibility.
    let all_terms = load_terminals(&mut client, &context, &run.job.limits, &manifest)?;
    let data = json(&manifest)?;
    let manifest_hash = blake3::hash(&data).to_hex().to_string();
    let manifest_bytes = data.len() as u64;
    client.put(&client.key("MANIFEST.json"), data)?;
    let production_ns = ns(production)?;
    observe(Event::ManifestCommitted {
        context: &context,
        manifest: &manifest,
        bytes: manifest_bytes,
        hash: &manifest_hash,
    })?;
    check(&token, until)?;
    let disk = Arc::new(crate::disk::Budget::new(64 * 1024 * 1024));
    let mut gather = Gather::with_budget(
        scratch.path(),
        context.job,
        context.workers,
        (0..context.workers as u64).collect(),
        None,
        Some(&disk),
    )?;
    let consumption = Instant::now();
    let (mut readers, reader_boots) = start(
        run,
        &context,
        (0..run.job.readers)
            .map(|worker| Role::Read {
                worker,
                width: run.job.readers,
                epoch: 1,
                manifest_bytes,
                manifest_hash: manifest_hash.clone(),
                gather: gather.address(),
            })
            .collect(),
        &token,
        until,
    )?;
    require(
        reader_boots.iter().all(|r| {
            producer_boots
                .iter()
                .all(|p| r.instance != p.instance && r.scratch != p.scratch)
        }),
        "remote reader must have fresh boot and scratch",
    )?;
    observe(Event::ReadersStarted {
        context: &context,
        boots: &reader_boots,
    })?;
    reap(&mut readers, &token, until)?;
    for (w, actor) in readers.iter_mut().enumerate() {
        let done = actor.done(&context, &token, until)?;
        require(
            done.terminals.is_empty()
                && done.finals.len() == (w..context.workers).step_by(run.job.readers).count(),
            "remote reader terminal coverage",
        )?;
        counters.add(&done.counters)?;
        for (f, p) in done
            .finals
            .iter()
            .zip((w..context.workers).step_by(run.job.readers))
        {
            let expected_rows = all_terms
                .iter()
                .filter(|t| t.partition == p)
                .try_fold(0, |a, t| add(a, t.terminal.rows))?;
            require(
                f.partition == p && f.input_rows == expected_rows,
                "remote final input conservation",
            )?;
            gather.verify_terminal(p as u64, &f.terminal)?;
            gather.commit(p as u64, AttemptId(0), &f.sent)?;
        }
    }
    drop(readers);
    let (batches, _) = gather.collect_bounded(
        OutputLimits {
            rows: run.job.plan.max_output_rows,
            bytes: run.job.plan.max_output_bytes,
        },
        |b| crate::physical::adapt(&run.job.plan, 1, &b),
    )?;
    let mut count = 0;
    for batch in &batches {
        let counts = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| Error::Invalid("remote result count schema".into()))?;
        for value in counts.values() {
            count = add(
                count,
                u64::try_from(*value)
                    .map_err(|_| Error::Invalid("remote negative count".into()))?,
            )?;
        }
    }
    let input_rows = run.job.plan.sources[0]
        .files
        .iter()
        .try_fold(0, |a, f| add(a, f.rows))?;
    require(
        count == input_rows,
        "remote whole-source/final count conservation",
    )?;
    run.job.verify(&token, until, false)?;
    check(&token, until)?;
    let consumption_ns = ns(consumption)?;
    let mut keys = vec![client.key("MANIFEST.json")];
    let mut retained_bytes = manifest_bytes;
    retained_bytes = add(
        retained_bytes,
        manifest
            .terminals
            .iter()
            .try_fold(0, |a, terminal| add(a, terminal.bytes))?,
    )?;
    for t in &all_terms {
        keys.push(client.key(&terminal_key(t.partition, t.producer)));
        for s in &t.segments {
            keys.push(client.key(&segment_key(t.partition, t.producer, s.sequence)));
            retained_bytes = add(retained_bytes, s.bytes)?;
        }
    }
    counters.add(&client.counters)?;
    let estimated_nano_usd = run.job.rates.estimate_nano_usd(&counters)?;
    let output = Output {
        batches,
        receipt: Receipt {
            version: VERSION,
            handoff_epoch: 1,
            context: context.clone(),
            effective: run.job.clone(),
            producers: producer_boots,
            readers: reader_boots,
            manifest_bytes,
            manifest_hash,
            counters: counters.clone(),
            production_ns,
            consumption_ns,
            wall_ns: ns(started)?,
            rate_provenance: "CONFIGURED".into(),
            observation_provenance: "MEASURED".into(),
            estimated_nano_usd,
        },
        quiesced: Quiesced {
            context: context.clone(),
            store: run.job.store.clone(),
            limits: run.job.limits.clone(),
            keys,
            rates: run.job.rates.clone(),
            execution_counters: counters.clone(),
            retained_bytes,
            completed_cleanup: None,
        },
    };
    verify_receipt(&output.receipt)?;
    check(&token, until)?;
    Ok(output)
}

/// Offline structural/identity/cost verification, not provider billing or signed attestation.
/// # Errors
/// Refuses altered configuration, source/plan/boot provenance, counters or cost arithmetic.
pub fn verify_receipt(receipt: &Receipt) -> Result<()> {
    let r = receipt;
    r.effective.validate()?;
    r.context.validate()?;
    require(
        r.version == VERSION
            && r.handoff_epoch == 1
            && r.context.job == r.effective.job
            && r.context.workers == r.effective.workers
            && r.context.plan
                == r.effective
                    .plan
                    .digest()
                    .map_err(|e| Error::Invalid(e.to_string()))?
            && r.context.source == super::digest(&r.effective.plan.sources)?
            && r.context.config == super::digest(&r.effective)?
            && r.context.store == r.effective.store.digest()?
            && super::hash(&r.manifest_hash)
            && (1..=super::MAX_JSON as u64).contains(&r.manifest_bytes)
            && r.producers.len() == r.effective.workers
            && r.readers.len() == r.effective.readers
            && r.rate_provenance == "CONFIGURED"
            && r.observation_provenance == "MEASURED"
            && r.estimated_nano_usd == r.effective.rates.estimate_nano_usd(&r.counters)?
            && add(r.production_ns, r.consumption_ns)? <= r.wall_ns,
        "remote receipt configuration/identity/cost",
    )?;
    let mut boots = std::collections::BTreeSet::new();
    let mut scratch = std::collections::BTreeSet::new();
    for boot in r.producers.iter().chain(&r.readers) {
        boot.validate()?;
        require(
            boots.insert(&boot.instance) && scratch.insert(&boot.scratch),
            "remote receipt fresh boot/scratch",
        )?;
    }
    let requests = add(
        add(r.counters.put, r.counters.head)?,
        add(r.counters.get, r.counters.delete)?,
    )?;
    require(
        requests == r.counters.http_requests
            && r.counters.http_request_body_bytes >= r.counters.write_bytes
            && r.counters.http_response_body_bytes >= r.counters.read_bytes,
        "remote receipt actual HTTP counters",
    )
}
