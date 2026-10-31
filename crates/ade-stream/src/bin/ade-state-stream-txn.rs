//! Separate v3 operator surface. Stdout is only the explicit committed-output reader.
use ade_exec::CancellationToken;
use ade_stream::{
    Error, Poll, Result, Source,
    kafka::KafkaSource,
    runtime::{self, Limited, Nonblocking},
    state::store,
    transactional::{Config, Transaction, epoch::Operation, savepoint, sink},
};
use std::{
    path::Path,
    thread,
    time::{Duration, Instant},
};

fn number(s: &str) -> Result<u64> {
    s.parse().map_err(|_| Error::Admission("positive integer"))
}
fn main() {
    let status = std::cell::Cell::new("REFUSED");
    if let Err(e) = execute(&status) {
        eprintln!(
            "{{\"type\":\"failure\",\"status\":\"{}\",\"reason\":{}}}",
            status.get(),
            serde_json::to_string(&e.to_string()).unwrap_or_default()
        );
        std::process::exit(1);
    }
}
#[allow(
    clippy::too_many_lines,
    reason = "Strict independent versioned operator CLI"
)]
fn execute(status: &std::cell::Cell<&str>) -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [config_path, command, rest @ ..] = args.as_slice() else {
        return Err(Error::Admission(
            "CONFIG3 run|resume|read|export|import|transition; physical rescale refused",
        ));
    };
    let config = Config::parse(&store_bytes(Path::new(config_path), 32768)?)?;
    let diagnostics = std::cell::RefCell::new(Limited {
        inner: Nonblocking::new(std::io::stderr())?,
        remaining: config.limits.diagnostic_bytes,
    });
    let token = CancellationToken::default();
    let signal = token.clone();
    ctrlc::set_handler(move || signal.cancel()).map_err(|_| Error::Admission("signal handler"))?;
    let mut pause_at = None;
    let mut fail_at = None;
    let mut records = None;
    let mut pause_ms = None;
    let mut op_args = Vec::new();
    let mut i = 0;
    while i < rest.len() {
        match rest[i].as_str() {
            "--pause-after-records" | "--pause-after-ms" | "--test-pause-at" | "--test-fail-at" => {
                let value = rest.get(i + 1).ok_or(Error::Admission("option value"))?;
                match rest[i].as_str() {
                    "--pause-after-records" => {
                        ade_stream::require(records.is_none(), "duplicate option")?;
                        records = Some(number(value)?);
                    }
                    "--pause-after-ms" => {
                        ade_stream::require(pause_ms.is_none(), "duplicate option")?;
                        pause_ms = Some(number(value)?);
                    }
                    "--test-pause-at" => {
                        ade_stream::require(pause_at.is_none(), "duplicate option")?;
                        pause_at = Some(value.clone());
                    }
                    _ => {
                        ade_stream::require(fail_at.is_none(), "duplicate option")?;
                        fail_at = Some(value.clone());
                    }
                }
                i += 2;
            }
            value if value.starts_with("--") => {
                return Err(Error::Admission(
                    "unsupported flag/physical rescale refused",
                ));
            }
            value => {
                op_args.push(value.to_string());
                i += 1;
            }
        }
    }
    ade_stream::require(
        ["run", "resume"].contains(&command.as_str()) || (records.is_none() && pause_ms.is_none()),
        "pause options require run/resume",
    )?;
    let points = [
        "source_assigned",
        "before_stage",
        "stage_created",
        "before_output_write",
        "output_half_written",
        "before_output_sync",
        "output_synced",
        "state_written",
        "state_synced",
        "before_manifest",
        "manifest_synced",
        "epoch_durable",
        "epoch_verified",
        "before_selector",
        "selector_half_written",
        "selector_synced",
        "selector_renamed",
        "selector_durable",
        "before_telemetry",
        "export_created",
        "export_object",
        "export_synced",
        "export_durable",
        "export_before_publish",
        "import_object",
        "import_before_selector",
        "import_durable",
    ];
    ade_stream::require(
        pause_at.as_deref().is_none_or(|p| points.contains(&p))
            && fail_at.as_deref().is_none_or(|p| points.contains(&p)),
        "unknown fault point",
    )?;
    let mut hook = |point: &str| -> Result<()> {
        if ["selector_renamed", "import_durable", "export_durable"].contains(&point) {
            status.set("UNKNOWN_AFTER_COMMIT");
        }
        runtime::trace(
            &mut *diagnostics.borrow_mut(),
            &serde_json::json!({"type":"txn_step","point":point}),
        )?;
        if fail_at.as_deref() == Some(point) {
            return Err(Error::State("injected transaction failure"));
        }
        if pause_at.as_deref() == Some(point) {
            while token.check().is_ok() {
                thread::sleep(Duration::from_millis(10));
            }
            return Err(Error::Cancelled);
        }
        Ok(())
    };
    match command.as_str() {
        "read" => {
            ade_stream::require(rest.is_empty(), "read takes no options")?;
            let records = sink::read(&config)?;
            let mut out = Limited {
                inner: Nonblocking::new(std::io::stdout())?,
                remaining: config.limits.history_bytes,
            };
            for r in records {
                runtime::trace(&mut out, &r)?;
            }
        }
        "export" => {
            let [parent, name] = op_args.as_slice() else {
                return Err(Error::Admission("export PARENT NAME"));
            };
            let (path, digest) = savepoint::export(&config, Path::new(parent), name, &mut hook)?;
            runtime::trace(
                &mut *diagnostics.borrow_mut(),
                &serde_json::json!({"type":"operation","status":"EXPORTED","path":path,"blake3":digest}),
            )?;
        }
        "import" => {
            let [path, digest, retired_head] = op_args.as_slice() else {
                return Err(Error::Admission(
                    "import SAVEPOINT EXACT_DIGEST RETIRED_PREDECESSOR_HEAD_BLAKE3",
                ));
            };
            let pointer =
                savepoint::import(&config, Path::new(path), digest, retired_head, &mut hook)?;
            runtime::trace(
                &mut *diagnostics.borrow_mut(),
                &serde_json::json!({"type":"operation","status":"IMPORTED","pointer":pointer}),
            )?;
        }
        "transition" => {
            let [codec, owners] = op_args.as_slice() else {
                return Err(Error::Admission("transition CODEC OWNERS; logical only"));
            };
            let pointer = Transaction::transition(
                config.clone(),
                u32::try_from(number(codec)?).map_err(|_| Error::Admission("codec"))?,
                usize::try_from(number(owners)?).map_err(|_| Error::Admission("owners"))?,
                &mut hook,
            )?;
            runtime::trace(
                &mut *diagnostics.borrow_mut(),
                &serde_json::json!({"type":"operation","status":"TRANSITIONED","pointer":pointer}),
            )?;
        }
        "run" | "resume" => {
            ade_stream::require(
                op_args.is_empty()
                    && records.is_none_or(|n| n > 0)
                    && pause_ms.is_none_or(|n| n > 0),
                "run options",
            )?;
            let mut txn = Transaction::open(
                config.clone(),
                command == "resume",
                CancellationToken::default(),
            )?;
            status.set("ABORTED");
            // Explicit saved vector is checked by connect before assignment, no fallback.
            let mut source_config = config.state.source.clone();
            for start in &mut source_config.starts {
                start.offset =
                    txn.engine.progress.partitions[&start.partition].next_offset_suggestion;
            }
            let mut source = KafkaSource::connect(
                source_config,
                txn.engine.progress.session,
                &txn.engine.progress,
            )?;
            ade_stream::require(
                source.partition_inventory() == config.state.topic_partitions,
                "source inventory",
            )?;
            source.enable_checkpoint_control()?;
            hook("source_assigned")?;
            runtime::trace(
                &mut *diagnostics.borrow_mut(),
                &serde_json::json!({"type":"admitted","session":txn.engine.progress.session,"progress":txn.engine.progress}),
            )?;
            let clock = Instant::now();
            let mut last = txn.engine.progress.forwarded_records;
            let mut last_cut = Instant::now();
            loop {
                let pause = token.check().is_err()
                    || records.is_some_and(|n| txn.engine.progress.forwarded_records >= n)
                    || pause_ms.is_some_and(|n| clock.elapsed().as_millis() >= u128::from(n));
                let due = txn.engine.progress.forwarded_records > last
                    && (txn.engine.progress.forwarded_records - last
                        >= config.state.checkpoint_every_records
                        || last_cut.elapsed().as_millis()
                            >= u128::from(config.state.checkpoint_every_ms));
                if due || pause {
                    source.pause()?;
                    source.verify(&txn.engine.progress)?;
                    let commit_started = Instant::now();
                    let pointer = txn.commit(
                        if pause {
                            Operation::Paused
                        } else {
                            Operation::Commit
                        },
                        &mut hook,
                    )?;
                    runtime::trace(
                        &mut *diagnostics.borrow_mut(),
                        &serde_json::json!({"type":"committed","pointer":pointer,"progress":txn.engine.progress,"elapsed_ns":commit_started.elapsed().as_nanos(),"client":source.snapshot(&txn.engine.progress)}),
                    )?;
                    status.set("COMMITTED");
                    last = txn.engine.progress.forwarded_records;
                    last_cut = Instant::now();
                    if pause {
                        source.shutdown()?;
                        runtime::trace(
                            &mut *diagnostics.borrow_mut(),
                            &serde_json::json!({"type":"operation","status":"PAUSED","pointer":pointer,"progress":txn.engine.progress}),
                        )?;
                        return Ok(());
                    }
                    source.resume()?;
                    status.set("ABORTED");
                }
                match source.poll()? {
                    Poll::Data(record) => {
                        let partition = record.partition;
                        let offset = record.offset;
                        txn.accept(record)?;
                        runtime::trace(
                            &mut *diagnostics.borrow_mut(),
                            &serde_json::json!({"type":"input","sequence":txn.engine.progress.forwarded_records,"partition":partition,"offset":offset}),
                        )?;
                    }
                    Poll::Stopped => {
                        return Err(Error::Source("unexpected source stop; no clean pause"));
                    }
                    Poll::Pending => thread::sleep(Duration::from_millis(5)),
                }
            }
        }
        _ => return Err(Error::Admission("v3 command; physical rescale refused")),
    }
    Ok(())
}
fn store_bytes(p: &Path, cap: u64) -> Result<Vec<u8>> {
    // Config is external operator input, not necessarily inside a private state directory.
    use std::io::Read;
    let fd = rustix::fs::open(
        p,
        rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK,
        rustix::fs::Mode::empty(),
    )
    .map_err(|_| Error::Admission("config open"))?;
    let f = std::fs::File::from(fd);
    let m = store::io(f.metadata())?;
    ade_stream::require(m.is_file() && m.len() <= cap, "regular bounded config")?;
    let mut b = Vec::new();
    store::io(f.take(cap + 1).read_to_end(&mut b))?;
    ade_stream::require(b.len() as u64 <= cap, "config cap")?;
    Ok(b)
}
