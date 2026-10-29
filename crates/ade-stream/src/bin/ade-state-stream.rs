//! Separate opt-in CLI; original ade-stream arguments/config remain unchanged.
use ade_stream::{
    Clock, Engine, Error, MonotonicClock, Result,
    kafka::KafkaSource,
    runtime::{Metrics, Nonblocking, Stop, run, trace},
    state::Config,
};
use std::{io::Read, os::fd::AsFd, process::ExitCode};
fn stderr_sink() -> Result<Nonblocking<std::fs::File>> {
    let fd = std::io::stderr()
        .as_fd()
        .try_clone_to_owned()
        .map_err(|_| Error::Sink)?;
    Nonblocking::new(std::fs::File::from(fd))
}
#[allow(
    clippy::too_many_lines,
    reason = "Explicit admission precedes side effects in one CLI transition"
)]
fn execute() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    ade_stream::require(
        !args.is_empty() && args.len() <= 8,
        "usage: ade-state-stream CONFIG [--restore] [--test-stop-records N | --test-stop-ms N] [--test-pause-at POINT]",
    )?;
    let fd = rustix::fs::open(
        args[0].as_str(),
        rustix::fs::OFlags::RDONLY
            | rustix::fs::OFlags::NONBLOCK
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )
    .map_err(|_| Error::Admission("stateful config read"))?;
    let file = std::fs::File::from(fd);
    ade_stream::require(
        file.metadata()
            .map_err(|_| Error::Admission("config metadata"))?
            .is_file(),
        "regular config",
    )?;
    let mut bytes = Vec::new();
    file.take(16385)
        .read_to_end(&mut bytes)
        .map_err(|_| Error::Admission("config read"))?;
    let mut config = Config::parse(&bytes)?;
    let mut restore = false;
    let mut stop = Stop::default();
    let mut pause = None;
    let mut index = 1;
    while index < args.len() {
        match args[index].as_str() {
            "--restore" => {
                ade_stream::require(!restore, "duplicate restore")?;
                restore = true;
                index += 1;
            }
            "--test-stop-records" | "--test-stop-ms" => {
                let count = args
                    .get(index + 1)
                    .and_then(|v| v.parse::<u64>().ok())
                    .ok_or(Error::Admission("positive stop"))?;
                ade_stream::require(count > 0, "positive stop")?;
                if args[index] == "--test-stop-records" {
                    stop.after_records = Some(count);
                } else {
                    stop.after_ms = Some(count);
                }
                index += 2;
            }
            "--test-pause-at" => {
                pause = Some(
                    args.get(index + 1)
                        .ok_or(Error::Admission("pause point"))?
                        .clone(),
                );
                index += 2;
            }
            _ => return Err(Error::Admission("stateful CLI flag")),
        }
    }
    let token = ade_exec::CancellationToken::default();
    let signal = token.clone();
    ctrlc::set_handler(move || signal.cancel()).map_err(|_| Error::Admission("signal handler"))?;
    let clock = MonotonicClock::default();
    let session = uuid::Uuid::new_v4();
    let mut engine = Engine::stateful(config.clone(), session, token.clone(), restore)?;
    if restore {
        for start in &mut config.source.starts {
            start.offset = engine.progress.partitions[&start.partition].next_offset_suggestion;
        }
    }
    engine.state.as_mut().expect("stateful").test_pause_at = pause;
    let mut diagnostics = stderr_sink()?;
    let mut source = KafkaSource::connect(config.source.clone(), session, &engine.progress)?;
    ade_stream::require(
        source.partition_inventory() == config.topic_partitions,
        "topic inventory differs",
    )?;
    source.enable_checkpoint_control()?;
    token.check().map_err(|_| Error::Cancelled)?;
    trace(
        &mut diagnostics,
        &serde_json::json!({"type":"admitted","version":1,"pid":std::process::id(),"session":session,"logical_fingerprint":config.fingerprint()?,"restore":restore,"generation":engine.state.as_ref().expect("state").generation,"progress":engine.progress,"owners":config.owners,"client":rdkafka::util::get_rdkafka_version()}),
    )?;
    let fd = std::io::stdout()
        .as_fd()
        .try_clone_to_owned()
        .map_err(|_| Error::Sink)?;
    let mut sink = Nonblocking::new(std::fs::File::from(fd))?;
    let mut metrics = Metrics::default();
    let mut result = run(
        &mut source,
        &mut engine,
        &mut sink,
        &mut diagnostics,
        &clock,
        &token,
        &stop,
        config.source.sink_stall_ms,
        &mut metrics,
    );
    let pending = engine.has_unconfirmed_input();
    engine.cancel();
    if let Err(e) = source.shutdown() {
        result = Err(e);
    }
    trace(
        &mut diagnostics,
        &serde_json::json!({"type":"terminal","status":if result.is_ok(){"STOPPED"}else{"FAILED"},"reason":result.as_ref().map_or_else(ToString::to_string,|s|(*s).into()),"failure_class":result.as_ref().err().map(Error::class),"progress":engine.progress,"metrics":metrics,"state":engine.state.as_ref().expect("state").store.metrics,"closed_window_late":engine.state.as_ref().expect("state").closed_window_late,"generation":engine.state.as_ref().expect("state").generation,"pending_input":pending,"elapsed_ms":clock.now_ms(),"broker":source.metrics(&engine.progress)}),
    )?;
    result.map(|_| ())
}
fn main() -> ExitCode {
    match execute() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            if let Ok(mut sink) = stderr_sink() {
                let _ = trace(
                    &mut sink,
                    &serde_json::json!({"type":"failure","failure_class":e.class(),"reason":e.to_string()}),
                );
            }
            ExitCode::FAILURE
        }
    }
}
