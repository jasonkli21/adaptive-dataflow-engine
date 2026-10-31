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
    #[cfg(feature = "unified-planner")]
    let (args, unified) = {
        let mut a = args;
        let u = ade_stream::unified::take_args(&mut a)?;
        (a, u)
    };
    ade_stream::require(
        !args.is_empty() && args.len() <= 20,
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
    #[cfg(feature = "unified-planner")]
    if let Some(a) = &unified {
        a.validate_state(&config, false)?;
    }
    #[cfg(feature = "adaptive-checkpoint")]
    let mut policy_path = None;
    #[cfg(feature = "adaptive-checkpoint")]
    let mut recover = false;
    #[cfg(feature = "adaptive-checkpoint")]
    let mut copy = ade_stream::adaptive::policy::Placement::Local;
    #[cfg(feature = "adaptive-checkpoint")]
    let mut copy_seen = false;
    #[cfg(feature = "adaptive-checkpoint")]
    let mut certificate_path = None;
    #[cfg(feature = "adaptive-checkpoint")]
    let mut certificate_hash = None;
    let mut restore = false;
    let mut stop = Stop::default();
    let mut pause = None;
    let mut index = 1;
    while index < args.len() {
        match args[index].as_str() {
            #[cfg(feature = "adaptive-checkpoint")]
            "--adaptive-policy" | "--certificate" | "--certificate-blake3" => {
                let value = args
                    .get(index + 1)
                    .ok_or(Error::Admission("adaptive CLI value"))?
                    .clone();
                let target = match args[index].as_str() {
                    "--adaptive-policy" => &mut policy_path,
                    "--certificate" => &mut certificate_path,
                    _ => &mut certificate_hash,
                };
                ade_stream::require(target.is_none(), "duplicate adaptive option")?;
                *target = Some(value);
                index += 2;
            }
            #[cfg(feature = "adaptive-checkpoint")]
            "--recover" => {
                ade_stream::require(!recover, "duplicate recover")?;
                recover = true;
                index += 1;
            }
            #[cfg(feature = "adaptive-checkpoint")]
            "--restore-copy" => {
                ade_stream::require(!copy_seen, "duplicate restore copy")?;
                copy_seen = true;
                copy = match args.get(index + 1).map(String::as_str) {
                    Some("local") => ade_stream::adaptive::policy::Placement::Local,
                    Some("remote") => ade_stream::adaptive::policy::Placement::Remote,
                    _ => return Err(Error::Admission("explicit restore copy")),
                };
                index += 2;
            }
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
    #[cfg(feature = "adaptive-checkpoint")]
    let (mut engine, mut recovery) = if let Some(path) = policy_path {
        ade_stream::require(!restore, "v2 uses --recover, never v1 --restore")?;
        let policy = ade_stream::adaptive::Policy::parse(&read_adaptive_file(&path, 16384)?)?;
        let certificate = if let Some(path) = certificate_path {
            ade_stream::require(recover, "certificate requires recover")?;
            let bytes = read_adaptive_file(&path, 32768)?;
            let expected = certificate_hash.ok_or(Error::Admission(
                "certificate requires operator-retained BLAKE3",
            ))?;
            ade_stream::require(
                ade_stream::adaptive::snapshot::digest(&expected)
                    && ade_stream::adaptive::snapshot::hash(&bytes) == expected,
                "operator certificate BLAKE3",
            )?;
            let cert = ade_stream::adaptive::snapshot::json(&bytes)?;
            Some(ade_stream::adaptive::snapshot::Selector {
                version: 2,
                certificate: cert,
                certificate_blake3: expected,
            })
        } else {
            ade_stream::require(
                certificate_hash.is_none(),
                "certificate hash requires certificate",
            )?;
            None
        };
        Engine::adaptive_stateful(
            config.clone(),
            policy,
            session,
            token.clone(),
            recover,
            copy,
            certificate,
        )?
    } else {
        ade_stream::require(
            !recover
                && certificate_path.is_none()
                && certificate_hash.is_none()
                && copy == ade_stream::adaptive::policy::Placement::Local,
            "adaptive options require policy",
        )?;
        (
            Engine::stateful(config.clone(), session, token.clone(), restore)?,
            if restore { "legacy_restore" } else { "fresh" }.into(),
        )
    };
    #[cfg(feature = "adaptive-checkpoint")]
    if recovery == "recovery_pending" {
        let sources = engine.adaptive_recovery_preflight()?;
        recovery = engine.finish_adaptive_recovery(sources)?;
    }
    #[cfg(not(feature = "adaptive-checkpoint"))]
    let mut engine = Engine::stateful(config.clone(), session, token.clone(), restore)?;
    if restore || engine.progress.forwarded_records > 0 {
        for start in &mut config.source.starts {
            start.offset = engine.progress.partitions[&start.partition].next_offset_suggestion;
        }
    }
    engine.state.as_mut().expect("stateful").test_pause_at = pause;
    let diagnostics = stderr_sink()?;
    #[cfg(feature = "adaptive-checkpoint")]
    let mut diagnostics = ade_stream::runtime::Limited {
        inner: diagnostics,
        remaining: engine
            .state
            .as_ref()
            .and_then(|s| s.adaptive.as_ref())
            .map_or(u64::MAX, |a| a.policy.limits.trace_bytes),
    };
    #[cfg(not(feature = "adaptive-checkpoint"))]
    let mut diagnostics = diagnostics;
    #[cfg(feature = "adaptive-checkpoint")]
    let origin = engine
        .state
        .as_ref()
        .and_then(|s| s.adaptive.as_ref())
        .map(|a| a.policy.recovery.original_starts.clone());
    #[cfg(feature = "unified-planner")]
    if let Some(a) = &unified {
        trace(
            &mut diagnostics,
            &serde_json::json!({"type":"unified_decision","decision":a.decision()}),
        )?;
    }
    #[cfg(feature = "unified-planner")]
    let mut source =
        KafkaSource::connect_unified(config.source.clone(), session, &engine.progress, unified)?;
    #[cfg(not(feature = "unified-planner"))]
    let mut source = KafkaSource::connect(config.source.clone(), session, &engine.progress)?;
    ade_stream::require(
        source.partition_inventory() == config.topic_partitions,
        "topic inventory differs",
    )?;
    source.enable_checkpoint_control()?;
    #[cfg(feature = "adaptive-checkpoint")]
    if let Some(origin) = origin
        && recovery == "full_replay_explicit_conflict_consent"
    {
        source.verify_original_starts(&origin)?;
    }
    #[cfg(feature = "adaptive-checkpoint")]
    if engine.state.as_ref().is_some_and(|s| s.adaptive.is_some()) {
        trace(
            &mut diagnostics,
            &serde_json::json!({"type":"recovery_decision","version":1,"path":recovery,"session":session,"progress":engine.progress,"model":engine.state.as_ref().and_then(|s|s.adaptive.as_ref()).and_then(|a|a.recovery_decision.as_ref()),"estimated_inputs":engine.state.as_ref().and_then(|s|s.adaptive.as_ref()).map(|a|&a.policy.recovery),"certificate_blake3":engine.state.as_ref().and_then(|s|s.adaptive.as_ref()).and_then(|a|a.current_hash.as_ref())}),
        )?;
    }
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
    let pending_window = engine
        .state
        .as_ref()
        .is_some_and(ade_stream::state::State::has_pending_close);
    let partial_output =
        result.as_ref().err().and_then(Error::partial_output).map(
            |(written, total)| serde_json::json!({"written_bytes":written,"frame_bytes":total}),
        );
    engine.cancel();
    if let Err(e) = source.shutdown() {
        result = Err(e);
    }
    trace(
        &mut diagnostics,
        &serde_json::json!({"type":"terminal","status":if result.is_ok(){"STOPPED"}else{"FAILED"},"reason":result.as_ref().map_or_else(ToString::to_string,|s|(*s).into()),"failure_class":result.as_ref().err().map(Error::class),"progress":engine.progress,"metrics":metrics,"state":engine.state.as_ref().expect("state").store.metrics,"closed_window_late":engine.state.as_ref().expect("state").closed_window_late,"generation":engine.state.as_ref().expect("state").generation,"pending_input":pending,"pending_window_output":pending_window,"partial_output":partial_output,"elapsed_ms":clock.now_ms(),"broker":source.metrics(&engine.progress)}),
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
                    &serde_json::json!({"type":"failure","failure_class":e.class(),"reason":e.to_string(),"partial_output":e.partial_output().map(|(written,total)|serde_json::json!({"written_bytes":written,"frame_bytes":total}))}),
                );
            }
            ExitCode::FAILURE
        }
    }
}

#[cfg(feature = "adaptive-checkpoint")]
fn read_adaptive_file(path: &str, cap: u64) -> Result<Vec<u8>> {
    use std::os::unix::fs::MetadataExt;
    let fd = rustix::fs::open(
        path,
        rustix::fs::OFlags::RDONLY
            | rustix::fs::OFlags::NONBLOCK
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )
    .map_err(|_| Error::Admission("adaptive regular descriptor"))?;
    let file = std::fs::File::from(fd);
    let m = file
        .metadata()
        .map_err(|_| Error::Admission("descriptor metadata"))?;
    ade_stream::require(
        m.is_file() && m.nlink() == 1 && m.len() <= cap,
        "adaptive descriptor size/link/type",
    )?;
    let mut bytes = Vec::new();
    file.take(cap + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| Error::Admission("descriptor read"))?;
    ade_stream::require(bytes.len() as u64 <= cap, "descriptor cap")?;
    Ok(bytes)
}
