use ade_stream::{
    Clock, Engine, MonotonicClock, StreamConfigV1,
    kafka::KafkaSource,
    runtime::{Metrics, Nonblocking, Stop, run, trace},
};
use std::{io::Read, os::fd::AsFd, process::ExitCode};
fn stderr_sink() -> ade_stream::Result<Nonblocking<std::fs::File>> {
    let fd = std::io::stderr()
        .as_fd()
        .try_clone_to_owned()
        .map_err(|_| ade_stream::Error::Sink)?;
    Nonblocking::new(std::fs::File::from(fd))
}
fn execute() -> ade_stream::Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    ade_stream::require(
        args.len() == 1 || args.len() == 3,
        "usage: ade-stream CONFIG [--test-stop-records N | --test-stop-ms N]",
    )?;
    let fd = rustix::fs::open(
        args[0].as_str(),
        rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::NONBLOCK | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )
    .map_err(|_| ade_stream::Error::Admission("config read"))?;
    let file = std::fs::File::from(fd);
    ade_stream::require(
        file.metadata()
            .map_err(|_| ade_stream::Error::Admission("config metadata"))?
            .is_file(),
        "config must be regular file",
    )?;
    let mut bytes = Vec::new();
    file.take(16385)
        .read_to_end(&mut bytes)
        .map_err(|_| ade_stream::Error::Admission("config read"))?;
    let config = StreamConfigV1::parse(&bytes)?;
    let mut stop = Stop::default();
    if args.len() == 3 {
        let n: u64 = args[2]
            .parse()
            .map_err(|_| ade_stream::Error::Admission("test stop positive integer"))?;
        ade_stream::require(n > 0, "positive test stop")?;
        match args[1].as_str() {
            "--test-stop-records" => stop.after_records = Some(n),
            "--test-stop-ms" => stop.after_ms = Some(n),
            _ => return Err(ade_stream::Error::Admission("test stop flag")),
        }
    }
    let token = ade_exec::CancellationToken::default();
    let signal = token.clone();
    ctrlc::set_handler(move || signal.cancel())
        .map_err(|_| ade_stream::Error::Admission("signal handler"))?;
    let clock = MonotonicClock::default();
    let session = uuid::Uuid::new_v4();
    let mut engine = Engine::new(config.clone(), session)?;
    let mut diagnostics = stderr_sink()?;
    let mut source = KafkaSource::connect(config.clone(), session, &engine.progress)?;
    token.check().map_err(|_| ade_stream::Error::Cancelled)?;
    trace(
        &mut diagnostics,
        &serde_json::json!({"type":"admitted", "version":1, "pid":std::process::id(),
        "session":session, "config_hash":config.identity()?, "cluster_id":config.expected_cluster_id,
        "topic":config.topic, "starts":config.starts, "policy":"max_forwarded_minus_allowance_v1",
        "allowance_ms":config.allowance_ms, "client":rdkafka::util::get_rdkafka_version(), "progress":engine.progress}),
    )?;
    let fd = std::io::stdout()
        .as_fd()
        .try_clone_to_owned()
        .map_err(|_| ade_stream::Error::Sink)?;
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
        config.sink_stall_ms,
        &mut metrics,
    );
    let discarded_in_flight = engine.has_unconfirmed_input();
    engine.cancel();
    if let Err(error) = source.shutdown() {
        result = Err(error);
    }
    let reason = result
        .as_ref()
        .map_or_else(ToString::to_string, |s| (*s).to_string());
    trace(
        &mut diagnostics,
        &serde_json::json!({"type":"terminal", "status":if result.is_ok() {"STOPPED"} else {"FAILED"},
        "reason":reason, "failure_class":result.as_ref().err().map(ade_stream::Error::class), "elapsed_ms":clock.now_ms(), "progress":engine.progress, "metrics":metrics,
        "broker_metrics":source.metrics(&engine.progress), "decoded_peak_bytes":engine.decoded_peak_bytes,
        "arrow_peak_bytes":engine.arrow_peak_bytes, "managed_in_flight_peak_bytes":engine.managed_in_flight_peak_bytes, "offset_suggestions_durable":false, "discarded_in_flight":discarded_in_flight}),
    )?;
    result.map(|_| ())
}
fn main() -> ExitCode {
    match execute() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            // Failure reporting must not turn a blocked stderr into an unbounded wait.
            if let Ok(mut diagnostic) = stderr_sink() {
                let _ = trace(
                    &mut diagnostic,
                    &serde_json::json!({"type":"failure", "failure_class":e.class(), "reason":e.to_string()}),
                );
            }
            ExitCode::FAILURE
        }
    }
}
