//! Acceptance-only barrier exercising the public recovery→connect API race.
#[cfg(feature = "remote-checkpoint")]
fn main() -> ade_stream::Result<()> {
    use std::io::{Read, Write};
    let args: Vec<String> = std::env::args().skip(1).collect();
    ade_stream::require(args.len() == 2, "CONFIG POLICY")?;
    let config = ade_stream::state::Config::parse(
        &std::fs::read(&args[0]).map_err(|_| ade_stream::Error::State("config"))?,
    )?;
    let policy = ade_stream::adaptive::Policy::parse(
        &std::fs::read(&args[1]).map_err(|_| ade_stream::Error::State("policy"))?,
    )?;
    let session = uuid::Uuid::new_v4();
    let (mut engine, _) = ade_stream::Engine::adaptive_stateful(
        config.clone(),
        policy,
        session,
        ade_exec::CancellationToken::default(),
        true,
        ade_stream::adaptive::policy::Placement::Local,
        None,
    )?;
    let sources = engine.adaptive_recovery_preflight()?;
    engine.finish_adaptive_recovery(sources)?;
    println!("PREFLIGHT_COMPLETE");
    std::io::stdout()
        .flush()
        .map_err(|_| ade_stream::Error::Sink)?;
    let mut byte = [0];
    std::io::stdin()
        .read_exact(&mut byte)
        .map_err(|_| ade_stream::Error::Sink)?;
    let mut source_config = config.source;
    for start in &mut source_config.starts {
        start.offset = engine.progress.partitions[&start.partition].next_offset_suggestion;
    }
    let _source =
        ade_stream::kafka::KafkaSource::connect(source_config, session, &engine.progress)?;
    Err(ade_stream::Error::State("race was not refused"))
}
#[cfg(not(feature = "remote-checkpoint"))]
fn main() {
    eprintln!("requires remote-checkpoint");
    std::process::exit(1);
}
