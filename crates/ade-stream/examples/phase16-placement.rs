//! Acceptance-only real-service probe: measured warm-up, then a policy placement change.
#[cfg(feature = "remote-checkpoint")]
#[allow(
    clippy::too_many_lines,
    reason = "Bounded standalone live-service policy experiment"
)]
fn main() -> ade_stream::Result<()> {
    use ade_stream::{
        Engine, Error, Poll, Source,
        adaptive::{Policy, policy::Placement},
        state::store,
    };
    let args: Vec<String> = std::env::args().skip(1).collect();
    ade_stream::require(args.len() == 3, "CONFIG POLICY ample|veto")?;
    let config = ade_stream::state::Config::parse(
        &std::fs::read(&args[0]).map_err(|_| Error::State("config"))?,
    )?;
    let policy = Policy::parse(&std::fs::read(&args[1]).map_err(|_| Error::State("policy"))?)?;
    let session = uuid::Uuid::new_v4();
    let token = ade_exec::CancellationToken::default();
    let (mut engine, _) = Engine::adaptive_stateful(
        config.clone(),
        policy,
        session,
        token,
        false,
        Placement::Local,
        None,
    )?;
    let mut source =
        ade_stream::kafka::KafkaSource::connect(config.source.clone(), session, &engine.progress)?;
    source.enable_checkpoint_control()?;
    let start = std::time::Instant::now();
    while engine.progress.forwarded_records < 240 {
        ade_stream::require(start.elapsed().as_secs() < 60, "probe deadline")?;
        match source.poll()? {
            Poll::Data(record) => {
                let prepare = std::time::Instant::now();
                engine.prepare(record)?;
                engine
                    .state
                    .as_mut()
                    .and_then(|s| s.adaptive.as_mut())
                    .ok_or(Error::State("adaptive"))?
                    .observation
                    .replay_ns_per_record = u64::try_from(prepare.elapsed().as_nanos())
                    .map_err(|_| Error::Overflow("record sample"))?
                    .max(1);
                engine.acknowledge()?;
            }
            Poll::Pending => continue,
            Poll::Stopped => return Err(Error::Source("unexpected stop")),
        }
        if engine.progress.forwarded_records % 80 == 0 {
            source.pause()?;
            source.verify(&engine.progress)?;
            let state = engine.state.as_mut().ok_or(Error::State("state"))?;
            if engine.progress.forwarded_records == 240 {
                let a = state.adaptive.as_mut().ok_or(Error::State("adaptive"))?;
                a.policy.placements = vec![Placement::Local, Placement::Remote];
                a.policy.static_placement = Placement::Local;
                a.policy.prices.local_write_byte = 100_000_000;
                a.policy.prices.remote_write_byte = 0;
                a.policy.prices.remote_request = 0;
                if args[2] == "veto" {
                    let r = a.policy.remote.as_ref().ok_or(Error::State("remote"))?;
                    a.policy.limits.remote_write_bytes =
                        ade_stream::adaptive::remote::reservations(&config.checkpoint_root, r)?
                            .write_bytes
                            + 66_000;
                }
                a.policy.validate()?;
                println!(
                    "{}",
                    serde_json::json!({"type":"before_transition","wire_time_ns":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_err(|_|Error::State("clock"))?.as_nanos(),"policy":a.policy})
                );
            }
            state.checkpoint(
                &engine.progress,
                u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX),
                &mut |_| Ok(()),
            )?;
            let a = state.adaptive.as_ref().ok_or(Error::State("adaptive"))?;
            println!(
                "{}",
                serde_json::json!({"type":"cut","decision":a.decision,"metrics":a.last_metrics,"progress":engine.progress,"certificate":a.current})
            );
            store::io(std::io::Write::flush(&mut std::io::stdout()))?;
            source.resume()?;
        }
    }
    source.shutdown()
}
#[cfg(not(feature = "remote-checkpoint"))]
fn main() {
    eprintln!("requires remote-checkpoint");
    std::process::exit(1);
}
