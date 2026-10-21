use ade_distributed::{ids::WorkerId, worker};
fn main() {
    if let Err(error) = run() {
        eprintln!("ade-worker: {error}");
        std::process::exit(1);
    }
}
fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() == 2 && args[1] == "--verify-topology" {
        use std::io::Read;
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Evidence {
            config: ade_distributed::topology::TopologyConfig,
            decision: ade_distributed::topology::Decision,
        }
        let mut bytes = Vec::new();
        std::io::stdin()
            .take(ade_distributed::protocol::MAX_CONTROL_BYTES as u64 + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() > ade_distributed::protocol::MAX_CONTROL_BYTES {
            return Err("overlarge topology evidence".into());
        }
        let evidence: Evidence = serde_json::from_slice(&bytes)?;
        evidence
            .decision
            .layout
            .validate(evidence.decision.layout.workers)?;
        let expected = ade_distributed::topology::decide(
            &evidence.config,
            evidence.decision.layout.workers,
            evidence.decision.observations.clone(),
        )?;
        if expected != evidence.decision {
            return Err("topology decision does not reproduce".into());
        }
        return Ok(());
    }
    if !(3..=4).contains(&args.len()) {
        return Err("usage: ade-worker WORKER_ID TEMP_PARENT [SHARED_OBJECT_ROOT]".into());
    }
    if let Some(shared) = args.get(3) {
        worker::serve_with_object_store(
            WorkerId(args[1].parse()?),
            "127.0.0.1:0".parse()?,
            "127.0.0.1:0".parse()?,
            std::path::Path::new(&args[2]),
            ade_distributed::ObjectStoreBackend::new(
                ade_distributed::ObjectStoreConfig {
                    backend: "shared_local_fixture".into(),
                    durability: ade_distributed::ObjectStoreDurability::WorkerProcessSurviving,
                },
                std::sync::Arc::new(object_store::local::LocalFileSystem::new_with_prefix(
                    shared,
                )?),
            )?,
            std::io::stdout().lock(),
        )?;
    } else {
        worker::serve(
            WorkerId(args[1].parse()?),
            "127.0.0.1:0".parse()?,
            "127.0.0.1:0".parse()?,
            std::path::Path::new(&args[2]),
            std::io::stdout().lock(),
        )?;
    }
    Ok(())
}
