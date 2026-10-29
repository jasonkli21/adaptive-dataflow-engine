//! Explicit experimental remote-object entrypoint; no managed operations flags.
use ade_distributed::remote;
use std::{fs::File, io::Read, path::Path};
fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() == 2 && args[1] == "--verify-receipt" {
        let mut bytes = Vec::new();
        std::io::stdin()
            .take(remote::MAX_JSON as u64 + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() > remote::MAX_JSON {
            return Err("receipt cap".into());
        }
        let receipt = serde_json::from_slice(&bytes)?;
        remote::verify_receipt(&receipt)?;
        println!("{{\"version\":1,\"receipt_structure_identity_cost\":\"VERIFIED\"}}");
        return Ok(());
    }
    if !(2..=3).contains(&args.len()) || args.get(2).is_some_and(|s| s != "--apply-gc") {
        return Err("usage: ade-remote RUN.json [--apply-gc] (GC defaults to dry-run)".into());
    }
    let file = File::open(Path::new(&args[1]))?;
    if !file.metadata()?.is_file() || file.metadata()?.len() > remote::MAX_JSON as u64 {
        return Err("remote config cap".into());
    }
    let mut bytes = vec![];
    file.take(remote::MAX_JSON as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > remote::MAX_JSON {
        return Err("remote config cap".into());
    }
    let request: remote::Run = serde_json::from_slice(&bytes)?;
    let mut result =
        remote::execute_observed(&request, ade_exec::CancellationToken::default(), |event| {
            match event {
                remote::Event::Admitted { context } => {
                    eprintln!("ADE_REMOTE_ATTEMPT {}", context.execution);
                }
                remote::Event::ProducersStarted { context, .. } => {
                    eprintln!("ADE_REMOTE_PRODUCERS_STARTED {}", context.execution);
                }
                remote::Event::ProducersExited { context, .. } => {
                    eprintln!("ADE_REMOTE_PRODUCERS_EXITED {}", context.execution);
                }
                remote::Event::ManifestCommitted { context, .. } => {
                    eprintln!("ADE_REMOTE_MANIFEST {}", context.execution);
                }
                remote::Event::ReadersStarted { context, .. } => {
                    eprintln!("ADE_REMOTE_READERS_STARTED {}", context.execution);
                }
            }
            Ok(())
        })?;
    let cleanup = result.cleanup(args.len() == 3)?;
    // All output is emitted only after every final, gather and source check succeeded.
    println!(
        "{}",
        serde_json::to_string(&serde_json::json!({ "version":1, "status":"SUCCEEDED",
        "receipt": result.receipt, "cleanup": cleanup, "rows": result.batches.iter().map(arrow::record_batch::RecordBatch::num_rows).sum::<usize>() }))?
    );
    Ok(())
}
fn main() {
    if let Err(error) = run() {
        // Provider errors were already mapped to fixed credential-free messages.
        eprintln!("ade-remote: {error}");
        std::process::exit(1);
    }
}
