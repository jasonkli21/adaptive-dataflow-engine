//! Measure the same admitted native plan without Flight transport.
#[allow(
    dead_code,
    reason = "Shared gateway query module includes Flight-only schema helpers"
)]
#[path = "../src/query.rs"]
mod query;
#[path = "../src/worker.rs"]
mod worker;

use ade_exec::CancellationToken;
use ade_frontend::Catalog;
use arrow::ipc::writer::StreamWriter;
use std::{path::PathBuf, time::Instant};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args_os().skip(1).collect::<Vec<_>>();
    let [root, request, python, script, digest, output] = args.as_slice() else {
        return Err("usage: phase22-native ROOT REQUEST PYTHON SCRIPT DIGEST ARROW_OUTPUT".into());
    };
    let root = PathBuf::from(root).canonicalize()?;
    let mut catalog = Catalog::default();
    catalog.register_manifest("input", &root, &root.join("manifest.json"))?;
    let registry = worker::Registry::new(
        PathBuf::from(python),
        PathBuf::from(script),
        digest.to_string_lossy().into_owned(),
    )?;
    let request = std::fs::read(request)?;
    let mut timings = Vec::new();
    for _ in 0..12 {
        let start = Instant::now();
        let bound = query::Query::parse(&request)?.bind(&catalog, Some(&registry), start)?;
        let schema = bound.schema();
        let (out, mut receipt) = bound.execute(
            &catalog,
            &root,
            Some(&registry),
            CancellationToken::default(),
        )?;
        receipt["wall_ns"] = serde_json::json!(start.elapsed().as_nanos());
        timings.push(receipt);
        let mut writer = StreamWriter::try_new(std::fs::File::create(output)?, &schema)?;
        for batch in &out.batches {
            writer.write(batch)?;
        }
        writer.finish()?;
    }
    println!("{}", serde_json::to_string(&timings)?);
    Ok(())
}
