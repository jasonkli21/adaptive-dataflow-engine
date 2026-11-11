use ade_connectors::{
    Catalog, Context, ResolvedSnapshotV1,
    batch::sink::{LocalSink, PendingSink, Sink},
    local::Local,
};
use std::{env, fs::File, io::Read, path::Path, time::Duration};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    let [root, descriptor, digest, stage, sql, output, key] = args.as_slice() else {
        return Err(
            "usage: ade-connect ROOT DESCRIPTOR EXPECTED_BLAKE3 STAGE_PARENT SELECT OUTPUT_DIR KEY"
                .into(),
        );
    };
    let context = Context::new(Duration::from_secs(120))?;
    let mut bytes = Vec::new();
    File::open(descriptor)?
        .take(256 * 1024 + 1)
        .read_to_end(&mut bytes)?;
    let snapshot = ResolvedSnapshotV1::from_json(&bytes)?;
    let mut catalog = Catalog::default();
    catalog.register(snapshot.entry.clone())?;
    let name = snapshot.entry.name.clone();
    let source = Local::new(Path::new(root), snapshot)?;
    let staged = catalog.stage(&name, &source, digest, Path::new(stage), &context)?;
    let result = staged.query(sql, &context).await?;
    let plan = result.diagnostics["plan_hash"]
        .as_str()
        .ok_or("missing native plan identity")?;
    let fields = serde_json::from_value(result.diagnostics["output_schema"].clone())?;
    let schema = ade_exec::physical::arrow_schema(&fields);
    let sink = LocalSink::new(Path::new(output))?;
    let mut pending = sink.prepare(key, plan, &schema, &context)?;
    for batch in result.batches() {
        pending.write(batch, &context)?;
    }
    let receipt = pending.commit(&context)?;
    println!(
        "{}",
        serde_json::json!({"receipt": receipt, "diagnostics": result.diagnostics})
    );
    Ok(())
}
