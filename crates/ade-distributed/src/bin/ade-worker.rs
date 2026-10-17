use ade_distributed::{ids::WorkerId, worker};
fn main() {
    if let Err(error) = run() {
        eprintln!("ade-worker: {error}");
        std::process::exit(1);
    }
}
fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
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
