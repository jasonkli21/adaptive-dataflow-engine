use ade_distributed::{ids::WorkerId, worker};
fn main() {
    if let Err(error) = run() {
        eprintln!("ade-worker: {error}");
        std::process::exit(1);
    }
}
fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        return Err("usage: ade-worker WORKER_ID TEMP_PARENT".into());
    }
    worker::serve(
        WorkerId(args[1].parse()?),
        "127.0.0.1:0".parse()?,
        "127.0.0.1:0".parse()?,
        std::path::Path::new(&args[2]),
        std::io::stdout().lock(),
    )?;
    Ok(())
}
