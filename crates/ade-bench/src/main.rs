use clap::Parser;
#[tokio::main]
async fn main() -> std::process::ExitCode {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();
    match ade_bench::cli::execute(ade_bench::cli::Cli::parse()).await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("ade-bench: {e}");
            std::process::ExitCode::FAILURE
        }
    }
}
