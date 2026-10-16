use clap::Parser;
#[tokio::main]
async fn main() -> std::process::ExitCode {
    let cli = ade_bench::cli::Cli::parse();
    // Measured children retain the recorded warn-level policy. Optional diagnostics
    // apply only to standalone validation, outside benchmark measurements.
    let log_level = if matches!(&cli.command, ade_bench::cli::Commands::Validate(_)) {
        std::env::var("ADE_LOG")
            .ok()
            .and_then(|value| value.parse::<tracing::Level>().ok())
            .unwrap_or(tracing::Level::WARN)
    } else {
        tracing::Level::WARN
    };
    tracing_subscriber::fmt()
        .with_max_level(log_level)
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();
    match ade_bench::cli::execute(cli).await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("ade-bench: {e}");
            std::process::ExitCode::FAILURE
        }
    }
}
