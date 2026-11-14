use clap::{Parser, Subcommand};
use std::{io, path::PathBuf, process::ExitCode};

#[derive(Parser)]
#[command(about = "Opt-in bounded local resource manager")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Create a private ledger and print the initial operator token once.
    Init {
        #[arg(long)]
        root: PathBuf,
        #[arg(long)]
        config: PathBuf,
    },
    /// Run the foreground service.
    Serve {
        #[arg(long)]
        root: PathBuf,
    },
    /// Read one credentialed JSON request on stdin and write one JSON response.
    Request {
        #[arg(long)]
        socket: PathBuf,
    },
}
fn main() -> ExitCode {
    let result = match Cli::parse().command {
        Command::Init { root, config } => {
            ade_resource::initialize(&root, &config).map(|token| println!("{token}"))
        }
        Command::Serve { root } => ade_resource::serve(&root),
        Command::Request { socket } => {
            ade_resource::client(&socket, io::stdin().lock()).map(|reply| println!("{reply}"))
        }
    };
    if let Err(error) = result {
        eprintln!("ade-resource: {error}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}
