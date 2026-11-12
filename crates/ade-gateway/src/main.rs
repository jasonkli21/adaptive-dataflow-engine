mod incoming;
mod query;
mod service;
mod worker;

use ade_frontend::Catalog;
use arrow_flight::flight_service_server::FlightServiceServer;
use clap::Parser;
use std::{net::SocketAddr, path::PathBuf, time::Duration};
use tokio::net::TcpListener;

#[derive(Parser)]
#[command(about = "Bounded loopback Arrow Flight SQL for native ADE")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:0")]
    listen: SocketAddr,
    #[arg(long)]
    root: PathBuf,
    #[arg(long)]
    manifest: PathBuf,
    #[arg(long, requires_all = ["script", "digest"])]
    python: Option<PathBuf>,
    #[arg(long, requires_all = ["python", "digest"])]
    script: Option<PathBuf>,
    #[arg(long, requires_all = ["python", "script"])]
    digest: Option<String>,
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    match serve(Args::parse()).await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(reason) => {
            eprintln!("gateway startup/service refused: {reason}");
            std::process::ExitCode::FAILURE
        }
    }
}

async fn serve(args: Args) -> Result<(), &'static str> {
    if !args.listen.ip().is_loopback() {
        return Err("numeric loopback address required");
    }
    let token = std::env::var("ADE_GATEWAY_TOKEN").map_err(|_| "operator token missing")?;
    let root = args
        .root
        .canonicalize()
        .map_err(|_| "source root unavailable")?;
    let manifest = args
        .manifest
        .canonicalize()
        .map_err(|_| "manifest unavailable")?;
    if !manifest.starts_with(&root) {
        return Err("manifest outside source root");
    }
    let mut catalog = Catalog::default();
    catalog
        .register_manifest("input", &root, &manifest)
        .map_err(|_| "source admission failed")?;
    let registry = match (args.python, args.script, args.digest) {
        (Some(python), Some(script), Some(digest)) => Some(
            worker::Registry::new(python, script, digest)
                .map_err(|_| "function registration failed")?,
        ),
        (None, None, None) => None,
        _ => return Err("incomplete function registration"),
    };
    let gateway = service::Gateway::new(catalog, registry, root, &token)
        .map_err(|_| "invalid gateway configuration")?;
    let listener = TcpListener::bind(args.listen)
        .await
        .map_err(|_| "loopback bind failed")?;
    println!(
        "{}",
        listener.local_addr().map_err(|_| "address unavailable")?
    );
    tonic::transport::Server::builder()
        .max_concurrent_streams(8)
        .max_connection_age(Duration::from_secs(120))
        .max_connection_age_grace(Duration::from_secs(15))
        .concurrency_limit_per_connection(8)
        .load_shed(true)
        .http2_max_header_list_size(8192)
        .timeout(Duration::from_secs(12))
        .add_service(
            FlightServiceServer::new(gateway)
                .max_decoding_message_size(20 * 1024)
                .max_encoding_message_size(query::RESULT_BYTES),
        )
        .serve_with_incoming(incoming::bounded(listener))
        .await
        .map_err(|_| "Flight transport failed")
}
