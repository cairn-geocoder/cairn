//! `cairn-serve` — airgap-ready HTTP geocoder runtime.

use anyhow::Result;
use cairn_api::{router, AppState};
use clap::Parser;
use std::{net::SocketAddr, path::PathBuf, sync::Arc};

#[derive(Parser, Debug)]
#[command(name = "cairn-serve", version, about = "Serve a Cairn bundle")]
struct Cli {
    /// Path to bundle directory or `bundle.tar`.
    #[arg(long)]
    bundle: PathBuf,

    /// Address to bind.
    #[arg(long, default_value = "0.0.0.0:8080")]
    bind: SocketAddr,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();
    let state = AppState {
        bundle_path: Arc::new(cli.bundle.clone()),
    };
    let app = router(state);

    tracing::info!(bind = %cli.bind, bundle = %cli.bundle.display(), "cairn-serve starting");

    let listener = tokio::net::TcpListener::bind(cli.bind).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
