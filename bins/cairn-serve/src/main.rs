//! `cairn-serve` — airgap-ready HTTP geocoder runtime.

use anyhow::{Context, Result};
use cairn_api::{router, AppState, Metrics};
use cairn_spatial::{AdminIndex, AdminLayer, NearestIndex, PointLayer};
use cairn_text::TextIndex;
use cairn_tile::read_manifest;
use clap::Parser;
use std::{net::SocketAddr, path::PathBuf, sync::Arc};

#[derive(Parser, Debug)]
#[command(name = "cairn-serve", version, about = "Serve a Cairn bundle")]
struct Cli {
    /// Path to bundle directory.
    #[arg(long)]
    bundle: PathBuf,

    /// Address to bind.
    #[arg(long, default_value = "0.0.0.0:8080")]
    bind: SocketAddr,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    let text_dir = cli.bundle.join("index/text");
    let text = if text_dir.exists() {
        tracing::info!(path = %text_dir.display(), "opening text index");
        Some(Arc::new(TextIndex::open(&text_dir).with_context(|| {
            format!("opening text index at {}", text_dir.display())
        })?))
    } else {
        tracing::warn!(path = %text_dir.display(), "no text index found; /v1/search will 503");
        None
    };

    let admin_path = cli.bundle.join("spatial/admin.bin");
    let admin = if admin_path.exists() {
        tracing::info!(path = %admin_path.display(), "loading admin layer");
        let layer = AdminLayer::read_from(&admin_path)
            .with_context(|| format!("loading admin layer at {}", admin_path.display()))?;
        let index = AdminIndex::build(layer);
        tracing::info!(features = index.len(), "admin index ready");
        Some(Arc::new(index))
    } else {
        tracing::warn!(path = %admin_path.display(), "no admin layer; /v1/reverse will 503");
        None
    };

    let points_path = cli.bundle.join("spatial/points.bin");
    let nearest = if points_path.exists() {
        tracing::info!(path = %points_path.display(), "loading point layer");
        let layer = PointLayer::read_from(&points_path)
            .with_context(|| format!("loading point layer at {}", points_path.display()))?;
        let index = NearestIndex::build(layer);
        tracing::info!(points = index.len(), "nearest index ready");
        Some(Arc::new(index))
    } else {
        tracing::warn!(path = %points_path.display(), "no point layer; nearest fallback off");
        None
    };

    let manifest = read_manifest(&cli.bundle.join("manifest.toml")).ok();
    let bundle_id = manifest
        .as_ref()
        .map(|m| m.bundle_id.clone())
        .unwrap_or_else(|| "unknown".into());
    let admin_features = admin.as_ref().map(|a| a.len() as u64).unwrap_or(0);
    let point_count = nearest.as_ref().map(|n| n.len() as u64).unwrap_or(0);
    let metrics = Arc::new(Metrics::new(bundle_id, admin_features, point_count));

    let state = AppState {
        bundle_path: Arc::new(cli.bundle.clone()),
        text,
        admin,
        nearest,
        metrics,
    };
    let app = router(state);

    tracing::info!(
        bind = %cli.bind,
        bundle = %cli.bundle.display(),
        "cairn-serve starting"
    );

    let listener = tokio::net::TcpListener::bind(cli.bind).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
