//! `cairn-serve` — airgap-ready HTTP geocoder runtime.

use anyhow::{Context, Result};
use cairn_api::{router, AppState, Metrics};
use cairn_spatial::{AdminIndex, NearestIndex};
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
    let manifest = read_manifest(&cli.bundle.join("manifest.toml")).ok();

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

    let admin = if !manifest
        .as_ref()
        .map(|m| m.admin_tiles.is_empty())
        .unwrap_or(true)
    {
        let entries = manifest.as_ref().unwrap().admin_tiles.clone();
        tracing::info!(tiles = entries.len(), "opening admin layer (partitioned)");
        let index = AdminIndex::open(&cli.bundle, entries);
        tracing::info!(features = index.len(), "admin index ready");
        Some(Arc::new(index))
    } else {
        tracing::warn!("no admin tiles in manifest; /v1/reverse will 503");
        None
    };

    let nearest = if !manifest
        .as_ref()
        .map(|m| m.point_tiles.is_empty())
        .unwrap_or(true)
    {
        let entries = manifest.as_ref().unwrap().point_tiles.clone();
        tracing::info!(tiles = entries.len(), "opening point layer (partitioned)");
        let index = NearestIndex::open(&cli.bundle, entries);
        tracing::info!(points = index.len(), "nearest index ready");
        Some(Arc::new(index))
    } else {
        tracing::warn!("no point tiles in manifest; nearest fallback off");
        None
    };

    let bundle_id = manifest
        .as_ref()
        .map(|m| m.bundle_id.clone())
        .unwrap_or_else(|| "unknown".into());
    let admin_features = admin.as_ref().map(|a| a.len() as u64).unwrap_or(0);
    let point_count = nearest.as_ref().map(|n| n.len() as u64).unwrap_or(0);
    let metrics = Arc::new(Metrics::new(bundle_id, admin_features, point_count));

    let api_key = std::env::var("CAIRN_API_KEY")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(|k| {
            tracing::info!(
                masked = mask_secret(&k),
                "API key loaded from CAIRN_API_KEY"
            );
            Arc::new(k)
        });
    if api_key.is_none() {
        tracing::warn!("CAIRN_API_KEY not set — /v1/* endpoints are open");
    }

    // Per-IP token-bucket. CAIRN_RATE_LIMIT="rate,burst" both as
    // floats. Common pattern: "10,20" = sustained 10 req/s, burst 20.
    // Absent / malformed = unlimited (default).
    let rate_limit = std::env::var("CAIRN_RATE_LIMIT")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .and_then(|raw| {
            let parts: Vec<&str> = raw.split(',').collect();
            let rate = parts.first()?.trim().parse::<f64>().ok()?;
            let burst = parts
                .get(1)
                .and_then(|s| s.trim().parse::<f64>().ok())
                .unwrap_or(rate * 2.0);
            tracing::info!(rate_per_sec = rate, burst, "rate limiter enabled");
            Some(Arc::new(cairn_api::RateLimiter::new(rate, burst)))
        });
    if rate_limit.is_none() {
        tracing::info!("CAIRN_RATE_LIMIT not set — /v1/* unthrottled");
    }

    // CAIRN_TRUST_PROXY=1 makes the rate limiter use the first IP in
    // X-Forwarded-For. Only safe when an ingress / reverse proxy
    // strips client-supplied XFF and re-appends its own. Default off
    // so a public deploy without a trusted proxy can't be spoofed.
    let trust_forwarded_for = std::env::var("CAIRN_TRUST_PROXY")
        .map(|v| matches!(v.trim(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false);
    if trust_forwarded_for {
        tracing::info!("CAIRN_TRUST_PROXY=on — X-Forwarded-For is the rate-limiter key");
    }

    // CAIRN_TRUSTED_PROXIES is a comma-separated CIDR allowlist
    // (e.g. "10.0.0.0/8,172.16.0.0/12,fd00::/8"). When non-empty, XFF
    // is honored only when the per-connection peer falls inside one
    // of these networks — defeats XFF spoofing from arbitrary internet
    // hosts even when CAIRN_TRUST_PROXY=on. Empty (default) trusts XFF
    // unconditionally (matches the previous behavior).
    let trusted_proxy_cidrs: Vec<cairn_api::TrustedCidr> = std::env::var("CAIRN_TRUSTED_PROXIES")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(|raw| {
            raw.split(',')
                .filter(|s| !s.trim().is_empty())
                .filter_map(|s| match cairn_api::TrustedCidr::parse(s) {
                    Ok(c) => Some(c),
                    Err(err) => {
                        tracing::warn!(spec = s, ?err, "skipping invalid CIDR");
                        None
                    }
                })
                .collect()
        })
        .unwrap_or_default();
    if !trusted_proxy_cidrs.is_empty() {
        tracing::info!(
            cidrs = trusted_proxy_cidrs.len(),
            "CAIRN_TRUSTED_PROXIES — XFF accepted only from listed CIDRs"
        );
    }

    let state = AppState {
        bundle_path: Arc::new(cli.bundle.clone()),
        text,
        admin,
        nearest,
        metrics,
        api_key,
        rate_limit,
        trust_forwarded_for,
        trusted_proxy_cidrs: Arc::new(trusted_proxy_cidrs),
    };
    let app = router(state);

    tracing::info!(
        bind = %cli.bind,
        bundle = %cli.bundle.display(),
        "cairn-serve starting"
    );

    let listener = tokio::net::TcpListener::bind(cli.bind).await?;
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .await?;
    Ok(())
}

/// Masks an API key for log output: keeps first 3 + last 3 chars.
fn mask_secret(s: &str) -> String {
    if s.len() <= 8 {
        "***".into()
    } else {
        format!("{}…{}", &s[..3], &s[s.len() - 3..])
    }
}
