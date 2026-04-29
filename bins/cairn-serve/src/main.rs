//! `cairn-serve` — airgap-ready HTTP geocoder runtime.

use anyhow::{Context, Result};
use cairn_api::{router, AppState, FederatedAdmin, FederatedNearest, FederatedText, Metrics};
use cairn_spatial::{AdminIndex, NearestIndex};
use cairn_text::TextIndex;
use cairn_tile::read_manifest;
use clap::Parser;
use std::{net::SocketAddr, path::PathBuf, sync::Arc};

#[derive(Parser, Debug)]
#[command(name = "cairn-serve", version, about = "Serve a Cairn bundle")]
struct Cli {
    /// Path to a single bundle directory. Mutually exclusive with
    /// `--bundles`. Kept for backwards compatibility — single-bundle
    /// deploys are the common case.
    #[arg(long, conflicts_with = "bundles")]
    bundle: Option<PathBuf>,

    /// Comma-separated list of bundle directories to federate. Each
    /// bundle keeps its own indices; queries fan out and merge by
    /// score (or distance, for reverse). Use to split a planet into
    /// continental shards without standing up multiple processes.
    #[arg(long, value_delimiter = ',')]
    bundles: Vec<PathBuf>,

    /// Address to bind.
    #[arg(long, default_value = "0.0.0.0:8080")]
    bind: SocketAddr,
}

/// Per-bundle handles loaded from disk.
struct LoadedBundle {
    path: PathBuf,
    bundle_id: String,
    text: Option<Arc<TextIndex>>,
    admin: Option<Arc<AdminIndex>>,
    nearest: Option<Arc<NearestIndex>>,
    admin_features: u64,
    point_count: u64,
}

fn load_bundle(path: &std::path::Path) -> Result<LoadedBundle> {
    let manifest = read_manifest(&path.join("manifest.toml")).ok();

    let text_dir = path.join("index/text");
    let text = if text_dir.exists() {
        tracing::info!(path = %text_dir.display(), "opening text index");
        Some(Arc::new(TextIndex::open(&text_dir).with_context(|| {
            format!("opening text index at {}", text_dir.display())
        })?))
    } else {
        tracing::warn!(path = %text_dir.display(), "no text index in bundle");
        None
    };

    let admin = match manifest.as_ref() {
        Some(m) if !m.admin_tiles.is_empty() => {
            let entries = m.admin_tiles.clone();
            tracing::info!(tiles = entries.len(), "opening admin layer (partitioned)");
            let index = AdminIndex::open(path, entries);
            tracing::info!(features = index.len(), "admin index ready");
            Some(Arc::new(index))
        }
        _ => {
            tracing::warn!("no admin tiles in manifest");
            None
        }
    };

    let nearest = match manifest.as_ref() {
        Some(m) if !m.point_tiles.is_empty() => {
            let entries = m.point_tiles.clone();
            tracing::info!(tiles = entries.len(), "opening point layer (partitioned)");
            let index = NearestIndex::open(path, entries);
            tracing::info!(points = index.len(), "nearest index ready");
            Some(Arc::new(index))
        }
        _ => {
            tracing::warn!("no point tiles in manifest");
            None
        }
    };

    let bundle_id = manifest
        .as_ref()
        .map(|m| m.bundle_id.clone())
        .unwrap_or_else(|| "unknown".into());
    let admin_features = admin.as_ref().map(|a| a.len() as u64).unwrap_or(0);
    let point_count = nearest.as_ref().map(|n| n.len() as u64).unwrap_or(0);
    Ok(LoadedBundle {
        path: path.to_path_buf(),
        bundle_id,
        text,
        admin,
        nearest,
        admin_features,
        point_count,
    })
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
    let bundle_paths: Vec<PathBuf> = if !cli.bundles.is_empty() {
        cli.bundles.clone()
    } else if let Some(p) = cli.bundle.clone() {
        vec![p]
    } else {
        return Err(anyhow::anyhow!(
            "either --bundle <DIR> or --bundles <a,b,c> is required"
        ));
    };
    if bundle_paths.len() > 1 {
        tracing::info!(
            bundles = bundle_paths.len(),
            "federation mode — fan-out search across all bundles"
        );
    }

    let mut loaded: Vec<LoadedBundle> = Vec::with_capacity(bundle_paths.len());
    for path in &bundle_paths {
        tracing::info!(path = %path.display(), "loading bundle");
        loaded.push(load_bundle(path)?);
    }

    let texts: Vec<Arc<TextIndex>> = loaded.iter().filter_map(|b| b.text.clone()).collect();
    let admins: Vec<Arc<AdminIndex>> = loaded.iter().filter_map(|b| b.admin.clone()).collect();
    let nearests: Vec<Arc<NearestIndex>> =
        loaded.iter().filter_map(|b| b.nearest.clone()).collect();

    let text = if texts.is_empty() {
        tracing::warn!("no text index in any bundle; /v1/search will 503");
        None
    } else {
        Some(Arc::new(FederatedText::from_many(texts)))
    };
    let admin = if admins.is_empty() {
        tracing::warn!("no admin layer in any bundle; /v1/reverse PIP off");
        None
    } else {
        Some(Arc::new(FederatedAdmin::from_many(admins)))
    };
    let nearest = if nearests.is_empty() {
        tracing::warn!("no point layer in any bundle; nearest fallback off");
        None
    } else {
        Some(Arc::new(FederatedNearest::from_many(nearests)))
    };

    let bundle_ids: Vec<String> = loaded.iter().map(|b| b.bundle_id.clone()).collect();
    let admin_features: u64 = loaded.iter().map(|b| b.admin_features).sum();
    let point_count: u64 = loaded.iter().map(|b| b.point_count).sum();
    // Composite metrics bundle_id: comma-joined for human-readable
    // /metrics output; structured Vec lives on AppState.bundle_ids.
    let metrics_bundle_id = if bundle_ids.len() == 1 {
        bundle_ids[0].clone()
    } else {
        bundle_ids.join(",")
    };
    let metrics = Arc::new(Metrics::new(metrics_bundle_id, admin_features, point_count));
    let primary_path = loaded
        .first()
        .map(|b| b.path.clone())
        .unwrap_or_else(|| bundle_paths[0].clone());

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

    let state = AppState::new(
        primary_path,
        cairn_api::BundleSnapshot {
            text,
            admin,
            nearest,
            bundle_ids: bundle_ids.clone(),
        },
        metrics,
        api_key,
        rate_limit,
        trust_forwarded_for,
        Arc::new(trusted_proxy_cidrs),
    );
    let app = router(state);

    tracing::info!(
        bind = %cli.bind,
        bundles = ?bundle_ids,
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
