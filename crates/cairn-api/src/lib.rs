//! HTTP API for the Cairn geocoder.
//!
//! Endpoints:
//!   GET /v1/search        forward + autocomplete + fuzzy + layer + focus
//!   GET /v1/reverse       (Phase 3)
//!   GET /v1/structured    (Phase 4)
//!   GET /healthz, /readyz

use axum::http::header;
use axum::{
    extract::{ConnectInfo, Query, State},
    http::{Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Json, Response},
    routing::{get, post},
    Router,
};
use cairn_place::Coord;
#[allow(unused_imports)]
use cairn_spatial::{AdminIndex, NearestIndex};
#[allow(unused_imports)]
use cairn_text::{Bbox, Hit, SearchMode, SearchOptions, TextError, TextIndex};

mod federated;
pub use federated::{FederatedAdmin, FederatedNearest, FederatedText};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tower_http::trace::{DefaultMakeSpan, DefaultOnRequest, DefaultOnResponse, TraceLayer};
use tracing::{error, Level};

/// Hand-rolled Prometheus-compatible metrics. Avoids adding a dep just to
/// emit a few counters. Hot paths bump atomics; `/metrics` formats them.
pub struct Metrics {
    pub started: Instant,
    pub bundle_id: String,
    pub admin_features: u64,
    pub point_count: u64,

    pub search_ok: AtomicU64,
    pub search_err: AtomicU64,
    pub autocomplete_ok: AtomicU64,
    pub structured_ok: AtomicU64,
    pub structured_err: AtomicU64,
    pub reverse_pip: AtomicU64,
    pub reverse_nearest: AtomicU64,
    pub reverse_empty: AtomicU64,
    pub bad_request: AtomicU64,
}

impl Metrics {
    pub fn new(bundle_id: String, admin_features: u64, point_count: u64) -> Self {
        Self {
            started: Instant::now(),
            bundle_id,
            admin_features,
            point_count,
            search_ok: AtomicU64::new(0),
            search_err: AtomicU64::new(0),
            autocomplete_ok: AtomicU64::new(0),
            structured_ok: AtomicU64::new(0),
            structured_err: AtomicU64::new(0),
            reverse_pip: AtomicU64::new(0),
            reverse_nearest: AtomicU64::new(0),
            reverse_empty: AtomicU64::new(0),
            bad_request: AtomicU64::new(0),
        }
    }
}

/// Phase 7a-U — bundle hot-reload backing. The mutable bundle
/// surface lives behind `ArcSwap` snapshots so an admin-triggered
/// reload can atomically swap the live indices without restarting
/// the process. Each request `.load_full()`s its own snapshot of
/// every field at the start of handling and is unaffected by a
/// reload that happens mid-request.
#[derive(Clone)]
pub struct AppState {
    pub bundle_path: Arc<std::path::PathBuf>,
    text_swap: Arc<arc_swap::ArcSwapOption<FederatedText>>,
    admin_swap: Arc<arc_swap::ArcSwapOption<FederatedAdmin>>,
    nearest_swap: Arc<arc_swap::ArcSwapOption<FederatedNearest>>,
    bundle_ids_swap: Arc<arc_swap::ArcSwap<Vec<String>>>,
    pub metrics: Arc<Metrics>,
    /// Optional API key. When `Some`, every request to `/v1/*` must
    /// present `X-API-Key: <key>` (or `?api_key=<key>`) or 401.
    pub api_key: Option<Arc<String>>,
    /// Optional per-IP token-bucket rate limiter. When `Some`, requests
    /// to `/v1/*` are throttled; absent = unlimited (default).
    pub rate_limit: Option<Arc<RateLimiter>>,
    /// When `true`, the rate limiter trusts the first entry of the
    /// `X-Forwarded-For` header as the originating client IP. Only safe
    /// behind a reverse proxy / ingress that strips client-supplied
    /// `X-Forwarded-For` and appends its own — otherwise an attacker
    /// can forge their IP and trivially bypass the per-IP bucket.
    /// Default `false`.
    pub trust_forwarded_for: bool,
    /// Optional CIDR allowlist of trusted reverse proxies. When non-empty,
    /// `X-Forwarded-For` is honored only when the per-connection peer
    /// is inside one of these networks. Tightens `trust_forwarded_for`
    /// against attackers who bypass the proxy. Empty = trust unconditionally
    /// (when `trust_forwarded_for` is also true).
    pub trusted_proxy_cidrs: Arc<Vec<TrustedCidr>>,
}

/// Bundle pieces a hot-reload swap needs to install in one go.
/// Built fresh from disk by the operator (or by the reload helper)
/// and then plugged into [`AppState::install_bundle`].
pub struct BundleSnapshot {
    pub text: Option<Arc<FederatedText>>,
    pub admin: Option<Arc<FederatedAdmin>>,
    pub nearest: Option<Arc<FederatedNearest>>,
    pub bundle_ids: Vec<String>,
}

impl AppState {
    /// Build the mutable surface from an initial bundle snapshot.
    /// Stable fields (api_key, rate_limit, trusted_proxy_cidrs, …)
    /// are constructed by the caller; this only seeds the swap-able
    /// indices.
    pub fn new(
        bundle_path: std::path::PathBuf,
        snapshot: BundleSnapshot,
        metrics: Arc<Metrics>,
        api_key: Option<Arc<String>>,
        rate_limit: Option<Arc<RateLimiter>>,
        trust_forwarded_for: bool,
        trusted_proxy_cidrs: Arc<Vec<TrustedCidr>>,
    ) -> Self {
        Self {
            bundle_path: Arc::new(bundle_path),
            text_swap: Arc::new(arc_swap::ArcSwapOption::from(snapshot.text)),
            admin_swap: Arc::new(arc_swap::ArcSwapOption::from(snapshot.admin)),
            nearest_swap: Arc::new(arc_swap::ArcSwapOption::from(snapshot.nearest)),
            bundle_ids_swap: Arc::new(arc_swap::ArcSwap::from(Arc::new(snapshot.bundle_ids))),
            metrics,
            api_key,
            rate_limit,
            trust_forwarded_for,
            trusted_proxy_cidrs,
        }
    }

    /// Phase 7a-U — atomically replace the bundle surface. Pre-built
    /// indices are pushed into the swaps in a single visible step;
    /// already-running requests keep their previous snapshot until
    /// they release it. Returns the previous bundle id list for
    /// logging.
    pub fn install_bundle(&self, snapshot: BundleSnapshot) -> Arc<Vec<String>> {
        self.text_swap.store(snapshot.text);
        self.admin_swap.store(snapshot.admin);
        self.nearest_swap.store(snapshot.nearest);
        self.bundle_ids_swap.swap(Arc::new(snapshot.bundle_ids))
    }

    /// Snapshot of the current text index, suitable for the lifetime
    /// of a single request handler.
    pub fn text(&self) -> Option<Arc<FederatedText>> {
        self.text_swap.load_full()
    }

    pub fn admin(&self) -> Option<Arc<FederatedAdmin>> {
        self.admin_swap.load_full()
    }

    pub fn nearest(&self) -> Option<Arc<FederatedNearest>> {
        self.nearest_swap.load_full()
    }

    pub fn bundle_ids(&self) -> Arc<Vec<String>> {
        self.bundle_ids_swap.load_full()
    }
}

/// Minimal CIDR matcher (no external dep). Stores the network address
/// already masked to its prefix length, so `contains` is one mask + one
/// equality check.
#[derive(Clone, Debug)]
pub struct TrustedCidr {
    network: std::net::IpAddr,
    prefix: u8,
}

impl TrustedCidr {
    pub fn parse(spec: &str) -> Result<Self, String> {
        let s = spec.trim();
        let (addr_part, prefix_part) = match s.split_once('/') {
            Some(p) => p,
            None => {
                let addr: std::net::IpAddr = s
                    .parse()
                    .map_err(|e| format!("not a CIDR or bare IP: {e}"))?;
                let prefix = if addr.is_ipv4() { 32 } else { 128 };
                return Ok(Self {
                    network: addr,
                    prefix,
                });
            }
        };
        let addr: std::net::IpAddr = addr_part
            .parse()
            .map_err(|e| format!("invalid IP in CIDR: {e}"))?;
        let prefix: u8 = prefix_part
            .parse()
            .map_err(|e| format!("invalid prefix in CIDR: {e}"))?;
        let max = if addr.is_ipv4() { 32 } else { 128 };
        if prefix > max {
            return Err(format!(
                "prefix {prefix} exceeds max {max} for the address family"
            ));
        }
        Ok(Self {
            network: mask_addr(addr, prefix),
            prefix,
        })
    }

    pub fn contains(&self, ip: std::net::IpAddr) -> bool {
        match (self.network, ip) {
            (std::net::IpAddr::V4(_), std::net::IpAddr::V4(_))
            | (std::net::IpAddr::V6(_), std::net::IpAddr::V6(_)) => {
                mask_addr(ip, self.prefix) == self.network
            }
            _ => false,
        }
    }
}

fn mask_addr(addr: std::net::IpAddr, prefix: u8) -> std::net::IpAddr {
    match addr {
        std::net::IpAddr::V4(v4) => {
            let bits = u32::from(v4);
            let masked = if prefix == 0 {
                0
            } else if prefix >= 32 {
                bits
            } else {
                bits & (!0u32 << (32 - prefix))
            };
            std::net::IpAddr::V4(std::net::Ipv4Addr::from(masked))
        }
        std::net::IpAddr::V6(v6) => {
            let bits = u128::from(v6);
            let masked = if prefix == 0 {
                0
            } else if prefix >= 128 {
                bits
            } else {
                bits & (!0u128 << (128 - prefix))
            };
            std::net::IpAddr::V6(std::net::Ipv6Addr::from(masked))
        }
    }
}

/// Token-bucket rate limiter, per remote IP. `rate_per_sec` tokens
/// refill into a bucket with capacity `burst`; each request consumes
/// one. Out-of-tokens → 429.
pub struct RateLimiter {
    rate_per_sec: f64,
    burst: f64,
    /// Per-IP buckets. Stale entries (> 5 min idle) get evicted on
    /// every Nth check so the map doesn't grow unbounded under DDoS.
    buckets: std::sync::Mutex<HashMap<std::net::IpAddr, RateBucket>>,
}

struct RateBucket {
    tokens: f64,
    last: std::time::Instant,
}

impl RateLimiter {
    /// Build a limiter that allows `rate_per_sec` sustained rate with a
    /// `burst` allowance. Burst < 1 is clamped to 1 so a single request
    /// always succeeds in isolation.
    pub fn new(rate_per_sec: f64, burst: f64) -> Self {
        Self {
            rate_per_sec: rate_per_sec.max(0.001),
            burst: burst.max(1.0),
            buckets: std::sync::Mutex::new(HashMap::new()),
        }
    }

    /// Try to consume one token for `ip`. Returns `true` on allow,
    /// `false` on deny.
    pub fn check(&self, ip: std::net::IpAddr) -> bool {
        let now = std::time::Instant::now();
        let mut map = self.buckets.lock().expect("rate limiter poisoned");
        // Stale eviction: drop entries idle for > 5 min on every 1024th
        // check to amortize the cost. Cheap O(n) walk.
        if !map.is_empty() && map.len() % 1024 == 0 {
            let cutoff = std::time::Duration::from_secs(300);
            map.retain(|_, b| now.duration_since(b.last) < cutoff);
        }
        let bucket = map.entry(ip).or_insert(RateBucket {
            tokens: self.burst,
            last: now,
        });
        let elapsed = now.duration_since(bucket.last).as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * self.rate_per_sec).min(self.burst);
        bucket.last = now;
        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    pub fn bucket_count(&self) -> usize {
        self.buckets.lock().map(|m| m.len()).unwrap_or(0)
    }
}

#[cfg(test)]
mod rate_tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[test]
    fn allows_burst_then_throttles() {
        // 1 req/s sustained, burst 5: 5 immediate calls allowed,
        // 6th rejected.
        let rl = RateLimiter::new(1.0, 5.0);
        let ip = std::net::IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
        for _ in 0..5 {
            assert!(rl.check(ip));
        }
        assert!(!rl.check(ip));
    }

    #[test]
    fn cidr_v4_contains_inside_and_excludes_outside() {
        let c = TrustedCidr::parse("10.0.0.0/8").unwrap();
        assert!(c.contains("10.0.0.1".parse().unwrap()));
        assert!(c.contains("10.255.255.255".parse().unwrap()));
        assert!(!c.contains("11.0.0.1".parse().unwrap()));
        assert!(!c.contains("192.168.1.1".parse().unwrap()));
    }

    #[test]
    fn cidr_v6_contains_inside_and_excludes_outside() {
        let c = TrustedCidr::parse("fd00::/8").unwrap();
        assert!(c.contains("fd00::1".parse().unwrap()));
        assert!(c.contains("fdff::ffff".parse().unwrap()));
        assert!(!c.contains("fc00::1".parse().unwrap()));
        assert!(!c.contains("2001:db8::1".parse().unwrap()));
    }

    #[test]
    fn cidr_bare_ip_is_host_route() {
        let c = TrustedCidr::parse("192.168.1.5").unwrap();
        assert!(c.contains("192.168.1.5".parse().unwrap()));
        assert!(!c.contains("192.168.1.6".parse().unwrap()));
    }

    #[test]
    fn cidr_rejects_bad_prefix() {
        assert!(TrustedCidr::parse("10.0.0.0/40").is_err());
        assert!(TrustedCidr::parse("not-an-ip").is_err());
        assert!(TrustedCidr::parse("10.0.0.0/abc").is_err());
    }

    #[test]
    fn separate_ips_have_separate_buckets() {
        let rl = RateLimiter::new(1.0, 1.0);
        let a = std::net::IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        let b = std::net::IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2));
        assert!(rl.check(a));
        assert!(rl.check(b));
        // Both exhausted now.
        assert!(!rl.check(a));
        assert!(!rl.check(b));
    }
}

/// Standard error envelope for every non-2xx JSON response.
///
/// Wire shape:
/// ```json
/// { "error": { "code": "snake_case_id", "message": "human readable" } }
/// ```
#[derive(Debug, Serialize)]
pub struct ApiError {
    #[serde(skip)]
    status: StatusCode,
    error: ApiErrorBody,
}

#[derive(Debug, Serialize)]
struct ApiErrorBody {
    code: &'static str,
    message: String,
}

impl ApiError {
    fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            error: ApiErrorBody {
                code,
                message: message.into(),
            },
        }
    }

    fn bad_request(code: &'static str, message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, code, message)
    }

    fn unavailable(code: &'static str, message: impl Into<String>) -> Self {
        Self::new(StatusCode::SERVICE_UNAVAILABLE, code, message)
    }

    fn unauthorized(code: &'static str, message: impl Into<String>) -> Self {
        Self::new(StatusCode::UNAUTHORIZED, code, message)
    }

    fn internal(code: &'static str, message: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, code, message)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = serde_json::json!({ "error": self.error });
        (self.status, Json(body)).into_response()
    }
}

pub fn router(state: AppState) -> Router {
    // Routes that require an API key when one is configured.
    let v1 = Router::new()
        .route("/v1/search", get(search))
        .route("/v1/reverse", get(reverse))
        .route("/v1/structured", get(structured))
        .route("/v1/parse", get(parse_addr))
        .route("/v1/expand", get(expand_addr))
        .route("/v1/place", get(place_lookup))
        .route("/v1/layers", get(layers_metadata))
        // Pelias-compatible aliases share the same handlers.
        .route("/v1/autocomplete", get(pelias_autocomplete))
        .route("/search", get(pelias_search))
        .route("/autocomplete", get(pelias_autocomplete))
        .route("/reverse", get(pelias_reverse))
        .route("/place", get(place_lookup))
        // Phase 7a-U — bundle hot-reload. Auth-gated (under the same
        // require_api_key middleware as the rest of /v1/*) so a
        // public deploy without a key still rejects reload attempts.
        .route("/admin/reload", post(admin_reload))
        // Layer order (last .route_layer is outermost → runs first):
        //   1. require_api_key  (cheap; unauthenticated traffic fails
        //      fast and never burns rate-limit tokens)
        //   2. rate_limit       (per-IP token bucket)
        //   3. handler
        .route_layer(middleware::from_fn_with_state(state.clone(), rate_limit))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_api_key,
        ));

    // Open routes (health / metrics / spec / info / sbom) bypass auth.
    let open = Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics))
        .route("/openapi.json", get(openapi_spec))
        .route("/v1/info", get(info))
        .route("/v1/sbom", get(sbom));

    open.merge(v1).with_state(state).layer(
        TraceLayer::new_for_http()
            .make_span_with(DefaultMakeSpan::new().level(Level::INFO))
            .on_request(DefaultOnRequest::new().level(Level::INFO))
            .on_response(DefaultOnResponse::new().level(Level::INFO)),
    )
}

/// Reject requests from IPs that have exhausted their token bucket.
/// When `state.rate_limit` is `None`, the layer is a no-op. The peer
/// address comes from `ConnectInfo<SocketAddr>` (axum injects it when
/// the server is started with `into_make_service_with_connect_info`).
/// Behind a reverse proxy with `X-Forwarded-For`, the per-connection
/// IP is the proxy's — fine for in-cluster deploys, less useful for
/// public exposure (reverse-proxy-aware client IP extraction is a
/// follow-up).
async fn rate_limit(
    State(state): State<AppState>,
    addr: Option<ConnectInfo<SocketAddr>>,
    request: Request<axum::body::Body>,
    next: Next,
) -> Result<Response, ApiError> {
    let Some(limiter) = state.rate_limit.as_deref() else {
        return Ok(next.run(request).await);
    };
    let client_ip = client_ip_for_rate_limit(
        state.trust_forwarded_for,
        &state.trusted_proxy_cidrs,
        &request,
        addr,
    );
    let Some(ip) = client_ip else {
        // No usable IP source (no ConnectInfo, no trusted XFF). Tests
        // hit this path; production always has ConnectInfo.
        return Ok(next.run(request).await);
    };
    if !limiter.check(ip) {
        return Err(ApiError::new(
            StatusCode::TOO_MANY_REQUESTS,
            "rate_limited",
            "request rate exceeded for this client",
        ));
    }
    Ok(next.run(request).await)
}

/// Resolve the client IP for rate-limiting purposes.
///
/// When `trust_xff` is set, the first `X-Forwarded-For` IP becomes the
/// rate-limiting key — but only if the per-connection peer is inside
/// `trusted_cidrs` (or the allowlist is empty, in which case XFF is
/// trusted unconditionally). Anything outside the allowlist falls back
/// to the per-connection peer, defeating XFF spoofing from arbitrary
/// internet hosts.
///
/// Without `trust_xff` an attacker can forge `X-Forwarded-For` and
/// bypass the bucket; the CIDR allowlist tightens that opt-in.
fn client_ip_for_rate_limit(
    trust_xff: bool,
    trusted_cidrs: &[TrustedCidr],
    request: &Request<axum::body::Body>,
    connect_info: Option<ConnectInfo<SocketAddr>>,
) -> Option<std::net::IpAddr> {
    let peer_ip = connect_info.map(|ConnectInfo(sa)| sa.ip());
    if trust_xff {
        let peer_trusted = match peer_ip {
            Some(ip) if !trusted_cidrs.is_empty() => trusted_cidrs.iter().any(|c| c.contains(ip)),
            // No CIDR allowlist configured — trust XFF unconditionally
            // (the trust_forwarded_for=true contract). Tests with no
            // ConnectInfo also hit this branch.
            _ => trusted_cidrs.is_empty(),
        };
        if peer_trusted {
            if let Some(value) = request.headers().get("x-forwarded-for") {
                if let Ok(s) = value.to_str() {
                    if let Some(first) = s.split(',').next() {
                        if let Ok(ip) = first.trim().parse::<std::net::IpAddr>() {
                            return Some(ip);
                        }
                    }
                }
            }
        }
    }
    peer_ip
}

/// Reject requests that don't carry the configured `X-API-Key` header
/// (or `?api_key=…`). When `state.api_key` is `None`, the layer is a
/// no-op so OSS deploys without auth keep working.
async fn require_api_key(
    State(state): State<AppState>,
    request: Request<axum::body::Body>,
    next: Next,
) -> Result<Response, ApiError> {
    let Some(expected) = state.api_key.as_deref() else {
        return Ok(next.run(request).await);
    };

    let header = request
        .headers()
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let query_key = request.uri().query().and_then(|q| {
        q.split('&').find_map(|kv| {
            let (k, v) = kv.split_once('=')?;
            (k == "api_key").then(|| v.to_string())
        })
    });

    let provided = header.as_deref().or(query_key.as_deref()).unwrap_or("");
    if provided != expected {
        return Err(ApiError::unauthorized(
            "missing_or_invalid_api_key",
            "set X-API-Key header or ?api_key= query parameter",
        ));
    }
    Ok(next.run(request).await)
}

#[derive(Serialize)]
struct StatusBody {
    status: &'static str,
}

async fn healthz() -> Json<StatusBody> {
    Json(StatusBody { status: "ok" })
}

/// Per-component readiness body. `ready: true` only when the
/// minimum-viable set is loaded (currently just the text index).
/// `components` reports each subsystem so operators can tell which
/// piece is missing on a degraded bundle.
#[derive(Serialize)]
struct ReadyBody {
    ready: bool,
    bundle_id: String,
    components: ReadyComponents,
}

#[derive(Serialize)]
struct ReadyComponents {
    text: bool,
    admin: bool,
    nearest: bool,
}

async fn readyz(State(state): State<AppState>) -> (StatusCode, Json<ReadyBody>) {
    let components = ReadyComponents {
        text: state.text().is_some(),
        admin: state.admin().is_some(),
        nearest: state.nearest().is_some(),
    };
    let ready = components.text;
    let status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    let body = ReadyBody {
        ready,
        bundle_id: state.metrics.bundle_id.clone(),
        components,
    };
    (status, Json(body))
}

#[derive(Serialize)]
struct InfoBody {
    bundle_id: String,
    /// Length 1 for single-bundle deploys, >1 when this serve
    /// process is federating several bundles. Operators inspecting
    /// `/v1/info` can confirm which shards a node is fronting.
    bundle_ids: Vec<String>,
    /// Number of bundles being served (== `bundle_ids.len()`).
    bundle_count: usize,
    started_at_unix: u64,
    uptime_seconds: u64,
    admin_features: u64,
    point_count: u64,
    bundle_path: String,
    auth_required: bool,
    rate_limited: bool,
}

/// `/v1/info` — operational metadata about this serve process. Useful
/// for clients that want to confirm which bundle they're talking to,
/// how long it's been up, and whether auth / rate limiting are active.
/// Does not require an API key (treated like /healthz so probes can
/// hit it without credentials).
/// Phase 7a-U — load bundle indices from disk into a [`BundleSnapshot`].
/// Shared between cairn-serve startup and the `/admin/reload` handler.
/// Single-bundle path; federated multi-bundle reload needs the `--bundles`
/// list passed in (out of scope for the per-process reload endpoint).
pub fn load_bundle_snapshot(path: &std::path::Path) -> Result<BundleSnapshot, std::io::Error> {
    let manifest = cairn_tile::read_manifest(&path.join("manifest.toml")).ok();

    let text_dir = path.join("index/text");
    let text = if text_dir.exists() {
        let idx = cairn_text::TextIndex::open(&text_dir).map_err(io_err)?;
        Some(Arc::new(FederatedText::from_single(Arc::new(idx))))
    } else {
        None
    };

    let admin = match manifest.as_ref() {
        Some(m) if !m.admin_tiles.is_empty() => {
            let entries = m.admin_tiles.clone();
            let idx = cairn_spatial::AdminIndex::open(path, entries);
            Some(Arc::new(FederatedAdmin::from_single(Arc::new(idx))))
        }
        _ => None,
    };

    let nearest = match manifest.as_ref() {
        Some(m) if !m.point_tiles.is_empty() => {
            let entries = m.point_tiles.clone();
            let idx = cairn_spatial::NearestIndex::open(path, entries);
            Some(Arc::new(FederatedNearest::from_single(Arc::new(idx))))
        }
        _ => None,
    };

    let bundle_id = manifest
        .as_ref()
        .map(|m| m.bundle_id.clone())
        .unwrap_or_else(|| "unknown".into());

    Ok(BundleSnapshot {
        text,
        admin,
        nearest,
        bundle_ids: vec![bundle_id],
    })
}

fn io_err<E: std::fmt::Display>(e: E) -> std::io::Error {
    std::io::Error::other(e.to_string())
}

#[derive(Serialize)]
struct ReloadResponse {
    previous_bundle_ids: Vec<String>,
    current_bundle_ids: Vec<String>,
    duration_ms: u128,
}

/// `POST /admin/reload` — Phase 7a-U bundle hot-reload. Re-reads the
/// bundle from `state.bundle_path` (operator orchestrates the symlink
/// or directory swap externally) and atomically installs the new
/// indices via [`AppState::install_bundle`]. In-flight requests
/// finish on the previous snapshot; new requests pick up the new
/// bundle on their next [`AppState::text`] / `admin` / `nearest` call.
async fn admin_reload(State(state): State<AppState>) -> Response {
    let started = Instant::now();
    let path = (*state.bundle_path).clone();
    let snapshot = match tokio::task::spawn_blocking(move || load_bundle_snapshot(&path)).await {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => {
            error!(error = %e, "admin_reload: load failed");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": "reload_load_failed",
                    "detail": e.to_string(),
                })),
            )
                .into_response();
        }
        Err(e) => {
            error!(error = %e, "admin_reload: blocking task panicked");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": "reload_task_panicked",
                    "detail": e.to_string(),
                })),
            )
                .into_response();
        }
    };
    let new_ids = snapshot.bundle_ids.clone();
    let prev = state.install_bundle(snapshot);
    let elapsed_ms = started.elapsed().as_millis();
    tracing::info!(
        previous = ?prev,
        current = ?new_ids,
        elapsed_ms,
        "admin_reload: bundle swapped"
    );
    Json(ReloadResponse {
        previous_bundle_ids: (*prev).clone(),
        current_bundle_ids: new_ids,
        duration_ms: elapsed_ms,
    })
    .into_response()
}

async fn info(State(state): State<AppState>) -> Json<InfoBody> {
    let started_unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
        .saturating_sub(state.metrics.started.elapsed().as_secs());
    Json(InfoBody {
        bundle_id: state.metrics.bundle_id.clone(),
        bundle_ids: (*state.bundle_ids()).clone(),
        bundle_count: state.bundle_ids().len(),
        started_at_unix: started_unix,
        uptime_seconds: state.metrics.started.elapsed().as_secs(),
        admin_features: state.metrics.admin_features,
        point_count: state.metrics.point_count,
        bundle_path: state.bundle_path.display().to_string(),
        auth_required: state.api_key.is_some(),
        rate_limited: state.rate_limit.is_some(),
    })
}

/// `/v1/sbom` — serve the bundle's CycloneDX 1.5 SBOM (written by
/// `cairn-build` at bundle creation time). Returns 404 when the
/// bundle predates the SBOM emitter; payload is application/
/// vnd.cyclonedx+json so dependency-track / grype / cyclonedx-cli
/// recognize it without sniffing.
async fn sbom(State(state): State<AppState>) -> Response {
    // `bundle_path` is the operator-controlled `--bundle` CLI arg
    // (set at process startup); the path component appended here is
    // the constant `"sbom.json"`. No user request data participates,
    // so this is not a path-injection sink despite the static-analysis
    // signal — codeql/rust/path-injection on this line is a known FP.
    const SBOM_FILENAME: &str = "sbom.json";
    let path = state.bundle_path.join(SBOM_FILENAME);
    let body = match std::fs::read(&path) {
        Ok(b) => b,
        Err(_) => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({
                    "error": "sbom_not_found",
                    "hint": "rebuild bundle with a recent cairn-build to populate sbom.json"
                })),
            )
                .into_response();
        }
    };
    Response::builder()
        .header(header::CONTENT_TYPE, "application/vnd.cyclonedx+json")
        .body(axum::body::Body::from(body))
        .unwrap_or_else(|_| {
            Json(serde_json::json!({"error": "sbom_response_build_failed"})).into_response()
        })
}

#[derive(Deserialize, Default)]
pub struct SearchQuery {
    #[serde(default)]
    pub q: String,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub mode: Option<String>,
    #[serde(default)]
    pub fuzzy: Option<u8>,
    /// Comma-separated kinds (e.g. `country,city`).
    #[serde(default)]
    pub layer: Option<String>,
    #[serde(default, rename = "focus.lat")]
    pub focus_lat: Option<f64>,
    #[serde(default, rename = "focus.lon")]
    pub focus_lon: Option<f64>,
    #[serde(default, rename = "focus.weight")]
    pub focus_weight: Option<f64>,
    /// Preferred language code (`de`, `fr`, `en`, …). Hits with a
    /// localized name in this language get a small score boost.
    #[serde(default)]
    pub lang: Option<String>,
    /// Comma-separated category filter (e.g. `health,hospital`).
    /// OR semantics across the list.
    #[serde(default)]
    pub categories: Option<String>,
    /// Pelias-spec viewport rect — hard-clip results outside the rect.
    /// All four required together; missing any = ignored.
    #[serde(default, rename = "boundary.rect.min_lat")]
    pub bbox_min_lat: Option<f64>,
    #[serde(default, rename = "boundary.rect.min_lon")]
    pub bbox_min_lon: Option<f64>,
    #[serde(default, rename = "boundary.rect.max_lat")]
    pub bbox_max_lat: Option<f64>,
    #[serde(default, rename = "boundary.rect.max_lon")]
    pub bbox_max_lon: Option<f64>,
    /// Opt-in DoubleMetaphone phonetic OR-clause. Recovers misspelled
    /// queries (`Smyth → Smith`, `Mueller → Müller`).
    #[serde(default)]
    pub phonetic: Option<bool>,
    /// Opt-in lexical-vector semantic rerank. Boosts hits whose
    /// name shares character-trigram structure with the query
    /// (`Vienna → Viennese`, `Trisenberg → Triesenberg`). See
    /// `cairn_text::semantic`.
    #[serde(default)]
    pub semantic: Option<bool>,
    /// When `true`, run the free-text query through
    /// `cairn_parse::parse` first; if the parser returns at least one
    /// useful structured component (postcode / city / country) those
    /// components are echoed in the response under `parsed` and used
    /// to derive an extra `?categories=postal` filter when a postcode
    /// dominates. With the `libpostal` cargo feature enabled this
    /// jumps quality on non-English addresses; without it, the
    /// heuristic parser handles common Latin-script shapes.
    #[serde(default)]
    pub autoparse: Option<bool>,
    /// When `true`, every result includes an `explain` block listing
    /// the BM25 baseline plus each rerank multiplier. Off by default
    /// to keep payloads small. Useful for debugging ranking
    /// surprises and for clients that want to surface "why this hit
    /// ranked here".
    #[serde(default)]
    pub explain: Option<bool>,
    /// Phase 7a-Q — temporal validity filter. `?valid_at=YYYY` (also
    /// negative for BC) returns only places whose
    /// `start_date`..=`end_date` window covers the given year. Places
    /// without OSM date tags pass every filter. No incumbent
    /// geocoder ships this today — surfaces historical names like
    /// `Königsberg` (1939) → Kaliningrad (modern).
    #[serde(default)]
    pub valid_at: Option<i64>,
}

#[derive(Serialize)]
struct SearchResponse<'a> {
    query: &'a str,
    mode: &'a str,
    /// When `?autoparse=true` was set and the parser found at least
    /// one structured component, echo it back so callers can see
    /// what the query was decomposed into.
    #[serde(skip_serializing_if = "Option::is_none")]
    parsed: Option<cairn_parse::ParsedAddress>,
    results: Vec<Hit>,
}

async fn search(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Query(params): Query<SearchQuery>,
) -> impl IntoResponse {
    let q = params.q.trim();
    if q.is_empty() {
        state.metrics.bad_request.fetch_add(1, Ordering::Relaxed);
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "query parameter 'q' is required" })),
        )
            .into_response();
    }

    let mode_label = params.mode.as_deref().unwrap_or("search").to_string();
    let mode = match mode_label.as_str() {
        "autocomplete" => SearchMode::Autocomplete,
        _ => SearchMode::Search,
    };
    let layers = params
        .layer
        .as_deref()
        .map(|s| {
            s.split(',')
                .map(|p| p.trim().to_lowercase())
                .filter(|p| !p.is_empty())
                .map(normalize_layer)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let mut categories = params
        .categories
        .as_deref()
        .map(|s| {
            s.split(',')
                .map(|p| p.trim().to_lowercase())
                .filter(|p| !p.is_empty())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    // Optional autoparse: run free-text through the address parser
    // (heuristic by default, libpostal CRF when the feature is on),
    // echo structured fields back to caller, and bias the search
    // when the parser surfaces high-signal components. Right now we
    // promote the postcode → `categories=postal` filter when no
    // explicit categories were passed AND the parser found a
    // postcode but no road — typical "9490" or "9490 Vaduz" lookup
    // shape. Future extensions: layer hint when country is recognized.
    let parsed = if params.autoparse.unwrap_or(false) {
        match cairn_parse::parse(q) {
            Ok(p) => {
                let postcode_only = p.postcode.is_some() && p.road.is_none();
                if postcode_only && categories.is_empty() {
                    categories.push("postal".into());
                }
                Some(p)
            }
            Err(_) => None,
        }
    } else {
        None
    };

    let focus = match (params.focus_lat, params.focus_lon) {
        (Some(lat), Some(lon)) => Some(Coord { lon, lat }),
        _ => None,
    };
    let bbox = match (
        params.bbox_min_lon,
        params.bbox_min_lat,
        params.bbox_max_lon,
        params.bbox_max_lat,
    ) {
        (Some(min_lon), Some(min_lat), Some(max_lon), Some(max_lat)) => Some(Bbox {
            min_lon,
            min_lat,
            max_lon,
            max_lat,
        }),
        _ => None,
    };

    let opts = SearchOptions {
        mode,
        limit: params.limit.unwrap_or(10).clamp(1, 100),
        fuzzy: params.fuzzy.unwrap_or(0),
        layers,
        focus,
        focus_weight: params.focus_weight.unwrap_or(0.5),
        prefer_lang: params
            .lang
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string),
        categories,
        bbox,
        phonetic: params.phonetic.unwrap_or(false),
        semantic: params.semantic.unwrap_or(false),
        explain: params.explain.unwrap_or(false),
        valid_at: params.valid_at,
    };

    let text = match state.text() {
        Some(t) => t,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({ "error": "text index not loaded" })),
            )
                .into_response();
        }
    };

    match text.search(q, &opts) {
        Ok(results) => {
            match mode {
                SearchMode::Search => state.metrics.search_ok.fetch_add(1, Ordering::Relaxed),
                SearchMode::Autocomplete => state
                    .metrics
                    .autocomplete_ok
                    .fetch_add(1, Ordering::Relaxed),
            };
            if wants_ndjson(&headers) {
                // `limit` is clamped to 1..=100 upstream
                // (search-options builder); the `min(MAX_HITS_FOR_PRE_ALLOC)`
                // here is belt-and-braces so a future change that raises
                // the upstream cap can't accidentally let user input
                // dictate an unbounded allocation.
                const BYTES_PER_HIT: usize = 256;
                const MAX_HITS_FOR_PRE_ALLOC: usize = 1_024;
                let body_capacity = results
                    .len()
                    .min(MAX_HITS_FOR_PRE_ALLOC)
                    .saturating_mul(BYTES_PER_HIT);
                let mut body = String::with_capacity(body_capacity);
                for hit in &results {
                    if let Ok(line) = serde_json::to_string(hit) {
                        body.push_str(&line);
                        body.push('\n');
                    }
                }
                axum::response::Response::builder()
                    .header(axum::http::header::CONTENT_TYPE, "application/x-ndjson")
                    .body(axum::body::Body::from(body))
                    .unwrap_or_else(|_| {
                        Json(serde_json::json!({"error": "ndjson_build_failed"})).into_response()
                    })
            } else {
                Json(SearchResponse {
                    query: q,
                    mode: &mode_label,
                    parsed,
                    results,
                })
                .into_response()
            }
        }
        Err(err) => {
            state.metrics.search_err.fetch_add(1, Ordering::Relaxed);
            error!(?err, q, mode = %mode_label, "search failed");
            map_err(err).into_response()
        }
    }
}

#[derive(Deserialize, Default)]
pub struct ReverseQuery {
    pub lat: Option<f64>,
    pub lon: Option<f64>,
    #[serde(default)]
    pub limit: Option<usize>,
    /// When PIP returns no admin features, fall back to the K nearest
    /// place centroids. 0 disables fallback.
    #[serde(default)]
    pub nearest: Option<usize>,
    /// Phase 7a-H — `?context=full` returns a structured response
    /// with the containing admin chain plus the nearest road, the
    /// nearest POI, and the nearest address all in one call. Default
    /// (None / "min") keeps the legacy single-list shape.
    #[serde(default)]
    pub context: Option<String>,
}

#[derive(Serialize)]
struct ReverseHit {
    place_id: u64,
    name: String,
    kind: String,
    level: u8,
    lon: f64,
    lat: f64,
    admin_path: Vec<u64>,
    distance_km: f64,
}

#[derive(Serialize)]
struct ReverseResponse {
    lat: f64,
    lon: f64,
    source: &'static str,
    results: Vec<ReverseHit>,
}

/// Phase 7a-H — structured reverse response combining admin chain
/// with the nearest road / POI / address in a single call.
#[derive(Serialize)]
struct ReverseFullResponse {
    lat: f64,
    lon: f64,
    /// Containing admin features finest-first (city → region → country).
    /// Empty when the probe falls outside every admin polygon.
    admin: Vec<ReverseHit>,
    /// Nearest place with `kind == "street"`. None when no roads in bundle.
    #[serde(skip_serializing_if = "Option::is_none")]
    nearest_road: Option<ReverseHit>,
    /// Nearest POI (kind == "poi" / "amenity" / shop / leisure / …).
    #[serde(skip_serializing_if = "Option::is_none")]
    nearest_poi: Option<ReverseHit>,
    /// Nearest address point (kind == "address"). Spotty coverage outside
    /// OpenAddresses imports.
    #[serde(skip_serializing_if = "Option::is_none")]
    nearest_address: Option<ReverseHit>,
}

/// Categorize a `kind` string into the three classes the
/// context-aware reverse endpoint exposes. Single source of truth so
/// the API layer + future filters agree.
fn classify_kind(kind: &str) -> Option<&'static str> {
    match kind {
        "street" | "highway" | "road" => Some("street"),
        "address" => Some("address"),
        "poi" | "amenity" | "shop" | "tourism" | "leisure" | "office" | "historic"
        | "healthcare" | "emergency" | "craft" => Some("poi"),
        _ => None,
    }
}

fn reverse_full(state: AppState, lat: f64, lon: f64, probe: Coord) -> Response {
    state.metrics.reverse_pip.fetch_add(1, Ordering::Relaxed);

    // Admin chain (PIP) — finest-first. Empty when probe is outside
    // every admin polygon, which is fine; the nearest-by-class
    // companions still answer.
    let admin_chain: Vec<ReverseHit> = state
        .admin()
        .map(|a| a.point_in_polygon(probe))
        .unwrap_or_default()
        .into_iter()
        .map(|f| ReverseHit {
            place_id: f.place_id,
            name: f.name.clone(),
            kind: f.kind.clone(),
            level: f.level,
            lon: f.centroid.lon,
            lat: f.centroid.lat,
            admin_path: f.admin_path.clone(),
            distance_km: haversine_km(lat, lon, f.centroid.lat, f.centroid.lon),
        })
        .collect();

    // Nearest-by-class. Each query is independent against the same
    // R*-tree; the filtered nearest_k_filtered widens slot coverage
    // automatically when the predicate is selective.
    let nearest_for = |target: &'static str| -> Option<ReverseHit> {
        let nearest = state.nearest()?;
        let pp = nearest
            .nearest_k_filtered(probe, 1, |p| classify_kind(&p.kind) == Some(target))
            .into_iter()
            .next()?;
        Some(ReverseHit {
            place_id: pp.place_id,
            name: pp.name.clone(),
            kind: pp.kind.clone(),
            level: pp.level,
            lon: pp.centroid.lon,
            lat: pp.centroid.lat,
            admin_path: pp.admin_path.clone(),
            distance_km: haversine_km(lat, lon, pp.centroid.lat, pp.centroid.lon),
        })
    };

    Json(ReverseFullResponse {
        lat,
        lon,
        admin: admin_chain,
        nearest_road: nearest_for("street"),
        nearest_poi: nearest_for("poi"),
        nearest_address: nearest_for("address"),
    })
    .into_response()
}

async fn reverse(
    State(state): State<AppState>,
    Query(params): Query<ReverseQuery>,
) -> impl IntoResponse {
    let (lat, lon) = match (params.lat, params.lon) {
        (Some(lat), Some(lon)) => (lat, lon),
        _ => {
            state.metrics.bad_request.fetch_add(1, Ordering::Relaxed);
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": "lat and lon are required"
                })),
            )
                .into_response();
        }
    };
    if !(-90.0..=90.0).contains(&lat) || !(-180.0..=180.0).contains(&lon) {
        state.metrics.bad_request.fetch_add(1, Ordering::Relaxed);
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "lat must be in [-90, 90] and lon in [-180, 180]"
            })),
        )
            .into_response();
    }

    let limit = params.limit.unwrap_or(10).clamp(1, 50);
    let nearest_k = params.nearest.unwrap_or(0).min(50);
    let probe = Coord { lon, lat };

    // Phase 7a-H — `?context=full` short-circuits the legacy
    // single-list response and returns admin chain + nearest road
    // + nearest POI + nearest address in one structured payload.
    if matches!(params.context.as_deref(), Some("full")) {
        return reverse_full(state, lat, lon, probe).into_response();
    }

    // PIP path first.
    if let Some(admin) = state.admin() {
        let matches = admin.point_in_polygon(probe);
        if !matches.is_empty() {
            state.metrics.reverse_pip.fetch_add(1, Ordering::Relaxed);
            let results: Vec<ReverseHit> = matches
                .iter()
                .take(limit)
                .map(|f| ReverseHit {
                    place_id: f.place_id,
                    name: f.name.clone(),
                    kind: f.kind.clone(),
                    level: f.level,
                    lon: f.centroid.lon,
                    lat: f.centroid.lat,
                    admin_path: f.admin_path.clone(),
                    distance_km: haversine_km(lat, lon, f.centroid.lat, f.centroid.lon),
                })
                .collect();
            return Json(ReverseResponse {
                lat,
                lon,
                source: "pip",
                results,
            })
            .into_response();
        }
    }

    // Fallback: nearest-K centroid query.
    if nearest_k > 0 {
        if let Some(nearest) = state.nearest() {
            state
                .metrics
                .reverse_nearest
                .fetch_add(1, Ordering::Relaxed);
            let hits = nearest.nearest_k(probe, nearest_k.min(limit));
            let results: Vec<ReverseHit> = hits
                .into_iter()
                .map(|p| ReverseHit {
                    place_id: p.place_id,
                    name: p.name.clone(),
                    kind: p.kind.clone(),
                    level: p.level,
                    lon: p.centroid.lon,
                    lat: p.centroid.lat,
                    admin_path: p.admin_path.clone(),
                    distance_km: haversine_km(lat, lon, p.centroid.lat, p.centroid.lon),
                })
                .collect();
            return Json(ReverseResponse {
                lat,
                lon,
                source: "nearest",
                results,
            })
            .into_response();
        }
    }

    if state.admin().is_none() && state.nearest().is_none() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "error": "no spatial layer loaded"
            })),
        )
            .into_response();
    }

    state.metrics.reverse_empty.fetch_add(1, Ordering::Relaxed);
    Json(ReverseResponse {
        lat,
        lon,
        source: "pip",
        results: Vec::new(),
    })
    .into_response()
}

fn haversine_km(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const EARTH_KM: f64 = 6371.0088;
    let to_rad = std::f64::consts::PI / 180.0;
    let phi1 = lat1 * to_rad;
    let phi2 = lat2 * to_rad;
    let dphi = (lat2 - lat1) * to_rad;
    let dlam = (lon2 - lon1) * to_rad;
    let a = (dphi / 2.0).sin().powi(2) + phi1.cos() * phi2.cos() * (dlam / 2.0).sin().powi(2);
    2.0 * EARTH_KM * a.sqrt().asin()
}

#[derive(Deserialize, Default)]
pub struct StructuredQuery {
    #[serde(default)]
    pub house_number: Option<String>,
    #[serde(default)]
    pub road: Option<String>,
    #[serde(default)]
    pub unit: Option<String>,
    #[serde(default)]
    pub postcode: Option<String>,
    #[serde(default)]
    pub city: Option<String>,
    #[serde(default)]
    pub district: Option<String>,
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub country: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default, rename = "focus.lat")]
    pub focus_lat: Option<f64>,
    #[serde(default, rename = "focus.lon")]
    pub focus_lon: Option<f64>,
    #[serde(default, rename = "focus.weight")]
    pub focus_weight: Option<f64>,
}

#[derive(Serialize)]
struct StructuredResponse<'a> {
    query: &'a str,
    layer_hint: &'a str,
    results: Vec<Hit>,
}

#[derive(Deserialize, Default)]
pub struct ParseQuery {
    #[serde(default)]
    pub q: String,
}

#[derive(Serialize)]
struct ParseResponse {
    query: String,
    parsed: cairn_parse::ParsedAddress,
}

async fn parse_addr(Query(params): Query<ParseQuery>) -> impl IntoResponse {
    let q = params.q.trim();
    if q.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "q required" })),
        )
            .into_response();
    }
    match cairn_parse::parse(q) {
        Ok(parsed) => Json(ParseResponse {
            query: q.to_string(),
            parsed,
        })
        .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": err.to_string() })),
        )
            .into_response(),
    }
}

#[derive(Serialize)]
struct ExpandResponse {
    query: String,
    expansions: Vec<String>,
}

async fn expand_addr(Query(params): Query<ParseQuery>) -> impl IntoResponse {
    let q = params.q.trim();
    if q.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "q required" })),
        )
            .into_response();
    }
    Json(ExpandResponse {
        query: q.to_string(),
        expansions: cairn_parse::expand(q),
    })
    .into_response()
}

async fn structured(
    State(state): State<AppState>,
    Query(params): Query<StructuredQuery>,
) -> impl IntoResponse {
    // Concatenate all non-empty parts in finest → coarsest order.
    let parts: Vec<&str> = [
        params.house_number.as_deref(),
        params.road.as_deref(),
        params.unit.as_deref(),
        params.postcode.as_deref(),
        params.city.as_deref(),
        params.district.as_deref(),
        params.region.as_deref(),
        params.country.as_deref(),
    ]
    .into_iter()
    .flatten()
    .map(str::trim)
    .filter(|s| !s.is_empty())
    .collect();

    if parts.is_empty() {
        state.metrics.bad_request.fetch_add(1, Ordering::Relaxed);
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "at least one structured field is required"
            })),
        )
            .into_response();
    }
    let query = parts.join(" ");

    // Layer hint: pick the finest non-empty field as the kind filter.
    // Address > Street > City > Region > Country — empty layer if none of
    // those signals are present (e.g. only postcode given).
    let (layer_hint, layers): (&str, Vec<String>) = if params
        .house_number
        .as_deref()
        .map(|s| !s.trim().is_empty())
        .unwrap_or(false)
    {
        ("address", vec!["address".into()])
    } else if params
        .road
        .as_deref()
        .map(|s| !s.trim().is_empty())
        .unwrap_or(false)
    {
        ("street", vec!["street".into()])
    } else if params
        .city
        .as_deref()
        .map(|s| !s.trim().is_empty())
        .unwrap_or(false)
    {
        ("city", vec!["city".into()])
    } else if params
        .region
        .as_deref()
        .map(|s| !s.trim().is_empty())
        .unwrap_or(false)
    {
        ("region", vec!["region".into()])
    } else if params
        .country
        .as_deref()
        .map(|s| !s.trim().is_empty())
        .unwrap_or(false)
    {
        ("country", vec!["country".into()])
    } else {
        ("any", Vec::new())
    };

    let focus = match (params.focus_lat, params.focus_lon) {
        (Some(lat), Some(lon)) => Some(Coord { lon, lat }),
        _ => None,
    };

    let opts = SearchOptions {
        mode: SearchMode::Search,
        limit: params.limit.unwrap_or(10).clamp(1, 100),
        fuzzy: 0,
        layers,
        focus,
        focus_weight: params.focus_weight.unwrap_or(0.5),
        prefer_lang: None,
        categories: Vec::new(),
        bbox: None,
        phonetic: false,
        semantic: false,
        explain: false,
        valid_at: None,
    };

    let text = match state.text() {
        Some(t) => t,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({ "error": "text index not loaded" })),
            )
                .into_response();
        }
    };

    match text.search(&query, &opts) {
        Ok(results) => {
            state.metrics.structured_ok.fetch_add(1, Ordering::Relaxed);
            Json(StructuredResponse {
                query: &query,
                layer_hint,
                results,
            })
            .into_response()
        }
        Err(err) => {
            state.metrics.structured_err.fetch_add(1, Ordering::Relaxed);
            error!(?err, query, "structured search failed");
            map_err(err).into_response()
        }
    }
}

async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    let m = state.metrics.as_ref();
    let uptime = m.started.elapsed().as_secs();
    let body = format!(
        "# HELP cairn_uptime_seconds Seconds since cairn-serve started.\n\
         # TYPE cairn_uptime_seconds gauge\n\
         cairn_uptime_seconds{{bundle_id=\"{bundle}\"}} {uptime}\n\
         # HELP cairn_admin_features Number of admin polygons loaded.\n\
         # TYPE cairn_admin_features gauge\n\
         cairn_admin_features{{bundle_id=\"{bundle}\"}} {admin}\n\
         # HELP cairn_point_count Number of place centroids in the nearest fallback layer.\n\
         # TYPE cairn_point_count gauge\n\
         cairn_point_count{{bundle_id=\"{bundle}\"}} {points}\n\
         # HELP cairn_requests_total HTTP requests handled, by endpoint and outcome.\n\
         # TYPE cairn_requests_total counter\n\
         cairn_requests_total{{endpoint=\"search\",outcome=\"ok\"}} {search_ok}\n\
         cairn_requests_total{{endpoint=\"search\",outcome=\"err\"}} {search_err}\n\
         cairn_requests_total{{endpoint=\"autocomplete\",outcome=\"ok\"}} {autocomplete_ok}\n\
         cairn_requests_total{{endpoint=\"structured\",outcome=\"ok\"}} {structured_ok}\n\
         cairn_requests_total{{endpoint=\"structured\",outcome=\"err\"}} {structured_err}\n\
         cairn_requests_total{{endpoint=\"reverse\",outcome=\"pip\"}} {reverse_pip}\n\
         cairn_requests_total{{endpoint=\"reverse\",outcome=\"nearest\"}} {reverse_nearest}\n\
         cairn_requests_total{{endpoint=\"reverse\",outcome=\"empty\"}} {reverse_empty}\n\
         cairn_requests_total{{endpoint=\"any\",outcome=\"bad_request\"}} {bad}\n",
        bundle = m.bundle_id,
        uptime = uptime,
        admin = m.admin_features,
        points = m.point_count,
        search_ok = m.search_ok.load(Ordering::Relaxed),
        search_err = m.search_err.load(Ordering::Relaxed),
        autocomplete_ok = m.autocomplete_ok.load(Ordering::Relaxed),
        structured_ok = m.structured_ok.load(Ordering::Relaxed),
        structured_err = m.structured_err.load(Ordering::Relaxed),
        reverse_pip = m.reverse_pip.load(Ordering::Relaxed),
        reverse_nearest = m.reverse_nearest.load(Ordering::Relaxed),
        reverse_empty = m.reverse_empty.load(Ordering::Relaxed),
        bad = m.bad_request.load(Ordering::Relaxed),
    );
    (
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        body,
    )
}

fn map_err(err: TextError) -> (StatusCode, Json<serde_json::Value>) {
    let (status, msg) = match &err {
        TextError::Query(_) => (StatusCode::BAD_REQUEST, format!("{err}")),
        _ => (StatusCode::INTERNAL_SERVER_ERROR, format!("{err}")),
    };
    (status, Json(serde_json::json!({ "error": msg })))
}

/// True when the request's `Accept` header asks for newline-delimited
/// JSON. Looks for an exact `application/x-ndjson` token; comma-
/// separated accept lists with that token anywhere also match. Quality
/// values are ignored — clients that want NDJSON will set it
/// explicitly. Falls back to JSON for anything else (including missing
/// header).
fn wants_ndjson(headers: &axum::http::HeaderMap) -> bool {
    headers
        .get(axum::http::header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .map(|s| {
            s.split(',').any(|tok| {
                tok.trim().split(';').next().unwrap_or("").trim() == "application/x-ndjson"
            })
        })
        .unwrap_or(false)
}

/// Map common Pelias / WhosOnFirst layer name aliases onto Cairn's
/// canonical kind tokens used inside the tantivy `kind` field. Anything
/// without an alias passes through unchanged so the call site stays
/// strict-by-default.
fn normalize_layer(s: String) -> String {
    match s.as_str() {
        "postalcode" | "postal_code" | "zip" | "zipcode" => "postcode".into(),
        "venue" | "venues" => "poi".into(),
        "locality" => "city".into(),
        "macroregion" => "region".into(),
        "localadmin" => "city".into(),
        _ => s,
    }
}

// ========================================================================
// Pelias-compatible shim
//
// Maps a subset of Pelias query params + response shape onto Cairn's
// internal handlers so that clients written against Pelias can drop in
// without code changes. Subset:
//   * /search             — forward search
//   * /v1/autocomplete    — prefix autocomplete
//   * /reverse            — point-in-polygon
// Full Pelias compat (place lookup, structured query, geocoding response
// metadata) lands in a follow-up phase if needed.
// ========================================================================

#[derive(Deserialize, Serialize, Default, Debug)]
struct PeliasSearchQuery {
    #[serde(default)]
    text: String,
    #[serde(default)]
    size: Option<usize>,
    #[serde(default)]
    layers: Option<String>,
    #[serde(default, rename = "focus.point.lat")]
    focus_lat: Option<f64>,
    #[serde(default, rename = "focus.point.lon")]
    focus_lon: Option<f64>,
    /// Preferred language code (Pelias spec also supports `lang=`).
    #[serde(default)]
    lang: Option<String>,
    /// Comma-separated category filter (Pelias-compatible). OR
    /// semantics across the list.
    #[serde(default)]
    categories: Option<String>,
    #[serde(default, rename = "boundary.rect.min_lat")]
    bbox_min_lat: Option<f64>,
    #[serde(default, rename = "boundary.rect.min_lon")]
    bbox_min_lon: Option<f64>,
    #[serde(default, rename = "boundary.rect.max_lat")]
    bbox_max_lat: Option<f64>,
    #[serde(default, rename = "boundary.rect.max_lon")]
    bbox_max_lon: Option<f64>,
    #[serde(default)]
    phonetic: Option<bool>,
    #[serde(default)]
    semantic: Option<bool>,
}

#[derive(Deserialize, Serialize, Default, Debug)]
struct PeliasReverseQuery {
    #[serde(default, rename = "point.lat")]
    lat: Option<f64>,
    #[serde(default, rename = "point.lon")]
    lon: Option<f64>,
    #[serde(default)]
    size: Option<usize>,
}

#[derive(Serialize)]
struct PeliasFeatureCollection<'a> {
    geocoding: PeliasGeocodingMeta<'a>,
    #[serde(rename = "type")]
    kind: &'static str,
    features: Vec<PeliasFeature>,
}

#[derive(Serialize)]
struct PeliasGeocodingMeta<'a> {
    version: &'static str,
    attribution: &'static str,
    engine: PeliasEngine<'a>,
    query: serde_json::Value,
}

#[derive(Serialize)]
struct PeliasEngine<'a> {
    name: &'static str,
    bundle_id: &'a str,
}

#[derive(Serialize)]
struct PeliasFeature {
    #[serde(rename = "type")]
    kind: &'static str,
    geometry: PeliasGeometry,
    properties: PeliasProperties,
}

#[derive(Serialize)]
struct PeliasGeometry {
    #[serde(rename = "type")]
    kind: &'static str,
    coordinates: [f64; 2],
}

#[derive(Serialize)]
struct PeliasProperties {
    id: String,
    layer: String,
    name: String,
    label: String,
    confidence: f32,
    #[serde(skip_serializing_if = "Option::is_none")]
    distance_km: Option<f64>,
}

fn hit_to_pelias_feature(h: Hit) -> PeliasFeature {
    // Prefer the canonical label produced by the text index (when the
    // bundle ships an admin_names sidecar). Fall back to "name (kind)"
    // for older bundles or reverse-PIP hits that bypass the index.
    let label = if !h.label.is_empty() {
        h.label.clone()
    } else if h.kind.is_empty() {
        h.name.clone()
    } else {
        format!("{} ({})", h.name, h.kind)
    };
    PeliasFeature {
        kind: "Feature",
        geometry: PeliasGeometry {
            kind: "Point",
            coordinates: [h.lon, h.lat],
        },
        properties: PeliasProperties {
            id: h.place_id.to_string(),
            layer: h.kind.clone(),
            name: h.name.clone(),
            label,
            // Pelias confidence is 0–1; we map score / (score+1) to keep
            // it monotonic without leaking BM25 magnitudes.
            confidence: h.score / (h.score + 1.0),
            distance_km: h.distance_km,
        },
    }
}

async fn pelias_search(
    State(state): State<AppState>,
    Query(params): Query<PeliasSearchQuery>,
) -> Result<Json<PeliasFeatureCollection<'static>>, ApiError> {
    pelias_search_impl(state, params, SearchMode::Search).await
}

async fn pelias_autocomplete(
    State(state): State<AppState>,
    Query(params): Query<PeliasSearchQuery>,
) -> Result<Json<PeliasFeatureCollection<'static>>, ApiError> {
    pelias_search_impl(state, params, SearchMode::Autocomplete).await
}

async fn pelias_search_impl(
    state: AppState,
    params: PeliasSearchQuery,
    mode: SearchMode,
) -> Result<Json<PeliasFeatureCollection<'static>>, ApiError> {
    let text = params.text.trim();
    if text.is_empty() {
        return Err(ApiError::bad_request(
            "missing_text",
            "the 'text' query parameter is required",
        ));
    }
    let layers = params
        .layers
        .as_deref()
        .map(|s| {
            s.split(',')
                .map(|p| p.trim().to_lowercase())
                .filter(|p| !p.is_empty())
                .map(normalize_layer)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let categories = params
        .categories
        .as_deref()
        .map(|s| {
            s.split(',')
                .map(|p| p.trim().to_lowercase())
                .filter(|p| !p.is_empty())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let focus = match (params.focus_lat, params.focus_lon) {
        (Some(lat), Some(lon)) => Some(Coord { lon, lat }),
        _ => None,
    };
    let bbox = match (
        params.bbox_min_lon,
        params.bbox_min_lat,
        params.bbox_max_lon,
        params.bbox_max_lat,
    ) {
        (Some(min_lon), Some(min_lat), Some(max_lon), Some(max_lat)) => Some(Bbox {
            min_lon,
            min_lat,
            max_lon,
            max_lat,
        }),
        _ => None,
    };
    let opts = SearchOptions {
        mode,
        limit: params.size.unwrap_or(10).clamp(1, 40),
        fuzzy: 0,
        layers,
        focus,
        focus_weight: 0.5,
        prefer_lang: params
            .lang
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string),
        categories,
        bbox,
        phonetic: params.phonetic.unwrap_or(false),
        semantic: params.semantic.unwrap_or(false),
        explain: false,
        valid_at: None,
    };
    let text_idx = state
        .text()
        .ok_or_else(|| ApiError::unavailable("text_index_unloaded", "text index not loaded"))?;
    let hits = text_idx.search(text, &opts).map_err(|err| match err {
        TextError::Query(_) => ApiError::bad_request("bad_query", err.to_string()),
        _ => ApiError::internal("text_search_failed", err.to_string()),
    })?;
    Ok(Json(PeliasFeatureCollection {
        geocoding: PeliasGeocodingMeta {
            version: "0.2",
            attribution: "© OpenStreetMap contributors, WhosOnFirst, OpenAddresses",
            engine: PeliasEngine {
                name: "cairn",
                bundle_id: bundle_id_static(&state),
            },
            query: serde_json::to_value(&params).unwrap_or_default(),
        },
        kind: "FeatureCollection",
        features: hits.into_iter().map(hit_to_pelias_feature).collect(),
    }))
}

async fn pelias_reverse(
    State(state): State<AppState>,
    Query(params): Query<PeliasReverseQuery>,
) -> Result<Json<PeliasFeatureCollection<'static>>, ApiError> {
    let (lat, lon) = match (params.lat, params.lon) {
        (Some(lat), Some(lon)) => (lat, lon),
        _ => {
            return Err(ApiError::bad_request(
                "missing_coords",
                "point.lat and point.lon are required",
            ))
        }
    };
    if !(-90.0..=90.0).contains(&lat) || !(-180.0..=180.0).contains(&lon) {
        return Err(ApiError::bad_request(
            "out_of_range",
            "point.lat must be in [-90,90] and point.lon in [-180,180]",
        ));
    }

    let admin = state
        .admin()
        .ok_or_else(|| ApiError::unavailable("admin_unloaded", "admin layer not loaded"))?;
    let limit = params.size.unwrap_or(10).clamp(1, 40);
    let probe = Coord { lon, lat };
    let matches = admin.point_in_polygon(probe);

    let features: Vec<PeliasFeature> = matches
        .iter()
        .take(limit)
        .map(|f| {
            let hit = Hit {
                place_id: f.place_id,
                name: f.name.clone(),
                kind: f.kind.clone(),
                level: f.level as u64,
                lon: f.centroid.lon,
                lat: f.centroid.lat,
                score: 1.0,
                admin_path: f.admin_path.clone(),
                distance_km: Some(haversine_km(lat, lon, f.centroid.lat, f.centroid.lon)),
                population: 0,
                label: String::new(),
                langs: Vec::new(),
                categories: Vec::new(),
                explain: None,
            };
            hit_to_pelias_feature(hit)
        })
        .collect();

    Ok(Json(PeliasFeatureCollection {
        geocoding: PeliasGeocodingMeta {
            version: "0.2",
            attribution: "© OpenStreetMap contributors, WhosOnFirst, OpenAddresses",
            engine: PeliasEngine {
                name: "cairn",
                bundle_id: bundle_id_static(&state),
            },
            query: serde_json::to_value(&params).unwrap_or_default(),
        },
        kind: "FeatureCollection",
        features,
    }))
}

/// Pelias-compatible `/v1/place?ids=X,Y`. Resolves comma-separated
/// place_ids to a Pelias FeatureCollection. Missing IDs are dropped
/// silently; a list with no matches returns an empty `features` array.
#[derive(Debug, Deserialize, Serialize)]
struct PlaceLookupQuery {
    ids: Option<String>,
}

async fn place_lookup(
    State(state): State<AppState>,
    Query(params): Query<PlaceLookupQuery>,
) -> Result<Json<PeliasFeatureCollection<'static>>, ApiError> {
    let raw = params.ids.as_deref().unwrap_or("").trim();
    if raw.is_empty() {
        return Err(ApiError::bad_request(
            "missing_ids",
            "the 'ids' query parameter is required (comma-separated place_ids)",
        ));
    }
    let ids: Vec<u64> = raw
        .split(',')
        .filter_map(|s| s.trim().parse::<u64>().ok())
        .collect();
    if ids.is_empty() {
        return Err(ApiError::bad_request(
            "bad_ids",
            "no valid u64 place_ids in 'ids' parameter",
        ));
    }
    let text = state
        .text()
        .ok_or_else(|| ApiError::unavailable("text_index_unloaded", "text index not loaded"))?;
    let hits = text
        .lookup_by_ids(&ids)
        .map_err(|err| ApiError::internal("text_lookup_failed", err.to_string()))?;
    Ok(Json(PeliasFeatureCollection {
        geocoding: PeliasGeocodingMeta {
            version: "0.2",
            attribution: "© OpenStreetMap contributors, WhosOnFirst, OpenAddresses",
            engine: PeliasEngine {
                name: "cairn",
                bundle_id: bundle_id_static(&state),
            },
            query: serde_json::to_value(&params).unwrap_or_default(),
        },
        kind: "FeatureCollection",
        features: hits.into_iter().map(hit_to_pelias_feature).collect(),
    }))
}

/// `/v1/layers` — list every layer/kind cairn understands. Pelias has
/// no equivalent endpoint but clients building a layer-filter UI need
/// the canonical list, so we expose it here.
async fn layers_metadata() -> Json<serde_json::Value> {
    use cairn_place::PlaceKind;
    let layers: Vec<&'static str> = [
        PlaceKind::Country,
        PlaceKind::Region,
        PlaceKind::County,
        PlaceKind::City,
        PlaceKind::District,
        PlaceKind::Neighborhood,
        PlaceKind::Street,
        PlaceKind::Address,
        PlaceKind::Poi,
        PlaceKind::Postcode,
    ]
    .into_iter()
    .map(cairn_text::kind_str)
    .collect();
    Json(serde_json::json!({
        "layers": layers,
        "description": "Allowed values for the 'layer' query parameter on /v1/search and /v1/reverse",
    }))
}

fn bundle_id_static(state: &AppState) -> &'static str {
    // The bundle id lives in `Metrics`. To embed it in a static-lifetime
    // response struct without copying for every request, leak the string
    // once. Acceptable: there's a single bundle per process.
    static ID: std::sync::OnceLock<&'static str> = std::sync::OnceLock::new();
    ID.get_or_init(|| Box::leak(state.metrics.bundle_id.clone().into_boxed_str()))
}

// ========================================================================
// OpenAPI spec
// ========================================================================

const OPENAPI_SPEC: &str = include_str!("../openapi.json");

async fn openapi_spec() -> impl IntoResponse {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json; charset=utf-8")],
        OPENAPI_SPEC,
    )
}
