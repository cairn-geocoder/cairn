//! HTTP API for the Cairn geocoder.
//!
//! Endpoints:
//!   GET /v1/search        forward + autocomplete + fuzzy + layer + focus
//!   GET /v1/reverse       (Phase 3)
//!   GET /v1/structured    (Phase 4)
//!   GET /healthz, /readyz

use axum::http::header;
use axum::{
    extract::{Query, State},
    http::{Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Json, Response},
    routing::get,
    Router,
};
use cairn_place::Coord;
use cairn_spatial::{AdminIndex, NearestIndex};
use cairn_text::{Hit, SearchMode, SearchOptions, TextError, TextIndex};
use serde::{Deserialize, Serialize};
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

#[derive(Clone)]
pub struct AppState {
    pub bundle_path: Arc<std::path::PathBuf>,
    pub text: Option<Arc<TextIndex>>,
    pub admin: Option<Arc<AdminIndex>>,
    pub nearest: Option<Arc<NearestIndex>>,
    pub metrics: Arc<Metrics>,
    /// Optional API key. When `Some`, every request to `/v1/*` must
    /// present `X-API-Key: <key>` (or `?api_key=<key>`) or 401.
    pub api_key: Option<Arc<String>>,
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
        // Pelias-compatible aliases share the same handlers.
        .route("/v1/autocomplete", get(pelias_autocomplete))
        .route("/search", get(pelias_search))
        .route("/autocomplete", get(pelias_autocomplete))
        .route("/reverse", get(pelias_reverse))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_api_key,
        ));

    // Open routes (health / metrics / spec) bypass auth.
    let open = Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics))
        .route("/openapi.json", get(openapi_spec));

    open.merge(v1).with_state(state).layer(
        TraceLayer::new_for_http()
            .make_span_with(DefaultMakeSpan::new().level(Level::INFO))
            .on_request(DefaultOnRequest::new().level(Level::INFO))
            .on_response(DefaultOnResponse::new().level(Level::INFO)),
    )
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

async fn readyz(State(state): State<AppState>) -> (StatusCode, Json<StatusBody>) {
    if state.text.is_some() {
        (StatusCode::OK, Json(StatusBody { status: "ready" }))
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(StatusBody {
                status: "no_text_index",
            }),
        )
    }
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
}

#[derive(Serialize)]
struct SearchResponse<'a> {
    query: &'a str,
    mode: &'a str,
    results: Vec<Hit>,
}

async fn search(
    State(state): State<AppState>,
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
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let focus = match (params.focus_lat, params.focus_lon) {
        (Some(lat), Some(lon)) => Some(Coord { lon, lat }),
        _ => None,
    };

    let opts = SearchOptions {
        mode,
        limit: params.limit.unwrap_or(10).clamp(1, 100),
        fuzzy: params.fuzzy.unwrap_or(0),
        layers,
        focus,
        focus_weight: params.focus_weight.unwrap_or(0.5),
    };

    let text = match state.text.as_ref() {
        Some(t) => t.clone(),
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
            Json(SearchResponse {
                query: q,
                mode: &mode_label,
                results,
            })
            .into_response()
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

    // PIP path first.
    if let Some(admin) = state.admin.as_ref() {
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
        if let Some(nearest) = state.nearest.as_ref() {
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

    if state.admin.is_none() && state.nearest.is_none() {
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
    };

    let text = match state.text.as_ref() {
        Some(t) => t.clone(),
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
    let label = if h.kind.is_empty() {
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
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let focus = match (params.focus_lat, params.focus_lon) {
        (Some(lat), Some(lon)) => Some(Coord { lon, lat }),
        _ => None,
    };
    let opts = SearchOptions {
        mode,
        limit: params.size.unwrap_or(10).clamp(1, 40),
        fuzzy: 0,
        layers,
        focus,
        focus_weight: 0.5,
    };
    let text_idx = state
        .text
        .as_ref()
        .ok_or_else(|| ApiError::unavailable("text_index_unloaded", "text index not loaded"))?
        .clone();
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
        .admin
        .as_ref()
        .ok_or_else(|| ApiError::unavailable("admin_unloaded", "admin layer not loaded"))?
        .clone();
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
