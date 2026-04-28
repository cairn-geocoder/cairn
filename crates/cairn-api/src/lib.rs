//! HTTP API for the Cairn geocoder.
//!
//! Endpoints:
//!   GET /v1/search        forward + autocomplete + fuzzy + layer + focus
//!   GET /v1/reverse       (Phase 3)
//!   GET /v1/structured    (Phase 4)
//!   GET /healthz, /readyz

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::get,
    Router,
};
use cairn_place::Coord;
use cairn_spatial::AdminIndex;
use cairn_text::{Hit, SearchMode, SearchOptions, TextError, TextIndex};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::error;

#[derive(Clone)]
pub struct AppState {
    pub bundle_path: Arc<std::path::PathBuf>,
    pub text: Option<Arc<TextIndex>>,
    pub admin: Option<Arc<AdminIndex>>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/v1/search", get(search))
        .route("/v1/reverse", get(reverse))
        .route("/v1/structured", get(structured))
        .with_state(state)
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
        Ok(results) => Json(SearchResponse {
            query: q,
            mode: &mode_label,
            results,
        })
        .into_response(),
        Err(err) => {
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
    results: Vec<ReverseHit>,
}

async fn reverse(
    State(state): State<AppState>,
    Query(params): Query<ReverseQuery>,
) -> impl IntoResponse {
    let (lat, lon) = match (params.lat, params.lon) {
        (Some(lat), Some(lon)) => (lat, lon),
        _ => {
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
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "lat must be in [-90, 90] and lon in [-180, 180]"
            })),
        )
            .into_response();
    }

    let admin = match state.admin.as_ref() {
        Some(a) => a.clone(),
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({ "error": "admin layer not loaded" })),
            )
                .into_response();
        }
    };

    let limit = params.limit.unwrap_or(10).clamp(1, 50);
    let probe = Coord { lon, lat };
    let matches = admin.point_in_polygon(probe);

    // Order is finest-containing-polygon first, courtesy of AdminIndex.
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

    Json(ReverseResponse { lat, lon, results }).into_response()
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

async fn structured() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "results": [],
        "note": "structured queries ship in Phase 4"
    }))
}

fn map_err(err: TextError) -> (StatusCode, Json<serde_json::Value>) {
    let (status, msg) = match &err {
        TextError::Query(_) => (StatusCode::BAD_REQUEST, format!("{err}")),
        _ => (StatusCode::INTERNAL_SERVER_ERROR, format!("{err}")),
    };
    (status, Json(serde_json::json!({ "error": msg })))
}
