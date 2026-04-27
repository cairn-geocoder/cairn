//! HTTP API for the Cairn geocoder.
//!
//! Endpoints:
//!   GET /v1/search       forward + autocomplete
//!   GET /v1/reverse      reverse
//!   GET /v1/structured   structured forward
//!   GET /healthz
//!   GET /readyz

use axum::{routing::get, Json, Router};
use serde::Serialize;
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub bundle_path: Arc<std::path::PathBuf>,
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

async fn readyz() -> Json<StatusBody> {
    Json(StatusBody { status: "ready" })
}

async fn search() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "results": [] }))
}

async fn reverse() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "results": [] }))
}

async fn structured() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "results": [] }))
}
