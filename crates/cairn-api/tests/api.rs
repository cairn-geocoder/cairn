//! In-process HTTP integration tests for cairn-api.
//!
//! Spins up a real router on a synthetic bundle (3 places + 1 admin
//! polygon + 3 nearest points) and fires HTTP requests through
//! `tower::ServiceExt::oneshot`. Catches API regressions without needing
//! a running cairn-serve.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use cairn_api::{router, AppState, Metrics};
use cairn_place::{Coord, LocalizedName, Place, PlaceId, PlaceKind};
use cairn_spatial::{AdminFeature, AdminIndex, AdminLayer, NearestIndex, PlacePoint, PointLayer};
use cairn_text::{build_index, TextIndex};
use geo_types::{LineString, MultiPolygon, Polygon};
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

static COUNTER: AtomicUsize = AtomicUsize::new(0);

fn tempdir() -> std::path::PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let d = std::env::temp_dir().join(format!(
        "cairn-api-test-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        n
    ));
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn vaduz() -> Place {
    Place {
        id: PlaceId::new(1, 49509, 1).unwrap(),
        kind: PlaceKind::City,
        names: vec![LocalizedName {
            lang: "default".into(),
            value: "Vaduz".into(),
        }],
        centroid: Coord {
            lon: 9.5209,
            lat: 47.141,
        },
        admin_path: vec![],
        tags: vec![],
    }
}

fn schaan() -> Place {
    Place {
        id: PlaceId::new(1, 49509, 2).unwrap(),
        kind: PlaceKind::City,
        names: vec![LocalizedName {
            lang: "default".into(),
            value: "Schaan".into(),
        }],
        centroid: Coord {
            lon: 9.5095,
            lat: 47.165,
        },
        admin_path: vec![],
        tags: vec![],
    }
}

fn liechtenstein() -> Place {
    Place {
        id: PlaceId::new(0, 49509, 1).unwrap(),
        kind: PlaceKind::Country,
        names: vec![LocalizedName {
            lang: "default".into(),
            value: "Liechtenstein".into(),
        }],
        centroid: Coord {
            lon: 9.555,
            lat: 47.13,
        },
        admin_path: vec![],
        tags: vec![],
    }
}

/// Liechtenstein-shaped square polygon for PIP tests.
fn liechtenstein_admin() -> AdminFeature {
    let ring = LineString::from(vec![
        (9.45, 47.05),
        (9.65, 47.05),
        (9.65, 47.27),
        (9.45, 47.27),
        (9.45, 47.05),
    ]);
    AdminFeature {
        place_id: PlaceId::new(0, 49509, 1).unwrap().0,
        level: 0,
        kind: "country".into(),
        name: "Liechtenstein".into(),
        centroid: Coord {
            lon: 9.555,
            lat: 47.13,
        },
        admin_path: vec![],
        polygon: MultiPolygon(vec![Polygon::new(ring, vec![])]),
    }
}

fn build_test_state() -> AppState {
    let bundle = tempdir();

    let places = vec![vaduz(), schaan(), liechtenstein()];
    let text_dir = bundle.join("index/text");
    build_index(&text_dir, places.clone()).unwrap();
    let text = TextIndex::open(&text_dir).unwrap();

    let admin_layer = AdminLayer {
        features: vec![liechtenstein_admin()],
    };
    let admin_path = bundle.join("spatial/admin.bin");
    admin_layer.write_to(&admin_path).unwrap();
    let admin = AdminIndex::build(admin_layer);

    let point_layer = PointLayer {
        points: places
            .iter()
            .map(|p| PlacePoint {
                place_id: p.id.0,
                level: p.id.level(),
                kind: cairn_text::kind_str(p.kind).to_string(),
                name: p.names[0].value.clone(),
                centroid: p.centroid,
                admin_path: vec![],
            })
            .collect(),
    };
    let points_path = bundle.join("spatial/points.bin");
    point_layer.write_to(&points_path).unwrap();
    let nearest = NearestIndex::build(point_layer);

    AppState {
        bundle_path: Arc::new(bundle),
        text: Some(Arc::new(text)),
        admin: Some(Arc::new(admin)),
        nearest: Some(Arc::new(nearest)),
        metrics: Arc::new(Metrics::new("test".into(), 1, 3)),
    }
}

async fn get_json(state: AppState, uri: &str) -> (StatusCode, Value) {
    let req = Request::get(uri).body(Body::empty()).unwrap();
    let resp = router(state).oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let body: Value = serde_json::from_slice(&bytes).unwrap_or_default();
    (status, body)
}

async fn get_text(state: AppState, uri: &str) -> (StatusCode, String) {
    let req = Request::get(uri).body(Body::empty()).unwrap();
    let resp = router(state).oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8_lossy(&bytes).to_string())
}

#[tokio::test]
async fn healthz_ok() {
    let (status, body) = get_json(build_test_state(), "/healthz").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "ok");
}

#[tokio::test]
async fn search_finds_vaduz() {
    let (status, body) = get_json(build_test_state(), "/v1/search?q=Vaduz&limit=3").await;
    assert_eq!(status, StatusCode::OK);
    let results = body["results"].as_array().unwrap();
    assert!(!results.is_empty());
    assert_eq!(results[0]["name"], "Vaduz");
    assert_eq!(results[0]["kind"], "city");
}

#[tokio::test]
async fn search_layer_filter_excludes_other_kinds() {
    let (_, body) = get_json(
        build_test_state(),
        "/v1/search?q=Liechtenstein&layer=country&limit=5",
    )
    .await;
    let results = body["results"].as_array().unwrap();
    assert!(!results.is_empty());
    for r in results {
        assert_eq!(r["kind"], "country");
    }
}

#[tokio::test]
async fn search_dedup_one_doc_per_place() {
    // Regression test for the Phase 4.5 dedup bug: a Place with multiple
    // localized names must produce exactly one Hit, not one per name.
    let (_, body) = get_json(build_test_state(), "/v1/search?q=Vaduz").await;
    let results = body["results"].as_array().unwrap();
    let vaduz_count = results.iter().filter(|r| r["name"] == "Vaduz").count();
    assert_eq!(vaduz_count, 1, "expected 1 Vaduz hit, got {vaduz_count}");
}

#[tokio::test]
async fn search_empty_q_400() {
    let (status, body) = get_json(build_test_state(), "/v1/search?q=").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body["error"].is_string());
}

#[tokio::test]
async fn structured_picks_layer_hint() {
    let (_, body) = get_json(build_test_state(), "/v1/structured?country=Liechtenstein").await;
    assert_eq!(body["layer_hint"], "country");
}

#[tokio::test]
async fn reverse_pip_returns_country() {
    let (_, body) = get_json(build_test_state(), "/v1/reverse?lat=47.14&lon=9.55&limit=5").await;
    assert_eq!(body["source"], "pip");
    let results = body["results"].as_array().unwrap();
    assert!(!results.is_empty());
    assert_eq!(results[0]["name"], "Liechtenstein");
}

#[tokio::test]
async fn reverse_outside_polygon_falls_back_to_nearest() {
    let (_, body) = get_json(
        build_test_state(),
        "/v1/reverse?lat=48.0&lon=10.5&nearest=2",
    )
    .await;
    assert_eq!(body["source"], "nearest");
    let results = body["results"].as_array().unwrap();
    assert!(!results.is_empty());
}

#[tokio::test]
async fn reverse_missing_coords_400() {
    let (status, _) = get_json(build_test_state(), "/v1/reverse").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn parse_endpoint_extracts_postcode() {
    let (status, body) = get_json(
        build_test_state(),
        "/v1/parse?q=Hauptstrasse%2012%2C%2010115%20Berlin%2C%20Deutschland",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["parsed"]["postcode"], "10115");
    assert_eq!(body["parsed"]["country"], "Deutschland");
}

#[tokio::test]
async fn expand_endpoint_lowercases() {
    let (status, body) = get_json(build_test_state(), "/v1/expand?q=123%20W%20Main%20St").await;
    assert_eq!(status, StatusCode::OK);
    let exps = body["expansions"].as_array().unwrap();
    assert!(!exps.is_empty());
    assert!(exps[0].as_str().unwrap().contains("street"));
}

#[tokio::test]
async fn metrics_emits_prometheus_text() {
    let (status, body) = get_text(build_test_state(), "/metrics").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("cairn_uptime_seconds"));
    assert!(body.contains("cairn_admin_features"));
    assert!(body.contains("cairn_requests_total"));
}
