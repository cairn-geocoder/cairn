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
    let nearest = NearestIndex::build(point_layer);

    AppState {
        bundle_path: Arc::new(bundle),
        text: Some(Arc::new(text)),
        admin: Some(Arc::new(admin)),
        nearest: Some(Arc::new(nearest)),
        metrics: Arc::new(Metrics::new("test".into(), 1, 3)),
        api_key: None,
    }
}

fn build_test_state_with_key(key: &str) -> AppState {
    let mut state = build_test_state();
    state.api_key = Some(Arc::new(key.to_string()));
    state
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
    // Legacy v1 handlers still emit the simpler `{"error": "msg"}` shape;
    // standardised envelope on Pelias + new endpoints. Both supported.
    assert!(body["error"].is_string() || body["error"]["message"].is_string());
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

#[tokio::test]
async fn openapi_spec_served() {
    let (status, body) = get_text(build_test_state(), "/openapi.json").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("\"openapi\""));
    assert!(body.contains("/v1/search"));
    assert!(body.contains("Pelias"));
}

#[tokio::test]
async fn pelias_search_returns_feature_collection() {
    let (status, body) = get_json(build_test_state(), "/search?text=Vaduz&size=3").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["type"], "FeatureCollection");
    assert_eq!(body["geocoding"]["engine"]["name"], "cairn");
    let features = body["features"].as_array().unwrap();
    assert!(!features.is_empty());
    assert_eq!(features[0]["type"], "Feature");
    assert_eq!(features[0]["geometry"]["type"], "Point");
    assert_eq!(features[0]["properties"]["name"], "Vaduz");
}

#[tokio::test]
async fn pelias_autocomplete_works_via_v1_path() {
    let (status, _) = get_json(build_test_state(), "/v1/autocomplete?text=Vad").await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn pelias_reverse_returns_feature_collection() {
    let (status, body) = get_json(
        build_test_state(),
        "/reverse?point.lat=47.14&point.lon=9.55",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["type"], "FeatureCollection");
    let features = body["features"].as_array().unwrap();
    assert!(!features.is_empty());
}

#[tokio::test]
async fn pelias_reverse_missing_coords_400() {
    let (status, body) = get_json(build_test_state(), "/reverse").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], "missing_coords");
}

#[tokio::test]
async fn auth_blocks_request_without_key() {
    let (status, body) = get_json(
        build_test_state_with_key("secret-key"),
        "/v1/search?q=Vaduz",
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body["error"]["code"], "missing_or_invalid_api_key");
}

#[tokio::test]
async fn auth_allows_request_with_valid_key_in_query() {
    let (status, _) = get_json(
        build_test_state_with_key("secret-key"),
        "/v1/search?q=Vaduz&api_key=secret-key",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn auth_allows_request_with_valid_header() {
    let req = Request::get("/v1/search?q=Vaduz")
        .header("X-API-Key", "secret-key")
        .body(Body::empty())
        .unwrap();
    let resp = router(build_test_state_with_key("secret-key"))
        .oneshot(req)
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn search_limit_caps_results() {
    let (_, body) = get_json(build_test_state(), "/v1/search?q=Vaduz&limit=1").await;
    let results = body["results"].as_array().unwrap();
    assert!(results.len() <= 1);
}

#[tokio::test]
async fn search_limit_clamps_to_100() {
    // Asking for limit=9999 must clamp at 100, not crash.
    let (status, _) = get_json(build_test_state(), "/v1/search?q=Vaduz&limit=9999").await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn search_fuzzy_recovers_single_typo() {
    // "Vadzu" is a single u<->z transposition of "Vaduz" (Damerau cost 1).
    let (status, body) = get_json(build_test_state(), "/v1/search?q=Vadzu&fuzzy=1").await;
    assert_eq!(status, StatusCode::OK);
    let results = body["results"].as_array().unwrap();
    assert!(
        results.iter().any(|r| r["name"] == "Vaduz"),
        "expected fuzzy=1 to recover 'Vadzu' -> 'Vaduz', got {results:?}"
    );
}

#[tokio::test]
async fn search_multi_layer_filter_unions() {
    let (_, body) = get_json(
        build_test_state(),
        "/v1/search?q=Vaduz&layer=city,country&limit=10",
    )
    .await;
    let results = body["results"].as_array().unwrap();
    for r in results {
        let kind = r["kind"].as_str().unwrap();
        assert!(
            kind == "city" || kind == "country",
            "unexpected kind in multi-layer filter: {kind}"
        );
    }
}

#[tokio::test]
async fn autocomplete_endpoint_returns_prefix_hits() {
    let (status, body) = get_json(build_test_state(), "/v1/autocomplete?text=Vad&size=5").await;
    assert_eq!(status, StatusCode::OK);
    // Pelias-shaped FeatureCollection.
    assert_eq!(body["type"], "FeatureCollection");
    let features = body["features"].as_array().unwrap();
    assert!(
        features.iter().any(|f| f["properties"]["name"] == "Vaduz"),
        "autocomplete should match 'Vad' prefix to Vaduz"
    );
}

#[tokio::test]
async fn reverse_limit_caps_results() {
    let (_, body) = get_json(
        build_test_state(),
        "/v1/reverse?lat=48.0&lon=10.5&nearest=2&limit=1",
    )
    .await;
    let results = body["results"].as_array().unwrap();
    assert!(results.len() <= 1);
}

#[tokio::test]
async fn metrics_counter_increments_after_search() {
    // Hit /v1/search, then /metrics, and confirm the search counter ticked.
    let state = build_test_state();
    let (_, _) = get_json(state.clone(), "/v1/search?q=Vaduz").await;
    let (status, body) = get_text(state, "/metrics").await;
    assert_eq!(status, StatusCode::OK);
    let line = body
        .lines()
        .find(|l| l.starts_with("cairn_requests_total") && l.contains("endpoint=\"search\""))
        .expect("expected a search counter row in /metrics");
    let val: f64 = line
        .split_whitespace()
        .last()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.0);
    assert!(
        val >= 1.0,
        "expected search counter >= 1 after a search call, got {val}"
    );
}

#[tokio::test]
async fn structured_returns_pelias_compat_layer_hint() {
    // Layer-hint precedence: postcode > address > street > neighborhood >
    // city > county > region > country. With both city + country set,
    // city wins.
    let (_, body) = get_json(
        build_test_state(),
        "/v1/structured?city=Vaduz&country=Liechtenstein",
    )
    .await;
    assert_eq!(body["layer_hint"], "city");
}

#[tokio::test]
async fn unknown_route_returns_404() {
    let req = Request::get("/v99/nope").body(Body::empty()).unwrap();
    let resp = router(build_test_state()).oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn auth_does_not_block_open_routes() {
    // /healthz, /readyz, /metrics, /openapi.json must work without a key.
    let state = build_test_state_with_key("secret-key");
    for path in ["/healthz", "/readyz", "/metrics", "/openapi.json"] {
        let req = Request::get(path).body(Body::empty()).unwrap();
        let resp = router(state.clone()).oneshot(req).await.unwrap();
        assert_ne!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "{path} should be open"
        );
    }
}
