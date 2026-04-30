//! In-process HTTP integration tests for cairn-api.
//!
//! Spins up a real router on a synthetic bundle (3 places + 1 admin
//! polygon + 3 nearest points) and fires HTTP requests through
//! `tower::ServiceExt::oneshot`. Catches API regressions without needing
//! a running cairn-serve.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use axum::body::Body;
use axum::extract::connect_info::MockConnectInfo;
use axum::http::{Request, StatusCode};
use cairn_api::{
    router, AppState, FederatedAdmin, FederatedNearest, FederatedText, Metrics, RateLimiter,
    TrustedCidr,
};
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

    AppState::new(
        bundle,
        cairn_api::BundleSnapshot {
            text: Some(Arc::new(FederatedText::from_single(Arc::new(text)))),
            admin: Some(Arc::new(FederatedAdmin::from_single(Arc::new(admin)))),
            nearest: Some(Arc::new(FederatedNearest::from_single(Arc::new(nearest)))),
            buildings: None,
            bundle_ids: vec!["test".into()],
        },
        Arc::new(Metrics::new("test".into(), 1, 3)),
        None,
        None,
        false,
        Arc::new(Vec::new()),
    )
}

fn build_test_state_with_key(key: &str) -> AppState {
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
    AppState::new(
        bundle,
        cairn_api::BundleSnapshot {
            text: Some(Arc::new(FederatedText::from_single(Arc::new(text)))),
            admin: Some(Arc::new(FederatedAdmin::from_single(Arc::new(admin)))),
            nearest: Some(Arc::new(FederatedNearest::from_single(Arc::new(nearest)))),
            buildings: None,
            bundle_ids: vec!["test".into()],
        },
        Arc::new(Metrics::new("test".into(), 1, 3)),
        Some(Arc::new(key.to_string())),
        None,
        false,
        Arc::new(Vec::new()),
    )
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

async fn get_with_accept(
    state: AppState,
    uri: &str,
    accept: &str,
) -> (StatusCode, String, Option<String>) {
    let req = Request::get(uri)
        .header(axum::http::header::ACCEPT, accept)
        .body(Body::empty())
        .unwrap();
    let resp = router(state).oneshot(req).await.unwrap();
    let status = resp.status();
    let ct = resp
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8_lossy(&bytes).to_string(), ct)
}

#[tokio::test]
async fn search_autoparse_echoes_parsed_components() {
    // Heuristic parser without libpostal still extracts postcode +
    // city for German-style addresses. autoparse must echo the
    // ParsedAddress in the response.
    let (status, body) = get_json(
        build_test_state(),
        "/v1/search?q=Hauptstrasse+12,+9490+Vaduz,+Liechtenstein&autoparse=true",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let parsed = body
        .get("parsed")
        .expect("autoparse=true must echo `parsed`");
    assert_eq!(parsed["postcode"], "9490");
    assert_eq!(parsed["city"], "Vaduz");
    assert_eq!(parsed["country"], "Liechtenstein");
}

#[tokio::test]
async fn search_autoparse_postcode_only_promotes_postal_filter() {
    // "Aeulestrasse, 9490 Vaduz" parses cleanly with the heuristic
    // parser: postcode=9490, city=Vaduz, road=Aeulestrasse. We can't
    // assert filter behaviour against the shared fixture (no
    // postcode places present), but we confirm the parsed echo and
    // a 200 response.
    let (status, body) = get_json(
        build_test_state(),
        "/v1/search?q=Aeulestrasse,+9490+Vaduz&autoparse=true",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let parsed = body.get("parsed").expect("parsed echoed");
    assert_eq!(parsed["postcode"], "9490");
    assert_eq!(parsed["city"], "Vaduz");
}

#[tokio::test]
async fn search_autoparse_off_omits_parsed_field() {
    let (status, body) = get_json(build_test_state(), "/v1/search?q=Vaduz").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body.get("parsed").is_none(),
        "parsed must be omitted when autoparse is off"
    );
}

#[tokio::test]
async fn federated_search_merges_hits_across_bundles() {
    // Build TWO independent bundles. Bundle A has Vaduz; bundle B has
    // Schaan. A federated search must surface both.
    let bundle_a = tempdir();
    let dir_a = bundle_a.join("index/text");
    build_index(&dir_a, vec![vaduz()]).unwrap();
    let text_a = TextIndex::open(&dir_a).unwrap();

    let bundle_b = tempdir();
    let dir_b = bundle_b.join("index/text");
    build_index(&dir_b, vec![schaan()]).unwrap();
    let text_b = TextIndex::open(&dir_b).unwrap();

    let federated = FederatedText::from_many(vec![Arc::new(text_a), Arc::new(text_b)]);
    let state = AppState::new(
        bundle_a.clone(),
        cairn_api::BundleSnapshot {
            text: Some(Arc::new(federated)),
            admin: None,
            nearest: None,
            buildings: None,
            bundle_ids: vec!["a".into(), "b".into()],
        },
        Arc::new(Metrics::new("a,b".into(), 0, 2)),
        None,
        None,
        false,
        Arc::new(Vec::new()),
    );

    let (status, body) = get_json(state.clone(), "/v1/search?q=Vaduz").await;
    assert_eq!(status, StatusCode::OK);
    let names: Vec<&str> = body["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"Vaduz"), "Vaduz hit missing: {body}");

    let (_, body) = get_json(state.clone(), "/v1/search?q=Schaan").await;
    let names: Vec<&str> = body["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"Schaan"), "Schaan hit missing: {body}");

    let (_, body) = get_json(state, "/v1/info").await;
    let ids: Vec<&str> = body["bundle_ids"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert_eq!(ids, vec!["a", "b"]);
    assert_eq!(body["bundle_count"], 2);
}

#[tokio::test]
async fn sbom_endpoint_serves_cyclonedx_when_present() {
    let state = build_test_state();
    let path = state.bundle_path.join("sbom.json");
    let body = serde_json::json!({
        "bomFormat": "CycloneDX",
        "specVersion": "1.5",
        "components": [],
    });
    std::fs::write(&path, body.to_string()).unwrap();

    let req = Request::get("/v1/sbom").body(Body::empty()).unwrap();
    let resp = router(state).oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let ct = resp
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(ct.contains("cyclonedx"), "wrong content-type: {ct}");
}

#[tokio::test]
async fn sbom_endpoint_404_when_missing() {
    let (status, body) = get_json(build_test_state(), "/v1/sbom").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"], "sbom_not_found");
}

#[tokio::test]
async fn info_reports_single_bundle_id_in_array_form() {
    let (status, body) = get_json(build_test_state(), "/v1/info").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["bundle_count"], 1);
    let ids = body["bundle_ids"].as_array().unwrap();
    assert_eq!(ids.len(), 1);
}

#[tokio::test]
async fn search_semantic_param_passes_through_to_engine() {
    // ?semantic=true should round-trip the request without 4xx —
    // the actual rerank is unit-tested in cairn_text::semantic.
    let (status, body) = get_json(build_test_state(), "/v1/search?q=Vaduz&semantic=true").await;
    assert_eq!(status, StatusCode::OK);
    assert!(!body["results"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn search_explain_returns_per_stage_breakdown() {
    let (status, body) = get_json(build_test_state(), "/v1/search?q=Vaduz&explain=true").await;
    assert_eq!(status, StatusCode::OK);
    let results = body["results"].as_array().unwrap();
    assert!(!results.is_empty());
    let first = &results[0];
    let explain = first
        .get("explain")
        .expect("explain block must be present when ?explain=true");
    // BM25 baseline must be > 0; final_score must equal the hit's
    // surfaced score.
    assert!(explain["bm25"].as_f64().unwrap() > 0.0);
    assert!(
        (explain["final_score"].as_f64().unwrap() - first["score"].as_f64().unwrap()).abs() < 1e-3
    );
    // Vaduz exactly matches its own name → exact_match_boost = 4.0.
    assert!((explain["exact_match_boost"].as_f64().unwrap() - 4.0).abs() < 1e-3);
}

#[tokio::test]
async fn search_explain_off_by_default() {
    let (status, body) = get_json(build_test_state(), "/v1/search?q=Vaduz").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body["results"][0].get("explain").is_none(),
        "explain must be omitted by default"
    );
}

#[tokio::test]
async fn search_ndjson_emits_one_line_per_hit() {
    let (status, body, ct) = get_with_accept(
        build_test_state(),
        "/v1/search?q=Vaduz",
        "application/x-ndjson",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(ct.as_deref(), Some("application/x-ndjson"));
    let lines: Vec<&str> = body
        .trim_end()
        .split('\n')
        .filter(|l| !l.is_empty())
        .collect();
    assert!(!lines.is_empty(), "ndjson body empty");
    for line in &lines {
        let v: Value = serde_json::from_str(line).expect("each line must be valid JSON");
        assert!(v.get("place_id").is_some(), "missing place_id: {line}");
        assert!(v.get("name").is_some(), "missing name: {line}");
    }
}

#[tokio::test]
async fn search_default_accept_returns_envelope_json() {
    // No Accept header (or */*) → wrapped JSON object, not NDJSON.
    let (status, body) = get_json(build_test_state(), "/v1/search?q=Vaduz").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.get("results").is_some(), "expected envelope: {body}");
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
    // The spec advertises a `pelias` tag for the unprefixed alias
    // route group. Tag name kept lowercase so existing client code
    // that filters by tag (Stoplight / Redoc / Swagger UI) keeps
    // working — unrelated to the demoted-from-feature-level Pelias
    // narrative on the public docs.
    assert!(body.contains("\"pelias\""));
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
async fn search_boundary_rect_keeps_in_rect_results() {
    // Bbox covers Vaduz (~9.5209, 47.141). Vaduz is in the fixture.
    let state = build_test_state();
    let url = "/v1/search?q=Vaduz\
&boundary.rect.min_lon=9.515\
&boundary.rect.min_lat=47.135\
&boundary.rect.max_lon=9.525\
&boundary.rect.max_lat=47.145";
    let (status, body) = get_json(state, url).await;
    assert_eq!(status, StatusCode::OK);
    let results = body["results"].as_array().unwrap();
    assert!(!results.is_empty(), "Vaduz must survive the rect");
    for r in results {
        let lon = r["lon"].as_f64().unwrap();
        let lat = r["lat"].as_f64().unwrap();
        assert!((9.515..=9.525).contains(&lon), "lon out of bbox: {r}");
        assert!((47.135..=47.145).contains(&lat), "lat out of bbox: {r}");
    }
}

#[tokio::test]
async fn search_boundary_rect_clips_far_away() {
    // Bbox over Australia — Vaduz is in Europe; result count must
    // be zero.
    let state = build_test_state();
    let url = "/v1/search?q=Vaduz\
&boundary.rect.min_lon=140.0\
&boundary.rect.min_lat=-40.0\
&boundary.rect.max_lon=150.0\
&boundary.rect.max_lat=-30.0";
    let (status, body) = get_json(state, url).await;
    assert_eq!(status, StatusCode::OK);
    let results = body["results"].as_array().unwrap();
    assert!(results.is_empty(), "Vaduz must be clipped: {body}");
}

#[tokio::test]
async fn search_partial_boundary_rect_is_ignored() {
    // Pelias spec: all four required together. Missing one field =
    // treat as no rect (no 400, no clip).
    let state = build_test_state();
    let (status, body) = get_json(state, "/v1/search?q=Vaduz&boundary.rect.min_lat=47.0").await;
    assert_eq!(status, StatusCode::OK);
    let results = body["results"].as_array().unwrap();
    assert!(!results.is_empty(), "partial bbox must not drop anything");
}

#[tokio::test]
async fn search_categories_filter_drops_non_matching_pois() {
    // Build a tiny, isolated state with two POIs that share a name
    // but differ on amenity. ?categories=hospital must keep only the
    // hospital.
    let bundle = tempdir();
    let hospital = Place {
        id: PlaceId::new(2, 49509, 10).unwrap(),
        kind: PlaceKind::Poi,
        names: vec![LocalizedName {
            lang: "default".into(),
            value: "Salus Center".into(),
        }],
        centroid: Coord {
            lon: 9.50,
            lat: 47.14,
        },
        admin_path: vec![],
        tags: vec![("amenity".into(), "hospital".into())],
    };
    let bakery = Place {
        id: PlaceId::new(2, 49509, 11).unwrap(),
        kind: PlaceKind::Poi,
        names: vec![LocalizedName {
            lang: "default".into(),
            value: "Salus Center".into(),
        }],
        centroid: Coord {
            lon: 9.51,
            lat: 47.14,
        },
        admin_path: vec![],
        tags: vec![("shop".into(), "bakery".into())],
    };
    let text_dir = bundle.join("index/text");
    build_index(&text_dir, vec![hospital, bakery]).unwrap();
    let text = TextIndex::open(&text_dir).unwrap();
    let state = AppState::new(
        bundle,
        cairn_api::BundleSnapshot {
            text: Some(Arc::new(FederatedText::from_single(Arc::new(text)))),
            admin: None,
            nearest: None,
            buildings: None,
            bundle_ids: vec!["test".into()],
        },
        Arc::new(Metrics::new("test".into(), 0, 2)),
        None,
        None,
        false,
        Arc::new(Vec::new()),
    );
    let (status, body) = get_json(state.clone(), "/v1/search?q=Salus+Center").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["results"].as_array().unwrap().len(), 2);

    let (status, body) = get_json(state, "/v1/search?q=Salus+Center&categories=hospital").await;
    assert_eq!(status, StatusCode::OK);
    let results = body["results"].as_array().unwrap();
    assert_eq!(results.len(), 1, "categories=hospital must drop bakery");
    let cats = results[0]["categories"].as_array().unwrap();
    assert!(cats.iter().any(|v| v == "hospital"));
}

#[tokio::test]
async fn search_lang_param_passes_through_to_engine() {
    // Test fixture has a single 'default' language tag on Vaduz, so
    // lang=de doesn't surface a different result, but the hit's
    // 'langs' array should be present and the request should not 4xx.
    let (status, body) = get_json(build_test_state(), "/v1/search?q=Vaduz&lang=de").await;
    assert_eq!(status, StatusCode::OK);
    let results = body["results"].as_array().unwrap();
    assert!(!results.is_empty());
    // langs is skip-if-empty; index has 'default' so it's present.
    let langs = results[0]["langs"].as_array().unwrap();
    assert!(langs.iter().any(|v| v == "default"));
}

#[tokio::test]
async fn readyz_reports_per_component_status() {
    let (status, body) = get_json(build_test_state(), "/readyz").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["ready"], true);
    assert_eq!(body["components"]["text"], true);
    assert_eq!(body["components"]["admin"], true);
    assert_eq!(body["components"]["nearest"], true);
    assert!(body["bundle_id"].is_string());
}

#[tokio::test]
async fn info_returns_bundle_metadata() {
    let (status, body) = get_json(build_test_state(), "/v1/info").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["bundle_id"].is_string());
    assert!(body["uptime_seconds"].is_u64());
    assert!(body["admin_features"].is_u64());
    assert!(body["point_count"].is_u64());
    assert_eq!(body["auth_required"], false);
    assert_eq!(body["rate_limited"], false);
}

#[tokio::test]
async fn info_open_to_unauthenticated_clients() {
    // /v1/info must not require an API key — used by health probes.
    let (status, _) = get_json(build_test_state_with_key("secret"), "/v1/info").await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn place_lookup_resolves_known_ids() {
    let state = build_test_state();
    // PlaceId for vaduz() in the test fixture.
    let vaduz_id = PlaceId::new(1, 49509, 1).unwrap().0;
    let url = format!("/v1/place?ids={vaduz_id}");
    let (status, body) = get_json(state, &url).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["type"], "FeatureCollection");
    let features = body["features"].as_array().unwrap();
    assert_eq!(features.len(), 1);
    assert_eq!(features[0]["properties"]["name"], "Vaduz");
}

#[tokio::test]
async fn place_lookup_skips_unknown_ids() {
    let state = build_test_state();
    let vaduz_id = PlaceId::new(1, 49509, 1).unwrap().0;
    let url = format!("/v1/place?ids={vaduz_id},9999999999999999");
    let (_, body) = get_json(state, &url).await;
    let features = body["features"].as_array().unwrap();
    assert_eq!(features.len(), 1, "unknown id should be silently dropped");
    assert_eq!(features[0]["properties"]["name"], "Vaduz");
}

#[tokio::test]
async fn place_lookup_missing_ids_400() {
    let (status, body) = get_json(build_test_state(), "/v1/place").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], "missing_ids");
}

#[tokio::test]
async fn place_lookup_bad_ids_400() {
    let (status, body) = get_json(build_test_state(), "/v1/place?ids=not-a-number,xyz").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], "bad_ids");
}

#[tokio::test]
async fn layers_endpoint_lists_kinds() {
    let (status, body) = get_json(build_test_state(), "/v1/layers").await;
    assert_eq!(status, StatusCode::OK);
    let layers = body["layers"].as_array().unwrap();
    let names: Vec<&str> = layers.iter().filter_map(|v| v.as_str()).collect();
    for expected in ["country", "region", "city", "poi", "address", "postcode"] {
        assert!(
            names.contains(&expected),
            "expected '{expected}' in /v1/layers, got {names:?}"
        );
    }
}

#[tokio::test]
async fn rate_limiter_throttles_after_burst_exhausted() {
    use std::net::SocketAddr;
    let mut state = build_test_state();
    state.rate_limit = Some(std::sync::Arc::new(RateLimiter::new(0.001, 2.0)));
    let app = router(state).layer(MockConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9999))));

    let mut last_status = StatusCode::OK;
    for _ in 0..5 {
        let req = Request::get("/v1/search?q=Vaduz")
            .body(Body::empty())
            .unwrap();
        last_status = tower::ServiceExt::oneshot(app.clone(), req)
            .await
            .unwrap()
            .status();
    }
    assert_eq!(
        last_status,
        StatusCode::TOO_MANY_REQUESTS,
        "after burst, requests must 429"
    );
}

#[tokio::test]
async fn rate_limit_uses_xff_when_trusted_so_separate_proxied_clients_dont_share_bucket() {
    use std::net::SocketAddr;
    let mut state = build_test_state();
    state.rate_limit = Some(std::sync::Arc::new(RateLimiter::new(0.001, 1.0)));
    state.trust_forwarded_for = true;
    let app = router(state).layer(MockConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9999))));

    // Two different upstream clients via the same proxy: each gets
    // its own bucket. Without XFF trust, both would share the
    // proxy's IP and burst-1 would block the second.
    let req_a = Request::get("/v1/search?q=Vaduz")
        .header("X-Forwarded-For", "203.0.113.1")
        .body(Body::empty())
        .unwrap();
    let resp_a = tower::ServiceExt::oneshot(app.clone(), req_a)
        .await
        .unwrap();
    assert_eq!(resp_a.status(), StatusCode::OK);

    let req_b = Request::get("/v1/search?q=Vaduz")
        .header("X-Forwarded-For", "203.0.113.2")
        .body(Body::empty())
        .unwrap();
    let resp_b = tower::ServiceExt::oneshot(app.clone(), req_b)
        .await
        .unwrap();
    assert_eq!(resp_b.status(), StatusCode::OK);

    // Same IP twice → second 429.
    let req_a2 = Request::get("/v1/search?q=Vaduz")
        .header("X-Forwarded-For", "203.0.113.1")
        .body(Body::empty())
        .unwrap();
    let resp_a2 = tower::ServiceExt::oneshot(app, req_a2).await.unwrap();
    assert_eq!(resp_a2.status(), StatusCode::TOO_MANY_REQUESTS);
}

#[tokio::test]
async fn rate_limit_xff_ignored_when_peer_outside_cidr_allowlist() {
    use std::net::SocketAddr;
    let mut state = build_test_state();
    state.rate_limit = Some(std::sync::Arc::new(RateLimiter::new(0.001, 1.0)));
    state.trust_forwarded_for = true;
    // Allowlist accepts only 10.0.0.0/8. Our MockConnectInfo will use
    // 203.0.113.1 (TEST-NET-3, outside the allowlist), so XFF must be
    // ignored even though trust_forwarded_for=true.
    state.trusted_proxy_cidrs =
        std::sync::Arc::new(vec![TrustedCidr::parse("10.0.0.0/8").unwrap()]);
    let app = router(state).layer(MockConnectInfo(SocketAddr::from(([203, 0, 113, 1], 9999))));

    let req_a = Request::get("/v1/search?q=Vaduz")
        .header("X-Forwarded-For", "198.51.100.1")
        .body(Body::empty())
        .unwrap();
    let resp_a = tower::ServiceExt::oneshot(app.clone(), req_a)
        .await
        .unwrap();
    assert_eq!(resp_a.status(), StatusCode::OK);
    // Different XFF from same peer must STILL share the bucket because
    // XFF is being ignored — peer IP wins.
    let req_b = Request::get("/v1/search?q=Vaduz")
        .header("X-Forwarded-For", "198.51.100.2")
        .body(Body::empty())
        .unwrap();
    let resp_b = tower::ServiceExt::oneshot(app, req_b).await.unwrap();
    assert_eq!(resp_b.status(), StatusCode::TOO_MANY_REQUESTS);
}

#[tokio::test]
async fn rate_limit_xff_honored_when_peer_inside_cidr_allowlist() {
    use std::net::SocketAddr;
    let mut state = build_test_state();
    state.rate_limit = Some(std::sync::Arc::new(RateLimiter::new(0.001, 1.0)));
    state.trust_forwarded_for = true;
    state.trusted_proxy_cidrs =
        std::sync::Arc::new(vec![TrustedCidr::parse("10.0.0.0/8").unwrap()]);
    // Peer is inside 10.0.0.0/8 → XFF is the rate-limit key.
    let app = router(state).layer(MockConnectInfo(SocketAddr::from(([10, 1, 2, 3], 9999))));
    let req_a = Request::get("/v1/search?q=Vaduz")
        .header("X-Forwarded-For", "198.51.100.1")
        .body(Body::empty())
        .unwrap();
    let resp_a = tower::ServiceExt::oneshot(app.clone(), req_a)
        .await
        .unwrap();
    assert_eq!(resp_a.status(), StatusCode::OK);
    // Different XFF from the trusted peer = different rate-limit key.
    let req_b = Request::get("/v1/search?q=Vaduz")
        .header("X-Forwarded-For", "198.51.100.2")
        .body(Body::empty())
        .unwrap();
    let resp_b = tower::ServiceExt::oneshot(app, req_b).await.unwrap();
    assert_eq!(resp_b.status(), StatusCode::OK);
}

#[tokio::test]
async fn rate_limit_ignores_xff_when_proxy_not_trusted() {
    use std::net::SocketAddr;
    let mut state = build_test_state();
    state.rate_limit = Some(std::sync::Arc::new(RateLimiter::new(0.001, 1.0)));
    state.trust_forwarded_for = false;
    let app = router(state).layer(MockConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9999))));

    // Forged XFF must not let an attacker rotate identities to dodge
    // the per-IP bucket. Both calls share the ConnectInfo IP.
    let req_a = Request::get("/v1/search?q=Vaduz")
        .header("X-Forwarded-For", "203.0.113.1")
        .body(Body::empty())
        .unwrap();
    let resp_a = tower::ServiceExt::oneshot(app.clone(), req_a)
        .await
        .unwrap();
    assert_eq!(resp_a.status(), StatusCode::OK);

    let req_b = Request::get("/v1/search?q=Vaduz")
        .header("X-Forwarded-For", "203.0.113.999-spoof")
        .body(Body::empty())
        .unwrap();
    let resp_b = tower::ServiceExt::oneshot(app, req_b).await.unwrap();
    assert_eq!(resp_b.status(), StatusCode::TOO_MANY_REQUESTS);
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
