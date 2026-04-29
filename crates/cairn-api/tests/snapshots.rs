//! Snapshot tests for /v1 response shapes.
//!
//! `insta` captures known-good JSON output from the synthetic test
//! bundle. Any wire-format drift fails the test, forcing a deliberate
//! review (run `cargo insta review` to accept changes). Volatile
//! fields (place_id is deterministic on this dataset; bundle_id is
//! constant; score is deterministic too) are NOT redacted — the
//! synthetic fixture is reproducible by construction.
//!
//! Update by running `cargo insta review` after a planned change.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use axum::body::Body;
use axum::http::Request;
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
        "cairn-api-snap-{}-{}-{}",
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
        admin_path: vec![PlaceId::new(0, 49509, 1).unwrap()],
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

fn build_state() -> AppState {
    let bundle = tempdir();
    let places = vec![vaduz(), liechtenstein()];
    let text_dir = bundle.join("index/text");
    build_index(&text_dir, places.clone()).unwrap();
    let text = TextIndex::open(&text_dir).unwrap();

    let admin = AdminIndex::build(AdminLayer {
        features: vec![liechtenstein_admin()],
    });
    let nearest = NearestIndex::build(PointLayer {
        points: places
            .iter()
            .map(|p| PlacePoint {
                place_id: p.id.0,
                level: p.id.level(),
                kind: cairn_text::kind_str(p.kind).to_string(),
                name: p.names[0].value.clone(),
                centroid: p.centroid,
                admin_path: p.admin_path.iter().map(|a| a.0).collect(),
            })
            .collect(),
    });

    AppState::new(
        bundle,
        cairn_api::BundleSnapshot {
            text: Some(Arc::new(cairn_api::FederatedText::from_single(Arc::new(
                text,
            )))),
            admin: Some(Arc::new(cairn_api::FederatedAdmin::from_single(Arc::new(
                admin,
            )))),
            nearest: Some(Arc::new(cairn_api::FederatedNearest::from_single(
                Arc::new(nearest),
            ))),
            bundle_ids: vec!["snapshot-bundle".into()],
        },
        Arc::new(Metrics::new("snapshot-bundle".into(), 2, 1)),
        None,
        None,
        false,
        Arc::new(Vec::new()),
    )
}

async fn fetch_json(state: AppState, uri: &str) -> Value {
    let req = Request::get(uri).body(Body::empty()).unwrap();
    let resp = router(state).oneshot(req).await.unwrap();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap_or(Value::Null)
}

#[tokio::test]
async fn snapshot_v1_search() {
    let body = fetch_json(build_state(), "/v1/search?q=Vaduz&limit=2").await;
    insta::assert_json_snapshot!("v1_search", body);
}

#[tokio::test]
async fn snapshot_v1_reverse_pip() {
    let body = fetch_json(build_state(), "/v1/reverse?lat=47.14&lon=9.55&limit=3").await;
    insta::assert_json_snapshot!("v1_reverse_pip", body);
}

#[tokio::test]
async fn snapshot_v1_structured() {
    let body = fetch_json(
        build_state(),
        "/v1/structured?country=Liechtenstein&limit=2",
    )
    .await;
    insta::assert_json_snapshot!("v1_structured", body);
}

#[tokio::test]
async fn snapshot_v1_layers() {
    let body = fetch_json(build_state(), "/v1/layers").await;
    insta::assert_json_snapshot!("v1_layers", body);
}

#[tokio::test]
async fn snapshot_v1_place() {
    let id = PlaceId::new(1, 49509, 1).unwrap().0;
    let body = fetch_json(build_state(), &format!("/v1/place?ids={id}")).await;
    insta::assert_json_snapshot!(
        "v1_place",
        body,
        {
            ".geocoding.engine.bundle_id" => "[bundle_id]",
        }
    );
}

#[tokio::test]
async fn snapshot_v1_expand() {
    let body = fetch_json(build_state(), "/v1/expand?q=123+Main+St").await;
    insta::assert_json_snapshot!("v1_expand", body);
}

#[tokio::test]
async fn snapshot_pelias_search() {
    let body = fetch_json(build_state(), "/search?text=Vaduz&size=2").await;
    insta::assert_json_snapshot!(
        "pelias_search",
        body,
        {
            ".geocoding.engine.bundle_id" => "[bundle_id]",
        }
    );
}

#[tokio::test]
async fn snapshot_error_envelope() {
    // Pin the error envelope shape so future changes need a deliberate review.
    let body = fetch_json(build_state(), "/v1/search?q=").await;
    insta::assert_json_snapshot!("v1_search_empty_error", body);
}
