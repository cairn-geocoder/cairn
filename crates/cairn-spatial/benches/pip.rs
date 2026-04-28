//! Microbenchmarks for the spatial hot paths.
//!
//! Run with: `cargo bench -p cairn-spatial`.
//!
//! Two scenarios:
//! 1. `pip_eager` — single-tile in-memory AdminIndex (`build`); measures
//!    pure PIP cost over a small admin set.
//! 2. `nearest_eager` — single-tile in-memory NearestIndex; measures
//!    nearest-k linear scan + sort cost.

use cairn_place::Coord;
use cairn_spatial::{AdminFeature, AdminIndex, AdminLayer, NearestIndex, PlacePoint, PointLayer};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use geo_types::{LineString, MultiPolygon, Polygon};

fn unit_square_at(cx: f64, cy: f64, half: f64) -> MultiPolygon<f64> {
    let ext = LineString::from(vec![
        (cx - half, cy - half),
        (cx + half, cy - half),
        (cx + half, cy + half),
        (cx - half, cy + half),
        (cx - half, cy - half),
    ]);
    MultiPolygon(vec![Polygon::new(ext, vec![])])
}

fn synthetic_admin_layer(n: usize) -> AdminLayer {
    let side = (n as f64).sqrt().ceil() as i32;
    let mut features = Vec::with_capacity(n);
    let mut id = 0u64;
    for i in 0..side {
        for j in 0..side {
            if features.len() >= n {
                break;
            }
            let lon = -90.0 + (i as f64) * 0.5;
            let lat = -45.0 + (j as f64) * 0.5;
            features.push(AdminFeature {
                place_id: id,
                level: 1,
                kind: "city".into(),
                name: format!("city-{id}"),
                centroid: Coord { lon, lat },
                admin_path: vec![],
                polygon: unit_square_at(lon, lat, 0.2),
            });
            id += 1;
        }
    }
    AdminLayer { features }
}

fn synthetic_point_layer(n: usize) -> PointLayer {
    let side = (n as f64).sqrt().ceil() as i32;
    let mut points = Vec::with_capacity(n);
    let mut id = 0u64;
    for i in 0..side {
        for j in 0..side {
            if points.len() >= n {
                break;
            }
            let lon = -90.0 + (i as f64) * 0.25;
            let lat = -45.0 + (j as f64) * 0.25;
            points.push(PlacePoint {
                place_id: id,
                level: 2,
                kind: "city".into(),
                name: format!("city-{id}"),
                centroid: Coord { lon, lat },
                admin_path: vec![],
            });
            id += 1;
        }
    }
    PointLayer { points }
}

fn bench_pip(c: &mut Criterion) {
    let layer = synthetic_admin_layer(1024);
    let idx = AdminIndex::build(layer);
    c.bench_function("pip_eager_1024", |b| {
        b.iter(|| {
            let q = Coord {
                lon: black_box(-45.0),
                lat: black_box(0.0),
            };
            let _ = idx.point_in_polygon(q);
        })
    });
}

fn bench_nearest(c: &mut Criterion) {
    let layer = synthetic_point_layer(4096);
    let idx = NearestIndex::build(layer);
    c.bench_function("nearest_k=10_4096pts", |b| {
        b.iter(|| {
            let q = Coord {
                lon: black_box(-45.0),
                lat: black_box(0.0),
            };
            let _ = idx.nearest_k(q, 10);
        })
    });
}

criterion_group!(benches, bench_pip, bench_nearest);
criterion_main!(benches);
