//! Spatial layer: admin polygons + R*-tree for reverse geocoding.
//!
//! Phase 3 scope:
//! - `AdminFeature` carries a `MultiPolygon`, its bounding box, and enough
//!   metadata to hydrate a reverse-geocoding hit (PlaceId, kind, level,
//!   centroid, default name, admin_path).
//! - `AdminLayer::write_to` / `read_from` round-trip the feature list to a
//!   bincode blob on disk.
//! - `AdminIndex` builds an R*-tree of bbox envelopes and runs
//!   point-in-polygon for matching candidates.

use cairn_place::Coord;
use geo_types::{Coord as GeoCoord, MultiPolygon, Rect};
use rstar::{RTree, RTreeObject, AABB};
use serde::{Deserialize, Serialize};
use std::path::Path;
use thiserror::Error;
use tracing::debug;

#[derive(Debug, Error)]
pub enum SpatialError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("bincode: {0}")]
    Bincode(#[from] bincode::Error),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AdminFeature {
    pub place_id: u64,
    pub level: u8,
    pub kind: String,
    pub name: String,
    pub centroid: Coord,
    pub admin_path: Vec<u64>,
    pub polygon: MultiPolygon<f64>,
}

impl AdminFeature {
    pub fn bbox(&self) -> Option<Rect<f64>> {
        bbox_of(&self.polygon)
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct AdminLayer {
    pub features: Vec<AdminFeature>,
}

impl AdminLayer {
    pub fn write_to(&self, path: &Path) -> Result<u64, SpatialError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let bytes = bincode::serialize(self)?;
        std::fs::write(path, &bytes)?;
        Ok(bytes.len() as u64)
    }

    pub fn read_from(path: &Path) -> Result<Self, SpatialError> {
        let bytes = std::fs::read(path)?;
        let layer: AdminLayer = bincode::deserialize(&bytes)?;
        Ok(layer)
    }
}

#[derive(Clone, Debug)]
struct BboxItem {
    aabb: AABB<[f64; 2]>,
    feature_idx: usize,
}

impl RTreeObject for BboxItem {
    type Envelope = AABB<[f64; 2]>;
    fn envelope(&self) -> Self::Envelope {
        self.aabb
    }
}

/// R*-tree built from `AdminFeature` bounding boxes.
pub struct AdminIndex {
    tree: RTree<BboxItem>,
    features: Vec<AdminFeature>,
}

impl AdminIndex {
    pub fn build(layer: AdminLayer) -> Self {
        let mut entries: Vec<BboxItem> = Vec::new();
        for (idx, feature) in layer.features.iter().enumerate() {
            if let Some(bbox) = feature.bbox() {
                let aabb =
                    AABB::from_corners([bbox.min().x, bbox.min().y], [bbox.max().x, bbox.max().y]);
                entries.push(BboxItem {
                    aabb,
                    feature_idx: idx,
                });
            }
        }
        debug!(
            features = layer.features.len(),
            entries = entries.len(),
            "AdminIndex built"
        );
        let tree = RTree::bulk_load(entries);
        Self {
            tree,
            features: layer.features,
        }
    }

    pub fn len(&self) -> usize {
        self.features.len()
    }

    pub fn is_empty(&self) -> bool {
        self.features.is_empty()
    }

    /// Reverse query: every feature whose polygon contains the point.
    /// Sorted finest-to-coarsest by bbox area (smallest first).
    pub fn point_in_polygon(&self, coord: Coord) -> Vec<&AdminFeature> {
        let q = [coord.lon, coord.lat];
        let envelope = AABB::from_point(q);
        let candidates = self.tree.locate_in_envelope_intersecting(&envelope);

        let probe = GeoCoord {
            x: coord.lon,
            y: coord.lat,
        };
        let mut hits: Vec<(&AdminFeature, f64)> = Vec::new();
        for entry in candidates {
            let feature = &self.features[entry.feature_idx];
            if contains_point(&feature.polygon, probe) {
                let area = bbox_area(&feature.polygon).unwrap_or(f64::MAX);
                hits.push((feature, area));
            }
        }
        hits.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        hits.into_iter().map(|(f, _)| f).collect()
    }
}

fn bbox_of(mp: &MultiPolygon<f64>) -> Option<Rect<f64>> {
    use geo::BoundingRect;
    mp.bounding_rect()
}

fn bbox_area(mp: &MultiPolygon<f64>) -> Option<f64> {
    let r = bbox_of(mp)?;
    Some((r.max().x - r.min().x).abs() * (r.max().y - r.min().y).abs())
}

fn contains_point(mp: &MultiPolygon<f64>, p: GeoCoord<f64>) -> bool {
    use geo::Contains;
    mp.contains(&p)
}

/// Single-place spatial item kept around for legacy reverse-by-centroid use.
#[derive(Clone, Debug)]
pub struct SpatialEntry {
    pub place_id: cairn_place::PlaceId,
    pub centroid: Coord,
}

impl RTreeObject for SpatialEntry {
    type Envelope = AABB<[f64; 2]>;
    fn envelope(&self) -> Self::Envelope {
        AABB::from_point([self.centroid.lon, self.centroid.lat])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use geo_types::{LineString, Polygon};

    fn unit_square_at(cx: f64, cy: f64) -> MultiPolygon<f64> {
        let ext = LineString::from(vec![
            (cx - 0.5, cy - 0.5),
            (cx + 0.5, cy - 0.5),
            (cx + 0.5, cy + 0.5),
            (cx - 0.5, cy + 0.5),
            (cx - 0.5, cy - 0.5),
        ]);
        MultiPolygon(vec![Polygon::new(ext, vec![])])
    }

    fn feature(place_id: u64, name: &str, kind: &str, cx: f64, cy: f64) -> AdminFeature {
        AdminFeature {
            place_id,
            level: 0,
            kind: kind.into(),
            name: name.into(),
            centroid: Coord { lon: cx, lat: cy },
            admin_path: vec![],
            polygon: unit_square_at(cx, cy),
        }
    }

    #[test]
    fn pip_returns_only_containing_features() {
        let layer = AdminLayer {
            features: vec![
                feature(1, "A", "country", 0.0, 0.0),
                feature(2, "B", "country", 5.0, 5.0),
            ],
        };
        let idx = AdminIndex::build(layer);

        let hit = idx.point_in_polygon(Coord { lon: 0.1, lat: 0.1 });
        assert_eq!(hit.len(), 1);
        assert_eq!(hit[0].name, "A");

        let miss = idx.point_in_polygon(Coord {
            lon: 100.0,
            lat: 0.0,
        });
        assert!(miss.is_empty());
    }

    #[test]
    fn pip_sorts_finest_first() {
        // Big country square + a smaller city square fully contained.
        let big = AdminFeature {
            place_id: 1,
            level: 0,
            kind: "country".into(),
            name: "Country".into(),
            centroid: Coord { lon: 0.0, lat: 0.0 },
            admin_path: vec![],
            polygon: MultiPolygon(vec![Polygon::new(
                LineString::from(vec![
                    (-1.0, -1.0),
                    (1.0, -1.0),
                    (1.0, 1.0),
                    (-1.0, 1.0),
                    (-1.0, -1.0),
                ]),
                vec![],
            )]),
        };
        let small = feature(2, "City", "city", 0.0, 0.0);
        let layer = AdminLayer {
            features: vec![big, small],
        };
        let idx = AdminIndex::build(layer);
        let hit = idx.point_in_polygon(Coord { lon: 0.0, lat: 0.0 });
        assert_eq!(hit.len(), 2);
        assert_eq!(hit[0].name, "City", "smaller polygon must be first");
        assert_eq!(hit[1].name, "Country");
    }

    #[test]
    fn layer_roundtrip_to_disk() {
        let layer = AdminLayer {
            features: vec![feature(1, "A", "country", 0.0, 0.0)],
        };
        let dir = std::env::temp_dir().join(format!(
            "cairn-spatial-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("admin.bin");
        let n = layer.write_to(&path).unwrap();
        assert!(n > 0);

        let read_back = AdminLayer::read_from(&path).unwrap();
        assert_eq!(read_back.features.len(), 1);
        assert_eq!(read_back.features[0].name, "A");
    }
}
