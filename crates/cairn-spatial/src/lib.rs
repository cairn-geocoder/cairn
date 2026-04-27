//! Spatial indexing (rstar) + point-in-polygon for reverse geocoding.
//!
//! Phase 0 stubs. Concrete index lands in Phase 3.

#![allow(dead_code)]

use cairn_place::{Coord, PlaceId};
use rstar::{PointDistance, RTreeObject, AABB};

#[derive(Clone, Debug)]
pub struct SpatialEntry {
    pub id: PlaceId,
    pub centroid: Coord,
}

impl RTreeObject for SpatialEntry {
    type Envelope = AABB<[f64; 2]>;
    fn envelope(&self) -> Self::Envelope {
        AABB::from_point([self.centroid.lon, self.centroid.lat])
    }
}

impl PointDistance for SpatialEntry {
    fn distance_2(&self, p: &[f64; 2]) -> f64 {
        let dx = self.centroid.lon - p[0];
        let dy = self.centroid.lat - p[1];
        dx * dx + dy * dy
    }
}
