//! Multi-bundle federation wrappers.
//!
//! `cairn-serve --bundles a/,b/,c/` lets a single process answer
//! requests against several bundles in parallel — the operational
//! pattern for splitting a planet into continental shards. Each
//! bundle keeps its own tantivy index, admin layer, nearest layer,
//! and `admin_names.json` sidecar.
//!
//! These wrappers fan a single API call across every bundle, then
//! merge results so handlers stay bundle-agnostic. A federation of
//! exactly one bundle short-circuits to a direct call so single-
//! bundle deploys pay no overhead.
//!
//! Score / distance comparisons happen across bundle outputs as if
//! they were one index. Place IDs are not globally unique — each
//! bundle has its own `(level, tile, local)` namespace — but Hits
//! carry pre-rendered names + labels (each bundle's `admin_names`
//! sidecar populates its own hits before merge), so collisions
//! don't cause cross-bundle label pollution.

use cairn_place::Coord;
use cairn_spatial::buildings::{Building, BuildingIndex};
use cairn_spatial::{AdminFeature, AdminIndex, NearestIndex, PlacePoint};
use cairn_text::{Hit, SearchOptions, TextError, TextIndex};
use rayon::prelude::*;
use std::sync::Arc;

/// Federated wrapper around N tantivy text indices.
pub struct FederatedText {
    bundles: Vec<Arc<TextIndex>>,
}

impl FederatedText {
    /// Wrap a single existing index. Used by single-bundle deploys
    /// so the federation layer stays a no-op.
    pub fn from_single(idx: Arc<TextIndex>) -> Self {
        Self { bundles: vec![idx] }
    }

    /// Wrap multiple bundles. Empty input panics — callers must
    /// guard at startup so that running with zero bundles is a
    /// configuration error, not a runtime mystery.
    pub fn from_many(bundles: Vec<Arc<TextIndex>>) -> Self {
        assert!(!bundles.is_empty(), "FederatedText requires >= 1 bundle");
        Self { bundles }
    }

    pub fn len(&self) -> usize {
        self.bundles.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bundles.is_empty()
    }

    /// Fan-out search. Each bundle runs the full pipeline (BM25 +
    /// rerank + label render). Results are concatenated, sorted by
    /// final score, truncated to `opts.limit`. Single-bundle case
    /// short-circuits to the underlying call.
    pub fn search(&self, query: &str, opts: &SearchOptions) -> Result<Vec<Hit>, TextError> {
        if self.bundles.len() == 1 {
            return self.bundles[0].search(query, opts);
        }
        // Phase 6e B2 — fan out across bundles in parallel. Each
        // tantivy search runs independently on its own thread; merge
        // happens after every shard returns. 2-3× p95 on multi-shard
        // deploys vs the prior sequential `for` loop.
        let per_bundle: Result<Vec<Vec<Hit>>, TextError> = self
            .bundles
            .par_iter()
            .map(|b| b.search(query, opts))
            .collect();
        let mut all: Vec<Hit> = per_bundle?.into_iter().flatten().collect();
        all.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        all.truncate(opts.limit);
        Ok(all)
    }

    /// `/v1/place?ids=…` resolver. Each bundle is asked for the full
    /// id list; misses are silently skipped per-bundle. Result order
    /// follows `ids` for the first bundle that hits each id, then
    /// the second, etc — same shape as the single-bundle path so
    /// callers don't have to special-case.
    pub fn lookup_by_ids(&self, ids: &[u64]) -> Result<Vec<Hit>, TextError> {
        if self.bundles.len() == 1 {
            return self.bundles[0].lookup_by_ids(ids);
        }
        let per_bundle: Result<Vec<Vec<Hit>>, TextError> = self
            .bundles
            .par_iter()
            .map(|b| b.lookup_by_ids(ids))
            .collect();
        Ok(per_bundle?.into_iter().flatten().collect())
    }

    /// Pelias-style gid resolver. Same fan-out shape as
    /// [`Self::lookup_by_ids`] but keys on the rebuild-stable
    /// `<source>:<type>:<id>` string. Suitable for the
    /// `/v1/place?ids=osm:way:1,wof:locality:2` endpoint.
    pub fn lookup_by_gids(&self, gids: &[String]) -> Result<Vec<Hit>, TextError> {
        if self.bundles.len() == 1 {
            return self.bundles[0].lookup_by_gids(gids);
        }
        let per_bundle: Result<Vec<Vec<Hit>>, TextError> = self
            .bundles
            .par_iter()
            .map(|b| b.lookup_by_gids(gids))
            .collect();
        Ok(per_bundle?.into_iter().flatten().collect())
    }
}

/// Federated wrapper around N admin layers.
pub struct FederatedAdmin {
    bundles: Vec<Arc<AdminIndex>>,
}

impl FederatedAdmin {
    pub fn from_single(idx: Arc<AdminIndex>) -> Self {
        Self { bundles: vec![idx] }
    }

    pub fn from_many(bundles: Vec<Arc<AdminIndex>>) -> Self {
        assert!(!bundles.is_empty(), "FederatedAdmin requires >= 1 bundle");
        Self { bundles }
    }

    /// Sum of admin-feature counts across every bundle. Used for the
    /// `cairn_admin_features` Prometheus gauge.
    pub fn len(&self) -> usize {
        self.bundles.iter().map(|b| b.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.bundles.iter().all(|b| b.is_empty())
    }

    /// Fan-out PIP. Each bundle's R*-tree returns its hits; the
    /// merged list is sorted finest-first (highest `level`) so the
    /// reverse handler's `take(limit)` semantics still surface the
    /// most-specific match.
    pub fn point_in_polygon(&self, coord: Coord) -> Vec<AdminFeature> {
        if self.bundles.len() == 1 {
            return self.bundles[0].point_in_polygon(coord);
        }
        let per_bundle: Vec<Vec<AdminFeature>> = self
            .bundles
            .par_iter()
            .map(|b| b.point_in_polygon(coord))
            .collect();
        let mut all: Vec<AdminFeature> = per_bundle.into_iter().flatten().collect();
        // Higher level = finer admin tier (city > country). Reverse
        // sort so leaf matches come first.
        all.sort_by_key(|f| std::cmp::Reverse(f.level));
        all
    }
}

/// Federated wrapper around N nearest layers.
pub struct FederatedNearest {
    bundles: Vec<Arc<NearestIndex>>,
}

impl FederatedNearest {
    pub fn from_single(idx: Arc<NearestIndex>) -> Self {
        Self { bundles: vec![idx] }
    }

    pub fn from_many(bundles: Vec<Arc<NearestIndex>>) -> Self {
        assert!(!bundles.is_empty(), "FederatedNearest requires >= 1 bundle");
        Self { bundles }
    }

    pub fn len(&self) -> usize {
        self.bundles.iter().map(|b| b.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.bundles.iter().all(|b| b.is_empty())
    }

    /// Fan-out kNN. Each bundle returns its own top-k; the merged
    /// list is re-sorted by haversine distance to the query point
    /// and truncated to `k` so the global top-k surfaces.
    pub fn nearest_k(&self, coord: Coord, k: usize) -> Vec<PlacePoint> {
        self.nearest_k_filtered(coord, k, |_| true)
    }

    /// Phase 7a-H — filtered kNN. Each bundle's filtered nearest list
    /// is merged + re-sorted globally. Used by the context-aware
    /// reverse endpoint to fetch nearest road / POI / address per
    /// federation member without dropping fed semantics.
    pub fn nearest_k_filtered<F>(&self, coord: Coord, k: usize, keep: F) -> Vec<PlacePoint>
    where
        F: Fn(&PlacePoint) -> bool + Sync,
    {
        if self.bundles.len() == 1 {
            return self.bundles[0].nearest_k_filtered(coord, k, &keep);
        }
        // Phase 6e B2 — parallel fan-out. `&keep` impls FnMut via the
        // stdlib blanket impl on `&F where F: Fn`, so the inner
        // NearestIndex method's FnMut bound is still satisfied.
        let per_bundle: Vec<Vec<PlacePoint>> = self
            .bundles
            .par_iter()
            .map(|b| b.nearest_k_filtered(coord, k, &keep))
            .collect();
        let mut all: Vec<PlacePoint> = per_bundle.into_iter().flatten().collect();
        all.sort_by(|a, b| {
            haversine_km(coord.lat, coord.lon, a.centroid.lat, a.centroid.lon)
                .partial_cmp(&haversine_km(
                    coord.lat,
                    coord.lon,
                    b.centroid.lat,
                    b.centroid.lon,
                ))
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        all.truncate(k);
        all
    }
}

/// Federated wrapper around N building layers (v0.3 lane A).
pub struct FederatedBuildings {
    bundles: Vec<Arc<BuildingIndex>>,
}

impl FederatedBuildings {
    pub fn from_single(idx: Arc<BuildingIndex>) -> Self {
        Self { bundles: vec![idx] }
    }

    pub fn from_many(bundles: Vec<Arc<BuildingIndex>>) -> Self {
        assert!(
            !bundles.is_empty(),
            "FederatedBuildings requires >= 1 bundle"
        );
        Self { bundles }
    }

    pub fn len(&self) -> usize {
        self.bundles.iter().map(|b| b.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.bundles.iter().all(|b| b.is_empty())
    }

    /// Buildings whose **outer ring** contains `coord` (strict
    /// rooftop-level PIP). Merged finest-first across all federation
    /// members so a courtyard inside a larger complex still wins
    /// ordering. For the bbox-only fast path see [`Self::at_bbox`].
    pub fn at(&self, coord: Coord) -> Vec<Building> {
        if self.bundles.len() == 1 {
            return self.bundles[0].at(coord);
        }
        let mut all: Vec<Building> = Vec::new();
        for b in &self.bundles {
            all.extend(b.at(coord));
        }
        Self::sort_by_bbox_area(&mut all);
        all
    }

    /// Bbox-only variant. Cheaper but over-includes (~10–30 % false
    /// positives at urban density on L- / U-shaped footprints).
    /// Useful for callers that already plan to refine downstream
    /// or want the union of overlapping bboxes for audit output.
    pub fn at_bbox(&self, coord: Coord) -> Vec<Building> {
        if self.bundles.len() == 1 {
            return self.bundles[0].at_bbox(coord);
        }
        let mut all: Vec<Building> = Vec::new();
        for b in &self.bundles {
            all.extend(b.at_bbox(coord));
        }
        Self::sort_by_bbox_area(&mut all);
        all
    }

    fn sort_by_bbox_area(buildings: &mut [Building]) {
        buildings.sort_by(|a, b| {
            let aa = (a.bbox[2] - a.bbox[0]).abs() * (a.bbox[3] - a.bbox[1]).abs();
            let ba = (b.bbox[2] - b.bbox[0]).abs() * (b.bbox[3] - b.bbox[1]).abs();
            aa.partial_cmp(&ba).unwrap_or(std::cmp::Ordering::Equal)
        });
    }

    /// Top-`k` buildings by centroid distance, merged across
    /// federation members and re-sorted globally so the absolute
    /// closest k surface even if any one bundle has only far hits.
    pub fn nearest_k(&self, coord: Coord, k: usize) -> Vec<Building> {
        if self.bundles.len() == 1 {
            return self.bundles[0].nearest_k(coord, k);
        }
        let mut all: Vec<Building> = Vec::new();
        for b in &self.bundles {
            all.extend(b.nearest_k(coord, k));
        }
        all.sort_by(|a, b| {
            let da = (a.centroid[0] - coord.lon).powi(2) + (a.centroid[1] - coord.lat).powi(2);
            let db = (b.centroid[0] - coord.lon).powi(2) + (b.centroid[1] - coord.lat).powi(2);
            da.partial_cmp(&db).unwrap_or(std::cmp::Ordering::Equal)
        });
        all.truncate(k);
        all
    }
}

fn haversine_km(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const R_KM: f64 = 6371.0;
    let to_rad = std::f64::consts::PI / 180.0;
    let phi1 = lat1 * to_rad;
    let phi2 = lat2 * to_rad;
    let dphi = (lat2 - lat1) * to_rad;
    let dlam = (lon2 - lon1) * to_rad;
    let h = (dphi / 2.0).sin().powi(2) + phi1.cos() * phi2.cos() * (dlam / 2.0).sin().powi(2);
    2.0 * R_KM * h.sqrt().asin()
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_spatial::PointLayer;

    fn point(id: u64, lon: f64, lat: f64, name: &str) -> PlacePoint {
        PlacePoint {
            place_id: id,
            level: 2,
            kind: "poi".into(),
            name: name.into(),
            centroid: Coord { lon, lat },
            admin_path: vec![],
        }
    }

    #[test]
    fn federated_nearest_merges_top_k_across_bundles() {
        let a = NearestIndex::build(PointLayer {
            points: vec![point(1, 9.5, 47.1, "Vaduz"), point(2, 9.6, 47.2, "Schaan")],
        });
        let b = NearestIndex::build(PointLayer {
            points: vec![point(3, 9.55, 47.15, "Triesen")],
        });
        let fed = FederatedNearest::from_many(vec![Arc::new(a), Arc::new(b)]);
        let probe = Coord {
            lon: 9.55,
            lat: 47.15,
        };
        let hits = fed.nearest_k(probe, 2);
        assert_eq!(hits.len(), 2);
        // Closest must be Triesen (id=3) — it's at the probe.
        assert_eq!(hits[0].place_id, 3);
    }

    #[test]
    fn federated_nearest_len_sums_across_bundles() {
        let a = NearestIndex::build(PointLayer {
            points: vec![point(1, 0.0, 0.0, "a")],
        });
        let b = NearestIndex::build(PointLayer {
            points: vec![point(2, 1.0, 1.0, "b"), point(3, 2.0, 2.0, "c")],
        });
        let fed = FederatedNearest::from_many(vec![Arc::new(a), Arc::new(b)]);
        assert_eq!(fed.len(), 3);
    }

    #[test]
    #[should_panic(expected = "requires >= 1 bundle")]
    fn empty_text_federation_panics() {
        let _ = FederatedText::from_many(Vec::new());
    }
}
