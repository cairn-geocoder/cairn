//! Spatial layers for the Cairn geocoder.
//!
//! Phase 6c scope:
//! - `AdminFeature` and `PlacePoint` are the canonical row types written
//!   to disk and consumed by the runtime indices.
//! - On disk, both types are partitioned per Valhalla-style tile under
//!   `spatial/admin/<level>/<a>/<b>/<id>.bin` and
//!   `spatial/points/<level>/<a>/<b>/<id>.bin`. `manifest.toml` carries
//!   one [`SpatialTileEntry`] per file.
//! - At runtime, `AdminIndex::open` and `NearestIndex::open` build an
//!   R*-tree of tile bboxes from the manifest entries. The actual
//!   per-tile feature/point list is loaded on first touch via
//!   `OnceLock<Arc<Vec<…>>>` and kept around for the process lifetime.
//!   No LRU eviction yet — country-scale memory is bounded; planet-scale
//!   eviction is a follow-up.
//! - Polygons that span multiple tiles are replicated into each tile
//!   they intersect at write time so a PIP query in any covered tile
//!   finds them. The replication factor is 1.0–2.5× in practice.
//! - For unit tests + legacy single-file callers, `AdminIndex::build(layer)`
//!   and `NearestIndex::build(layer)` keep the in-memory eager path —
//!   they construct a single virtual tile with the full feature list.

use cairn_place::Coord;
use cairn_tile::{Level, SpatialTileEntry, TileCoord};
use geo_types::{Coord as GeoCoord, MultiPolygon, Rect};
use lru::LruCache;
use rstar::{RTree, RTreeObject, AABB};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use thiserror::Error;
use tracing::debug;

pub mod archived;

/// Default LRU capacity (per index) when the caller doesn't pick one.
/// Each entry holds one tile's feature/point list. Country-scale bundles
/// rarely exceed a few hundred non-empty tiles; planet-scale callers
/// should tune this down once memory pressure shows up.
pub const DEFAULT_TILE_CACHE_ENTRIES: usize = 1024;

#[derive(Debug, Error)]
pub enum SpatialError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("bincode: {0}")]
    Bincode(#[from] bincode::Error),
    #[error("archived: {0}")]
    Archived(String),
    #[error("unknown tile level {0}")]
    UnknownLevel(u8),
}

// ============================================================
// Row types
// ============================================================

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

/// Compact place pointer used for the nearest-neighbour fallback layer.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PlacePoint {
    pub place_id: u64,
    pub level: u8,
    pub kind: String,
    pub name: String,
    pub centroid: Coord,
    pub admin_path: Vec<u64>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct AdminLayer {
    pub features: Vec<AdminFeature>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PointLayer {
    pub points: Vec<PlacePoint>,
}

// ============================================================
// On-disk path layout
// ============================================================

fn admin_rel_path(level: u8, tile_id: u32) -> String {
    let a = tile_id / 1_000_000 % 1000;
    let b = tile_id / 1000 % 1000;
    format!("spatial/admin/{level}/{a:03}/{b:03}/{tile_id}.bin")
}

fn points_rel_path(level: u8, tile_id: u32) -> String {
    let a = tile_id / 1_000_000 % 1000;
    let b = tile_id / 1000 % 1000;
    format!("spatial/points/{level}/{a:03}/{b:03}/{tile_id}.bin")
}

fn tile_bbox(level: Level, tile_id: u32) -> (f64, f64, f64, f64) {
    TileCoord::from_id(level, tile_id).bbox()
}

// ============================================================
// Cross-source dedup
// ============================================================

/// Quantize a coordinate to a ~100m grid cell (≈ 0.001° at the equator)
/// for grouping near-identical centroids regardless of float noise.
fn quantize_centroid(c: Coord) -> (i32, i32) {
    let lon = (c.lon * 1000.0).round() as i32;
    let lat = (c.lat * 1000.0).round() as i32;
    (lon, lat)
}

/// Score a feature's "admin richness". A WoF feature with a parent chain
/// (`admin_path`) is preferred over a bare OSM relation that landed with
/// an empty path; ties break on name length so non-empty names beat empty.
fn dedup_score(f: &AdminFeature) -> (usize, usize) {
    (f.admin_path.len(), f.name.len())
}

/// Collapse near-duplicate AdminFeatures emitted by overlapping sources
/// (typically WoF + OSM both shipping the same country / region polygon).
/// Two features collide when they share `kind` and their centroids quantize
/// to the same ~100m grid cell. Tiebreaker order:
///   1. `--source-priority` rank (lower index wins).
///   2. Richer feature (longer admin_path, then longer name).
///
/// `priority` is a list of [`SourceKind`]s in preferred order. Empty
/// priority falls back to richness-only.
///
/// Polygon geometry is left untouched on the winner; we don't try to
/// reconcile slightly different OSM and WoF rings here, only avoid the
/// duplicate user-visible result.
pub fn dedupe_features(
    items: Vec<(AdminFeature, cairn_place::SourceKind)>,
    priority: &[cairn_place::SourceKind],
) -> Vec<AdminFeature> {
    let mut best: BTreeMap<(String, i32, i32), (AdminFeature, cairn_place::SourceKind)> =
        BTreeMap::new();
    let mut dropped = 0usize;
    for (feat, src) in items {
        let key = (
            feat.kind.clone(),
            quantize_centroid(feat.centroid).0,
            quantize_centroid(feat.centroid).1,
        );
        match best.get(&key) {
            Some((existing, existing_src))
                if !feature_better(&feat, src, existing, *existing_src, priority) =>
            {
                dropped += 1;
            }
            _ => {
                if best.insert(key, (feat, src)).is_some() {
                    dropped += 1;
                }
            }
        }
    }
    if dropped > 0 {
        debug!(
            dropped,
            kept = best.len(),
            "dedupe_features collapsed near-duplicates"
        );
    }
    best.into_values().map(|(f, _)| f).collect()
}

fn feature_better(
    a: &AdminFeature,
    a_src: cairn_place::SourceKind,
    b: &AdminFeature,
    b_src: cairn_place::SourceKind,
    priority: &[cairn_place::SourceKind],
) -> bool {
    let a_rank = priority.iter().position(|p| *p == a_src);
    let b_rank = priority.iter().position(|p| *p == b_src);
    match (a_rank, b_rank) {
        (Some(ar), Some(br)) if ar != br => ar < br,
        (Some(_), None) => true,
        (None, Some(_)) => false,
        _ => dedup_score(a) > dedup_score(b),
    }
}

// ============================================================
// Polygon simplification
// ============================================================

/// Apply Douglas–Peucker simplification to every AdminFeature's polygon
/// in place. `tolerance_deg` is the maximum vertex deviation from the
/// original ring, in degrees of latitude/longitude (no projection). At
/// the equator, ~0.0001° ≈ 11 m, ~0.001° ≈ 111 m, ~0.01° ≈ 1.1 km.
///
/// Sub-meter tolerances barely shrink anything; admin boundaries don't
/// need that resolution. Country / region / county polygons work well at
/// 0.0005 – 0.005°. City polygons at 0.0001 – 0.0005°. Anything bigger
/// risks visible simplification of dense urban edges.
///
/// Skipped when `tolerance_deg <= 0.0`. Mutates in place — callers
/// already build the layer fresh per import, so reusing the same
/// allocations is fine.
pub fn simplify_admin_layer(layer: &mut AdminLayer, tolerance_deg: f64) {
    if tolerance_deg <= 0.0 {
        return;
    }
    use geo::Simplify;
    for feat in &mut layer.features {
        feat.polygon = feat.polygon.simplify(&tolerance_deg);
    }
}

// ============================================================
// Partitioned write
// ============================================================

/// Write the admin layer to per-tile files under `bundle_root`. Each
/// feature is replicated into every tile its bbox intersects so PIP
/// queries in any of those tiles still find it.
///
/// On-disk format is rkyv (`cairn_spatial::archived::serialize_layer`)
/// with a 16-byte aligned header. PIP at runtime ray-casts directly on
/// the archived ring vertices via `pip_archived`, skipping
/// `MultiPolygon<f64>` hydration entirely.
pub fn write_admin_partitioned(
    bundle_root: &Path,
    layer: &AdminLayer,
) -> Result<Vec<SpatialTileEntry>, SpatialError> {
    let mut buckets: BTreeMap<(u8, u32), Vec<AdminFeature>> = BTreeMap::new();
    for feat in &layer.features {
        let level = Level::from_u8(feat.level).ok_or(SpatialError::UnknownLevel(feat.level))?;
        let bbox = match feat.bbox() {
            Some(b) => b,
            None => continue,
        };
        let lo_tile = TileCoord::from_coord(
            level,
            Coord {
                lon: bbox.min().x,
                lat: bbox.min().y,
            },
        );
        let hi_tile = TileCoord::from_coord(
            level,
            Coord {
                lon: bbox.max().x,
                lat: bbox.max().y,
            },
        );
        for row in lo_tile.row..=hi_tile.row {
            for col in lo_tile.col..=hi_tile.col {
                let tc = TileCoord { level, row, col };
                buckets
                    .entry((tc.level.as_u8(), tc.id()))
                    .or_default()
                    .push(feat.clone());
            }
        }
    }

    let mut entries = Vec::with_capacity(buckets.len());
    for ((level_u8, tile_id), feats) in buckets {
        let rel = admin_rel_path(level_u8, tile_id);
        let abs = bundle_root.join(&rel);
        if let Some(parent) = abs.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let archived_features: Vec<archived::ArchivedAdminFeature> =
            feats.iter().map(archived::to_archived).collect();
        let archived_layer = archived::ArchivedAdminLayer {
            features: archived_features,
        };
        let blob = archived::serialize_layer(&archived_layer)
            .map_err(|e| SpatialError::Archived(format!("{e:?}")))?;
        std::fs::write(&abs, &blob[..])?;
        let level = Level::from_u8(level_u8).ok_or(SpatialError::UnknownLevel(level_u8))?;
        let (min_lon, min_lat, max_lon, max_lat) = tile_bbox(level, tile_id);
        entries.push(SpatialTileEntry {
            level: level_u8,
            tile_id,
            min_lon,
            min_lat,
            max_lon,
            max_lat,
            item_count: feats.len() as u64,
            byte_size: blob.len() as u64,
            blake3: blake3::hash(&blob[..]).to_hex().to_string(),
            rel_path: rel,
        });
    }
    Ok(entries)
}

/// Write the point layer to per-tile bincode files. Each PlacePoint
/// lands in the single tile its centroid falls into.
///
/// Format choice: bincode (not rkyv). Tried rkyv and measured a 68 %
/// bundle-size regression on the Liechtenstein corpus
/// (246 kB → 414 kB) — rkyv's per-field offset metadata dominates for
/// String-heavy structs like PlacePoint, where rings of f64 vertices
/// don't amortize the overhead. Linear-scan nearest-k doesn't benefit
/// from zero-copy mmap either, so there's no runtime win to pay for
/// the size penalty.
pub fn write_points_partitioned(
    bundle_root: &Path,
    layer: &PointLayer,
) -> Result<Vec<SpatialTileEntry>, SpatialError> {
    let mut buckets: BTreeMap<(u8, u32), Vec<PlacePoint>> = BTreeMap::new();
    for p in &layer.points {
        let level = Level::from_u8(p.level).ok_or(SpatialError::UnknownLevel(p.level))?;
        let tc = TileCoord::from_coord(level, p.centroid);
        buckets
            .entry((tc.level.as_u8(), tc.id()))
            .or_default()
            .push(p.clone());
    }

    let mut entries = Vec::with_capacity(buckets.len());
    for ((level_u8, tile_id), pts) in buckets {
        let rel = points_rel_path(level_u8, tile_id);
        let abs = bundle_root.join(&rel);
        if let Some(parent) = abs.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let layer = PointLayer {
            points: pts.clone(),
        };
        let bytes = bincode::serialize(&layer)?;
        std::fs::write(&abs, &bytes)?;
        let level = Level::from_u8(level_u8).ok_or(SpatialError::UnknownLevel(level_u8))?;
        let (min_lon, min_lat, max_lon, max_lat) = tile_bbox(level, tile_id);
        entries.push(SpatialTileEntry {
            level: level_u8,
            tile_id,
            min_lon,
            min_lat,
            max_lon,
            max_lat,
            item_count: pts.len() as u64,
            byte_size: bytes.len() as u64,
            blake3: blake3::hash(&bytes).to_hex().to_string(),
            rel_path: rel,
        });
    }
    Ok(entries)
}

// ============================================================
// AdminIndex (R*-tree of tile bboxes + lazy per-tile load)
// ============================================================

#[derive(Clone, Debug)]
struct TileEnvelope {
    aabb: AABB<[f64; 2]>,
    slot_idx: usize,
}

impl RTreeObject for TileEnvelope {
    type Envelope = AABB<[f64; 2]>;
    fn envelope(&self) -> Self::Envelope {
        self.aabb
    }
}

enum AdminTileSource {
    Eager(Arc<archived::AdminTileArchive>),
    Disk(PathBuf),
}

pub struct AdminIndex {
    slots: Vec<AdminTileSource>,
    tree: RTree<TileEnvelope>,
    cache: Mutex<LruCache<usize, Arc<archived::AdminTileArchive>>>,
    total_items: u64,
}

impl AdminIndex {
    fn load_slot(&self, idx: usize) -> Arc<archived::AdminTileArchive> {
        match &self.slots[idx] {
            AdminTileSource::Eager(arc) => arc.clone(),
            AdminTileSource::Disk(path) => {
                {
                    let mut cache = self.cache.lock().expect("admin cache poisoned");
                    if let Some(arc) = cache.get(&idx) {
                        return arc.clone();
                    }
                }
                let arc = read_admin_tile(path);
                let mut cache = self.cache.lock().expect("admin cache poisoned");
                cache.put(idx, arc.clone());
                arc
            }
        }
    }
}

fn read_admin_tile(path: &Path) -> Arc<archived::AdminTileArchive> {
    match archived::AdminTileArchive::from_path(path) {
        Ok(t) => Arc::new(t),
        Err(err) => {
            debug!(?err, ?path, "admin tile mmap+validate failed");
            // Fallback: synthesize an empty archive so callers don't
            // crash. An empty AdminTileArchive is built via from_aligned
            // with a serialized empty layer.
            let empty =
                archived::serialize_layer(&archived::ArchivedAdminLayer::default()).unwrap();
            Arc::new(archived::AdminTileArchive::from_aligned(empty).unwrap())
        }
    }
}

impl AdminIndex {
    /// Build a one-tile in-memory index from a fully-loaded AdminLayer.
    /// Useful for tests + small legacy callers.
    pub fn build(layer: AdminLayer) -> Self {
        let total_items = layer.features.len() as u64;
        let world_bbox = layer
            .features
            .iter()
            .filter_map(|f| f.bbox())
            .fold(None::<Rect<f64>>, |acc, r| match acc {
                None => Some(r),
                Some(prev) => Some(Rect::new(
                    GeoCoord {
                        x: prev.min().x.min(r.min().x),
                        y: prev.min().y.min(r.min().y),
                    },
                    GeoCoord {
                        x: prev.max().x.max(r.max().x),
                        y: prev.max().y.max(r.max().y),
                    },
                )),
            })
            .unwrap_or_else(|| {
                Rect::new(
                    GeoCoord {
                        x: -180.0,
                        y: -90.0,
                    },
                    GeoCoord { x: 180.0, y: 90.0 },
                )
            });
        let aabb = AABB::from_corners(
            [world_bbox.min().x, world_bbox.min().y],
            [world_bbox.max().x, world_bbox.max().y],
        );
        let archived_features: Vec<archived::ArchivedAdminFeature> =
            layer.features.iter().map(archived::to_archived).collect();
        let archived_layer = archived::ArchivedAdminLayer {
            features: archived_features,
        };
        let blob = archived::serialize_layer(&archived_layer)
            .expect("serialize_layer infallible for in-memory data");
        let tile = archived::AdminTileArchive::from_aligned(blob)
            .expect("just-serialized layer must validate");
        let slot = AdminTileSource::Eager(Arc::new(tile));
        let tree = RTree::bulk_load(vec![TileEnvelope { aabb, slot_idx: 0 }]);
        Self {
            slots: vec![slot],
            tree,
            cache: Mutex::new(LruCache::new(
                NonZeroUsize::new(DEFAULT_TILE_CACHE_ENTRIES).unwrap(),
            )),
            total_items,
        }
    }

    /// Open a partitioned admin index with the default LRU cache size.
    pub fn open(bundle_root: &Path, entries: Vec<SpatialTileEntry>) -> Self {
        Self::open_with_cache(bundle_root, entries, DEFAULT_TILE_CACHE_ENTRIES)
    }

    /// Open a partitioned admin index with a custom LRU cache size.
    pub fn open_with_cache(
        bundle_root: &Path,
        entries: Vec<SpatialTileEntry>,
        cache_entries: usize,
    ) -> Self {
        let mut slots: Vec<AdminTileSource> = Vec::with_capacity(entries.len());
        let mut envs: Vec<TileEnvelope> = Vec::with_capacity(entries.len());
        let mut total_items = 0u64;
        for (idx, e) in entries.iter().enumerate() {
            total_items += e.item_count;
            slots.push(AdminTileSource::Disk(bundle_root.join(&e.rel_path)));
            envs.push(TileEnvelope {
                aabb: AABB::from_corners([e.min_lon, e.min_lat], [e.max_lon, e.max_lat]),
                slot_idx: idx,
            });
        }
        let tree = RTree::bulk_load(envs);
        debug!(
            tile_count = slots.len(),
            total_items, cache_entries, "AdminIndex opened"
        );
        let capacity = NonZeroUsize::new(cache_entries.max(1)).unwrap();
        Self {
            slots,
            tree,
            cache: Mutex::new(LruCache::new(capacity)),
            total_items,
        }
    }

    pub fn len(&self) -> usize {
        self.total_items as usize
    }

    pub fn is_empty(&self) -> bool {
        self.total_items == 0
    }

    /// Number of per-tile feature lists currently held in the LRU cache.
    pub fn cache_len(&self) -> usize {
        self.cache.lock().map(|c| c.len()).unwrap_or(0)
    }

    /// Reverse query: every feature whose polygon contains the point.
    /// Sorted finest-to-coarsest by bbox area (smallest first). Tiles
    /// are loaded lazily on first touch (mmap + rkyv `check_archived_root`
    /// once, then zero-copy access on every subsequent call). PIP runs
    /// directly on the archived ring vertices via
    /// [`archived::pip_archived_ref`]; the runtime `MultiPolygon` /
    /// `geo::Contains` path is gone. Hits hydrate to [`AdminFeature`]
    /// only at return time via `Deserialize`.
    pub fn point_in_polygon(&self, coord: Coord) -> Vec<AdminFeature> {
        use rkyv::Deserialize as RkyvDeserialize;
        let q = [coord.lon, coord.lat];
        let envelope = AABB::from_point(q);
        let candidate_idxs: Vec<usize> = self
            .tree
            .locate_in_envelope_intersecting(&envelope)
            .map(|entry| entry.slot_idx)
            .collect();

        // Collect (place_id, area) pairs for hits; defer hydration to
        // the end so we touch the archived ring vertex array only once
        // per PIP, and only deserialize the winners.
        let mut hits: BTreeMap<u64, (archived::ArchivedAdminFeature, f64)> = BTreeMap::new();
        for idx in candidate_idxs {
            let tile = self.load_slot(idx);
            let layer_ref = tile.archived();
            for feat_ref in layer_ref.features.iter() {
                if !archived::pip_archived_ref(feat_ref, q) {
                    continue;
                }
                let area = archived_ref_bbox_area(feat_ref).unwrap_or(f64::MAX);
                let place_id = feat_ref.place_id;
                let owned: archived::ArchivedAdminFeature =
                    RkyvDeserialize::deserialize(feat_ref, &mut rkyv::Infallible)
                        .expect("infallible deserializer");
                hits.entry(place_id)
                    .and_modify(|e| {
                        if area < e.1 {
                            *e = (owned.clone(), area);
                        }
                    })
                    .or_insert((owned, area));
            }
        }
        let mut sorted: Vec<(archived::ArchivedAdminFeature, f64)> = hits.into_values().collect();
        sorted.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        sorted
            .into_iter()
            .map(|(f, _)| archived::from_archived(&f))
            .collect()
    }
}

/// Bbox area on the archived ref's first polygon. Used for the
/// finest-first ordering at PIP return time.
fn archived_ref_bbox_area(
    feat: &<archived::ArchivedAdminFeature as rkyv::Archive>::Archived,
) -> Option<f64> {
    let bbox = feat.polygon_bboxes.first()?;
    if bbox[0].is_nan() {
        return None;
    }
    Some((bbox[2] - bbox[0]).abs() * (bbox[3] - bbox[1]).abs())
}

// ============================================================
// NearestIndex (R*-tree of tile bboxes + lazy per-tile load)
// ============================================================

enum PointTileSource {
    Eager(Arc<Vec<PlacePoint>>),
    Disk(PathBuf),
}

pub struct NearestIndex {
    slots: Vec<PointTileSource>,
    tree: RTree<TileEnvelope>,
    cache: Mutex<LruCache<usize, Arc<Vec<PlacePoint>>>>,
    total_items: u64,
}

fn read_point_tile(path: &Path) -> Arc<Vec<PlacePoint>> {
    match std::fs::read(path) {
        Ok(bytes) => match bincode::deserialize::<PointLayer>(&bytes) {
            Ok(layer) => Arc::new(layer.points),
            Err(err) => {
                debug!(?err, ?path, "point tile decode failed");
                Arc::new(Vec::new())
            }
        },
        Err(err) => {
            debug!(?err, ?path, "point tile read failed");
            Arc::new(Vec::new())
        }
    }
}

impl NearestIndex {
    fn load_slot(&self, idx: usize) -> Arc<Vec<PlacePoint>> {
        match &self.slots[idx] {
            PointTileSource::Eager(arc) => arc.clone(),
            PointTileSource::Disk(path) => {
                {
                    let mut cache = self.cache.lock().expect("nearest cache poisoned");
                    if let Some(arc) = cache.get(&idx) {
                        return arc.clone();
                    }
                }
                let arc = read_point_tile(path);
                let mut cache = self.cache.lock().expect("nearest cache poisoned");
                cache.put(idx, arc.clone());
                arc
            }
        }
    }

    pub fn build(layer: PointLayer) -> Self {
        let total_items = layer.points.len() as u64;
        let aabb = if layer.points.is_empty() {
            AABB::from_corners([-180.0, -90.0], [180.0, 90.0])
        } else {
            let mut min_lon = f64::INFINITY;
            let mut min_lat = f64::INFINITY;
            let mut max_lon = f64::NEG_INFINITY;
            let mut max_lat = f64::NEG_INFINITY;
            for p in &layer.points {
                min_lon = min_lon.min(p.centroid.lon);
                min_lat = min_lat.min(p.centroid.lat);
                max_lon = max_lon.max(p.centroid.lon);
                max_lat = max_lat.max(p.centroid.lat);
            }
            AABB::from_corners([min_lon, min_lat], [max_lon, max_lat])
        };
        let slot = PointTileSource::Eager(Arc::new(layer.points));
        let tree = RTree::bulk_load(vec![TileEnvelope { aabb, slot_idx: 0 }]);
        Self {
            slots: vec![slot],
            tree,
            cache: Mutex::new(LruCache::new(
                NonZeroUsize::new(DEFAULT_TILE_CACHE_ENTRIES).unwrap(),
            )),
            total_items,
        }
    }

    pub fn open(bundle_root: &Path, entries: Vec<SpatialTileEntry>) -> Self {
        Self::open_with_cache(bundle_root, entries, DEFAULT_TILE_CACHE_ENTRIES)
    }

    pub fn open_with_cache(
        bundle_root: &Path,
        entries: Vec<SpatialTileEntry>,
        cache_entries: usize,
    ) -> Self {
        let mut slots: Vec<PointTileSource> = Vec::with_capacity(entries.len());
        let mut envs: Vec<TileEnvelope> = Vec::with_capacity(entries.len());
        let mut total_items = 0u64;
        for (idx, e) in entries.iter().enumerate() {
            total_items += e.item_count;
            slots.push(PointTileSource::Disk(bundle_root.join(&e.rel_path)));
            envs.push(TileEnvelope {
                aabb: AABB::from_corners([e.min_lon, e.min_lat], [e.max_lon, e.max_lat]),
                slot_idx: idx,
            });
        }
        let tree = RTree::bulk_load(envs);
        debug!(
            tile_count = slots.len(),
            total_items, cache_entries, "NearestIndex opened"
        );
        let capacity = NonZeroUsize::new(cache_entries.max(1)).unwrap();
        Self {
            slots,
            tree,
            cache: Mutex::new(LruCache::new(capacity)),
            total_items,
        }
    }

    pub fn len(&self) -> usize {
        self.total_items as usize
    }

    pub fn is_empty(&self) -> bool {
        self.total_items == 0
    }

    pub fn cache_len(&self) -> usize {
        self.cache.lock().map(|c| c.len()).unwrap_or(0)
    }

    /// Return the `k` nearest places to the given coord. Tiles are
    /// ranked by squared distance from the query to the tile bbox, then
    /// loaded lazily in that order until we've gathered enough candidate
    /// points to fill `k`.
    ///
    /// Linear scan within candidates is fine at country scale; planet-scale
    /// will switch to per-tile R*-trees + bounded heap merge.
    pub fn nearest_k(&self, coord: Coord, k: usize) -> Vec<PlacePoint> {
        if k == 0 || self.total_items == 0 {
            return Vec::new();
        }

        // Rank tile slots by squared distance from the query to the tile
        // bbox. Bbox-to-point distance is 0 if the point is inside.
        let mut ranked: Vec<(usize, f64)> = self
            .slots
            .iter()
            .enumerate()
            .map(|(idx, _)| (idx, slot_bbox_dist2(self, idx, coord)))
            .collect();
        ranked.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        let mut gathered: Vec<PlacePoint> = Vec::new();
        for (slot_idx, _) in ranked {
            gathered.extend(self.load_slot(slot_idx).iter().cloned());
            if gathered.len() >= k * 4 {
                break;
            }
        }

        gathered.sort_by(|a, b| {
            let da = (a.centroid.lon - coord.lon).powi(2) + (a.centroid.lat - coord.lat).powi(2);
            let db = (b.centroid.lon - coord.lon).powi(2) + (b.centroid.lat - coord.lat).powi(2);
            da.partial_cmp(&db).unwrap_or(std::cmp::Ordering::Equal)
        });
        gathered.into_iter().take(k).collect()
    }
}

// ============================================================
// Helpers
// ============================================================

fn bbox_of(mp: &MultiPolygon<f64>) -> Option<Rect<f64>> {
    use geo::BoundingRect;
    mp.bounding_rect()
}

/// Squared distance from a coord to a slot's tile bbox. 0 if inside.
fn slot_bbox_dist2(idx: &NearestIndex, slot_idx: usize, q: Coord) -> f64 {
    // The R*-tree's TileEnvelope carries the bbox we want.
    let env = idx
        .tree
        .iter()
        .find(|e| e.slot_idx == slot_idx)
        .map(|e| e.aabb)
        .unwrap_or_else(|| AABB::from_corners([-180.0, -90.0], [180.0, 90.0]));
    let dx = (q.lon - env.lower()[0])
        .max(0.0)
        .max(env.upper()[0] - q.lon);
    let dy = (q.lat - env.lower()[1])
        .max(0.0)
        .max(env.upper()[1] - q.lat);
    // The above max-trick is wrong; replace with proper bbox-distance:
    let cx = q.lon.clamp(env.lower()[0], env.upper()[0]);
    let cy = q.lat.clamp(env.lower()[1], env.upper()[1]);
    let dxc = q.lon - cx;
    let dyc = q.lat - cy;
    let _ = (dx, dy);
    dxc * dxc + dyc * dyc
}

// ============================================================
// Tests
// ============================================================

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

    fn tempdir() -> std::path::PathBuf {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let d = std::env::temp_dir().join(format!(
            "cairn-spatial-test-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            COUNTER.fetch_add(1, Ordering::Relaxed),
        ));
        std::fs::create_dir_all(&d).unwrap();
        d
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
        assert_eq!(hit[0].name, "City");
        assert_eq!(hit[1].name, "Country");
    }

    #[test]
    fn nearest_index_finds_closest() {
        let pl = PointLayer {
            points: vec![
                PlacePoint {
                    place_id: 1,
                    level: 1,
                    kind: "city".into(),
                    name: "A".into(),
                    centroid: Coord { lon: 0.0, lat: 0.0 },
                    admin_path: vec![],
                },
                PlacePoint {
                    place_id: 2,
                    level: 1,
                    kind: "city".into(),
                    name: "B".into(),
                    centroid: Coord { lon: 5.0, lat: 5.0 },
                    admin_path: vec![],
                },
                PlacePoint {
                    place_id: 3,
                    level: 1,
                    kind: "city".into(),
                    name: "C".into(),
                    centroid: Coord {
                        lon: 10.0,
                        lat: 10.0,
                    },
                    admin_path: vec![],
                },
            ],
        };
        let idx = NearestIndex::build(pl);
        let hits = idx.nearest_k(Coord { lon: 4.0, lat: 4.0 }, 2);
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].name, "B");
        assert_eq!(hits[1].name, "A");
    }

    #[test]
    fn admin_partitioned_roundtrip_and_lazy_pip() {
        // Two L0 (4°×4°) features at distinct centroids → distinct tiles.
        let dir = tempdir();
        let layer = AdminLayer {
            features: vec![
                feature(10, "Alpha", "country", 0.5, 0.5),
                feature(20, "Beta", "country", 7.0, 7.0),
            ],
        };
        let entries = write_admin_partitioned(&dir, &layer).unwrap();
        assert!(entries.len() >= 2);
        // Each feature's polygon may straddle a tile edge, so up to ~4
        // entries are valid. At minimum: 2 tiles, one containing each
        // feature's centroid.
        let touches_a = entries
            .iter()
            .any(|e| e.min_lon <= 0.5 && e.max_lon >= 0.5 && e.min_lat <= 0.5 && e.max_lat >= 0.5);
        let touches_b = entries
            .iter()
            .any(|e| e.min_lon <= 7.0 && e.max_lon >= 7.0 && e.min_lat <= 7.0 && e.max_lat >= 7.0);
        assert!(touches_a && touches_b);

        // Re-open lazily and run PIP — the dedupe-by-place_id step must
        // collapse any cross-tile replication into one hit per Alpha/Beta.
        let idx = AdminIndex::open(&dir, entries);
        let hit_a = idx.point_in_polygon(Coord { lon: 0.5, lat: 0.5 });
        assert_eq!(hit_a.iter().filter(|f| f.name == "Alpha").count(), 1);
        let hit_b = idx.point_in_polygon(Coord { lon: 7.0, lat: 7.0 });
        assert_eq!(hit_b.iter().filter(|f| f.name == "Beta").count(), 1);
        let miss = idx.point_in_polygon(Coord {
            lon: 100.0,
            lat: 0.0,
        });
        assert!(miss.is_empty());
    }

    #[test]
    fn admin_lru_evicts_when_capacity_exceeded() {
        let dir = tempdir();
        let layer = AdminLayer {
            features: vec![
                feature(10, "Alpha", "country", 0.5, 0.5),
                feature(20, "Beta", "country", 7.0, 7.0),
                feature(30, "Gamma", "country", -5.0, 12.0),
            ],
        };
        let entries = write_admin_partitioned(&dir, &layer).unwrap();
        // Cache size 1 — every fresh PIP that touches a different tile
        // evicts the previous one.
        let idx = AdminIndex::open_with_cache(&dir, entries, 1);
        assert_eq!(idx.cache_len(), 0);
        let _ = idx.point_in_polygon(Coord { lon: 0.5, lat: 0.5 });
        assert_eq!(idx.cache_len(), 1);
        let _ = idx.point_in_polygon(Coord { lon: 7.0, lat: 7.0 });
        assert_eq!(idx.cache_len(), 1);
        let _ = idx.point_in_polygon(Coord {
            lon: -5.0,
            lat: 12.0,
        });
        assert_eq!(idx.cache_len(), 1);
    }

    #[test]
    fn dedupe_collapses_same_kind_same_centroid() {
        let osm = AdminFeature {
            place_id: 1,
            level: 0,
            kind: "country".into(),
            name: "Liechtenstein".into(),
            centroid: Coord {
                lon: 9.5554,
                lat: 47.166,
            },
            admin_path: vec![],
            polygon: unit_square_at(9.5554, 47.166),
        };
        let wof = AdminFeature {
            place_id: 2,
            level: 0,
            kind: "country".into(),
            name: "Liechtenstein".into(),
            centroid: Coord {
                lon: 9.5554,
                lat: 47.166,
            },
            admin_path: vec![85633723, 102191581],
            polygon: unit_square_at(9.5554, 47.166),
        };
        let kept = dedupe_features(
            vec![
                (osm, cairn_place::SourceKind::Unknown),
                (wof, cairn_place::SourceKind::Unknown),
            ],
            &[],
        );
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].place_id, 2, "WoF (richer admin_path) should win");
    }

    #[test]
    fn dedupe_keeps_distinct_kinds() {
        let country = feature(1, "Liechtenstein", "country", 9.5554, 47.166);
        let region = feature(2, "Liechtenstein", "region", 9.5554, 47.166);
        let kept = dedupe_features(
            vec![
                (country, cairn_place::SourceKind::Unknown),
                (region, cairn_place::SourceKind::Unknown),
            ],
            &[],
        );
        assert_eq!(kept.len(), 2);
    }

    #[test]
    fn dedupe_keeps_distant_centroids() {
        let a = feature(1, "Townville", "city", 0.0, 0.0);
        let b = feature(2, "Townville", "city", 0.5, 0.5);
        let kept = dedupe_features(
            vec![
                (a, cairn_place::SourceKind::Unknown),
                (b, cairn_place::SourceKind::Unknown),
            ],
            &[],
        );
        assert_eq!(kept.len(), 2);
    }

    #[test]
    fn dedupe_priority_overrides_admin_path_richness() {
        let mut osm = AdminFeature {
            place_id: 1,
            level: 0,
            kind: "country".into(),
            name: "Liechtenstein".into(),
            centroid: Coord {
                lon: 9.5554,
                lat: 47.166,
            },
            admin_path: vec![1, 2, 3], // pretend OSM has chain
            polygon: unit_square_at(9.5554, 47.166),
        };
        let wof = AdminFeature {
            place_id: 2,
            level: 0,
            kind: "country".into(),
            name: "Liechtenstein".into(),
            centroid: Coord {
                lon: 9.5554,
                lat: 47.166,
            },
            admin_path: vec![],
            polygon: unit_square_at(9.5554, 47.166),
        };
        let _ = &mut osm;
        let kept = dedupe_features(
            vec![
                (osm, cairn_place::SourceKind::Osm),
                (wof, cairn_place::SourceKind::Wof),
            ],
            &[cairn_place::SourceKind::Wof, cairn_place::SourceKind::Osm],
        );
        assert_eq!(kept.len(), 1);
        assert_eq!(
            kept[0].place_id, 2,
            "WoF wins via priority even though OSM has the longer admin_path"
        );
    }

    #[test]
    fn points_partitioned_roundtrip() {
        let dir = tempdir();
        let layer = PointLayer {
            points: vec![
                PlacePoint {
                    place_id: 1,
                    level: 1,
                    kind: "city".into(),
                    name: "A".into(),
                    centroid: Coord { lon: 0.0, lat: 0.0 },
                    admin_path: vec![],
                },
                PlacePoint {
                    place_id: 2,
                    level: 1,
                    kind: "city".into(),
                    name: "B".into(),
                    centroid: Coord { lon: 5.0, lat: 5.0 },
                    admin_path: vec![],
                },
            ],
        };
        let entries = write_points_partitioned(&dir, &layer).unwrap();
        assert_eq!(entries.len(), 2);
        let idx = NearestIndex::open(&dir, entries);
        let hits = idx.nearest_k(Coord { lon: 4.0, lat: 4.0 }, 2);
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].name, "B");
    }
}
