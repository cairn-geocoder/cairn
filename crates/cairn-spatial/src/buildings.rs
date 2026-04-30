//! Per-tile rkyv `BuildingLayer` for v0.3 lane A.
//!
//! Mirrors the [`crate::archived`] admin-tile pattern (16-byte
//! aligned header + rkyv body + `from_path` mmap loader) but with a
//! shape tuned for many-tiny polygons:
//!
//! - **No interior rings.** Building holes are < 0.1% of rows and
//!   doubling on-disk footprint isn't worth the geometry fidelity.
//!   The importer drops them at parse time.
//! - **Bbox per building.** A point-in-bbox prefilter answers
//!   "what building rows overlap this query?" without touching ring
//!   vertex bytes for the 99% of buildings that miss the bbox.
//! - **Outer ring stored verbatim** so visualizers + true PIP can
//!   run when the operator opts in.
//!
//! On-disk path: `spatial/buildings/<level>/<a>/<b>/<id>.bin`. Header
//! magic `CRBL` distinguishes this format from the admin tiles
//! (`CRAD`). Bumping `VERSION_RAW` is the migration story for any
//! future schema change.

use cairn_place::Coord;
use cairn_tile::{Level, SpatialTileEntry, TileCoord};
use rkyv::ser::serializers::AllocSerializer;
use rkyv::ser::Serializer;
use rkyv::{AlignedVec, Archive, Deserialize, Infallible, Serialize};
use serde::{Deserialize as SerdeDeserialize, Serialize as SerdeSerialize};
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::path::Path;
use thiserror::Error;
use tracing::debug;

const MAGIC: &[u8; 4] = b"CRBL";
const VERSION_RAW: u32 = 1;
const HEADER_LEN: usize = 16;

#[derive(Debug, Error)]
pub enum BuildingError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("header: {0}")]
    Header(&'static str),
    #[error("validate: {0}")]
    Validate(String),
    #[error("serialize: {0}")]
    Serialize(String),
    #[error("unknown tile level {0}")]
    UnknownLevel(u8),
}

/// Runtime building footprint. The serialization format is rkyv
/// (`ArchivedBuilding`); this struct exists so callers in
/// `cairn-build` and downstream consumers can talk in plain types.
#[derive(Clone, Debug, SerdeSerialize, SerdeDeserialize)]
pub struct Building {
    pub id: String,
    pub centroid: [f64; 2],
    pub bbox: [f64; 4],
    pub outer_ring: Vec<[f64; 2]>,
    pub height: Option<f64>,
}

#[derive(Clone, Debug, Default, SerdeSerialize, SerdeDeserialize)]
pub struct BuildingLayer {
    pub buildings: Vec<Building>,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
#[archive(check_bytes)]
pub struct ArchivedBuilding {
    pub id: String,
    pub centroid: [f64; 2],
    pub bbox: [f64; 4],
    pub outer_ring: Vec<[f64; 2]>,
    /// f64 height in meters; `f64::NAN` represents "unknown" so the
    /// archived form can stay POD without an `Option` indirection.
    pub height: f64,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
#[archive(check_bytes)]
pub struct ArchivedBuildingLayer {
    pub buildings: Vec<ArchivedBuilding>,
}

impl From<&Building> for ArchivedBuilding {
    fn from(b: &Building) -> Self {
        Self {
            id: b.id.clone(),
            centroid: b.centroid,
            bbox: b.bbox,
            outer_ring: b.outer_ring.clone(),
            height: b.height.unwrap_or(f64::NAN),
        }
    }
}

impl From<&ArchivedBuilding> for Building {
    fn from(a: &ArchivedBuilding) -> Self {
        Self {
            id: a.id.clone(),
            centroid: a.centroid,
            bbox: a.bbox,
            outer_ring: a.outer_ring.clone(),
            height: if a.height.is_nan() {
                None
            } else {
                Some(a.height)
            },
        }
    }
}

fn buildings_rel_path(level: u8, tile_id: u32) -> String {
    let a = tile_id / 1_000_000 % 1000;
    let b = tile_id / 1000 % 1000;
    format!("spatial/buildings/{level}/{a:03}/{b:03}/{tile_id}.bin")
}

fn tile_bbox(level: Level, tile_id: u32) -> (f64, f64, f64, f64) {
    TileCoord::from_id(level, tile_id).bbox()
}

/// Serialize an [`ArchivedBuildingLayer`] into a 16-byte aligned
/// header + rkyv payload. Mirrors `archived::serialize_layer`.
pub fn serialize_layer(layer: &ArchivedBuildingLayer) -> Result<AlignedVec, BuildingError> {
    let mut serializer = AllocSerializer::<4096>::default();
    serializer
        .serialize_value(layer)
        .map_err(|e| BuildingError::Serialize(format!("{e:?}")))?;
    let body = serializer.into_serializer().into_inner();

    let mut out = AlignedVec::with_capacity(HEADER_LEN + body.len());
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&VERSION_RAW.to_le_bytes());
    out.extend_from_slice(&(body.len() as u64).to_le_bytes());
    debug_assert_eq!(out.len(), HEADER_LEN);
    out.extend_from_slice(&body);
    Ok(out)
}

pub fn deserialize_layer(blob: &[u8]) -> Result<ArchivedBuildingLayer, BuildingError> {
    let (off, body_len) = parse_header(blob)?;
    let body = &blob[off..off + body_len];
    let mut aligned = AlignedVec::with_capacity(body.len());
    aligned.extend_from_slice(body);
    let archived = rkyv::check_archived_root::<ArchivedBuildingLayer>(&aligned)
        .map_err(|e| BuildingError::Validate(format!("{e:?}")))?;
    Deserialize::<ArchivedBuildingLayer, _>::deserialize(archived, &mut Infallible)
        .map_err(|e| BuildingError::Validate(format!("{e:?}")))
}

pub fn write_layer(path: &Path, layer: &ArchivedBuildingLayer) -> Result<(), BuildingError> {
    let blob = serialize_layer(layer)?;
    let mut f = std::fs::File::create(path)?;
    f.write_all(&blob)?;
    Ok(())
}

pub fn read_layer(path: &Path) -> Result<ArchivedBuildingLayer, BuildingError> {
    let mut f = std::fs::File::open(path)?;
    let mut buf = Vec::new();
    f.read_to_end(&mut buf)?;
    deserialize_layer(&buf)
}

fn parse_header(raw: &[u8]) -> Result<(usize, usize), BuildingError> {
    if raw.len() < HEADER_LEN {
        return Err(BuildingError::Header("truncated header"));
    }
    if &raw[0..4] != MAGIC {
        return Err(BuildingError::Header("bad magic"));
    }
    let version = u32::from_le_bytes(raw[4..8].try_into().unwrap());
    if version != VERSION_RAW {
        return Err(BuildingError::Header("unknown version"));
    }
    let body_len = u64::from_le_bytes(raw[8..16].try_into().unwrap()) as usize;
    if raw.len() < HEADER_LEN + body_len {
        return Err(BuildingError::Header("truncated body"));
    }
    Ok((HEADER_LEN, body_len))
}

/// Partition a [`BuildingLayer`] across Valhalla-style tiles at the
/// given level, write each tile's blob to
/// `<bundle_root>/spatial/buildings/<level>/<a>/<b>/<id>.bin`, and
/// return the per-tile [`SpatialTileEntry`] list for the caller to
/// stamp on the manifest.
///
/// A building whose bbox spans multiple tiles is **replicated** into
/// each tile it intersects — the same trick `write_admin_partitioned`
/// uses. Replication factor is ~1.0 for buildings (most are smaller
/// than a tile by orders of magnitude); the cost is negligible.
pub fn write_buildings_partitioned(
    bundle_root: &Path,
    layer: &BuildingLayer,
    level: Level,
) -> Result<Vec<SpatialTileEntry>, BuildingError> {
    let mut buckets: BTreeMap<(u8, u32), Vec<&Building>> = BTreeMap::new();
    for b in &layer.buildings {
        if !b.bbox[0].is_finite() {
            continue;
        }
        let lo_tile = TileCoord::from_coord(
            level,
            Coord {
                lon: b.bbox[0],
                lat: b.bbox[1],
            },
        );
        let hi_tile = TileCoord::from_coord(
            level,
            Coord {
                lon: b.bbox[2],
                lat: b.bbox[3],
            },
        );
        for row in lo_tile.row..=hi_tile.row {
            for col in lo_tile.col..=hi_tile.col {
                let tc = TileCoord { level, row, col };
                buckets.entry((tc.level.as_u8(), tc.id())).or_default().push(b);
            }
        }
    }

    let mut entries = Vec::with_capacity(buckets.len());
    for ((level_u8, tile_id), bs) in buckets {
        let rel = buildings_rel_path(level_u8, tile_id);
        let abs = bundle_root.join(&rel);
        if let Some(parent) = abs.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let archived: Vec<ArchivedBuilding> = bs.iter().map(|b| ArchivedBuilding::from(*b)).collect();
        let layer = ArchivedBuildingLayer {
            buildings: archived,
        };
        let blob = serialize_layer(&layer)?;
        std::fs::write(&abs, &blob[..])?;
        let lvl = Level::from_u8(level_u8).ok_or(BuildingError::UnknownLevel(level_u8))?;
        let (mn_lon, mn_lat, mx_lon, mx_lat) = tile_bbox(lvl, tile_id);
        entries.push(SpatialTileEntry {
            level: level_u8,
            tile_id,
            min_lon: mn_lon,
            min_lat: mn_lat,
            max_lon: mx_lon,
            max_lat: mx_lat,
            item_count: bs.len() as u64,
            byte_size: blob.len() as u64,
            blake3: blake3::hash(&blob[..]).to_hex().to_string(),
            rel_path: rel,
        });
    }
    debug!(tile_count = entries.len(), "buildings partitioned written");
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn building(id: &str, cx: f64, cy: f64) -> Building {
        let half = 0.0005;
        Building {
            id: id.into(),
            centroid: [cx, cy],
            bbox: [cx - half, cy - half, cx + half, cy + half],
            outer_ring: vec![
                [cx - half, cy - half],
                [cx + half, cy - half],
                [cx + half, cy + half],
                [cx - half, cy + half],
                [cx - half, cy - half],
            ],
            height: Some(12.0),
        }
    }

    fn tempdir() -> std::path::PathBuf {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let d = std::env::temp_dir().join(format!(
            "cairn-buildings-test-{}-{}-{}",
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
    fn serialize_roundtrip_preserves_height_and_ring() {
        let layer = ArchivedBuildingLayer {
            buildings: vec![ArchivedBuilding::from(&building("b1", 9.5, 47.1))],
        };
        let blob = serialize_layer(&layer).unwrap();
        let back = deserialize_layer(&blob).unwrap();
        assert_eq!(back.buildings.len(), 1);
        assert_eq!(back.buildings[0].id, "b1");
        assert!((back.buildings[0].height - 12.0).abs() < 1e-9);
        assert_eq!(back.buildings[0].outer_ring.len(), 5);
    }

    #[test]
    fn nan_height_round_trips_as_none() {
        let mut b = building("b2", 0.0, 0.0);
        b.height = None;
        let arc = ArchivedBuilding::from(&b);
        assert!(arc.height.is_nan());
        let back: Building = (&arc).into();
        assert!(back.height.is_none());
    }

    #[test]
    fn partitioned_write_writes_one_blob_per_tile() {
        let dir = tempdir();
        let layer = BuildingLayer {
            buildings: vec![
                building("a", 9.5314, 47.3769),
                building("b", 9.5325, 47.3771),
                building("c", 11.0, 48.0), // far enough to land in another tile at L2
            ],
        };
        let entries = write_buildings_partitioned(&dir, &layer, Level::L2).unwrap();
        assert!(entries.len() >= 2, "{} entries", entries.len());
        for e in &entries {
            let blob = std::fs::read(dir.join(&e.rel_path)).unwrap();
            assert_eq!(blob.len() as u64, e.byte_size);
            // Header magic survives the write.
            assert_eq!(&blob[0..4], MAGIC);
        }
    }
}
