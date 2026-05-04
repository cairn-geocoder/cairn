//! Tile coordinate scheme + bundle manifest schema + tile blob IO.
//!
//! Three-level grid inspired by Valhalla:
//! - L0: 4° × 4° (countries / regions)
//! - L1: 1° × 1° (cities / postcodes)
//! - L2: 0.25° × 0.25° (streets / addresses / POIs)
//!
//! Tile blob format (on disk):
//! ```text
//! magic   : 8 bytes = b"CAIRN-T1"
//! version : u32 LE
//! payload : rkyv-archived Vec<Place>  (root archived at end)
//! ```

use cairn_place::{Coord, Place};
use rkyv::ser::serializers::AllocSerializer;
use rkyv::ser::Serializer;
use rkyv::Deserialize as RkyvDeserialize;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use thiserror::Error;

pub const TILE_MAGIC: &[u8; 8] = b"CAIRN-T1";
/// Version 1: raw rkyv payload after the header.
pub const TILE_FORMAT_VERSION_RAW: u32 = 1;
/// Version 2: zstd-compressed rkyv payload after the header.
pub const TILE_FORMAT_VERSION_ZSTD: u32 = 2;
/// Latest supported tile-format version. Writers default to this; readers
/// accept any known version.
pub const TILE_FORMAT_VERSION: u32 = TILE_FORMAT_VERSION_RAW;
/// Header is padded to 16 bytes so that the rkyv payload that follows is
/// 16-aligned when the file is read into an `AlignedVec`.
pub const HEADER_LEN: usize = 16;

/// Compression scheme applied to a tile blob payload.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum TileCompression {
    #[default]
    None,
    Zstd,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum Level {
    L0,
    L1,
    L2,
}

impl Level {
    pub fn cell_deg(self) -> f64 {
        match self {
            Level::L0 => 4.0,
            Level::L1 => 1.0,
            Level::L2 => 0.25,
        }
    }

    pub fn columns(self) -> u32 {
        (360.0 / self.cell_deg()) as u32
    }

    pub fn rows(self) -> u32 {
        (180.0 / self.cell_deg()) as u32
    }

    pub fn as_u8(self) -> u8 {
        match self {
            Level::L0 => 0,
            Level::L1 => 1,
            Level::L2 => 2,
        }
    }

    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Level::L0),
            1 => Some(Level::L1),
            2 => Some(Level::L2),
            _ => None,
        }
    }

    pub fn all() -> [Level; 3] {
        [Level::L0, Level::L1, Level::L2]
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct TileCoord {
    pub level: Level,
    pub row: u32,
    pub col: u32,
}

impl TileCoord {
    pub fn from_coord(level: Level, c: Coord) -> Self {
        let cell = level.cell_deg();
        let cols = level.columns();
        let rows = level.rows();
        let col = (((c.lon + 180.0) / cell).floor() as u32).min(cols - 1);
        let row = (((c.lat + 90.0) / cell).floor() as u32).min(rows - 1);
        TileCoord { level, row, col }
    }

    pub fn id(self) -> u32 {
        self.row * self.level.columns() + self.col
    }

    pub fn relative_path(self) -> String {
        let id = self.id();
        let bucket_a = id / 1_000_000 % 1000;
        let bucket_b = id / 1000 % 1000;
        format!(
            "tiles/{lvl}/{a:03}/{b:03}/{id}.bin",
            lvl = self.level.as_u8(),
            a = bucket_a,
            b = bucket_b,
            id = id,
        )
    }

    /// Bounding box of this tile in (`min_lon`, `min_lat`, `max_lon`,
    /// `max_lat`) lon/lat degrees.
    pub fn bbox(self) -> (f64, f64, f64, f64) {
        let cell = self.level.cell_deg();
        let min_lon = -180.0 + self.col as f64 * cell;
        let min_lat = -90.0 + self.row as f64 * cell;
        (min_lon, min_lat, min_lon + cell, min_lat + cell)
    }

    /// Reconstruct a `TileCoord` from a serialized `(level, tile_id)` pair.
    pub fn from_id(level: Level, tile_id: u32) -> Self {
        let cols = level.columns();
        Self {
            level,
            row: tile_id / cols,
            col: tile_id % cols,
        }
    }
}

/// Returns true if the two `(min_lon, min_lat, max_lon, max_lat)` boxes overlap.
pub fn bbox_intersects(a: (f64, f64, f64, f64), b: (f64, f64, f64, f64)) -> bool {
    !(a.2 < b.0 || a.0 > b.2 || a.3 < b.1 || a.1 > b.3)
}

/// Returns true if `(lon, lat)` lies inside `(min_lon, min_lat, max_lon, max_lat)`.
pub fn bbox_contains(bbox: (f64, f64, f64, f64), lon: f64, lat: f64) -> bool {
    lon >= bbox.0 && lon <= bbox.2 && lat >= bbox.1 && lat <= bbox.3
}

/// Top-level bundle manifest. Serialized as `manifest.toml` at bundle root.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Manifest {
    pub schema_version: u32,
    pub built_at: String,
    pub bundle_id: String,
    #[serde(default)]
    pub sources: Vec<SourceVersion>,
    #[serde(default)]
    pub tiles: Vec<TileEntry>,
    /// Per-tile admin polygon files (partitioned for lazy load). Empty
    /// when the bundle was built without a polygon source.
    #[serde(default)]
    pub admin_tiles: Vec<SpatialTileEntry>,
    /// Per-tile point files used by the nearest-fallback layer. Empty
    /// only on degenerate bundles with zero places.
    #[serde(default)]
    pub point_tiles: Vec<SpatialTileEntry>,
    /// Per-tile building footprint files. Populated by
    /// `cairn-build augment --buildings` (v0.3 lane A); empty on
    /// bundles built without the augmenter. Schema-additive: bundles
    /// without this field still load via `#[serde(default)]`.
    #[serde(default)]
    pub building_tiles: Vec<SpatialTileEntry>,
    /// Per-file blake3 hashes for the tantivy text index segment files.
    /// Populated by `cairn-build build` and recomputed by `cairn-build
    /// verify` so a corrupt segment fails the integrity check.
    #[serde(default)]
    pub text_files: Vec<TextFileEntry>,
}

/// Manifest entry for a single tantivy index segment file. Identified
/// by a path relative to the bundle root (e.g.
/// `index/text/meta.json`); blake3 covers the on-disk bytes verbatim.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TextFileEntry {
    pub rel_path: String,
    pub byte_size: u64,
    pub blake3: String,
}

/// Manifest entry for a single per-tile spatial file (admin polygons
/// or nearest-fallback point centroids).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SpatialTileEntry {
    pub level: u8,
    pub tile_id: u32,
    pub min_lon: f64,
    pub min_lat: f64,
    pub max_lon: f64,
    pub max_lat: f64,
    pub item_count: u64,
    pub byte_size: u64,
    pub blake3: String,
    /// Path relative to the bundle root, e.g.
    /// `spatial/admin/0/000/049/49509.bin`.
    pub rel_path: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SourceVersion {
    pub name: String,
    pub version: String,
    pub blake3: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TileEntry {
    pub level: u8,
    pub tile_id: u32,
    pub blake3: String,
    pub byte_size: u64,
    pub place_count: u32,
    /// Compression scheme applied to the tile payload (default `None`
    /// means raw rkyv bytes — preserves backward compatibility with v1
    /// bundles that don't carry this field).
    #[serde(default, skip_serializing_if = "is_compression_none")]
    pub compression: TileCompression,
}

fn is_compression_none(c: &TileCompression) -> bool {
    matches!(c, TileCompression::None)
}

/// Manifest entry for a single-file bundle artifact (admin polygons,
/// nearest-fallback points, etc.).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ArtifactEntry {
    pub blake3: String,
    pub byte_size: u64,
    pub item_count: u64,
}

#[derive(Debug, Error)]
pub enum TileError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("toml encode: {0}")]
    TomlEncode(#[from] toml::ser::Error),
    #[error("toml decode: {0}")]
    TomlDecode(#[from] toml::de::Error),
    #[error("bad magic in {0}")]
    BadMagic(String),
    #[error("unsupported version {0}")]
    BadVersion(u32),
    #[error("rkyv serialize: {0}")]
    RkyvSerialize(String),
    #[error("rkyv validate: {0}")]
    RkyvValidate(String),
    #[error("blake3 mismatch for {path}: expected {expected}, got {actual}")]
    Blake3Mismatch {
        path: String,
        expected: String,
        actual: String,
    },
}

/// Bucket places into tiles at the given level.
pub fn bucket_places(level: Level, places: Vec<Place>) -> HashMap<TileCoord, Vec<Place>> {
    let mut map: HashMap<TileCoord, Vec<Place>> = HashMap::new();
    for p in places {
        let coord = TileCoord::from_coord(level, p.centroid);
        map.entry(coord).or_default().push(p);
    }
    map
}

/// Encode a tile blob to bytes (header + payload). Compression is opt-in
/// via the `compression` argument; the version word in the header marks
/// which scheme was used so readers can dispatch correctly.
pub fn encode_tile(places: &[Place], compression: TileCompression) -> Result<Vec<u8>, TileError> {
    let mut serializer = AllocSerializer::<4096>::default();
    serializer
        .serialize_value(&places.to_vec())
        .map_err(|e| TileError::RkyvSerialize(format!("{e:?}")))?;
    let raw = serializer.into_serializer().into_inner();

    let (version, payload) = match compression {
        TileCompression::None => (TILE_FORMAT_VERSION_RAW, raw.to_vec()),
        TileCompression::Zstd => {
            let compressed = zstd::stream::encode_all(raw.as_slice(), 0)?;
            (TILE_FORMAT_VERSION_ZSTD, compressed)
        }
    };

    let mut out = Vec::with_capacity(HEADER_LEN + payload.len());
    out.extend_from_slice(TILE_MAGIC);
    out.extend_from_slice(&version.to_le_bytes());
    out.extend_from_slice(&[0u8; 4]); // padding to 16-byte alignment
    out.extend_from_slice(&payload);
    Ok(out)
}

/// Write a tile blob to disk; return blake3 hex digest + byte size.
pub fn write_tile(
    path: &Path,
    places: &[Place],
    compression: TileCompression,
) -> Result<(String, u64), TileError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let bytes = encode_tile(places, compression)?;
    let hash = blake3::hash(&bytes).to_hex().to_string();
    let len = bytes.len() as u64;
    let mut f = File::create(path)?;
    f.write_all(&bytes)?;
    f.sync_all()?;
    Ok((hash, len))
}

/// Read a tile blob from disk and decode it into `Vec<Place>`.
///
/// Phase 1 deserializes eagerly; Phase 3 will switch reverse-geocoding
/// hot paths to mmap + zero-copy archived access.
pub fn read_tile(path: &Path) -> Result<Vec<Place>, TileError> {
    let bytes = std::fs::read(path)?;
    decode_tile(&bytes, path)
}

fn decode_tile(bytes: &[u8], path: &Path) -> Result<Vec<Place>, TileError> {
    if bytes.len() < HEADER_LEN {
        return Err(TileError::BadMagic(path.display().to_string()));
    }
    if &bytes[0..8] != TILE_MAGIC {
        return Err(TileError::BadMagic(path.display().to_string()));
    }
    let mut v = [0u8; 4];
    v.copy_from_slice(&bytes[8..12]);
    let version = u32::from_le_bytes(v);
    let payload = &bytes[HEADER_LEN..];

    // Decompress (if needed) into an AlignedVec so rkyv's check sees the
    // 16-byte alignment it requires.
    let aligned = match version {
        TILE_FORMAT_VERSION_RAW => {
            let mut out = rkyv::AlignedVec::with_capacity(payload.len());
            out.extend_from_slice(payload);
            out
        }
        TILE_FORMAT_VERSION_ZSTD => {
            let raw = zstd::stream::decode_all(payload)?;
            let mut out = rkyv::AlignedVec::with_capacity(raw.len());
            out.extend_from_slice(&raw);
            out
        }
        _ => return Err(TileError::BadVersion(version)),
    };

    let archived = rkyv::check_archived_root::<Vec<Place>>(&aligned)
        .map_err(|e| TileError::RkyvValidate(format!("{e:?}")))?;
    // Phase 7d — Place.tags now use `Arc<str>` for build-time
    // sharing; rkyv's `Arc<str>` deserialize requires a deserializer
    // that implements `SharedDeserializeRegistry`. `Infallible` does
    // not, so swap in `SharedDeserializeMap` here. Per-tile
    // deserialize cost is unaffected because the shared map starts
    // empty for each tile decode.
    let mut deserializer = rkyv::de::deserializers::SharedDeserializeMap::new();
    let places: Vec<Place> = RkyvDeserialize::<Vec<Place>, _>::deserialize(archived, &mut deserializer)
        .map_err(|e| TileError::RkyvValidate(format!("{e:?}")))?;
    Ok(places)
}

/// Phase 6d C3 — backing storage for a validated archived place tile.
/// Mirrors `cairn_spatial::archived::AdminTileBytes`. Three variants:
/// - `Owned` — eager-built or zstd-decompressed payload in heap.
/// - `Mapped` — raw mmap, payload aligned to 16 bytes.
/// - `OwnedMappedCopy` — mmap exists but payload offset is not 16-aligned;
///   keep the mmap alive for the lifetime of the heap copy so OS page
///   cache is shared.
enum PlaceTileBytes {
    #[allow(dead_code)]
    Owned(rkyv::AlignedVec),
    Mapped(memmap2::Mmap),
    OwnedMappedCopy(rkyv::AlignedVec, #[allow(dead_code)] memmap2::Mmap),
}

/// Phase 6d C3 — mmap-backed zero-copy archive of a place tile.
///
/// `read_tile` materializes a `Vec<Place>` for every cache miss, which is
/// fine for small bundles but burns CPU + heap on planet-scale workloads
/// hitting tens of thousands of distinct tiles. `PlaceTileArchive` skips
/// that step: validates the rkyv payload once at construction, then
/// returns a borrowed `Archived<Vec<Place>>` that callers iterate
/// directly. Page-cache shared across processes via the kernel.
///
/// Compression handling:
/// - `TILE_FORMAT_VERSION_RAW` tiles: validate + hold the mmap. True
///   zero-copy.
/// - `TILE_FORMAT_VERSION_ZSTD` tiles: must be decompressed into an
///   `AlignedVec`. Holds both the mmap and the decompressed buffer.
///   The mmap drop runs after the buffer goes out of scope.
pub struct PlaceTileArchive {
    bytes: PlaceTileBytes,
    body_offset: usize,
    body_len: usize,
    item_count: usize,
}

impl PlaceTileArchive {
    /// Open a tile from disk. Validates the rkyv archive once; subsequent
    /// `archived()` calls are zero-cost.
    pub fn from_path(path: &Path) -> Result<Self, TileError> {
        let file = std::fs::File::open(path)?;
        let mmap = unsafe { memmap2::Mmap::map(&file)? };
        if mmap.len() < HEADER_LEN {
            return Err(TileError::BadMagic(path.display().to_string()));
        }
        if &mmap[0..8] != TILE_MAGIC {
            return Err(TileError::BadMagic(path.display().to_string()));
        }
        let mut v = [0u8; 4];
        v.copy_from_slice(&mmap[8..12]);
        let version = u32::from_le_bytes(v);
        let payload = &mmap[HEADER_LEN..];

        match version {
            TILE_FORMAT_VERSION_RAW => {
                let payload_ptr = unsafe { mmap.as_ptr().add(HEADER_LEN) };
                if (payload_ptr as usize) % 16 != 0 {
                    let mut aligned = rkyv::AlignedVec::with_capacity(payload.len());
                    aligned.extend_from_slice(payload);
                    let archived = rkyv::check_archived_root::<Vec<Place>>(&aligned)
                        .map_err(|e| TileError::RkyvValidate(format!("{e:?}")))?;
                    let item_count = archived.len();
                    let body_len = aligned.len();
                    return Ok(Self {
                        bytes: PlaceTileBytes::OwnedMappedCopy(aligned, mmap),
                        body_offset: 0,
                        body_len,
                        item_count,
                    });
                }
                let archived = rkyv::check_archived_root::<Vec<Place>>(payload)
                    .map_err(|e| TileError::RkyvValidate(format!("{e:?}")))?;
                let item_count = archived.len();
                let body_len = payload.len();
                Ok(Self {
                    bytes: PlaceTileBytes::Mapped(mmap),
                    body_offset: HEADER_LEN,
                    body_len,
                    item_count,
                })
            }
            TILE_FORMAT_VERSION_ZSTD => {
                let raw = zstd::stream::decode_all(payload)?;
                let mut aligned = rkyv::AlignedVec::with_capacity(raw.len());
                aligned.extend_from_slice(&raw);
                let archived = rkyv::check_archived_root::<Vec<Place>>(&aligned)
                    .map_err(|e| TileError::RkyvValidate(format!("{e:?}")))?;
                let item_count = archived.len();
                let body_len = aligned.len();
                Ok(Self {
                    bytes: PlaceTileBytes::OwnedMappedCopy(aligned, mmap),
                    body_offset: 0,
                    body_len,
                    item_count,
                })
            }
            _ => Err(TileError::BadVersion(version)),
        }
    }

    fn payload(&self) -> &[u8] {
        let raw: &[u8] = match &self.bytes {
            PlaceTileBytes::Owned(v) => v,
            PlaceTileBytes::Mapped(m) => &m[..],
            PlaceTileBytes::OwnedMappedCopy(v, _) => v,
        };
        &raw[self.body_offset..self.body_offset + self.body_len]
    }

    /// Zero-copy reference into the archived `Vec<Place>`. Sound because
    /// every constructor validates via `check_archived_root` first.
    pub fn archived(&self) -> &<Vec<Place> as rkyv::Archive>::Archived {
        unsafe { rkyv::archived_root::<Vec<Place>>(self.payload()) }
    }

    pub fn item_count(&self) -> usize {
        self.item_count
    }
}

pub fn write_manifest(path: &Path, manifest: &Manifest) -> Result<(), TileError> {
    let s = toml::to_string_pretty(manifest)?;
    std::fs::write(path, s)?;
    Ok(())
}

pub fn read_manifest(path: &Path) -> Result<Manifest, TileError> {
    let s = std::fs::read_to_string(path)?;
    let m: Manifest = toml::from_str(&s)?;
    Ok(m)
}

/// Verify every tile referenced by a manifest matches its recorded blake3.
pub fn verify_bundle(bundle_root: &Path) -> Result<VerifyReport, TileError> {
    let manifest_path = bundle_root.join("manifest.toml");
    let manifest = read_manifest(&manifest_path)?;
    let mut report = VerifyReport {
        manifest_path: manifest_path.display().to_string(),
        tiles_checked: 0,
        ..Default::default()
    };
    for entry in &manifest.tiles {
        let level = Level::from_u8(entry.level).ok_or(TileError::BadVersion(entry.level as u32))?;
        let cols = level.columns();
        let coord = TileCoord {
            level,
            row: entry.tile_id / cols,
            col: entry.tile_id % cols,
        };
        let path = bundle_root.join(coord.relative_path());
        let bytes = std::fs::read(&path)?;
        let actual = blake3::hash(&bytes).to_hex().to_string();
        report.tiles_checked += 1;
        if actual != entry.blake3 {
            report.failures.push(VerifyFailure {
                path: path.display().to_string(),
                expected: entry.blake3.clone(),
                actual,
            });
        }
    }
    Ok(report)
}

#[derive(Clone, Debug, Default)]
pub struct VerifyReport {
    pub manifest_path: String,
    pub tiles_checked: u64,
    pub failures: Vec<VerifyFailure>,
}

impl VerifyReport {
    pub fn ok(&self) -> bool {
        self.failures.is_empty()
    }
}

#[derive(Clone, Debug)]
pub struct VerifyFailure {
    pub path: String,
    pub expected: String,
    pub actual: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_place::{LocalizedName, PlaceId, PlaceKind};

    fn vaduz() -> Place {
        Place {
            id: PlaceId::new(1, 100, 1).unwrap(),
            kind: PlaceKind::City,
            names: vec![LocalizedName {
                lang: "en".into(),
                value: "Vaduz".into(),
            }],
            centroid: Coord {
                lon: 9.5209,
                lat: 47.1410,
            },
            admin_path: vec![],
            tags: vec![("place".into(), "city".into())],
        }
    }

    #[test]
    fn tile_from_zurich() {
        let c = Coord {
            lon: 8.5417,
            lat: 47.3769,
        };
        let t = TileCoord::from_coord(Level::L1, c);
        assert_eq!(t.level, Level::L1);
        assert_eq!(t.col, 188);
        assert_eq!(t.row, 137);
    }

    #[test]
    fn tile_id_within_bounds() {
        let c = Coord {
            lon: 179.99,
            lat: 89.99,
        };
        let t = TileCoord::from_coord(Level::L2, c);
        assert!(t.id() < Level::L2.columns() * Level::L2.rows());
    }

    #[test]
    fn tile_bbox_round_trips_via_from_id() {
        let c = Coord {
            lon: 8.5417,
            lat: 47.3769,
        };
        let t = TileCoord::from_coord(Level::L1, c);
        let id = t.id();
        let restored = TileCoord::from_id(Level::L1, id);
        assert_eq!(t, restored);

        let (min_lon, min_lat, max_lon, max_lat) = t.bbox();
        assert!(min_lon <= c.lon && c.lon <= max_lon);
        assert!(min_lat <= c.lat && c.lat <= max_lat);
        assert!((max_lon - min_lon - 1.0).abs() < 1e-9);
    }

    #[test]
    fn bbox_intersects_predicate() {
        // Liechtenstein box ≈ (9.47, 47.05, 9.64, 47.27)
        let li = (9.47, 47.05, 9.64, 47.27);
        // L1 tile around (9, 47) → bbox (9, 47, 10, 48)
        assert!(bbox_intersects((9.0, 47.0, 10.0, 48.0), li));
        // Far away tile in the Pacific
        assert!(!bbox_intersects((-160.0, 0.0, -159.0, 1.0), li));
    }

    #[test]
    fn tile_blob_roundtrip() {
        let dir = tempdir_for_test();
        let coord = TileCoord::from_coord(Level::L1, vaduz().centroid);
        let path = dir.join(coord.relative_path());
        let places = vec![vaduz()];
        let (hash, size) = write_tile(&path, &places, TileCompression::None).unwrap();
        assert!(size > HEADER_LEN as u64);
        assert_eq!(hash.len(), 64);

        let decoded = read_tile(&path).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].names[0].value, "Vaduz");
    }

    #[test]
    fn tile_zstd_roundtrip_smaller_than_raw() {
        let dir = tempdir_for_test();
        // Build a non-trivial tile: 50 copies of vaduz with longer names.
        let big: Vec<Place> = (0..50)
            .map(|i| {
                let mut p = vaduz();
                p.id = PlaceId::new(1, 100, i).unwrap();
                p.names = vec![cairn_place::LocalizedName {
                    lang: "default".into(),
                    value: format!("Vaduz alias number {i:04} for compression"),
                }];
                p
            })
            .collect();
        let coord = TileCoord::from_coord(Level::L1, vaduz().centroid);
        let raw_path = dir.join("raw.bin");
        let zst_path = dir.join("zst.bin");
        let (_, raw_size) = write_tile(&raw_path, &big, TileCompression::None).unwrap();
        let (_, zst_size) = write_tile(&zst_path, &big, TileCompression::Zstd).unwrap();
        assert!(
            zst_size < raw_size,
            "expected zstd to compress; raw={raw_size} zst={zst_size}"
        );
        // Read both back; they must yield identical Place lists.
        let raw_back = read_tile(&raw_path).unwrap();
        let zst_back = read_tile(&zst_path).unwrap();
        assert_eq!(raw_back.len(), 50);
        assert_eq!(zst_back.len(), 50);
        assert_eq!(raw_back[0].names[0].value, zst_back[0].names[0].value);
        // Quiet unused-coord warning when tests evolve.
        let _ = coord;
    }

    #[test]
    fn place_tile_archive_roundtrip_raw() {
        let dir = tempdir_for_test();
        let coord = TileCoord::from_coord(Level::L1, vaduz().centroid);
        let path = dir.join(coord.relative_path());
        let places = vec![vaduz()];
        write_tile(&path, &places, TileCompression::None).unwrap();
        let arch = PlaceTileArchive::from_path(&path).unwrap();
        assert_eq!(arch.item_count(), 1);
        let archived_places = arch.archived();
        assert_eq!(archived_places.len(), 1);
        assert_eq!(
            archived_places[0].names[0].value.as_str(),
            "Vaduz",
            "archived name must roundtrip via mmap"
        );
    }

    #[test]
    fn place_tile_archive_roundtrip_zstd() {
        let dir = tempdir_for_test();
        let big: Vec<Place> = (0..32)
            .map(|i| {
                let mut p = vaduz();
                p.id = PlaceId::new(1, 200, i).unwrap();
                p.names = vec![cairn_place::LocalizedName {
                    lang: "default".into(),
                    value: format!("Vaduz mmap roundtrip {i:04}"),
                }];
                p
            })
            .collect();
        let path = dir.join("zstd.bin");
        write_tile(&path, &big, TileCompression::Zstd).unwrap();
        let arch = PlaceTileArchive::from_path(&path).unwrap();
        assert_eq!(arch.item_count(), 32);
        let archived_places = arch.archived();
        assert_eq!(archived_places.len(), 32);
        assert!(archived_places[0]
            .names
            .iter()
            .any(|n| n.value.as_str().starts_with("Vaduz mmap")));
    }

    #[test]
    fn place_tile_archive_rejects_corrupt_magic() {
        let dir = tempdir_for_test();
        let path = dir.join("bad.bin");
        let coord = TileCoord::from_coord(Level::L1, vaduz().centroid);
        let _ = coord;
        write_tile(&path, &[vaduz()], TileCompression::None).unwrap();
        // Flip a magic byte in place.
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[0] = b'X';
        std::fs::write(&path, &bytes).unwrap();
        match PlaceTileArchive::from_path(&path) {
            Err(TileError::BadMagic(_)) => {}
            Err(e) => panic!("expected BadMagic, got {e:?}"),
            Ok(_) => panic!("expected BadMagic, got Ok"),
        }
    }

    #[test]
    fn manifest_verify_detects_corruption() {
        let dir = tempdir_for_test();
        let coord = TileCoord::from_coord(Level::L1, vaduz().centroid);
        let path = dir.join(coord.relative_path());
        let places = vec![vaduz()];
        let (hash, size) = write_tile(&path, &places, TileCompression::None).unwrap();

        let manifest = Manifest {
            schema_version: 1,
            built_at: "2026-04-28T00:00:00Z".into(),
            bundle_id: "test".into(),
            sources: vec![],
            tiles: vec![TileEntry {
                level: coord.level.as_u8(),
                tile_id: coord.id(),
                blake3: hash.clone(),
                byte_size: size,
                place_count: places.len() as u32,
                compression: TileCompression::None,
            }],
            admin_tiles: vec![],
            point_tiles: vec![],
            building_tiles: vec![],
            text_files: vec![],
        };
        write_manifest(&dir.join("manifest.toml"), &manifest).unwrap();

        let report = verify_bundle(&dir).unwrap();
        assert!(report.ok());
        assert_eq!(report.tiles_checked, 1);

        std::fs::write(&path, b"corrupted bytes here, not a real tile").unwrap();
        let report = verify_bundle(&dir).unwrap();
        assert!(!report.ok());
        assert_eq!(report.failures.len(), 1);
    }

    #[test]
    fn v3_manifest_loads_with_empty_building_tiles() {
        // A pre-v0.3 bundle would write a manifest with no
        // `building_tiles = [...]` block. Confirm we round-trip
        // it: deserialize, observe the field defaults to empty,
        // re-serialize, deserialize again unchanged.
        let dir = tempdir_for_test();
        let v3_toml = r#"
schema_version = 3
built_at = "2026-04-28T00:00:00Z"
bundle_id = "v3-fixture"
sources = []
tiles = []
admin_tiles = []
point_tiles = []
text_files = []
"#;
        let manifest_path = dir.join("manifest.toml");
        std::fs::write(&manifest_path, v3_toml).unwrap();
        let m = read_manifest(&manifest_path).unwrap();
        assert_eq!(m.schema_version, 3);
        assert!(m.building_tiles.is_empty());

        // Round-trip: writing back is byte-stable for the
        // building_tiles default (still serializes as empty list).
        write_manifest(&manifest_path, &m).unwrap();
        let m2 = read_manifest(&manifest_path).unwrap();
        assert_eq!(m2.schema_version, 3);
        assert!(m2.building_tiles.is_empty());
    }

    fn tempdir_for_test() -> std::path::PathBuf {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let d = std::env::temp_dir().join(format!(
            "cairn-tile-test-{}-{}-{}",
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
}
