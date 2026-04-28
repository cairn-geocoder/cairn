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
use std::io::{Read, Write};
use std::path::Path;
use thiserror::Error;

pub const TILE_MAGIC: &[u8; 8] = b"CAIRN-T1";
pub const TILE_FORMAT_VERSION: u32 = 1;
/// Header is padded to 16 bytes so that the rkyv payload that follows is
/// 16-aligned when the file is read into an `AlignedVec`.
pub const HEADER_LEN: usize = 16;

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
    #[serde(default)]
    pub admin: Option<ArtifactEntry>,
    #[serde(default)]
    pub points: Option<ArtifactEntry>,
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

/// Encode a tile blob to bytes (header + rkyv payload).
pub fn encode_tile(places: &[Place]) -> Result<Vec<u8>, TileError> {
    let mut serializer = AllocSerializer::<4096>::default();
    serializer
        .serialize_value(&places.to_vec())
        .map_err(|e| TileError::RkyvSerialize(format!("{e:?}")))?;
    let payload = serializer.into_serializer().into_inner();

    let mut out = Vec::with_capacity(HEADER_LEN + payload.len());
    out.extend_from_slice(TILE_MAGIC);
    out.extend_from_slice(&TILE_FORMAT_VERSION.to_le_bytes());
    out.extend_from_slice(&[0u8; 4]); // padding to 16-byte alignment
    out.extend_from_slice(&payload);
    Ok(out)
}

/// Write a tile blob to disk; return blake3 hex digest + byte size.
pub fn write_tile(path: &Path, places: &[Place]) -> Result<(String, u64), TileError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let bytes = encode_tile(places)?;
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
    let mut f = File::open(path)?;
    let metadata = f.metadata()?;
    let mut buf = rkyv::AlignedVec::with_capacity(metadata.len() as usize);
    let mut tmp = vec![0u8; 64 * 1024];
    loop {
        let n = f.read(&mut tmp)?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&tmp[..n]);
    }
    decode_tile(&buf, path)
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
    if version != TILE_FORMAT_VERSION {
        return Err(TileError::BadVersion(version));
    }
    let payload = &bytes[HEADER_LEN..];
    let archived = rkyv::check_archived_root::<Vec<Place>>(payload)
        .map_err(|e| TileError::RkyvValidate(format!("{e:?}")))?;
    let places: Vec<Place> =
        RkyvDeserialize::<Vec<Place>, _>::deserialize(archived, &mut rkyv::Infallible)
            .map_err(|e| TileError::RkyvValidate(format!("{e:?}")))?;
    Ok(places)
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
        let (hash, size) = write_tile(&path, &places).unwrap();
        assert!(size > HEADER_LEN as u64);
        assert_eq!(hash.len(), 64);

        let decoded = read_tile(&path).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].names[0].value, "Vaduz");
    }

    #[test]
    fn manifest_verify_detects_corruption() {
        let dir = tempdir_for_test();
        let coord = TileCoord::from_coord(Level::L1, vaduz().centroid);
        let path = dir.join(coord.relative_path());
        let places = vec![vaduz()];
        let (hash, size) = write_tile(&path, &places).unwrap();

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
            }],
            admin: None,
            points: None,
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
