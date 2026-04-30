//! GeoParquet → `Place` stream.
//!
//! Phase 7b lane J. GeoParquet is Apache Parquet plus a per-column
//! `geometry` (WKB-encoded). It's the bulk-feed format the modern
//! geo ecosystem (Overture Maps, Source Cooperative, big-tech open
//! data drops) standardizes on. This crate is the **enabler** for
//! lane K (Overture); the column-mapping shape is intentionally
//! generic enough to ingest arbitrary GeoParquet drops.
//!
//! ## Scope (v1)
//!
//! - **Geometry**: WKB Point only (LE or BE). LineString / Polygon
//!   pass through as their bbox centroid (TODO) or are skipped
//!   (current default).
//! - **Columns**: configurable mapping for `lon`/`lat`/`name`/`kind`
//!   / language. All other columns fold into [`Place::tags`] unless
//!   filtered by [`TagPolicy::keep`].
//! - **Output**: `Vec<Place>` ready for the standard tile bucketing +
//!   dedup pipeline. Caller stamps `SourceKind::Generic` (or
//!   `Overture` when the lane K wrapper is used).
//!
//! Future work:
//! - Multi-row-group streaming (today we read the whole file into
//!   memory, fine for country-scale Overture drops up to ~5 GB).
//! - Polygon / MultiPolygon geometries → `AdminFeature` for admin
//!   datasets (Overture admins).
//! - Parquet metadata-driven CRS handling (today we trust WGS84).

use arrow_array::{Array, BinaryArray, Float64Array, StringArray};
use cairn_place::{
    stable_hash_gid, synthesize_gid, Coord, LocalizedName, Place, PlaceId, PlaceKind, GID_TAG,
};
use cairn_tile::{Level, TileCoord};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::Deserialize;
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use thiserror::Error;
use tracing::{debug, info};

#[derive(Debug, Error)]
pub enum ImportError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("parquet: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("arrow: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),
    #[error("toml: {0}")]
    Toml(#[from] toml::de::Error),
    #[error("placeid: {0}")]
    PlaceId(#[from] cairn_place::PlaceIdError),
    #[error("config: {0}")]
    Config(String),
    #[error("wkb: {0}")]
    Wkb(String),
}

/// Column / property name mapping + per-source defaults. Mirrors
/// the shape of `cairn_import_generic::Config` so operators don't
/// have to learn two configs; identical TOML files work for either
/// importer.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub map: ColumnMap,
    #[serde(default)]
    pub defaults: Defaults,
    #[serde(default)]
    pub tags: TagPolicy,
}

impl Config {
    /// Overlay caller-set fields onto a preset so the caller's wins
    /// where set and the preset fills in the rest. Centralizes the
    /// "is this field unset?" rules per field type so wrappers
    /// (e.g. `cairn-import-overture`'s theme presets) don't each
    /// re-implement them.
    ///
    /// "Unset" rules per field:
    /// - [`String`] columns: empty string.
    /// - [`Option`] columns: `None`.
    /// - [`Vec`] policies: empty.
    pub fn fill_defaults_from(&mut self, preset: Config) {
        let Config {
            map: pmap,
            defaults: pdefaults,
            tags: ptags,
        } = preset;
        if self.map.geometry.is_empty() {
            self.map.geometry = pmap.geometry;
        }
        if self.map.lon.is_none() {
            self.map.lon = pmap.lon;
        }
        if self.map.lat.is_none() {
            self.map.lat = pmap.lat;
        }
        if self.map.name.is_empty() {
            self.map.name = pmap.name;
        }
        if self.map.kind.is_none() {
            self.map.kind = pmap.kind;
        }
        if self.map.lang.is_none() {
            self.map.lang = pmap.lang;
        }
        if self.defaults.kind.is_empty() {
            self.defaults.kind = pdefaults.kind;
        }
        if self.defaults.lang.is_empty() {
            self.defaults.lang = pdefaults.lang;
        }
        if self.tags.keep.is_empty() {
            self.tags.keep = ptags.keep;
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ColumnMap {
    /// Column carrying the WKB-encoded geometry. GeoParquet
    /// convention is `geometry`; Overture drops use the same name.
    #[serde(default = "default_geometry_col")]
    pub geometry: String,
    /// Optional columns carrying lon/lat directly. When present they
    /// take precedence over the WKB column (cheaper, no decode).
    #[serde(default)]
    pub lon: Option<String>,
    #[serde(default)]
    pub lat: Option<String>,
    /// Column carrying the primary name. Default `name`.
    #[serde(default = "default_name_col")]
    pub name: String,
    /// Column whose value maps to a [`PlaceKind`].
    #[serde(default)]
    pub kind: Option<String>,
    /// Column carrying a language tag.
    #[serde(default)]
    pub lang: Option<String>,
}

fn default_geometry_col() -> String {
    "geometry".into()
}
fn default_name_col() -> String {
    "name".into()
}

impl Default for ColumnMap {
    fn default() -> Self {
        Self {
            geometry: default_geometry_col(),
            lon: None,
            lat: None,
            name: default_name_col(),
            kind: None,
            lang: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Defaults {
    #[serde(default = "default_kind_str")]
    pub kind: String,
    #[serde(default = "default_lang")]
    pub lang: String,
    #[serde(default = "default_level")]
    pub level: u8,
}

fn default_kind_str() -> String {
    "poi".into()
}
fn default_lang() -> String {
    "default".into()
}
fn default_level() -> u8 {
    2
}

impl Default for Defaults {
    fn default() -> Self {
        Self {
            kind: default_kind_str(),
            lang: default_lang(),
            level: default_level(),
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct TagPolicy {
    #[serde(default)]
    pub keep: Vec<String>,
}

fn parse_kind(s: &str) -> Option<PlaceKind> {
    Some(match s.trim().to_lowercase().as_str() {
        "country" => PlaceKind::Country,
        "region" | "state" | "province" => PlaceKind::Region,
        "county" => PlaceKind::County,
        "city" | "town" | "village" | "locality" => PlaceKind::City,
        "district" => PlaceKind::District,
        "neighborhood" | "neighbourhood" | "suburb" => PlaceKind::Neighborhood,
        "street" | "highway" | "road" => PlaceKind::Street,
        "address" => PlaceKind::Address,
        "poi" | "amenity" | "shop" | "tourism" => PlaceKind::Poi,
        "postcode" | "zip" | "postalcode" => PlaceKind::Postcode,
        _ => return None,
    })
}

#[derive(Default)]
struct Counters {
    rows_seen: u64,
    rows_emitted: u64,
    skipped_no_coords: u64,
    skipped_no_name: u64,
    skipped_bad_geometry: u64,
}

/// Read a GeoParquet file into a `Vec<Place>` per the supplied config.
pub fn import(path: &Path, cfg: &Config) -> Result<Vec<Place>, ImportError> {
    info!(path = %path.display(), "opening GeoParquet");
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let schema = builder.schema().clone();
    let reader = builder.build()?;

    // Resolve column indices once up front. Missing required columns
    // bail with a helpful error rather than silently emitting zero
    // rows.
    let geom_idx = schema.index_of(&cfg.map.geometry).ok();
    let lon_idx = cfg.map.lon.as_deref().and_then(|c| schema.index_of(c).ok());
    let lat_idx = cfg.map.lat.as_deref().and_then(|c| schema.index_of(c).ok());
    let name_idx = schema.index_of(&cfg.map.name).ok();
    let kind_idx = cfg
        .map
        .kind
        .as_deref()
        .and_then(|c| schema.index_of(c).ok());
    let lang_idx = cfg
        .map
        .lang
        .as_deref()
        .and_then(|c| schema.index_of(c).ok());

    if geom_idx.is_none() && (lon_idx.is_none() || lat_idx.is_none()) {
        return Err(ImportError::Config(format!(
            "no geometry source — column '{}' missing AND no explicit lon/lat columns",
            cfg.map.geometry
        )));
    }

    // Pre-compute the set of column indices that are "mapped" (so the
    // tag-fold pass excludes them).
    let mapped: std::collections::HashSet<usize> =
        [geom_idx, lon_idx, lat_idx, name_idx, kind_idx, lang_idx]
            .into_iter()
            .flatten()
            .collect();
    let tag_policy = &cfg.tags;
    let tag_idxs: Vec<(String, usize)> = schema
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(i, f)| {
            if mapped.contains(&i) {
                return None;
            }
            if !tag_policy.keep.is_empty() && !tag_policy.keep.iter().any(|k| k == f.name()) {
                return None;
            }
            Some((f.name().clone(), i))
        })
        .collect();

    let default_kind = parse_kind(&cfg.defaults.kind).unwrap_or(PlaceKind::Poi);
    let default_lang = cfg.defaults.lang.clone();
    let level = Level::from_u8(cfg.defaults.level).unwrap_or(Level::L2);

    let mut places: Vec<Place> = Vec::new();
    let mut counters = Counters::default();
    let mut local_counters: HashMap<(u8, u32), u64> = HashMap::new();

    for batch in reader {
        let batch = batch?;
        let n_rows = batch.num_rows();
        counters.rows_seen += n_rows as u64;

        let geom_col = geom_idx.and_then(|i| {
            batch
                .column(i)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .cloned()
        });
        let lon_col = lon_idx.and_then(|i| {
            batch
                .column(i)
                .as_any()
                .downcast_ref::<Float64Array>()
                .cloned()
        });
        let lat_col = lat_idx.and_then(|i| {
            batch
                .column(i)
                .as_any()
                .downcast_ref::<Float64Array>()
                .cloned()
        });
        let name_col = name_idx.and_then(|i| {
            batch
                .column(i)
                .as_any()
                .downcast_ref::<StringArray>()
                .cloned()
        });
        let kind_col = kind_idx.and_then(|i| {
            batch
                .column(i)
                .as_any()
                .downcast_ref::<StringArray>()
                .cloned()
        });
        let lang_col = lang_idx.and_then(|i| {
            batch
                .column(i)
                .as_any()
                .downcast_ref::<StringArray>()
                .cloned()
        });

        for row in 0..n_rows {
            let (lon, lat) = match (lon_col.as_ref(), lat_col.as_ref(), geom_col.as_ref()) {
                (Some(lon_a), Some(lat_a), _) if !lon_a.is_null(row) && !lat_a.is_null(row) => {
                    (lon_a.value(row), lat_a.value(row))
                }
                (_, _, Some(g)) if !g.is_null(row) => match decode_wkb_point(g.value(row)) {
                    Ok(c) => c,
                    Err(_) => {
                        counters.skipped_bad_geometry += 1;
                        continue;
                    }
                },
                _ => {
                    counters.skipped_no_coords += 1;
                    continue;
                }
            };
            if !(-180.0..=180.0).contains(&lon)
                || !(-90.0..=90.0).contains(&lat)
                || (lon == 0.0 && lat == 0.0)
            {
                counters.skipped_no_coords += 1;
                continue;
            }

            let name = name_col
                .as_ref()
                .filter(|a| !a.is_null(row))
                .map(|a| a.value(row).to_string())
                .filter(|s| !s.trim().is_empty());
            let name = match name {
                Some(n) => n,
                None => {
                    counters.skipped_no_name += 1;
                    continue;
                }
            };

            let kind = kind_col
                .as_ref()
                .filter(|a| !a.is_null(row))
                .and_then(|a| parse_kind(a.value(row)))
                .unwrap_or(default_kind);

            let lang = lang_col
                .as_ref()
                .filter(|a| !a.is_null(row))
                .map(|a| a.value(row).to_string())
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(|| default_lang.clone());

            let tags: Vec<(String, String)> = tag_idxs
                .iter()
                .filter_map(|(name, idx)| {
                    let arr = batch.column(*idx);
                    if arr.is_null(row) {
                        return None;
                    }
                    // Best-effort stringify of common arrow types.
                    let v = if let Some(s) = arr.as_any().downcast_ref::<StringArray>() {
                        Some(s.value(row).to_string())
                    } else if let Some(f) = arr.as_any().downcast_ref::<Float64Array>() {
                        Some(f.value(row).to_string())
                    } else if let Some(i) = arr.as_any().downcast_ref::<arrow_array::Int64Array>() {
                        Some(i.value(row).to_string())
                    } else {
                        arr.as_any()
                            .downcast_ref::<arrow_array::BooleanArray>()
                            .map(|b| b.value(row).to_string())
                    };
                    v.filter(|s| !s.is_empty()).map(|s| (name.clone(), s))
                })
                .collect();
            if let Some(p) = build_place(
                lon,
                lat,
                &name,
                &lang,
                kind,
                &tags,
                level,
                &mut local_counters,
            )? {
                places.push(p);
                counters.rows_emitted += 1;
            }
        }
    }

    info!(
        rows_seen = counters.rows_seen,
        rows_emitted = counters.rows_emitted,
        skipped_no_coords = counters.skipped_no_coords,
        skipped_no_name = counters.skipped_no_name,
        skipped_bad_geometry = counters.skipped_bad_geometry,
        "GeoParquet import done"
    );
    Ok(places)
}

/// Decode a WKB Point. Accepts ISO WKB and EWKB-with-SRID; rejects
/// other geometry types. Returns `(lon, lat)`.
///
/// Layout (ISO WKB Point):
/// ```text
/// byte 0       : 0x01 (LE) or 0x00 (BE)
/// bytes 1..5   : u32 = 1 (Point) | 0x80000001 (PointZ) | 0x40000001 (PointM)
///                ... or with SRID flag set: |= 0x20000000
/// bytes 5..13  : f64 lon
/// bytes 13..21 : f64 lat
/// ```
pub fn decode_wkb_point(bytes: &[u8]) -> Result<(f64, f64), ImportError> {
    if bytes.len() < 21 {
        return Err(ImportError::Wkb(format!(
            "WKB too short: {} bytes",
            bytes.len()
        )));
    }
    let le = match bytes[0] {
        0x01 => true,
        0x00 => false,
        b => {
            return Err(ImportError::Wkb(format!(
                "bad WKB byte-order byte: 0x{b:02x}"
            )))
        }
    };
    let read_u32 = |off: usize| -> u32 {
        let chunk: [u8; 4] = bytes[off..off + 4].try_into().unwrap();
        if le {
            u32::from_le_bytes(chunk)
        } else {
            u32::from_be_bytes(chunk)
        }
    };
    let read_f64 = |off: usize| -> f64 {
        let chunk: [u8; 8] = bytes[off..off + 8].try_into().unwrap();
        if le {
            f64::from_le_bytes(chunk)
        } else {
            f64::from_be_bytes(chunk)
        }
    };

    let mut geom_type = read_u32(1);
    let mut payload_off = 5;
    // EWKB SRID flag (PostGIS-style): high bit set, SRID follows the
    // type. We don't care about SRID — assume WGS84.
    const EWKB_SRID_FLAG: u32 = 0x2000_0000;
    if geom_type & EWKB_SRID_FLAG != 0 {
        if bytes.len() < payload_off + 4 {
            return Err(ImportError::Wkb("EWKB SRID truncated".into()));
        }
        payload_off += 4;
        geom_type &= !EWKB_SRID_FLAG;
    }
    // Mask off Z/M dimension flags. ISO WKB type codes are small
    // ints (1..=7) and PostGIS encodes Z/M variants as `1000+type`
    // (PointZ=1001), `2000+type` (PointM=2001), `3000+type` (ZM).
    // 16 bits cover everything up to 3xxx; an 8-bit mask would
    // clip PolygonZ=1003 → 0xEB which is wrong. We discard the
    // dimension itself (no z/m fields parsed) but accept the row.
    let base = geom_type & 0xFFFF;
    if base != 1 {
        return Err(ImportError::Wkb(format!(
            "expected Point geometry (type=1), got type={base}"
        )));
    }
    if bytes.len() < payload_off + 16 {
        return Err(ImportError::Wkb("Point payload truncated".into()));
    }
    let lon = read_f64(payload_off);
    let lat = read_f64(payload_off + 8);
    Ok((lon, lat))
}

#[allow(clippy::too_many_arguments)]
fn build_place(
    lon: f64,
    lat: f64,
    name: &str,
    lang: &str,
    kind: PlaceKind,
    tags: &[(String, String)],
    level: Level,
    local_counters: &mut HashMap<(u8, u32), u64>,
) -> Result<Option<Place>, ImportError> {
    let centroid = Coord { lon, lat };
    let tile = TileCoord::from_coord(level, centroid);
    let key = (level.as_u8(), tile.id());
    let local = local_counters.entry(key).or_insert(0);
    let local_id = *local;
    *local += 1;
    let id = PlaceId::new(level.as_u8(), tile.id(), local_id)?;
    debug!(?id, "emit place");
    let mut tags = tags.to_vec();
    // Generic GeoParquet → Pelias-compatible gid. Prefer the upstream
    // `id` column (folded into tags by the row mapper) when present;
    // otherwise hash (kind, name, centroid) so bookmarks are still
    // stable across rebuilds. Source slug = "parquet" — narrower
    // wrappers (e.g. cairn-import-overture) overwrite this with
    // their own source slug downstream.
    let upstream_id = tags
        .iter()
        .find(|(k, _)| k == "id")
        .map(|(_, v)| v.clone());
    let gid = match upstream_id
        .as_deref()
        .and_then(|raw| synthesize_gid("parquet", "place", raw))
    {
        Some(g) => g,
        None => stable_hash_gid("parquet", "place", name, centroid),
    };
    tags.push((GID_TAG.into(), gid));
    Ok(Some(Place {
        id,
        kind,
        names: vec![LocalizedName {
            lang: lang.to_string(),
            value: name.to_string(),
        }],
        centroid,
        admin_path: Vec::new(),
        tags,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_wkb_point_le() {
        // 01 01000000 0000000000405340 0000000000005740
        // Point @ (78.0, 112.0) — endianness check.
        let mut buf = vec![0x01]; // LE
        buf.extend_from_slice(&1u32.to_le_bytes()); // Point
        buf.extend_from_slice(&78.0_f64.to_le_bytes());
        buf.extend_from_slice(&112.0_f64.to_le_bytes());
        let (lon, lat) = decode_wkb_point(&buf).unwrap();
        assert!((lon - 78.0).abs() < 1e-9);
        assert!((lat - 112.0).abs() < 1e-9);
    }

    #[test]
    fn decode_wkb_point_be() {
        let mut buf = vec![0x00];
        buf.extend_from_slice(&1u32.to_be_bytes());
        buf.extend_from_slice(&9.5314_f64.to_be_bytes());
        buf.extend_from_slice(&47.3769_f64.to_be_bytes());
        let (lon, lat) = decode_wkb_point(&buf).unwrap();
        assert!((lon - 9.5314).abs() < 1e-9);
        assert!((lat - 47.3769).abs() < 1e-9);
    }

    #[test]
    fn decode_wkb_ewkb_with_srid() {
        // Type byte has SRID flag (0x20000001) → 4 extra bytes for
        // SRID, then payload. Decoder should skip the SRID.
        let mut buf = vec![0x01];
        let geom_type: u32 = 0x2000_0001;
        buf.extend_from_slice(&geom_type.to_le_bytes());
        buf.extend_from_slice(&4326u32.to_le_bytes()); // SRID = 4326
        buf.extend_from_slice(&9.5314_f64.to_le_bytes());
        buf.extend_from_slice(&47.3769_f64.to_le_bytes());
        let (lon, lat) = decode_wkb_point(&buf).unwrap();
        assert!((lon - 9.5314).abs() < 1e-9);
        assert!((lat - 47.3769).abs() < 1e-9);
    }

    #[test]
    fn decode_wkb_rejects_short_input() {
        assert!(decode_wkb_point(&[0x01, 0x01]).is_err());
        assert!(decode_wkb_point(&[]).is_err());
    }

    #[test]
    fn decode_wkb_rejects_non_point() {
        let mut buf = vec![0x01];
        buf.extend_from_slice(&2u32.to_le_bytes()); // 2 = LineString
        buf.extend_from_slice(&[0u8; 16]);
        assert!(decode_wkb_point(&buf).is_err());
    }

    #[test]
    fn config_round_trips() {
        let toml = r#"
[map]
geometry = "geom"
name = "title"
kind = "kind_col"

[defaults]
level = 2

[tags]
keep = ["amenity", "phone"]
        "#;
        let cfg: Config = toml::from_str(toml).unwrap();
        assert_eq!(cfg.map.geometry, "geom");
        assert_eq!(cfg.map.name, "title");
        assert_eq!(cfg.tags.keep.len(), 2);
    }
}
