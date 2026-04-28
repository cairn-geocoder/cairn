//! Flat, rkyv-friendly mirror of [`AdminFeature`].
//!
//! Status: scaffolding only. We don't write archived admin tiles in the
//! default build — bincode is still the wire format. The long pole is
//! point-in-polygon directly against archived ring data; without that,
//! switching the format pays the conversion cost back at PIP time and
//! nets zero. See ROADMAP.md "Phase 6c" for the gating plan.
//!
//! What this module provides:
//! - [`ArchivedAdminFeature`] — a flat `polygon_rings: Vec<Vec<Vec<[f64; 2]>>>`
//!   shape that rkyv can derive on without geo_types interop hassle.
//! - Round-trip helpers `to_archived` / `from_archived` so callers can
//!   experiment with rkyv-mmap reads without touching the runtime.
//! - `serialize_layer` / `deserialize_layer` write the same 16-byte
//!   aligned header pattern that `cairn-tile` uses, so archived admin
//!   files share one format story across the project.

use crate::AdminFeature;
use cairn_place::Coord;
use geo_types::{Coord as GeoCoord, LineString, MultiPolygon, Polygon};
use rkyv::ser::serializers::AllocSerializer;
use rkyv::ser::Serializer;
use rkyv::{AlignedVec, Archive, Deserialize, Infallible, Serialize};
use std::io::{self, Read, Write};
use std::path::Path;

const MAGIC: &[u8; 4] = b"CRAD";
const VERSION_RAW: u32 = 1;
const HEADER_LEN: usize = 16;

/// Flat mirror of [`AdminFeature`] suitable for `#[derive(Archive)]`.
/// Stores rings as nested `Vec<[f64; 2]>` so rkyv can lay everything out
/// contiguously without a geo_types adapter. Centroid is `[lon, lat]`.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
#[archive(check_bytes)]
pub struct ArchivedAdminFeature {
    pub place_id: u64,
    pub level: u8,
    pub kind: String,
    pub name: String,
    pub centroid: [f64; 2],
    pub admin_path: Vec<u64>,
    /// `polygon_rings[poly_idx][ring_idx][vertex_idx] = [lon, lat]`.
    /// Ring 0 of each polygon is the outer ring; rings 1.. are holes.
    pub polygon_rings: Vec<Vec<Vec<[f64; 2]>>>,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
#[archive(check_bytes)]
pub struct ArchivedAdminLayer {
    pub features: Vec<ArchivedAdminFeature>,
}

/// Convert a runtime [`AdminFeature`] into the flat archived form.
pub fn to_archived(f: &AdminFeature) -> ArchivedAdminFeature {
    let polygon_rings: Vec<Vec<Vec<[f64; 2]>>> = f
        .polygon
        .0
        .iter()
        .map(|poly| {
            let mut rings: Vec<Vec<[f64; 2]>> = Vec::with_capacity(1 + poly.interiors().len());
            rings.push(linestring_to_vec(poly.exterior()));
            for hole in poly.interiors() {
                rings.push(linestring_to_vec(hole));
            }
            rings
        })
        .collect();

    ArchivedAdminFeature {
        place_id: f.place_id,
        level: f.level,
        kind: f.kind.clone(),
        name: f.name.clone(),
        centroid: [f.centroid.lon, f.centroid.lat],
        admin_path: f.admin_path.clone(),
        polygon_rings,
    }
}

/// Convert a flat archived feature back into the runtime form. Used when
/// the runtime path still wants `geo::Contains` over `MultiPolygon<f64>`
/// before the custom PIP-on-archived implementation lands.
pub fn from_archived(a: &ArchivedAdminFeature) -> AdminFeature {
    let polys: Vec<Polygon<f64>> = a
        .polygon_rings
        .iter()
        .map(|poly| {
            let mut iter = poly.iter();
            let outer = iter
                .next()
                .map(vec_to_linestring)
                .unwrap_or_else(|| LineString::new(Vec::new()));
            let holes: Vec<LineString<f64>> = iter.map(vec_to_linestring).collect();
            Polygon::new(outer, holes)
        })
        .collect();
    AdminFeature {
        place_id: a.place_id,
        level: a.level,
        kind: a.kind.clone(),
        name: a.name.clone(),
        centroid: Coord {
            lon: a.centroid[0],
            lat: a.centroid[1],
        },
        admin_path: a.admin_path.clone(),
        polygon: MultiPolygon(polys),
    }
}

fn linestring_to_vec(ls: &LineString<f64>) -> Vec<[f64; 2]> {
    ls.0.iter().map(|c| [c.x, c.y]).collect()
}

fn vec_to_linestring(v: &Vec<[f64; 2]>) -> LineString<f64> {
    LineString(v.iter().map(|p| GeoCoord { x: p[0], y: p[1] }).collect())
}

/// Serialize a layer to a 16-byte aligned blob: 4-byte magic, 4-byte
/// version, 8-byte payload-length-le, then the rkyv payload. Same
/// pattern as cairn-tile so callers reusing the existing mmap loader
/// don't have to learn a second format.
pub fn serialize_layer(layer: &ArchivedAdminLayer) -> Result<AlignedVec, ArchivedError> {
    let mut serializer = AllocSerializer::<4096>::default();
    serializer
        .serialize_value(layer)
        .map_err(|e| ArchivedError::Serialize(format!("{e:?}")))?;
    let body = serializer.into_serializer().into_inner();

    let mut out = AlignedVec::with_capacity(HEADER_LEN + body.len());
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&VERSION_RAW.to_le_bytes());
    out.extend_from_slice(&(body.len() as u64).to_le_bytes());
    debug_assert_eq!(out.len(), HEADER_LEN);
    out.extend_from_slice(&body);
    Ok(out)
}

/// Deserialize a layer from a header+payload blob. Validates the rkyv
/// archive via `check_archived_root` before materializing.
pub fn deserialize_layer(blob: &[u8]) -> Result<ArchivedAdminLayer, ArchivedError> {
    if blob.len() < HEADER_LEN {
        return Err(ArchivedError::Header("truncated header"));
    }
    if &blob[0..4] != MAGIC {
        return Err(ArchivedError::Header("bad magic"));
    }
    let version = u32::from_le_bytes(blob[4..8].try_into().unwrap());
    if version != VERSION_RAW {
        return Err(ArchivedError::Header("unknown version"));
    }
    let body_len = u64::from_le_bytes(blob[8..16].try_into().unwrap()) as usize;
    if blob.len() < HEADER_LEN + body_len {
        return Err(ArchivedError::Header("truncated body"));
    }
    let body = &blob[HEADER_LEN..HEADER_LEN + body_len];

    // rkyv requires 16-aligned input. Re-stage into an AlignedVec.
    let mut aligned = AlignedVec::with_capacity(body.len());
    aligned.extend_from_slice(body);
    let archived = rkyv::check_archived_root::<ArchivedAdminLayer>(&aligned)
        .map_err(|e| ArchivedError::Validate(format!("{e:?}")))?;
    Deserialize::<ArchivedAdminLayer, _>::deserialize(archived, &mut Infallible)
        .map_err(|_| ArchivedError::Deserialize)
}

pub fn write_layer(path: &Path, layer: &ArchivedAdminLayer) -> Result<(), ArchivedError> {
    let blob = serialize_layer(layer)?;
    let mut f = std::fs::File::create(path)?;
    f.write_all(&blob)?;
    Ok(())
}

pub fn read_layer(path: &Path) -> Result<ArchivedAdminLayer, ArchivedError> {
    let mut f = std::fs::File::open(path)?;
    let mut buf = Vec::new();
    f.read_to_end(&mut buf)?;
    deserialize_layer(&buf)
}

#[derive(Debug, thiserror::Error)]
pub enum ArchivedError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("rkyv serialize: {0}")]
    Serialize(String),
    #[error("rkyv validate: {0}")]
    Validate(String),
    #[error("rkyv deserialize")]
    Deserialize,
    #[error("header: {0}")]
    Header(&'static str),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AdminFeature;
    use geo_types::{LineString, Polygon};

    fn sample() -> AdminFeature {
        let outer = LineString::from(vec![
            (0.0, 0.0),
            (10.0, 0.0),
            (10.0, 10.0),
            (0.0, 10.0),
            (0.0, 0.0),
        ]);
        let hole = LineString::from(vec![
            (4.0, 4.0),
            (6.0, 4.0),
            (6.0, 6.0),
            (4.0, 6.0),
            (4.0, 4.0),
        ]);
        AdminFeature {
            place_id: 42,
            level: 0,
            kind: "country".into(),
            name: "Atlantis".into(),
            centroid: Coord { lon: 5.0, lat: 5.0 },
            admin_path: vec![1, 2, 3],
            polygon: MultiPolygon(vec![Polygon::new(outer, vec![hole])]),
        }
    }

    #[test]
    fn round_trip_preserves_fields() {
        let f = sample();
        let archived = to_archived(&f);
        let back = from_archived(&archived);
        assert_eq!(back.place_id, f.place_id);
        assert_eq!(back.kind, f.kind);
        assert_eq!(back.name, f.name);
        assert_eq!(back.admin_path, f.admin_path);
        assert_eq!(back.polygon.0.len(), 1);
        assert_eq!(back.polygon.0[0].interiors().len(), 1);
        assert_eq!(back.polygon.0[0].exterior().0.len(), 5);
    }

    #[test]
    fn serialize_round_trip() {
        let layer = ArchivedAdminLayer {
            features: vec![to_archived(&sample()), to_archived(&sample())],
        };
        let blob = serialize_layer(&layer).unwrap();
        assert!(blob.len() > HEADER_LEN);
        assert_eq!(&blob[0..4], MAGIC);
        let back = deserialize_layer(&blob).unwrap();
        assert_eq!(back.features.len(), 2);
        assert_eq!(back.features[0].name, "Atlantis");
        assert_eq!(back.features[0].polygon_rings[0].len(), 2); // outer + 1 hole
    }
}
