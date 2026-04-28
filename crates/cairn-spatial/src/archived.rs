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
    /// Precomputed outer-ring bbox per polygon: `[min_x, min_y, max_x, max_y]`.
    /// Lets the PIP fast-path skip whole polygons without sweeping vertices.
    /// Same length as `polygon_rings`.
    pub polygon_bboxes: Vec<[f64; 4]>,
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

    let polygon_bboxes: Vec<[f64; 4]> = polygon_rings
        .iter()
        .map(|poly| {
            poly.first()
                .and_then(|outer| ring_bbox(outer))
                .unwrap_or([f64::NAN, f64::NAN, f64::NAN, f64::NAN])
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
        polygon_bboxes,
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

// ============================================================
// Point-in-polygon directly on archived ring data
// ============================================================

/// W. Randolph Franklin's branch-light ray-casting test. Counts edges
/// of the closed ring that cross a rightward ray from `(px, py)`. The
/// `(ay > py) != (by > py)` straddle test plus a single multiply-add
/// for the x intersection is the tightest correct formulation.
#[inline]
fn ring_crossings(ring: &[[f64; 2]], px: f64, py: f64) -> u32 {
    let n = ring.len();
    if n < 2 {
        return 0;
    }
    let mut crossings: u32 = 0;
    for i in 0..n - 1 {
        let (ax, ay) = (ring[i][0], ring[i][1]);
        let (bx, by) = (ring[i + 1][0], ring[i + 1][1]);
        if (ay > py) != (by > py) {
            let x_at = (bx - ax) * (py - ay) / (by - ay) + ax;
            if x_at > px {
                crossings += 1;
            }
        }
    }
    crossings
}

#[inline]
fn ring_bbox(ring: &[[f64; 2]]) -> Option<[f64; 4]> {
    if ring.is_empty() {
        return None;
    }
    let mut min_x = f64::INFINITY;
    let mut min_y = f64::INFINITY;
    let mut max_x = f64::NEG_INFINITY;
    let mut max_y = f64::NEG_INFINITY;
    for v in ring {
        if v[0] < min_x {
            min_x = v[0];
        }
        if v[0] > max_x {
            max_x = v[0];
        }
        if v[1] < min_y {
            min_y = v[1];
        }
        if v[1] > max_y {
            max_y = v[1];
        }
    }
    Some([min_x, min_y, max_x, max_y])
}

#[inline]
fn point_in_bbox(bbox: &[f64; 4], px: f64, py: f64) -> bool {
    px >= bbox[0] && px <= bbox[2] && py >= bbox[1] && py <= bbox[3]
}

/// Returns true if the point is contained in the multipolygon defined
/// by `feat.polygon_rings`. Each polygon's ring 0 is the outer ring;
/// rings 1.. are holes. The point is "in" a polygon iff the ray crosses
/// its outer ring an odd number of times AND every hole an even number
/// of times. The point is in the multipolygon iff it's in any polygon.
///
/// This is the runtime equivalent of `geo::Contains` over the hydrated
/// `MultiPolygon<f64>`, but skips the hydration step.
pub fn pip_archived(feat: &ArchivedAdminFeature, point: [f64; 2]) -> bool {
    let (px, py) = (point[0], point[1]);
    for (poly_idx, poly) in feat.polygon_rings.iter().enumerate() {
        if poly.is_empty() {
            continue;
        }
        // O(1) bbox prefilter using the precomputed outer-ring bbox.
        // A point outside the bbox can't be inside the polygon.
        if let Some(bbox) = feat.polygon_bboxes.get(poly_idx) {
            if !bbox[0].is_nan() && !point_in_bbox(bbox, px, py) {
                continue;
            }
        }
        if ring_crossings(&poly[0], px, py) % 2 == 0 {
            continue;
        }
        let mut in_hole = false;
        for hole in poly.iter().skip(1) {
            // Hole bbox isn't precomputed (rare; computing inline is fine).
            if let Some(hbbox) = ring_bbox(hole) {
                if !point_in_bbox(&hbbox, px, py) {
                    continue;
                }
            }
            if ring_crossings(hole, px, py) % 2 == 1 {
                in_hole = true;
                break;
            }
        }
        if !in_hole {
            return true;
        }
    }
    false
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
    fn pip_inside_outside() {
        let f = sample();
        let a = to_archived(&f);
        // (1,1) is inside the 0..10 outer.
        assert!(pip_archived(&a, [1.0, 1.0]));
        // (5,5) is the hole center → outside.
        assert!(!pip_archived(&a, [5.0, 5.0]));
        // (-1,-1) clearly outside.
        assert!(!pip_archived(&a, [-1.0, -1.0]));
        // (4.5, 4.5) is just inside the hole → outside.
        assert!(!pip_archived(&a, [4.5, 4.5]));
        // (3, 3) is between hole and outer → inside.
        assert!(pip_archived(&a, [3.0, 3.0]));
    }

    #[test]
    fn pip_matches_geo_contains() {
        // Diff test: for a deterministic mesh of probes, our ray-casting
        // PIP must agree with geo::Contains over the hydrated form.
        use geo::Contains;
        use geo_types::Coord as GeoCoord;
        let f = sample();
        let a = to_archived(&f);
        let mut disagreements = Vec::new();
        for i in -3..=13 {
            for j in -3..=13 {
                let px = i as f64 + 0.13; // off-vertex probes
                let py = j as f64 + 0.27;
                let ours = pip_archived(&a, [px, py]);
                let geo_says = f.polygon.contains(&GeoCoord { x: px, y: py });
                if ours != geo_says {
                    disagreements.push((px, py, ours, geo_says));
                }
            }
        }
        assert!(
            disagreements.is_empty(),
            "PIP disagreed at: {disagreements:?}"
        );
    }

    #[test]
    fn pip_empty_multipolygon() {
        let a = ArchivedAdminFeature {
            place_id: 0,
            level: 0,
            kind: "country".into(),
            name: "Empty".into(),
            centroid: [0.0, 0.0],
            admin_path: vec![],
            polygon_rings: vec![],
            polygon_bboxes: vec![],
        };
        assert!(!pip_archived(&a, [0.0, 0.0]));
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
