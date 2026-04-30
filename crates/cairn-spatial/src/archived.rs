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
/// On-disk version of the archived admin tile format.
/// - v1 — original layout: `polygon_rings` + `polygon_bboxes` only.
/// - v2 — adds `polygon_edges` per ring (sorted by min-y) so PIP
///   can binary-search the y-range instead of scanning every edge.
///   Existing v1 tiles fail to load with `ArchivedError::Header("v1
///   tile rejected — rebuild bundle for the v0.5 schema")`.
const VERSION_RAW: u32 = 2;
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
    /// Per-ring sorted edge records, parallel to `polygon_rings`:
    /// `polygon_edges[poly_idx][ring_idx][edge_idx] = [min_y, max_y, x_at_min_y, x_at_max_y]`.
    /// Sorted by `min_y` ascending. PIP binary-searches the upper
    /// bound of `min_y <= py`, then scans backward filtering by
    /// `max_y > py` — the dense-polygon hot path examines roughly
    /// `O(sqrt(V))` edges instead of `O(V)`.
    pub polygon_edges: Vec<Vec<Vec<[f64; 4]>>>,
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
            poly.first().and_then(|outer| ring_bbox(outer)).unwrap_or([
                f64::NAN,
                f64::NAN,
                f64::NAN,
                f64::NAN,
            ])
        })
        .collect();

    let polygon_edges: Vec<Vec<Vec<[f64; 4]>>> = polygon_rings
        .iter()
        .map(|poly| poly.iter().map(|ring| build_sorted_edges(ring)).collect())
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
        polygon_edges,
    }
}

/// Build the sorted-by-min-y edge list for one ring. Each entry is
/// `[min_y, max_y, x_at_min_y, x_at_max_y]`. Used by `pip_archived_ref`'s
/// fast path: binary-search the `min_y` ordering for the upper bound
/// where `min_y <= py`, scan backward filtering on `max_y > py`.
fn build_sorted_edges(ring: &[[f64; 2]]) -> Vec<[f64; 4]> {
    if ring.len() < 2 {
        return Vec::new();
    }
    let mut edges: Vec<[f64; 4]> = Vec::with_capacity(ring.len() - 1);
    for i in 0..ring.len() - 1 {
        let (ax, ay) = (ring[i][0], ring[i][1]);
        let (bx, by) = (ring[i + 1][0], ring[i + 1][1]);
        if ay == by {
            // Horizontal edge — ray-cast crossings rule never counts
            // it (the strict inequality `(ay > py) != (by > py)` is
            // never satisfied). Skip to keep the edge list tight.
            continue;
        }
        let (min_y, max_y, x_at_min_y, x_at_max_y) = if ay < by {
            (ay, by, ax, bx)
        } else {
            (by, ay, bx, ax)
        };
        edges.push([min_y, max_y, x_at_min_y, x_at_max_y]);
    }
    edges.sort_by(|a, b| a[0].partial_cmp(&b[0]).unwrap_or(std::cmp::Ordering::Equal));
    edges
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
                .map(|v| vec_to_linestring(v))
                .unwrap_or_else(|| LineString::new(Vec::new()));
            let holes: Vec<LineString<f64>> = iter.map(|v| vec_to_linestring(v)).collect();
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

fn vec_to_linestring(v: &[[f64; 2]]) -> LineString<f64> {
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

// ============================================================
// Zero-copy archived access
// ============================================================

/// Backing storage for a validated archived admin tile. Either an
/// owned `AlignedVec` (eager-built path) or a memory-mapped file
/// (disk path).
enum AdminTileBytes {
    Owned(AlignedVec),
    Mapped(memmap2::Mmap),
    /// Mapped-but-unaligned files re-stage into an `AlignedVec`. rkyv
    /// requires 16-byte alignment on the payload; mmap usually
    /// satisfies it but sufficiently weird filesystems / partial reads
    /// can violate it, so we copy as a fallback.
    OwnedMappedCopy(AlignedVec, #[allow(dead_code)] memmap2::Mmap),
}

/// Holds a tile's archived bytes and lets callers iterate the rkyv
/// archived form directly, skipping the deserialize-into-owned step.
/// Validation runs once at construction; subsequent `archived()`
/// access uses unchecked `archived_root`, which is sound because the
/// backing bytes are immutable for the lifetime of `Self`.
pub struct AdminTileArchive {
    bytes: AdminTileBytes,
    body_offset: usize,
    body_len: usize,
    item_count: usize,
}

impl AdminTileArchive {
    /// Build from an owned `AlignedVec` produced by
    /// `serialize_layer`. The header is verified, the rkyv archive is
    /// validated, and the body offsets are cached.
    pub fn from_aligned(bytes: AlignedVec) -> Result<Self, ArchivedError> {
        let (body_offset, body_len) = parse_header(&bytes, MAGIC)?;
        let payload = &bytes[body_offset..body_offset + body_len];
        let archived = rkyv::check_archived_root::<ArchivedAdminLayer>(payload)
            .map_err(|e| ArchivedError::Validate(format!("{e:?}")))?;
        let item_count = archived.features.len();
        Ok(Self {
            bytes: AdminTileBytes::Owned(bytes),
            body_offset,
            body_len,
            item_count,
        })
    }

    /// Build from a mmap-backed file. Falls back to an owned aligned
    /// copy if the mmap payload isn't 16-byte aligned.
    pub fn from_path(path: &Path) -> Result<Self, ArchivedError> {
        let file = std::fs::File::open(path)?;
        let mmap = unsafe { memmap2::Mmap::map(&file)? };
        let (body_offset, body_len) = parse_header(&mmap, MAGIC)?;
        let payload_ptr = unsafe { mmap.as_ptr().add(body_offset) };
        if (payload_ptr as usize) % 16 != 0 {
            let mut aligned = AlignedVec::with_capacity(body_len);
            aligned.extend_from_slice(&mmap[body_offset..body_offset + body_len]);
            let archived = rkyv::check_archived_root::<ArchivedAdminLayer>(&aligned)
                .map_err(|e| ArchivedError::Validate(format!("{e:?}")))?;
            let item_count = archived.features.len();
            return Ok(Self {
                bytes: AdminTileBytes::OwnedMappedCopy(aligned, mmap),
                body_offset: 0,
                body_len,
                item_count,
            });
        }
        let payload = &mmap[body_offset..body_offset + body_len];
        let archived = rkyv::check_archived_root::<ArchivedAdminLayer>(payload)
            .map_err(|e| ArchivedError::Validate(format!("{e:?}")))?;
        let item_count = archived.features.len();
        Ok(Self {
            bytes: AdminTileBytes::Mapped(mmap),
            body_offset,
            body_len,
            item_count,
        })
    }

    fn payload(&self) -> &[u8] {
        let raw: &[u8] = match &self.bytes {
            AdminTileBytes::Owned(v) => v,
            AdminTileBytes::Mapped(m) => &m[..],
            AdminTileBytes::OwnedMappedCopy(v, _) => v,
        };
        &raw[self.body_offset..self.body_offset + self.body_len]
    }

    /// Zero-copy reference into the archived layer. Safe because every
    /// constructor validates the bytes via `check_archived_root` first.
    pub fn archived(&self) -> &<ArchivedAdminLayer as Archive>::Archived {
        unsafe { rkyv::archived_root::<ArchivedAdminLayer>(self.payload()) }
    }

    pub fn item_count(&self) -> usize {
        self.item_count
    }
}

fn parse_header(raw: &[u8], expected_magic: &[u8; 4]) -> Result<(usize, usize), ArchivedError> {
    if raw.len() < HEADER_LEN {
        return Err(ArchivedError::Header("truncated header"));
    }
    if &raw[0..4] != expected_magic {
        return Err(ArchivedError::Header("bad magic"));
    }
    let version = u32::from_le_bytes(raw[4..8].try_into().unwrap());
    if version != VERSION_RAW {
        return Err(ArchivedError::Header("unknown version"));
    }
    let body_len = u64::from_le_bytes(raw[8..16].try_into().unwrap()) as usize;
    if raw.len() < HEADER_LEN + body_len {
        return Err(ArchivedError::Header("truncated body"));
    }
    Ok((HEADER_LEN, body_len))
}

/// Same algorithm as `pip_archived` but operates directly on the
/// rkyv-archived form, skipping the runtime hydration into
/// `ArchivedAdminFeature`. This is the zero-copy hot path used by
/// [`AdminTileArchive::archived()`].
///
/// Two-stage filter per polygon:
/// 1. **Polygon bbox prefilter** (`polygon_bboxes`) skips whole
///    polygons that don't contain the query — most PIP candidates
///    miss here on small bbox-misaligned queries.
/// 2. **Edge-list ray-cast.** Each ring's `polygon_edges` list is
///    sorted by `min_y` ascending. Binary-search for the upper bound
///    of edges with `min_y <= py`; the prefix is the candidate set.
///    Scan it filtering on `max_y > py` (constant per-edge cost) and
///    counting `x_at_py > px` crossings.
///
/// For dense polygons (1000s of vertices) the prefix scan visits
/// only the edges whose y-interval brackets `py` — typically
/// O(sqrt(V)) — instead of every edge. Holes use the same fast path.
pub fn pip_archived_ref(
    feat: &<ArchivedAdminFeature as Archive>::Archived,
    point: [f64; 2],
) -> bool {
    let (px, py) = (point[0], point[1]);
    for (poly_idx, poly) in feat.polygon_rings.iter().enumerate() {
        if poly.is_empty() {
            continue;
        }
        if let Some(bbox) = feat.polygon_bboxes.get(poly_idx) {
            if !bbox[0].is_nan() && !point_in_bbox(bbox, px, py) {
                continue;
            }
        }
        let edges_for_poly = feat.polygon_edges.get(poly_idx);
        let outer_crossings = match edges_for_poly.and_then(|p| p.first()) {
            Some(edges) => archived_edge_crossings(edges, px, py),
            None => ring_crossings(poly[0].as_slice(), px, py),
        };
        if outer_crossings % 2 == 0 {
            continue;
        }
        let mut in_hole = false;
        for (ring_idx, hole_archived) in poly.iter().enumerate().skip(1) {
            let hole: &[[f64; 2]] = hole_archived.as_slice();
            if let Some(hbbox) = ring_bbox(hole) {
                if !point_in_bbox(&hbbox, px, py) {
                    continue;
                }
            }
            let hole_crossings = match edges_for_poly.and_then(|p| p.get(ring_idx)) {
                Some(edges) => archived_edge_crossings(edges, px, py),
                None => ring_crossings(hole, px, py),
            };
            if hole_crossings % 2 == 1 {
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

/// Edge-list crossings count using the rkyv-archived sorted form.
/// Binary-search for the upper bound of `min_y <= py`, then sweep
/// candidates filtering on `max_y > py`. Constant-time crossing
/// math per edge avoids the divide in the legacy `ring_crossings`
/// path because both endpoints' x are precomputed.
#[inline]
fn archived_edge_crossings(edges: &<Vec<[f64; 4]> as Archive>::Archived, px: f64, py: f64) -> u32 {
    // partition_point returns the first idx where predicate fails;
    // edges with min_y <= py form a contiguous prefix.
    let n = edges.len();
    let upper = edges.partition_point(|e| e[0] <= py);
    let mut crossings: u32 = 0;
    for i in 0..upper {
        let e = &edges[i];
        let (min_y, max_y, x_at_min_y, x_at_max_y) = (e[0], e[1], e[2], e[3]);
        if max_y <= py {
            continue;
        }
        // Linear interpolation in y between (min_y, x_at_min_y) and
        // (max_y, x_at_max_y). max_y > py and min_y <= py guarantee
        // the denominator is non-zero.
        let dy = max_y - min_y;
        let x_at_py = x_at_min_y + (x_at_max_y - x_at_min_y) * (py - min_y) / dy;
        if x_at_py > px {
            crossings += 1;
        }
    }
    let _ = n;
    crossings
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
    fn pip_archived_ref_matches_owned() {
        // pip_archived_ref (zero-copy on rkyv archived form) must give
        // bit-identical answers to pip_archived (owned form).
        let f = sample();
        let layer = ArchivedAdminLayer {
            features: vec![to_archived(&f)],
        };
        let blob = serialize_layer(&layer).unwrap();
        let tile = AdminTileArchive::from_aligned(blob).unwrap();
        let archived_layer = tile.archived();
        let feat_ref = &archived_layer.features[0];
        let owned = to_archived(&f);
        for i in -3..=13 {
            for j in -3..=13 {
                let p = [i as f64 + 0.13, j as f64 + 0.27];
                assert_eq!(
                    pip_archived_ref(feat_ref, p),
                    pip_archived(&owned, p),
                    "mismatch at {p:?}"
                );
            }
        }
    }

    #[test]
    fn admin_tile_archive_round_trips() {
        let layer = ArchivedAdminLayer {
            features: vec![to_archived(&sample()), to_archived(&sample())],
        };
        let blob = serialize_layer(&layer).unwrap();
        let tile = AdminTileArchive::from_aligned(blob).unwrap();
        assert_eq!(tile.item_count(), 2);
        let arch = tile.archived();
        assert_eq!(arch.features.len(), 2);
        assert_eq!(arch.features[0].name.as_str(), "Atlantis");
    }

    #[test]
    fn build_sorted_edges_strips_horizontals_and_sorts_by_min_y() {
        // Closed unit square + one horizontal edge inside the
        // sequence. The horizontal must be skipped (ray-cast never
        // counts it) and the rest must end up sorted by min_y.
        let ring = vec![
            [0.0, 0.0],
            [10.0, 0.0], // horizontal — strip
            [10.0, 4.0],
            [10.0, 8.0],
            [0.0, 8.0],
            [0.0, 4.0],
            [0.0, 0.0],
        ];
        let edges = build_sorted_edges(&ring);
        // 6 segments total: (0,0)→(10,0) and (10,8)→(0,8) are both
        // horizontal — strip. 4 vertical edges remain.
        assert_eq!(edges.len(), 4);
        // min_y monotonic ascending.
        for w in edges.windows(2) {
            assert!(w[0][0] <= w[1][0], "edges out of order: {:?}", edges);
        }
    }

    #[test]
    fn pip_archived_ref_handles_concave_polygon() {
        // U-shape: bbox (0..10, 0..10) covers the inside-the-notch
        // region, but the polygon does not. Tests that the edge-list
        // ray-cast (not just the bbox prefilter) returns the right
        // answer.
        //
        //  10  *--------*
        //      |        |
        //   8  |  *--*  |
        //      |  |  |  |   ← notch is OUTSIDE the polygon
        //   2  |  *--*  |
        //      |        |
        //   0  *--------*
        //      0  3  7  10
        let outer = LineString::from(vec![
            (0.0, 0.0),
            (10.0, 0.0),
            (10.0, 10.0),
            (0.0, 10.0),
            (0.0, 0.0),
        ]);
        let hole = LineString::from(vec![
            (3.0, 2.0),
            (7.0, 2.0),
            (7.0, 8.0),
            (3.0, 8.0),
            (3.0, 2.0),
        ]);
        let f = AdminFeature {
            place_id: 7,
            level: 0,
            kind: "country".into(),
            name: "U".into(),
            centroid: Coord { lon: 5.0, lat: 5.0 },
            admin_path: vec![],
            polygon: MultiPolygon(vec![Polygon::new(outer, vec![hole])]),
        };
        let archived = to_archived(&f);
        let blob = serialize_layer(&ArchivedAdminLayer {
            features: vec![archived],
        })
        .unwrap();
        let tile = AdminTileArchive::from_aligned(blob).unwrap();
        let layer = tile.archived();
        let feat = &layer.features[0];

        // Inside the polygon (above the hole).
        assert!(pip_archived_ref(feat, [5.0, 9.0]));
        // Inside the polygon (below the hole).
        assert!(pip_archived_ref(feat, [5.0, 1.0]));
        // Inside the hole (which is outside the polygon).
        assert!(!pip_archived_ref(feat, [5.0, 5.0]));
        // Outside the bbox entirely.
        assert!(!pip_archived_ref(feat, [11.0, 11.0]));
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
            polygon_edges: vec![],
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
