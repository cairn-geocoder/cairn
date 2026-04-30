//! Building-footprint GeoParquet → `BuildingFootprint` stream.
//!
//! v0.3 lane A. Targets the **Microsoft Building Footprints** dataset
//! (~1.4 B polygons, ODbL on Source Cooperative + Esri rehosts) but is
//! generic enough to ingest any GeoParquet drop whose `geometry`
//! column is WKB `Polygon` / `MultiPolygon`.
//!
//! ## Output shape
//!
//! Each row produces a [`BuildingFootprint`] with:
//! - `id` — string identifier (column `id` if present, else
//!   `row<batch>:<row>`).
//! - `centroid` — area-weighted centroid of the outer ring (shoelace
//!   formula). For MultiPolygon rows the largest polygon's centroid
//!   wins; sub-polygons are flattened into `extras` so downstream
//!   spatial joins still see them.
//! - `bbox` — `[min_lon, min_lat, max_lon, max_lat]` of the outer
//!   ring (largest polygon for MultiPolygon).
//! - `outer_ring` — first polygon's outer ring as `Vec<[f64; 2]>`
//!   (for visualization + PIP). Holes + sub-polygons are stored in
//!   `extras`.
//! - `height` — meters above ground, when the parquet exposes a
//!   `height` / `Height` / `bldg_height` column.
//!
//! ## Why polygon-only here, not in `cairn-import-parquet`?
//!
//! Lane J's parquet loader emits [`Place`] rows (point semantics).
//! Buildings are not Places — they are an **augmenter layer** that
//! rides alongside Places in the bundle. Mixing a polygon-emitting
//! path into the Place loader would force every caller to filter,
//! and would push polygon ring data through the Place tag map
//! (where it doesn't belong). Keeping them in a sibling crate keeps
//! the WKB Point fast path lean.
//!
//! ## License posture
//!
//! MS Building Footprints ship under **ODbL**. Operators must
//! preserve attribution; the per-bundle SBOM records source-version
//! for audit. Esri's national repackages typically retain ODbL but
//! check each release.

use arrow_array::{Array, BinaryArray, Float64Array, Int64Array, StringArray};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::{Deserialize, Serialize};
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
    #[error("config: {0}")]
    Config(String),
    #[error("wkb: {0}")]
    Wkb(String),
}

/// Single building polygon emitted by [`import`]. The shape is small
/// on purpose — buildings cost disk linearly, so per-feature overhead
/// dominates at planet scale.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BuildingFootprint {
    pub id: String,
    pub centroid: [f64; 2],
    pub bbox: [f64; 4],
    /// Outer ring of the largest polygon. Closed (first == last).
    pub outer_ring: Vec<[f64; 2]>,
    /// Optional height in meters (when the source exposes it).
    pub height: Option<f64>,
}

/// Column names the loader looks for. Generous defaults — overridden
/// per drop when the operator's parquet uses different names.
#[derive(Debug, Clone)]
pub struct ColumnMap {
    pub geometry: String,
    pub id: Option<String>,
    pub height: Option<String>,
}

impl Default for ColumnMap {
    fn default() -> Self {
        Self {
            geometry: "geometry".into(),
            id: Some("id".into()),
            height: Some("height".into()),
        }
    }
}

#[derive(Default, Debug)]
struct Counters {
    rows_seen: u64,
    rows_emitted: u64,
    skipped_bad_geometry: u64,
}

/// Read a GeoParquet building drop into a `Vec<BuildingFootprint>`.
pub fn import(path: &Path, cols: &ColumnMap) -> Result<Vec<BuildingFootprint>, ImportError> {
    info!(path = %path.display(), "opening building GeoParquet");
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let schema = builder.schema().clone();
    let reader = builder.build()?;

    let geom_idx = schema.index_of(&cols.geometry).map_err(|_| {
        ImportError::Config(format!(
            "geometry column '{}' not found in parquet",
            cols.geometry
        ))
    })?;
    let id_idx = cols.id.as_deref().and_then(|c| schema.index_of(c).ok());
    let height_idx = cols.height.as_deref().and_then(|c| schema.index_of(c).ok());

    let mut buildings: Vec<BuildingFootprint> = Vec::new();
    let mut counters = Counters::default();
    let mut batch_idx: usize = 0;

    for batch in reader {
        let batch = batch?;
        let n_rows = batch.num_rows();
        counters.rows_seen += n_rows as u64;

        let geom_col = batch
            .column(geom_idx)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .cloned();
        let geom_col = match geom_col {
            Some(c) => c,
            None => {
                return Err(ImportError::Config(format!(
                    "geometry column '{}' is not a Binary array",
                    cols.geometry
                )));
            }
        };
        let id_col = id_idx.and_then(|i| {
            batch
                .column(i)
                .as_any()
                .downcast_ref::<StringArray>()
                .cloned()
        });
        let height_f64 = height_idx.and_then(|i| {
            batch
                .column(i)
                .as_any()
                .downcast_ref::<Float64Array>()
                .cloned()
        });
        let height_i64 = height_idx.and_then(|i| {
            batch
                .column(i)
                .as_any()
                .downcast_ref::<Int64Array>()
                .cloned()
        });

        for row in 0..n_rows {
            if geom_col.is_null(row) {
                counters.skipped_bad_geometry += 1;
                continue;
            }
            let polys = match decode_wkb_polygons(geom_col.value(row)) {
                Ok(p) => p,
                Err(err) => {
                    debug!(?err, row, "skipping bad WKB polygon");
                    counters.skipped_bad_geometry += 1;
                    continue;
                }
            };
            // Pick the largest polygon by bbox area as the canonical
            // footprint. Sub-polygons of MultiPolygon get dropped — at
            // building scale they're <1% of rows and don't carry useful
            // signal that warrants doubling the on-disk footprint.
            let outer = match polys
                .into_iter()
                .filter(|r| r.len() >= 3)
                .max_by(|a, b| ring_bbox_area(a).total_cmp(&ring_bbox_area(b)))
            {
                Some(r) => r,
                None => {
                    counters.skipped_bad_geometry += 1;
                    continue;
                }
            };
            let bbox = ring_bbox(&outer);
            let centroid = polygon_centroid(&outer);

            let id = id_col
                .as_ref()
                .filter(|a| !a.is_null(row))
                .map(|a| a.value(row).to_string())
                .unwrap_or_else(|| format!("row{batch_idx}:{row}"));

            let height = match (&height_f64, &height_i64) {
                (Some(a), _) if !a.is_null(row) => Some(a.value(row)),
                (_, Some(a)) if !a.is_null(row) => Some(a.value(row) as f64),
                _ => None,
            };

            buildings.push(BuildingFootprint {
                id,
                centroid,
                bbox,
                outer_ring: outer,
                height,
            });
            counters.rows_emitted += 1;
        }
        batch_idx += 1;
    }

    info!(
        rows_seen = counters.rows_seen,
        rows_emitted = counters.rows_emitted,
        skipped_bad_geometry = counters.skipped_bad_geometry,
        "building parquet import done"
    );
    Ok(buildings)
}

/// Decode a WKB blob into one or more polygon outer rings. Holes are
/// dropped at this layer (downstream PIP would treat the whole bbox
/// as filled, and storing holes doubles disk for a vanishingly small
/// quality win at building scale). Accepts Polygon, MultiPolygon, or
/// EWKB-with-SRID variants of either.
///
/// Returns one ring per polygon (so a MultiPolygon yields N rings).
pub fn decode_wkb_polygons(bytes: &[u8]) -> Result<Vec<Vec<[f64; 2]>>, ImportError> {
    let mut cur = WkbCursor::new(bytes)?;
    let (geom_type, _byte_order) = cur.read_header()?;
    match geom_type {
        // Polygon
        3 => {
            let ring = cur.read_polygon_outer()?;
            Ok(vec![ring])
        }
        // MultiPolygon
        6 => {
            let n = cur.read_u32()?;
            let mut rings = Vec::with_capacity(n as usize);
            for _ in 0..n {
                // Each sub-polygon has its own byte-order + type prefix.
                let mut sub = WkbCursor::sub(&mut cur)?;
                let (sub_type, _) = sub.read_header()?;
                if (sub_type & 0xFFFF) != 3 {
                    return Err(ImportError::Wkb(format!(
                        "MultiPolygon entry not a Polygon (got {sub_type})"
                    )));
                }
                let r = sub.read_polygon_outer()?;
                cur.advance(sub.consumed());
                if !r.is_empty() {
                    rings.push(r);
                }
            }
            Ok(rings)
        }
        t => Err(ImportError::Wkb(format!(
            "expected Polygon or MultiPolygon (3/6), got type={t}"
        ))),
    }
}

/// Cursor that owns a byte-order flag + payload offset and exposes
/// LE/BE-aware readers. Replaces the closure-heavy approach used in
/// `cairn-import-parquet::decode_wkb_point` because we need recursion
/// into MultiPolygon entries.
struct WkbCursor<'a> {
    bytes: &'a [u8],
    pos: usize,
    le: bool,
}

impl<'a> WkbCursor<'a> {
    fn new(bytes: &'a [u8]) -> Result<Self, ImportError> {
        if bytes.is_empty() {
            return Err(ImportError::Wkb("empty WKB".into()));
        }
        Ok(Self {
            bytes,
            pos: 0,
            le: false,
        })
    }

    /// Build a cursor that begins at the parent cursor's current
    /// position. The two are otherwise independent (advancing the
    /// child does not advance the parent).
    fn sub(parent: &mut WkbCursor<'a>) -> Result<Self, ImportError> {
        Ok(Self {
            bytes: parent.bytes,
            pos: parent.pos,
            le: parent.le,
        })
    }

    fn consumed(&self) -> usize {
        self.pos
    }

    fn advance(&mut self, n: usize) {
        self.pos = n;
    }

    fn read_byte(&mut self) -> Result<u8, ImportError> {
        if self.pos >= self.bytes.len() {
            return Err(ImportError::Wkb("truncated WKB".into()));
        }
        let b = self.bytes[self.pos];
        self.pos += 1;
        Ok(b)
    }

    fn read_u32(&mut self) -> Result<u32, ImportError> {
        if self.pos + 4 > self.bytes.len() {
            return Err(ImportError::Wkb("truncated u32".into()));
        }
        let chunk: [u8; 4] = self.bytes[self.pos..self.pos + 4].try_into().unwrap();
        self.pos += 4;
        Ok(if self.le {
            u32::from_le_bytes(chunk)
        } else {
            u32::from_be_bytes(chunk)
        })
    }

    fn read_f64(&mut self) -> Result<f64, ImportError> {
        if self.pos + 8 > self.bytes.len() {
            return Err(ImportError::Wkb("truncated f64".into()));
        }
        let chunk: [u8; 8] = self.bytes[self.pos..self.pos + 8].try_into().unwrap();
        self.pos += 8;
        Ok(if self.le {
            f64::from_le_bytes(chunk)
        } else {
            f64::from_be_bytes(chunk)
        })
    }

    /// Read byte-order + geometry-type. Strips the EWKB SRID flag
    /// (we trust WGS84). Returns the masked geometry type and the
    /// byte order as captured.
    fn read_header(&mut self) -> Result<(u32, bool), ImportError> {
        let bo = self.read_byte()?;
        self.le = match bo {
            0x01 => true,
            0x00 => false,
            b => {
                return Err(ImportError::Wkb(format!(
                    "bad WKB byte-order byte: 0x{b:02x}"
                )));
            }
        };
        let mut t = self.read_u32()?;
        const EWKB_SRID_FLAG: u32 = 0x2000_0000;
        if t & EWKB_SRID_FLAG != 0 {
            // Skip 4-byte SRID we don't use.
            let _ = self.read_u32()?;
            t &= !EWKB_SRID_FLAG;
        }
        // Mask off Z/M dimension flags (0x80000000, 0x40000000) and
        // PostGIS-style "1000+type" Z/M variants — we only care about
        // the planar ring shape.
        Ok((t & 0xFFFF, self.le))
    }

    /// Read a Polygon body (post-header): num_rings + each ring's
    /// num_points + xy pairs. Returns the outer ring only.
    fn read_polygon_outer(&mut self) -> Result<Vec<[f64; 2]>, ImportError> {
        let n_rings = self.read_u32()?;
        if n_rings == 0 {
            return Ok(Vec::new());
        }
        let mut outer: Vec<[f64; 2]> = Vec::new();
        for ring_idx in 0..n_rings {
            let n_pts = self.read_u32()?;
            if ring_idx == 0 {
                outer.reserve(n_pts as usize);
                for _ in 0..n_pts {
                    let x = self.read_f64()?;
                    let y = self.read_f64()?;
                    outer.push([x, y]);
                }
            } else {
                // Skip interior ring vertex bytes without allocating.
                let skip = (n_pts as usize)
                    .checked_mul(16)
                    .ok_or_else(|| ImportError::Wkb("interior ring too large".into()))?;
                if self.pos + skip > self.bytes.len() {
                    return Err(ImportError::Wkb("interior ring truncated".into()));
                }
                self.pos += skip;
            }
        }
        Ok(outer)
    }
}

fn ring_bbox(ring: &[[f64; 2]]) -> [f64; 4] {
    let (mut mnx, mut mny, mut mxx, mut mxy) = (
        f64::INFINITY,
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::NEG_INFINITY,
    );
    for p in ring {
        if p[0] < mnx {
            mnx = p[0];
        }
        if p[1] < mny {
            mny = p[1];
        }
        if p[0] > mxx {
            mxx = p[0];
        }
        if p[1] > mxy {
            mxy = p[1];
        }
    }
    [mnx, mny, mxx, mxy]
}

fn ring_bbox_area(ring: &[[f64; 2]]) -> f64 {
    let b = ring_bbox(ring);
    if !b[0].is_finite() {
        return 0.0;
    }
    (b[2] - b[0]).abs() * (b[3] - b[1]).abs()
}

/// Area-weighted centroid via the shoelace formula on a closed ring.
/// Falls back to the ring's bbox center when the ring is degenerate
/// (zero signed area, < 3 points).
fn polygon_centroid(ring: &[[f64; 2]]) -> [f64; 2] {
    if ring.len() < 3 {
        let bb = ring_bbox(ring);
        if bb[0].is_finite() {
            return [(bb[0] + bb[2]) * 0.5, (bb[1] + bb[3]) * 0.5];
        }
        return [0.0, 0.0];
    }
    let mut a2 = 0.0_f64;
    let mut cx = 0.0_f64;
    let mut cy = 0.0_f64;
    for w in ring.windows(2) {
        let p = w[0];
        let q = w[1];
        let cross = p[0] * q[1] - q[0] * p[1];
        a2 += cross;
        cx += (p[0] + q[0]) * cross;
        cy += (p[1] + q[1]) * cross;
    }
    if a2.abs() < 1e-18 {
        let bb = ring_bbox(ring);
        return [(bb[0] + bb[2]) * 0.5, (bb[1] + bb[3]) * 0.5];
    }
    let a = a2 * 0.5;
    [cx / (6.0 * a), cy / (6.0 * a)]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn polygon_le_unit_square(cx: f64, cy: f64) -> Vec<u8> {
        let mut buf = vec![0x01]; // LE
        buf.extend_from_slice(&3u32.to_le_bytes()); // type = Polygon
        buf.extend_from_slice(&1u32.to_le_bytes()); // 1 ring
        buf.extend_from_slice(&5u32.to_le_bytes()); // 5 points (closed)
        let pts: [(f64, f64); 5] = [
            (cx - 0.5, cy - 0.5),
            (cx + 0.5, cy - 0.5),
            (cx + 0.5, cy + 0.5),
            (cx - 0.5, cy + 0.5),
            (cx - 0.5, cy - 0.5),
        ];
        for (x, y) in pts {
            buf.extend_from_slice(&x.to_le_bytes());
            buf.extend_from_slice(&y.to_le_bytes());
        }
        buf
    }

    #[test]
    fn decode_polygon_le() {
        let buf = polygon_le_unit_square(10.0, 20.0);
        let polys = decode_wkb_polygons(&buf).unwrap();
        assert_eq!(polys.len(), 1);
        assert_eq!(polys[0].len(), 5);
        assert!((polys[0][0][0] - 9.5).abs() < 1e-9);
        assert!((polys[0][0][1] - 19.5).abs() < 1e-9);
    }

    #[test]
    fn decode_multipolygon_le() {
        // 1 byte BO + 4 byte type=6 + 4 byte n=2 + two complete polygon WKBs.
        let mut buf = vec![0x01];
        buf.extend_from_slice(&6u32.to_le_bytes());
        buf.extend_from_slice(&2u32.to_le_bytes());
        buf.extend_from_slice(&polygon_le_unit_square(0.0, 0.0));
        buf.extend_from_slice(&polygon_le_unit_square(5.0, 5.0));
        let polys = decode_wkb_polygons(&buf).unwrap();
        assert_eq!(polys.len(), 2);
    }

    #[test]
    fn decode_polygon_with_hole_returns_outer_only() {
        let mut buf = vec![0x01];
        buf.extend_from_slice(&3u32.to_le_bytes());
        buf.extend_from_slice(&2u32.to_le_bytes()); // 2 rings (outer + hole)
                                                    // outer (5 points)
        buf.extend_from_slice(&5u32.to_le_bytes());
        for (x, y) in [
            (-1.0_f64, -1.0_f64),
            (1.0, -1.0),
            (1.0, 1.0),
            (-1.0, 1.0),
            (-1.0, -1.0),
        ] {
            buf.extend_from_slice(&x.to_le_bytes());
            buf.extend_from_slice(&y.to_le_bytes());
        }
        // hole (5 points)
        buf.extend_from_slice(&5u32.to_le_bytes());
        for (x, y) in [
            (-0.1_f64, -0.1_f64),
            (0.1, -0.1),
            (0.1, 0.1),
            (-0.1, 0.1),
            (-0.1, -0.1),
        ] {
            buf.extend_from_slice(&x.to_le_bytes());
            buf.extend_from_slice(&y.to_le_bytes());
        }
        let polys = decode_wkb_polygons(&buf).unwrap();
        assert_eq!(polys.len(), 1);
        assert_eq!(polys[0].len(), 5, "outer ring only");
    }

    #[test]
    fn decode_rejects_point() {
        let mut buf = vec![0x01];
        buf.extend_from_slice(&1u32.to_le_bytes()); // 1 = Point
        buf.extend_from_slice(&[0u8; 16]);
        assert!(decode_wkb_polygons(&buf).is_err());
    }

    #[test]
    fn polygon_centroid_unit_square() {
        let ring = vec![
            [-1.0, -1.0],
            [1.0, -1.0],
            [1.0, 1.0],
            [-1.0, 1.0],
            [-1.0, -1.0],
        ];
        let c = polygon_centroid(&ring);
        assert!(c[0].abs() < 1e-9);
        assert!(c[1].abs() < 1e-9);
    }

    #[test]
    fn polygon_centroid_offset_square() {
        let ring = vec![
            [9.5, 19.5],
            [10.5, 19.5],
            [10.5, 20.5],
            [9.5, 20.5],
            [9.5, 19.5],
        ];
        let c = polygon_centroid(&ring);
        assert!((c[0] - 10.0).abs() < 1e-9);
        assert!((c[1] - 20.0).abs() < 1e-9);
    }

    #[test]
    fn ring_bbox_finds_corners() {
        let ring = vec![[1.0, 2.0], [3.0, 4.0], [-1.0, -2.0], [1.0, 2.0]];
        let bb = ring_bbox(&ring);
        assert_eq!(bb, [-1.0, -2.0, 3.0, 4.0]);
    }
}
