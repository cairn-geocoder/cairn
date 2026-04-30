//! Generic CSV + GeoJSON importer for operator-supplied Place data.
//!
//! Phase 7b lane M. Loads arbitrary tabular or feature-collection
//! files into `Place` records the rest of the build pipeline can
//! consume. No upstream provenance constraint — operators feed
//! internal facility lists, custom POI dumps, branch directories,
//! refugee camp maps, whatever is locally relevant.
//!
//! ## CSV shape
//!
//! Headers are lower-cased before lookup. The minimum recognized
//! schema:
//!
//! - `lon` / `longitude` / `lng` / `x`
//! - `lat` / `latitude` / `y`
//! - `name`
//!
//! Optional columns the importer understands:
//!
//! - `kind` — one of `city`, `neighborhood`, `country`, `region`,
//!   `street`, `address`, `poi`, `postcode`. Anything else falls
//!   through to `kind = poi`.
//! - `population` — integer count; populates the population boost
//!   downstream.
//! - `lang` — language tag for the name (default `default`).
//! - any other column is captured as a `(key, value)` tag pair so
//!   downstream tooling (categories filter, SBOM) can reflect the
//!   operator's data shape.
//!
//! ## GeoJSON shape
//!
//! Standard `FeatureCollection`. Each `Feature` produces a Place:
//!
//! - `geometry.type = Point` → centroid is the point coords directly.
//! - `geometry.type = Polygon` / `MultiPolygon` → centroid is the
//!   first ring's first vertex (operator should pre-compute proper
//!   centroids when accuracy matters; this is a fallback).
//! - `properties.name` is the primary name. `properties.name:lang`
//!   adds localized variants (`name:de`, `name:fr-CH`, …).
//! - All other properties become tags.

use cairn_place::{
    stable_hash_gid, synthesize_gid, Coord, LocalizedName, Place, PlaceId, PlaceKind, GID_TAG,
};
use cairn_tile::{Level, TileCoord};
use std::collections::HashMap;
use std::path::Path;
use thiserror::Error;
use tracing::{info, warn};

#[derive(Debug, Error)]
pub enum ImportError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("csv: {0}")]
    Csv(#[from] csv::Error),
    // Boxed because `geojson::Error` is ~200 B; clippy flags the
    // `Err` variant size otherwise. We never branch on the inner
    // type so opaque boxing costs nothing.
    #[error("geojson: {0}")]
    Geojson(#[from] Box<geojson::Error>),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("placeid: {0}")]
    PlaceId(#[from] cairn_place::PlaceIdError),
}

/// Counters for diagnostics. Surfaced to the caller for logging.
#[derive(Default, Clone, Debug)]
pub struct Counters {
    pub rows_seen: u64,
    pub emitted: u64,
    pub skipped_no_coords: u64,
    pub skipped_no_name: u64,
    pub skipped_unsupported_geometry: u64,
}

const LON_HEADERS: &[&str] = &["lon", "longitude", "lng", "x"];
const LAT_HEADERS: &[&str] = &["lat", "latitude", "y"];

/// Import a single CSV file. Yields one Place per row that has both
/// coordinates AND a non-empty name.
pub fn import_csv(path: &Path) -> Result<(Vec<Place>, Counters), ImportError> {
    info!(path = %path.display(), "opening generic CSV");
    let mut rdr = csv::ReaderBuilder::new()
        .flexible(true)
        .trim(csv::Trim::All)
        .from_path(path)?;

    let headers: Vec<String> = rdr
        .headers()?
        .iter()
        .map(|h| h.trim().to_lowercase())
        .collect();
    let lon_idx = pick_index(&headers, LON_HEADERS);
    let lat_idx = pick_index(&headers, LAT_HEADERS);
    let name_idx = pick_index(&headers, &["name"]);
    let kind_idx = pick_index(&headers, &["kind"]);
    let pop_idx = pick_index(&headers, &["population", "pop"]);
    let lang_idx = pick_index(&headers, &["lang", "language"]);

    let mut counters = Counters::default();
    let mut places = Vec::new();
    let mut local_counters: HashMap<(u8, u32), u64> = HashMap::new();

    if lon_idx.is_none() || lat_idx.is_none() {
        warn!(
            path = %path.display(),
            headers = ?headers,
            "CSV missing lon/lat columns; nothing to import"
        );
        return Ok((places, counters));
    }
    let lon_idx = lon_idx.unwrap();
    let lat_idx = lat_idx.unwrap();

    for record in rdr.records() {
        let record = record?;
        counters.rows_seen += 1;
        let (lon, lat) = match (
            record.get(lon_idx).and_then(parse_f64),
            record.get(lat_idx).and_then(parse_f64),
        ) {
            (Some(a), Some(b)) => (a, b),
            _ => {
                counters.skipped_no_coords += 1;
                continue;
            }
        };
        let name = name_idx
            .and_then(|i| record.get(i))
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        let Some(name) = name else {
            counters.skipped_no_name += 1;
            continue;
        };

        let kind = kind_idx
            .and_then(|i| record.get(i))
            .map(|s| s.trim().to_lowercase())
            .map(|s| parse_kind(&s))
            .unwrap_or(PlaceKind::Poi);
        let lang = lang_idx
            .and_then(|i| record.get(i))
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "default".into());

        let mut tags: Vec<(String, String)> = Vec::new();
        for (i, header) in headers.iter().enumerate() {
            if i == lon_idx
                || i == lat_idx
                || Some(i) == name_idx
                || Some(i) == kind_idx
                || Some(i) == lang_idx
            {
                continue;
            }
            if let Some(v) = record.get(i) {
                let v = v.trim();
                if !v.is_empty() {
                    tags.push((header.clone(), v.to_string()));
                }
            }
        }
        if let Some(i) = pop_idx {
            // pop_idx already captured above, but keep as a tag too so
            // text-side logic that pulls "population" from tags sees
            // it. Skip in the for-loop above since we want a stable
            // location for it.
            if let Some(v) = record.get(i) {
                let v = v.trim();
                if !v.is_empty() && !tags.iter().any(|(k, _)| k == "population") {
                    tags.push(("population".into(), v.to_string()));
                }
            }
        }

        let level = level_for_kind(kind);
        let tile = TileCoord::from_coord(level, Coord { lon, lat });
        let key = (level.as_u8(), tile.id());
        let local = local_counters.entry(key).or_insert(0);
        let local_id = *local;
        *local += 1;
        let id = PlaceId::new(level.as_u8(), tile.id(), local_id)?;

        let centroid = Coord { lon, lat };
        let gid = generic_gid(&tags, kind, &name, centroid);
        tags.push((GID_TAG.into(), gid));
        places.push(Place {
            id,
            kind,
            names: vec![LocalizedName { lang, value: name }],
            centroid,
            admin_path: vec![],
            tags,
        });
        counters.emitted += 1;
    }
    info!(
        rows_seen = counters.rows_seen,
        emitted = counters.emitted,
        skipped_no_coords = counters.skipped_no_coords,
        skipped_no_name = counters.skipped_no_name,
        "generic CSV import done"
    );
    Ok((places, counters))
}

/// Import a single GeoJSON file. `FeatureCollection` is the expected
/// top-level; bare `Feature` and bare `Geometry` are tolerated for
/// convenience.
pub fn import_geojson(path: &Path) -> Result<(Vec<Place>, Counters), ImportError> {
    info!(path = %path.display(), "opening generic GeoJSON");
    let raw = std::fs::read_to_string(path)?;
    let geojson: geojson::GeoJson = raw.parse().map_err(|e| ImportError::Geojson(Box::new(e)))?;
    let features: Vec<geojson::Feature> = match geojson {
        geojson::GeoJson::FeatureCollection(fc) => fc.features,
        geojson::GeoJson::Feature(f) => vec![f],
        geojson::GeoJson::Geometry(g) => vec![geojson::Feature {
            bbox: None,
            geometry: Some(g),
            id: None,
            properties: None,
            foreign_members: None,
        }],
    };

    let mut counters = Counters::default();
    let mut places = Vec::new();
    let mut local_counters: HashMap<(u8, u32), u64> = HashMap::new();

    for feat in features {
        counters.rows_seen += 1;
        let coord = match feat.geometry.as_ref().and_then(centroid_of) {
            Some(c) => c,
            None => {
                counters.skipped_unsupported_geometry += 1;
                continue;
            }
        };
        let props = feat.properties.unwrap_or_default();
        let primary_name = props
            .get("name")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        let Some(primary) = primary_name else {
            counters.skipped_no_name += 1;
            continue;
        };

        // Localized names from `name:<lang>` properties.
        let mut names = vec![LocalizedName {
            lang: "default".into(),
            value: primary,
        }];
        for (k, v) in props.iter() {
            let Some(stripped) = k.strip_prefix("name:") else {
                continue;
            };
            if stripped.is_empty() {
                continue;
            }
            if let Some(value) = v.as_str() {
                let value = value.trim();
                if !value.is_empty() {
                    names.push(LocalizedName {
                        lang: stripped.to_string(),
                        value: value.to_string(),
                    });
                }
            }
        }

        let kind = props
            .get("kind")
            .or_else(|| props.get("class"))
            .and_then(|v| v.as_str())
            .map(parse_kind)
            .unwrap_or(PlaceKind::Poi);

        let mut tags: Vec<(String, String)> = Vec::new();
        for (k, v) in props.iter() {
            if k == "name" || k.starts_with("name:") || k == "kind" || k == "class" {
                continue;
            }
            if let Some(s) = v.as_str() {
                let s = s.trim();
                if !s.is_empty() {
                    tags.push((k.clone(), s.to_string()));
                }
            } else if v.is_number() {
                tags.push((k.clone(), v.to_string()));
            }
        }

        let level = level_for_kind(kind);
        let tile = TileCoord::from_coord(level, coord);
        let key = (level.as_u8(), tile.id());
        let local = local_counters.entry(key).or_insert(0);
        let local_id = *local;
        *local += 1;
        let id = PlaceId::new(level.as_u8(), tile.id(), local_id)?;

        let primary_name = names
            .iter()
            .find(|n| n.lang == "default")
            .or_else(|| names.first())
            .map(|n| n.value.clone())
            .unwrap_or_default();
        let gid = generic_gid(&tags, kind, &primary_name, coord);
        tags.push((GID_TAG.into(), gid));
        places.push(Place {
            id,
            kind,
            names,
            centroid: coord,
            admin_path: vec![],
            tags,
        });
        counters.emitted += 1;
    }
    info!(
        rows_seen = counters.rows_seen,
        emitted = counters.emitted,
        skipped_unsupported_geometry = counters.skipped_unsupported_geometry,
        skipped_no_name = counters.skipped_no_name,
        "generic GeoJSON import done"
    );
    Ok((places, counters))
}

/// Centroid extraction. Point geometries pass through; ring-based
/// geometries fall back to the first vertex of the first outer ring
/// (operators should pre-compute proper centroids when precision
/// matters).
/// Pelias-compatible gid for a generic CSV / GeoJSON row. Prefers
/// the operator-supplied `id` tag (when the source file ships a
/// stable identifier column); otherwise hashes (kind, name, ~100 m
/// centroid quantize) so bookmarks survive rebuilds when the row
/// itself is unchanged.
fn generic_gid(
    tags: &[(String, String)],
    kind: PlaceKind,
    name: &str,
    centroid: Coord,
) -> String {
    let upstream = tags
        .iter()
        .find(|(k, _)| k == "id")
        .map(|(_, v)| v.as_str());
    if let Some(id) = upstream {
        if let Some(gid) = synthesize_gid("generic", kind_slug(kind), id) {
            return gid;
        }
    }
    stable_hash_gid("generic", kind_slug(kind), name, centroid)
}

fn kind_slug(kind: PlaceKind) -> &'static str {
    match kind {
        PlaceKind::Country => "country",
        PlaceKind::Region => "region",
        PlaceKind::County => "county",
        PlaceKind::City => "locality",
        PlaceKind::District => "borough",
        PlaceKind::Neighborhood => "neighbourhood",
        PlaceKind::Street => "street",
        PlaceKind::Address => "address",
        PlaceKind::Postcode => "postalcode",
        PlaceKind::Poi => "venue",
    }
}

fn centroid_of(geometry: &geojson::Geometry) -> Option<Coord> {
    use geojson::Value;
    match &geometry.value {
        Value::Point(pt) if pt.len() >= 2 => Some(Coord {
            lon: pt[0],
            lat: pt[1],
        }),
        Value::Polygon(rings) => rings
            .first()
            .and_then(|r| r.first())
            .filter(|p| p.len() >= 2)
            .map(|p| Coord {
                lon: p[0],
                lat: p[1],
            }),
        Value::MultiPolygon(polys) => polys
            .first()
            .and_then(|rings| rings.first())
            .and_then(|r| r.first())
            .filter(|p| p.len() >= 2)
            .map(|p| Coord {
                lon: p[0],
                lat: p[1],
            }),
        Value::MultiPoint(pts) => pts.first().filter(|p| p.len() >= 2).map(|p| Coord {
            lon: p[0],
            lat: p[1],
        }),
        _ => None,
    }
}

fn pick_index(headers: &[String], aliases: &[&str]) -> Option<usize> {
    for alias in aliases {
        if let Some(i) = headers.iter().position(|h| h == alias) {
            return Some(i);
        }
    }
    None
}

fn parse_f64(s: &str) -> Option<f64> {
    let t = s.trim();
    if t.is_empty() {
        return None;
    }
    t.parse::<f64>().ok()
}

fn parse_kind(raw: &str) -> PlaceKind {
    match raw.trim().to_lowercase().as_str() {
        "country" => PlaceKind::Country,
        "region" | "state" | "province" => PlaceKind::Region,
        "county" => PlaceKind::County,
        "city" | "town" | "locality" | "village" => PlaceKind::City,
        "neighborhood" | "neighbourhood" | "borough" => PlaceKind::Neighborhood,
        "street" | "highway" | "road" => PlaceKind::Street,
        "address" | "house" => PlaceKind::Address,
        "postcode" | "postalcode" | "zip" | "zipcode" => PlaceKind::Postcode,
        _ => PlaceKind::Poi,
    }
}

fn level_for_kind(kind: PlaceKind) -> Level {
    match kind {
        PlaceKind::Country => Level::L0,
        PlaceKind::Region => Level::L0,
        PlaceKind::County => Level::L1,
        PlaceKind::City => Level::L1,
        _ => Level::L2,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::tempdir;

    fn write_file(dir: &Path, name: &str, body: &str) -> std::path::PathBuf {
        let p = dir.join(name);
        let mut f = std::fs::File::create(&p).unwrap();
        f.write_all(body.as_bytes()).unwrap();
        p
    }

    #[test]
    fn csv_minimum_schema_lon_lat_name() {
        let dir = tempdir().unwrap();
        let body = "lon,lat,name\n9.5,47.1,Vaduz\n13.4,52.5,Berlin\n";
        let p = write_file(dir.path(), "min.csv", body);
        let (places, c) = import_csv(&p).unwrap();
        assert_eq!(c.rows_seen, 2);
        assert_eq!(c.emitted, 2);
        assert_eq!(places.len(), 2);
        assert_eq!(places[0].names[0].value, "Vaduz");
        assert_eq!(places[0].centroid.lon, 9.5);
    }

    #[test]
    fn csv_alt_lon_lat_headers() {
        let dir = tempdir().unwrap();
        let body = "longitude,latitude,name\n2.3,48.8,Paris\n";
        let p = write_file(dir.path(), "alt.csv", body);
        let (places, _) = import_csv(&p).unwrap();
        assert_eq!(places.len(), 1);
        assert_eq!(places[0].centroid.lat, 48.8);
    }

    #[test]
    fn csv_skips_rows_missing_coords_or_name() {
        let dir = tempdir().unwrap();
        let body = "lon,lat,name\n,47.1,Vaduz\n9.5,,Vaduz\n9.5,47.1,\n9.5,47.1,Real\n";
        let p = write_file(dir.path(), "skip.csv", body);
        let (places, c) = import_csv(&p).unwrap();
        assert_eq!(places.len(), 1);
        assert_eq!(c.skipped_no_coords, 2);
        assert_eq!(c.skipped_no_name, 1);
        assert_eq!(c.emitted, 1);
    }

    #[test]
    fn csv_extra_columns_become_tags() {
        let dir = tempdir().unwrap();
        let body = "lon,lat,name,kind,population,country\n9.5,47.1,Vaduz,city,5400,LI\n";
        let p = write_file(dir.path(), "tags.csv", body);
        let (places, _) = import_csv(&p).unwrap();
        assert_eq!(places[0].kind, PlaceKind::City);
        let tags = &places[0].tags;
        assert!(tags.iter().any(|(k, v)| k == "population" && v == "5400"));
        assert!(tags.iter().any(|(k, v)| k == "country" && v == "LI"));
    }

    #[test]
    fn csv_missing_lon_returns_empty_with_warning() {
        let dir = tempdir().unwrap();
        let body = "id,name\n1,Foo\n";
        let p = write_file(dir.path(), "bad.csv", body);
        let (places, c) = import_csv(&p).unwrap();
        assert!(places.is_empty());
        assert_eq!(c.rows_seen, 0);
    }

    #[test]
    fn geojson_point_feature_emits_place() {
        let dir = tempdir().unwrap();
        let body = r#"{
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": { "type": "Point", "coordinates": [9.52, 47.14] },
                    "properties": { "name": "Vaduz", "kind": "city", "name:de": "Vaduz" }
                }
            ]
        }"#;
        let p = write_file(dir.path(), "point.geojson", body);
        let (places, c) = import_geojson(&p).unwrap();
        assert_eq!(c.emitted, 1);
        let p = &places[0];
        assert_eq!(p.kind, PlaceKind::City);
        assert_eq!(
            p.names.iter().find(|n| n.lang == "default").unwrap().value,
            "Vaduz"
        );
        assert!(p.names.iter().any(|n| n.lang == "de"));
    }

    #[test]
    fn geojson_polygon_uses_first_vertex_as_fallback() {
        let dir = tempdir().unwrap();
        let body = r#"{
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [[[9.5,47.1],[9.6,47.1],[9.6,47.2],[9.5,47.2],[9.5,47.1]]]
                    },
                    "properties": { "name": "Vaduz district" }
                }
            ]
        }"#;
        let p = write_file(dir.path(), "poly.geojson", body);
        let (places, _) = import_geojson(&p).unwrap();
        assert_eq!(places.len(), 1);
        assert_eq!(places[0].centroid.lon, 9.5);
        assert_eq!(places[0].centroid.lat, 47.1);
    }

    #[test]
    fn geojson_skips_unsupported_geometry() {
        let dir = tempdir().unwrap();
        let body = r#"{
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": { "type": "LineString", "coordinates": [[0,0],[1,1]] },
                    "properties": { "name": "Some street" }
                },
                {
                    "type": "Feature",
                    "geometry": { "type": "Point", "coordinates": [2,3] },
                    "properties": { "name": "OK" }
                }
            ]
        }"#;
        let p = write_file(dir.path(), "mixed.geojson", body);
        let (places, c) = import_geojson(&p).unwrap();
        assert_eq!(places.len(), 1);
        assert_eq!(c.skipped_unsupported_geometry, 1);
    }

    #[test]
    fn geojson_skips_features_without_name() {
        let dir = tempdir().unwrap();
        let body = r#"{
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": { "type": "Point", "coordinates": [1,2] },
                    "properties": { "id": "no-name" }
                }
            ]
        }"#;
        let p = write_file(dir.path(), "noname.geojson", body);
        let (places, c) = import_geojson(&p).unwrap();
        assert!(places.is_empty());
        assert_eq!(c.skipped_no_name, 1);
    }

    #[test]
    fn parse_kind_falls_back_to_poi() {
        assert_eq!(parse_kind("city"), PlaceKind::City);
        assert_eq!(parse_kind("CITY"), PlaceKind::City);
        assert_eq!(parse_kind("street"), PlaceKind::Street);
        assert_eq!(parse_kind("nonsense"), PlaceKind::Poi);
        assert_eq!(parse_kind(""), PlaceKind::Poi);
    }
}
