//! WhosOnFirst SQLite bundle → admin `Place` stream.
//!
//! Phase 2 scope:
//! - Read `spr` (current rows) + `names` (preferred multilingual rows).
//! - Walk `parent_id` chain to build each Place's `admin_path` (root → leaf).
//!
//! Polygon geometry stays on disk; Phase 3 reverse-geocoding wires it up
//! through the `geojson` table.

use cairn_place::{synthesize_gid, Coord, LocalizedName, Place, PlaceId, PlaceKind, GID_TAG};
use cairn_spatial::{AdminFeature, AdminLayer};
use cairn_tile::{Level, TileCoord};
use geo_types::{LineString, MultiPolygon, Polygon};
use geojson::{GeoJson, Geometry, Value as GjValue};
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use std::collections::HashMap;
use std::path::Path;
use thiserror::Error;
use tracing::{debug, info, warn};

const MAX_ADMIN_DEPTH: usize = 12;

#[derive(Debug, Error)]
pub enum ImportError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("placeid: {0}")]
    PlaceId(#[from] cairn_place::PlaceIdError),
    #[error("geojson: {0}")]
    GeoJson(#[from] Box<geojson::Error>),
    #[error("json: {0}")]
    Json(#[from] Box<serde_json::Error>),
}

impl From<geojson::Error> for ImportError {
    fn from(e: geojson::Error) -> Self {
        ImportError::GeoJson(Box::new(e))
    }
}

impl From<serde_json::Error> for ImportError {
    fn from(e: serde_json::Error) -> Self {
        ImportError::Json(Box::new(e))
    }
}

/// Result of a full WoF import: places + matching admin polygons.
pub struct WofImport {
    pub places: Vec<Place>,
    pub admin_layer: AdminLayer,
}

#[derive(Default)]
struct Counters {
    rows_seen: u64,
    rows_emitted: u64,
    skipped_unknown_placetype: u64,
    skipped_no_centroid: u64,
    skipped_no_names: u64,
}

struct RawRow {
    wof_id: i64,
    parent_wof_id: Option<i64>,
    canonical_name: Option<String>,
    placetype: String,
    country: Option<String>,
    centroid: Coord,
    kind: PlaceKind,
}

pub fn import(sqlite_path: &Path) -> Result<WofImport, ImportError> {
    info!(path = %sqlite_path.display(), "opening WhosOnFirst SQLite");
    let conn = Connection::open_with_flags(
        sqlite_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;

    let names_by_id = load_names(&conn)?;
    debug!(distinct_ids = names_by_id.len(), "loaded preferred names");

    let mut counters = Counters::default();
    let raw_rows = load_spr(&conn, &mut counters)?;

    // Allocate PlaceIds and build wof_id → PlaceId lookup before walking the
    // parent chain so admin_path entries point at the same PlaceIds we emit.
    let mut local_counters: HashMap<(u8, u32), u64> = HashMap::new();
    let mut place_id_by_wof: HashMap<i64, PlaceId> = HashMap::new();
    let mut assigned: Vec<(RawRow, PlaceId)> = Vec::with_capacity(raw_rows.len());
    for row in raw_rows {
        let level = level_for_kind(row.kind);
        let tile = TileCoord::from_coord(level, row.centroid);
        let key = (level.as_u8(), tile.id());
        let local = local_counters.entry(key).or_insert(0);
        let local_id = *local;
        *local += 1;
        let place_id = PlaceId::new(level.as_u8(), tile.id(), local_id)?;
        place_id_by_wof.insert(row.wof_id, place_id);
        assigned.push((row, place_id));
    }

    // Build a parent-chain lookup: wof_id → parent_wof_id, used in walking.
    let parent_lookup: HashMap<i64, Option<i64>> = assigned
        .iter()
        .map(|(r, _)| (r.wof_id, r.parent_wof_id))
        .collect();

    let mut places = Vec::with_capacity(assigned.len());
    for (row, place_id) in assigned {
        let mut names = build_localized_names(row.wof_id, &names_by_id, row.canonical_name);
        if names.is_empty() {
            counters.skipped_no_names += 1;
            continue;
        }
        // Stable order: default first, then alphabetical lang.
        names.sort_by(|a, b| match (a.lang.as_str(), b.lang.as_str()) {
            ("default", _) => std::cmp::Ordering::Less,
            (_, "default") => std::cmp::Ordering::Greater,
            (x, y) => x.cmp(y),
        });

        let admin_path = walk_admin_path(row.wof_id, &parent_lookup, &place_id_by_wof);

        let mut tags = vec![
            ("source".into(), "wof".into()),
            ("wof_id".into(), row.wof_id.to_string()),
        ];
        if let Some(c) = row.country.as_deref() {
            if !c.is_empty() {
                tags.push(("ISO3166-1".into(), c.to_string()));
            }
        }
        tags.push(("placetype".into(), row.placetype.clone()));
        // Pelias-compatible global identifier. WoF placetype is the
        // upstream "kind" and `wof_id` is the upstream stable id, so
        // the gid is identical to what Pelias emits for the same row.
        if let Some(gid) = synthesize_gid("wof", &row.placetype, &row.wof_id.to_string()) {
            tags.push((GID_TAG.into(), gid));
        }

        places.push(Place {
            id: place_id,
            kind: row.kind,
            names,
            centroid: row.centroid,
            admin_path,
            tags,
        });
        counters.rows_emitted += 1;
    }

    info!(
        rows_seen = counters.rows_seen,
        emitted = counters.rows_emitted,
        skipped_unknown_placetype = counters.skipped_unknown_placetype,
        skipped_no_centroid = counters.skipped_no_centroid,
        skipped_no_names = counters.skipped_no_names,
        "WoF SPR import done"
    );

    let admin_layer = build_admin_layer(&conn, &places)?;
    info!(
        features = admin_layer.features.len(),
        "WoF polygon import done"
    );

    Ok(WofImport {
        places,
        admin_layer,
    })
}

fn build_admin_layer(conn: &Connection, places: &[Place]) -> Result<AdminLayer, ImportError> {
    let mut wof_id_by_place: HashMap<PlaceId, i64> = HashMap::new();
    for place in places {
        if let Some((_, wof_id_str)) = place.tags.iter().find(|(k, _)| k == "wof_id") {
            if let Ok(wof_id) = wof_id_str.parse::<i64>() {
                wof_id_by_place.insert(place.id, wof_id);
            }
        }
    }

    let mut stmt = conn.prepare("SELECT body FROM geojson WHERE id = ?1")?;
    let mut features: Vec<AdminFeature> = Vec::new();
    let mut skipped_no_geojson = 0u64;
    let mut skipped_unparseable = 0u64;

    for place in places {
        let wof_id = match wof_id_by_place.get(&place.id) {
            Some(id) => *id,
            None => continue,
        };
        let body: Option<String> = stmt
            .query_row([wof_id], |row| row.get::<_, String>(0))
            .optional()?;
        let body = match body {
            Some(b) => b,
            None => {
                skipped_no_geojson += 1;
                continue;
            }
        };

        let polygon = match parse_geojson_geometry(&body) {
            Ok(Some(p)) => p,
            Ok(None) => {
                skipped_unparseable += 1;
                continue;
            }
            Err(err) => {
                warn!(?err, wof_id, "failed to parse WoF geojson body");
                skipped_unparseable += 1;
                continue;
            }
        };

        let default_name = place
            .names
            .iter()
            .find(|n| n.lang == "default")
            .or_else(|| place.names.first())
            .map(|n| n.value.clone())
            .unwrap_or_default();
        let kind_str = match place.kind {
            PlaceKind::Country => "country",
            PlaceKind::Region => "region",
            PlaceKind::County => "county",
            PlaceKind::City => "city",
            PlaceKind::District => "district",
            PlaceKind::Neighborhood => "neighborhood",
            PlaceKind::Street => "street",
            PlaceKind::Address => "address",
            PlaceKind::Poi => "poi",
            PlaceKind::Postcode => "postcode",
        };

        features.push(AdminFeature {
            place_id: place.id.0,
            level: place.id.level(),
            kind: kind_str.into(),
            name: default_name,
            centroid: place.centroid,
            admin_path: place.admin_path.iter().map(|p| p.0).collect(),
            polygon,
        });
    }

    if skipped_no_geojson > 0 || skipped_unparseable > 0 {
        debug!(skipped_no_geojson, skipped_unparseable, "WoF polygon skips");
    }
    Ok(AdminLayer { features })
}

fn parse_geojson_geometry(body: &str) -> Result<Option<MultiPolygon<f64>>, ImportError> {
    let gj: GeoJson = body.parse::<GeoJson>()?;
    let geometry = match gj {
        GeoJson::Feature(f) => f.geometry,
        GeoJson::Geometry(g) => Some(g),
        GeoJson::FeatureCollection(_) => None,
    };
    let g = match geometry {
        Some(g) => g,
        None => return Ok(None),
    };
    Ok(geometry_to_multipolygon(&g))
}

fn geometry_to_multipolygon(g: &Geometry) -> Option<MultiPolygon<f64>> {
    match &g.value {
        GjValue::Polygon(rings) => Some(MultiPolygon(vec![rings_to_polygon(rings)])),
        GjValue::MultiPolygon(polys) => {
            let polygons: Vec<Polygon<f64>> =
                polys.iter().map(|rings| rings_to_polygon(rings)).collect();
            Some(MultiPolygon(polygons))
        }
        _ => None,
    }
}

fn rings_to_polygon(rings: &[Vec<Vec<f64>>]) -> Polygon<f64> {
    let mut iter = rings.iter();
    let exterior = iter
        .next()
        .map(|r| coords_to_linestring(r))
        .unwrap_or_else(|| LineString(vec![]));
    let interiors: Vec<LineString<f64>> = iter.map(|r| coords_to_linestring(r)).collect();
    Polygon::new(exterior, interiors)
}

fn coords_to_linestring(ring: &[Vec<f64>]) -> LineString<f64> {
    LineString::from(
        ring.iter()
            .filter_map(|p| {
                if p.len() >= 2 {
                    Some((p[0], p[1]))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>(),
    )
}

fn load_spr(conn: &Connection, counters: &mut Counters) -> Result<Vec<RawRow>, ImportError> {
    let mut stmt = conn.prepare(
        "SELECT id, parent_id, name, placetype, country, latitude, longitude \
         FROM spr \
         WHERE is_current != 0 AND is_deprecated = 0 AND is_ceased = 0",
    )?;
    let mut rows = stmt.query([])?;
    let mut out = Vec::new();
    while let Some(row) = rows.next()? {
        counters.rows_seen += 1;
        let wof_id: i64 = row.get(0)?;
        let parent_wof_id: Option<i64> = row.get(1).optional()?.flatten();
        let canonical_name: Option<String> = row.get(2).optional()?.flatten();
        let placetype: String = row.get(3)?;
        let country: Option<String> = row.get(4).optional()?.flatten();
        let lat: Option<f64> = row.get(5).optional()?.flatten();
        let lon: Option<f64> = row.get(6).optional()?.flatten();

        let kind = match map_placetype(&placetype) {
            Some(k) => k,
            None => {
                counters.skipped_unknown_placetype += 1;
                continue;
            }
        };
        let (lon, lat) = match (lon, lat) {
            (Some(lon), Some(lat)) if !(lon == 0.0 && lat == 0.0) => (lon, lat),
            _ => {
                counters.skipped_no_centroid += 1;
                continue;
            }
        };

        out.push(RawRow {
            wof_id,
            parent_wof_id: parent_wof_id.filter(|p| *p > 0),
            canonical_name,
            placetype,
            country,
            centroid: Coord { lon, lat },
            kind,
        });
    }
    Ok(out)
}

fn walk_admin_path(
    start_wof_id: i64,
    parent_lookup: &HashMap<i64, Option<i64>>,
    place_id_by_wof: &HashMap<i64, PlaceId>,
) -> Vec<PlaceId> {
    let mut chain: Vec<PlaceId> = Vec::new();
    let mut seen: std::collections::HashSet<i64> = std::collections::HashSet::new();
    seen.insert(start_wof_id);
    let mut cursor = parent_lookup.get(&start_wof_id).copied().flatten();
    while let Some(parent_wof_id) = cursor {
        if !seen.insert(parent_wof_id) {
            break; // cycle guard
        }
        if chain.len() >= MAX_ADMIN_DEPTH {
            break;
        }
        if let Some(pid) = place_id_by_wof.get(&parent_wof_id) {
            chain.push(*pid);
        }
        cursor = parent_lookup.get(&parent_wof_id).copied().flatten();
    }
    // Walked leaf → root; reverse so admin_path runs root → leaf.
    chain.reverse();
    chain
}

fn load_names(conn: &Connection) -> Result<HashMap<i64, Vec<(String, String)>>, ImportError> {
    let mut stmt = conn.prepare(
        "SELECT id, language, name FROM names \
         WHERE privateuse = 'preferred' AND language IS NOT NULL AND name IS NOT NULL",
    )?;
    let mut map: HashMap<i64, Vec<(String, String)>> = HashMap::new();
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let id: i64 = row.get(0)?;
        let lang: String = row.get(1)?;
        let name: String = row.get(2)?;
        if lang.is_empty() || name.is_empty() {
            continue;
        }
        map.entry(id).or_default().push((lang, name));
    }
    Ok(map)
}

fn build_localized_names(
    id: i64,
    names_by_id: &HashMap<i64, Vec<(String, String)>>,
    canonical: Option<String>,
) -> Vec<LocalizedName> {
    let mut out: Vec<LocalizedName> = Vec::new();
    if let Some(name) = canonical {
        if !name.is_empty() {
            out.push(LocalizedName {
                lang: "default".into(),
                value: name,
            });
        }
    }
    if let Some(rows) = names_by_id.get(&id) {
        for (lang, value) in rows {
            out.push(LocalizedName {
                lang: lang.clone(),
                value: value.clone(),
            });
        }
    }
    out
}

fn map_placetype(pt: &str) -> Option<PlaceKind> {
    Some(match pt {
        "country" | "dependency" => PlaceKind::Country,
        "macroregion" | "region" => PlaceKind::Region,
        "macrocounty" | "county" | "localadmin" => PlaceKind::County,
        "borough" | "locality" | "metro_area" => PlaceKind::City,
        "neighbourhood" | "microhood" | "campus" => PlaceKind::Neighborhood,
        "postalcode" => PlaceKind::Postcode,
        _ => return None,
    })
}

fn level_for_kind(kind: PlaceKind) -> Level {
    match kind {
        PlaceKind::Country | PlaceKind::Region => Level::L0,
        PlaceKind::County | PlaceKind::City | PlaceKind::Postcode => Level::L1,
        PlaceKind::District
        | PlaceKind::Neighborhood
        | PlaceKind::Street
        | PlaceKind::Address
        | PlaceKind::Poi => Level::L2,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placetype_classifications() {
        assert_eq!(map_placetype("country"), Some(PlaceKind::Country));
        assert_eq!(map_placetype("region"), Some(PlaceKind::Region));
        assert_eq!(map_placetype("locality"), Some(PlaceKind::City));
        assert_eq!(map_placetype("localadmin"), Some(PlaceKind::County));
        assert_eq!(
            map_placetype("neighbourhood"),
            Some(PlaceKind::Neighborhood)
        );
        assert!(map_placetype("planet").is_none());
    }

    #[test]
    fn level_assignment() {
        assert_eq!(level_for_kind(PlaceKind::Country), Level::L0);
        assert_eq!(level_for_kind(PlaceKind::City), Level::L1);
        assert_eq!(level_for_kind(PlaceKind::Neighborhood), Level::L2);
    }

    #[test]
    fn walk_admin_path_terminates_on_cycle() {
        let mut parents: HashMap<i64, Option<i64>> = HashMap::new();
        parents.insert(1, Some(2));
        parents.insert(2, Some(3));
        parents.insert(3, Some(1)); // cycle 3 → 1
        let mut place_ids: HashMap<i64, PlaceId> = HashMap::new();
        place_ids.insert(2, PlaceId::new(0, 1, 1).unwrap());
        place_ids.insert(3, PlaceId::new(0, 1, 2).unwrap());
        let chain = walk_admin_path(1, &parents, &place_ids);
        assert!(chain.len() <= 2, "cycle must not loop forever");
    }

    #[test]
    fn walk_admin_path_root_first() {
        // 4 → 3 → 2 → 1 (root)
        let mut parents: HashMap<i64, Option<i64>> = HashMap::new();
        parents.insert(4, Some(3));
        parents.insert(3, Some(2));
        parents.insert(2, Some(1));
        parents.insert(1, None);
        let mut place_ids: HashMap<i64, PlaceId> = HashMap::new();
        place_ids.insert(3, PlaceId::new(0, 1, 30).unwrap());
        place_ids.insert(2, PlaceId::new(0, 1, 20).unwrap());
        place_ids.insert(1, PlaceId::new(0, 1, 10).unwrap());
        let chain = walk_admin_path(4, &parents, &place_ids);
        assert_eq!(chain.len(), 3);
        assert_eq!(chain[0], place_ids[&1]);
        assert_eq!(chain[1], place_ids[&2]);
        assert_eq!(chain[2], place_ids[&3]);
    }
}
