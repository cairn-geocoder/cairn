//! OpenStreetMap PBF → `Place` stream.
//!
//! Phase 4 scope:
//! - Nodes tagged `place=*` with a `name=*` → admin/city/neighborhood Places.
//! - Nodes tagged with POI keys (amenity, shop, tourism, office, leisure,
//!   historic) plus a name → POI Places at L2.
//! - Ways tagged `highway=<road class>` with a name → Street Places at L2,
//!   centroid = mean of cached node coordinates.
//!
//! Two-pass over the PBF: pass 1 caches every node's `(lon, lat)`, pass 2
//! emits Places (including ways resolved against the cache).

use cairn_place::{Coord, LocalizedName, Place, PlaceId, PlaceKind};
use cairn_tile::{Level, TileCoord};
use osmpbf::{DenseNode, Element, ElementReader, Node, Way};
use std::collections::HashMap;
use std::path::Path;
use thiserror::Error;
use tracing::{debug, info};

#[derive(Debug, Error)]
pub enum ImportError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("osmpbf: {0}")]
    Osm(#[from] osmpbf::Error),
    #[error("placeid: {0}")]
    PlaceId(#[from] cairn_place::PlaceIdError),
}

#[derive(Default)]
struct Counters {
    nodes_seen: u64,
    nodes_emitted: u64,
    ways_seen: u64,
    ways_emitted: u64,
    skipped_no_name: u64,
    skipped_unknown_kind: u64,
    skipped_way_no_coords: u64,
}

type NodeCoords = HashMap<i64, [f64; 2]>;

pub fn import(pbf_path: &Path) -> Result<Vec<Place>, ImportError> {
    info!(path = %pbf_path.display(), "opening OSM PBF (pass 1: node coords)");
    let node_coords = load_node_coords(pbf_path)?;
    info!(nodes_cached = node_coords.len(), "node coord cache built");

    info!("OSM PBF pass 2: emit Places");
    let reader = ElementReader::from_path(pbf_path)?;
    let mut places = Vec::new();
    let mut counters = Counters::default();
    let mut local_counters: HashMap<(u8, u32), u64> = HashMap::new();

    reader.for_each(|element| match element {
        Element::Node(n) => {
            counters.nodes_seen += 1;
            if let Some(p) = node_to_place(&n, &mut local_counters, &mut counters) {
                places.push(p);
            }
        }
        Element::DenseNode(n) => {
            counters.nodes_seen += 1;
            if let Some(p) = dense_node_to_place(&n, &mut local_counters, &mut counters) {
                places.push(p);
            }
        }
        Element::Way(w) => {
            counters.ways_seen += 1;
            if let Some(p) = way_to_place(&w, &node_coords, &mut local_counters, &mut counters) {
                places.push(p);
            }
        }
        Element::Relation(_) => {}
    })?;

    info!(
        nodes_seen = counters.nodes_seen,
        nodes_emitted = counters.nodes_emitted,
        ways_seen = counters.ways_seen,
        ways_emitted = counters.ways_emitted,
        skipped_no_name = counters.skipped_no_name,
        skipped_unknown_kind = counters.skipped_unknown_kind,
        skipped_way_no_coords = counters.skipped_way_no_coords,
        "OSM import done"
    );
    Ok(places)
}

fn load_node_coords(pbf_path: &Path) -> Result<NodeCoords, ImportError> {
    let reader = ElementReader::from_path(pbf_path)?;
    let mut out: NodeCoords = HashMap::new();
    reader.for_each(|element| match element {
        Element::Node(n) => {
            out.insert(n.id(), [n.lon(), n.lat()]);
        }
        Element::DenseNode(n) => {
            out.insert(n.id(), [n.lon(), n.lat()]);
        }
        _ => {}
    })?;
    Ok(out)
}

fn node_to_place(
    node: &Node<'_>,
    local_counters: &mut HashMap<(u8, u32), u64>,
    counters: &mut Counters,
) -> Option<Place> {
    let tags = collect_tags(node.tags());
    build_place_from_centroid(node.lon(), node.lat(), &tags, local_counters, counters)
}

fn dense_node_to_place(
    node: &DenseNode<'_>,
    local_counters: &mut HashMap<(u8, u32), u64>,
    counters: &mut Counters,
) -> Option<Place> {
    let tags = collect_tags(node.tags());
    build_place_from_centroid(node.lon(), node.lat(), &tags, local_counters, counters)
}

fn way_to_place(
    way: &Way<'_>,
    node_coords: &NodeCoords,
    local_counters: &mut HashMap<(u8, u32), u64>,
    counters: &mut Counters,
) -> Option<Place> {
    let tags = collect_tags(way.tags());
    if !is_named_highway(&tags) {
        return None;
    }
    let centroid = match way_centroid(way, node_coords) {
        Some(c) => c,
        None => {
            counters.skipped_way_no_coords += 1;
            return None;
        }
    };
    let names = collect_names(&tags);
    if names.is_empty() {
        counters.skipped_no_name += 1;
        return None;
    }

    let kind = PlaceKind::Street;
    let level = level_for_kind(kind);
    let tile = TileCoord::from_coord(level, centroid);
    let key = (level.as_u8(), tile.id());
    let local = local_counters.entry(key).or_insert(0);
    let local_id = *local;
    *local += 1;
    let id = match PlaceId::new(level.as_u8(), tile.id(), local_id) {
        Ok(id) => id,
        Err(err) => {
            debug!(?err, "PlaceId overflow on way; skipping");
            return None;
        }
    };

    counters.ways_emitted += 1;
    Some(Place {
        id,
        kind,
        names,
        centroid,
        admin_path: vec![],
        tags: filter_tags(&tags),
    })
}

fn way_centroid(way: &Way<'_>, node_coords: &NodeCoords) -> Option<Coord> {
    let mut sum_lon = 0.0f64;
    let mut sum_lat = 0.0f64;
    let mut n = 0u64;
    for ref_id in way.refs() {
        if let Some([lon, lat]) = node_coords.get(&ref_id) {
            sum_lon += *lon;
            sum_lat += *lat;
            n += 1;
        }
    }
    if n == 0 {
        return None;
    }
    Some(Coord {
        lon: sum_lon / n as f64,
        lat: sum_lat / n as f64,
    })
}

fn build_place_from_centroid(
    lon: f64,
    lat: f64,
    tags: &[(String, String)],
    local_counters: &mut HashMap<(u8, u32), u64>,
    counters: &mut Counters,
) -> Option<Place> {
    let kind = match place_kind(tags) {
        Some(k) => k,
        None => {
            counters.skipped_unknown_kind += 1;
            return None;
        }
    };
    let names = collect_names(tags);
    if names.is_empty() {
        counters.skipped_no_name += 1;
        return None;
    }

    let centroid = Coord { lon, lat };
    let level = level_for_kind(kind);
    let tile = TileCoord::from_coord(level, centroid);
    let key = (level.as_u8(), tile.id());
    let local = local_counters.entry(key).or_insert(0);
    let local_id = *local;
    *local += 1;
    let id = match PlaceId::new(level.as_u8(), tile.id(), local_id) {
        Ok(id) => id,
        Err(err) => {
            debug!(?err, "PlaceId overflow; skipping");
            return None;
        }
    };

    counters.nodes_emitted += 1;
    Some(Place {
        id,
        kind,
        names,
        centroid,
        admin_path: vec![],
        tags: filter_tags(tags),
    })
}

fn place_kind(tags: &[(String, String)]) -> Option<PlaceKind> {
    if let Some(val) = tag_value(tags, "place") {
        return Some(match val {
            "country" => PlaceKind::Country,
            "state" | "region" | "province" => PlaceKind::Region,
            "county" => PlaceKind::County,
            "city" | "town" | "village" | "hamlet" | "isolated_dwelling" => PlaceKind::City,
            "suburb" | "neighbourhood" | "quarter" | "borough" => PlaceKind::Neighborhood,
            "locality" => PlaceKind::City,
            _ => return None,
        });
    }
    if POI_KEYS.iter().any(|k| tag_value(tags, k).is_some()) {
        return Some(PlaceKind::Poi);
    }
    None
}

const POI_KEYS: &[&str] = &[
    "amenity",
    "shop",
    "tourism",
    "office",
    "leisure",
    "historic",
    "craft",
    "emergency",
    "healthcare",
];

const HIGHWAY_KEEP: &[&str] = &[
    "motorway",
    "trunk",
    "primary",
    "secondary",
    "tertiary",
    "unclassified",
    "residential",
    "living_street",
    "service",
    "pedestrian",
    "road",
    "track",
];

fn is_named_highway(tags: &[(String, String)]) -> bool {
    let Some(hwy) = tag_value(tags, "highway") else {
        return false;
    };
    HIGHWAY_KEEP.contains(&hwy)
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

fn collect_names(tags: &[(String, String)]) -> Vec<LocalizedName> {
    let mut names = Vec::new();
    for (k, v) in tags {
        if k == "name" {
            names.push(LocalizedName {
                lang: "default".into(),
                value: v.clone(),
            });
        } else if let Some(lang) = k.strip_prefix("name:") {
            if !lang.is_empty() && !lang.contains(':') {
                names.push(LocalizedName {
                    lang: lang.to_string(),
                    value: v.clone(),
                });
            }
        }
    }
    names
}

const KEPT_TAG_KEYS: &[&str] = &[
    "place",
    "highway",
    "amenity",
    "shop",
    "tourism",
    "office",
    "leisure",
    "historic",
    "craft",
    "emergency",
    "healthcare",
    "boundary",
    "admin_level",
    "ISO3166-1",
    "ISO3166-2",
    "wikidata",
    "population",
    "postal_code",
    "addr:postcode",
    "addr:city",
    "addr:country",
];

fn filter_tags(tags: &[(String, String)]) -> Vec<(String, String)> {
    tags.iter()
        .filter(|(k, _)| KEPT_TAG_KEYS.contains(&k.as_str()))
        .cloned()
        .collect()
}

fn tag_value<'a>(tags: &'a [(String, String)], key: &str) -> Option<&'a str> {
    tags.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str())
}

fn collect_tags<'a, I: IntoIterator<Item = (&'a str, &'a str)>>(iter: I) -> Vec<(String, String)> {
    iter.into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tags(items: &[(&str, &str)]) -> Vec<(String, String)> {
        items
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn place_kind_classifications() {
        assert_eq!(
            place_kind(&tags(&[("place", "city")])),
            Some(PlaceKind::City)
        );
        assert_eq!(
            place_kind(&tags(&[("place", "country")])),
            Some(PlaceKind::Country)
        );
        assert_eq!(
            place_kind(&tags(&[("place", "neighbourhood")])),
            Some(PlaceKind::Neighborhood)
        );
        assert_eq!(
            place_kind(&tags(&[("amenity", "cafe"), ("name", "Joe's")])),
            Some(PlaceKind::Poi)
        );
        assert_eq!(
            place_kind(&tags(&[("shop", "bakery")])),
            Some(PlaceKind::Poi)
        );
        assert!(place_kind(&tags(&[("highway", "residential")])).is_none(),);
    }

    #[test]
    fn highway_filter() {
        assert!(is_named_highway(&tags(&[("highway", "residential")])));
        assert!(is_named_highway(&tags(&[("highway", "primary")])));
        assert!(!is_named_highway(&tags(&[("highway", "footway")])));
        assert!(!is_named_highway(&tags(&[("amenity", "cafe")])));
    }

    #[test]
    fn collects_localized_names() {
        let t = tags(&[
            ("name", "Vaduz"),
            ("name:de", "Vaduz"),
            ("name:fr", "Vaduz"),
            ("name:zh-Hant", "瓦杜茲"),
            ("alt_name:de", "should be ignored"),
        ]);
        let names = collect_names(&t);
        assert_eq!(names.len(), 4);
        assert!(names.iter().any(|n| n.lang == "default"));
        assert!(names.iter().any(|n| n.lang == "de"));
        assert!(names.iter().any(|n| n.lang == "zh-Hant"));
    }

    #[test]
    fn filter_keeps_relevant_tags() {
        let t = tags(&[
            ("place", "city"),
            ("name", "Vaduz"),
            ("population", "5450"),
            ("ISO3166-1", "LI"),
            ("source", "TIGER"),
            ("amenity", "cafe"),
            ("highway", "primary"),
        ]);
        let kept = filter_tags(&t);
        let keys: Vec<&str> = kept.iter().map(|(k, _)| k.as_str()).collect();
        assert!(keys.contains(&"place"));
        assert!(keys.contains(&"population"));
        assert!(keys.contains(&"ISO3166-1"));
        assert!(keys.contains(&"amenity"));
        assert!(keys.contains(&"highway"));
        assert!(!keys.contains(&"source"));
        assert!(!keys.contains(&"name"));
    }
}
