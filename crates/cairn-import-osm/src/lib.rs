//! OpenStreetMap PBF → `Place` stream.
//!
//! Phase 1 scope: nodes tagged `place=*` with a `name=*`. Streets, ways,
//! POIs, and addresses land in later phases.

use cairn_place::{Coord, LocalizedName, Place, PlaceId, PlaceKind};
use cairn_tile::{Level, TileCoord};
use osmpbf::{DenseNode, Element, ElementReader, Node};
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
    skipped_no_name: u64,
    skipped_unknown_kind: u64,
}

/// Read an OSM PBF and return a `Vec<Place>` containing every node we know
/// how to map to a [`Place`].
pub fn import(pbf_path: &Path) -> Result<Vec<Place>, ImportError> {
    info!(path = %pbf_path.display(), "opening OSM PBF");
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
        Element::Way(_) | Element::Relation(_) => {}
    })?;

    info!(
        nodes_seen = counters.nodes_seen,
        emitted = counters.nodes_emitted,
        skipped_no_name = counters.skipped_no_name,
        skipped_unknown_kind = counters.skipped_unknown_kind,
        "OSM import done"
    );
    Ok(places)
}

fn node_to_place(
    node: &Node<'_>,
    local_counters: &mut HashMap<(u8, u32), u64>,
    counters: &mut Counters,
) -> Option<Place> {
    let tags = collect_tags(node.tags());
    build_place(node.lon(), node.lat(), &tags, local_counters, counters)
}

fn dense_node_to_place(
    node: &DenseNode<'_>,
    local_counters: &mut HashMap<(u8, u32), u64>,
    counters: &mut Counters,
) -> Option<Place> {
    let tags = collect_tags(node.tags());
    build_place(node.lon(), node.lat(), &tags, local_counters, counters)
}

fn collect_tags<'a, I: IntoIterator<Item = (&'a str, &'a str)>>(iter: I) -> Vec<(String, String)> {
    iter.into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn build_place(
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
            debug!(?err, "PlaceId overflow, skipping");
            return None;
        }
    };

    let kept_tags = filter_tags(tags);
    counters.nodes_emitted += 1;

    Some(Place {
        id,
        kind,
        names,
        centroid,
        admin_path: vec![],
        tags: kept_tags,
    })
}

fn place_kind(tags: &[(String, String)]) -> Option<PlaceKind> {
    let val = tag_value(tags, "place")?;
    Some(match val {
        "country" => PlaceKind::Country,
        "state" | "region" | "province" => PlaceKind::Region,
        "county" => PlaceKind::County,
        "city" | "town" | "village" | "hamlet" | "isolated_dwelling" => PlaceKind::City,
        "suburb" | "neighbourhood" | "quarter" | "borough" => PlaceKind::Neighborhood,
        "locality" => PlaceKind::City,
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
    "boundary",
    "admin_level",
    "ISO3166-1",
    "ISO3166-2",
    "wikidata",
    "population",
    "postal_code",
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
        assert!(place_kind(&tags(&[("amenity", "cafe")])).is_none());
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
        ]);
        let kept = filter_tags(&t);
        let keys: Vec<&str> = kept.iter().map(|(k, _)| k.as_str()).collect();
        assert!(keys.contains(&"place"));
        assert!(keys.contains(&"population"));
        assert!(keys.contains(&"ISO3166-1"));
        assert!(!keys.contains(&"source"));
        assert!(!keys.contains(&"name"));
    }
}
