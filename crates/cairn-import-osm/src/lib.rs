//! OpenStreetMap PBF → `Place` stream + admin polygon layer.
//!
//! Phase 4–6d scope:
//! - Nodes tagged `place=*` with a `name=*` → admin/city/neighborhood Places.
//! - Nodes tagged with POI keys (amenity, shop, tourism, office, leisure,
//!   historic) plus a name → POI Places at L2.
//! - Ways tagged `highway=<road class>` with a name → Street Places at L2,
//!   centroid = mean of cached node coordinates.
//! - Relations tagged `boundary=administrative` (or `type=multipolygon`
//!   with a `boundary=*` tag) → admin polygons. Outer-role member ways
//!   are stitched into closed rings via endpoint matching; ways that
//!   don't close are dropped with a warning. `admin_level` maps to
//!   `PlaceKind` (2=country, 4=region, 6=county, 8=city, 10=neighborhood).
//!
//! Two passes over the PBF:
//!   1. `load_node_coords`: cache every node's `(lon, lat)`.
//!   2. Single sweep that emits Places, caches `way_id → Vec<NodeId>` as
//!      ways stream by, and uses the accumulated cache to assemble
//!      admin polygons when relations stream by (PBF order: nodes →
//!      ways → relations, so ways are always available when relations
//!      arrive).

use cairn_place::{Coord, LocalizedName, Place, PlaceId, PlaceKind};
use cairn_spatial::{AdminFeature, AdminLayer};
use cairn_tile::{Level, TileCoord};
use geo_types::{LineString, MultiPolygon, Polygon};
use osmpbf::{DenseNode, Element, ElementReader, Node, Relation, Way};
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
    relations_seen: u64,
    relations_emitted: u64,
    skipped_no_name: u64,
    skipped_unknown_kind: u64,
    skipped_way_no_coords: u64,
    skipped_relation_open_ring: u64,
    skipped_relation_no_outer: u64,
    interpolated_addresses: u64,
}

type NodeCoords = HashMap<i64, [f64; 2]>;
type WayNodes = HashMap<i64, Vec<i64>>;

/// `addr:housenumber` value at a given node, plus the optional
/// `addr:street` co-tagged with it. Captured in pass 1 so that pass 2 can
/// resolve `addr:interpolation` way endpoints without a third pass.
#[derive(Clone, Debug)]
struct NodeAddr {
    housenumber: String,
    street: Option<String>,
}
type NodeAddrs = HashMap<i64, NodeAddr>;

/// Aggregate output of an OSM PBF import.
pub struct OsmImport {
    pub places: Vec<Place>,
    pub admin_layer: AdminLayer,
}

pub fn import(pbf_path: &Path) -> Result<OsmImport, ImportError> {
    info!(path = %pbf_path.display(), "OSM PBF pass 1: node coords + addr nodes");
    let (node_coords, node_addrs) = load_node_caches(pbf_path)?;
    info!(
        nodes_cached = node_coords.len(),
        addr_nodes = node_addrs.len(),
        "node caches built"
    );

    info!("OSM PBF pass 2: places + ways + interpolation + admin relations");
    let reader = ElementReader::from_path(pbf_path)?;
    let mut places = Vec::new();
    let mut admin_features: Vec<AdminFeature> = Vec::new();
    let mut counters = Counters::default();
    let mut local_counters: HashMap<(u8, u32), u64> = HashMap::new();
    let mut admin_local_counters: HashMap<(u8, u32), u64> = HashMap::new();
    let mut way_nodes: WayNodes = HashMap::new();

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
            way_nodes.insert(w.id(), w.refs().collect());
            if let Some(p) = way_to_place(&w, &node_coords, &mut local_counters, &mut counters) {
                places.push(p);
            }
            interpolate_way_addresses(
                &w,
                &node_coords,
                &node_addrs,
                &mut local_counters,
                &mut counters,
                &mut places,
            );
        }
        Element::Relation(r) => {
            counters.relations_seen += 1;
            if let Some(feat) = relation_to_admin(
                &r,
                &way_nodes,
                &node_coords,
                &mut admin_local_counters,
                &mut counters,
            ) {
                admin_features.push(feat);
            }
        }
    })?;

    info!(
        nodes_seen = counters.nodes_seen,
        nodes_emitted = counters.nodes_emitted,
        ways_seen = counters.ways_seen,
        ways_emitted = counters.ways_emitted,
        relations_seen = counters.relations_seen,
        relations_emitted = counters.relations_emitted,
        skipped_no_name = counters.skipped_no_name,
        skipped_unknown_kind = counters.skipped_unknown_kind,
        skipped_way_no_coords = counters.skipped_way_no_coords,
        skipped_relation_open_ring = counters.skipped_relation_open_ring,
        skipped_relation_no_outer = counters.skipped_relation_no_outer,
        interpolated_addresses = counters.interpolated_addresses,
        "OSM import done"
    );
    Ok(OsmImport {
        places,
        admin_layer: AdminLayer {
            features: admin_features,
        },
    })
}

fn load_node_caches(pbf_path: &Path) -> Result<(NodeCoords, NodeAddrs), ImportError> {
    let reader = ElementReader::from_path(pbf_path)?;
    let mut coords: NodeCoords = HashMap::new();
    let mut addrs: NodeAddrs = HashMap::new();
    reader.for_each(|element| match element {
        Element::Node(n) => {
            coords.insert(n.id(), [n.lon(), n.lat()]);
            if let Some(addr) = node_addr_from_tags(n.tags()) {
                addrs.insert(n.id(), addr);
            }
        }
        Element::DenseNode(n) => {
            coords.insert(n.id(), [n.lon(), n.lat()]);
            if let Some(addr) = node_addr_from_tags(n.tags()) {
                addrs.insert(n.id(), addr);
            }
        }
        _ => {}
    })?;
    Ok((coords, addrs))
}

fn node_addr_from_tags<'a>(tags: impl IntoIterator<Item = (&'a str, &'a str)>) -> Option<NodeAddr> {
    let mut housenumber: Option<String> = None;
    let mut street: Option<String> = None;
    for (k, v) in tags {
        match k {
            "addr:housenumber" => housenumber = Some(v.to_string()),
            "addr:street" => street = Some(v.to_string()),
            _ => {}
        }
    }
    housenumber.map(|hn| NodeAddr {
        housenumber: hn,
        street,
    })
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

/// Synthesize Address Places from an `addr:interpolation` way.
///
/// Phase 6.1 scope: linear interpolation along a 2-node way whose endpoints
/// both carry `addr:housenumber`. Multi-segment ways are skipped — they
/// need polyline arc-length distribution, which lands in a follow-up.
/// `addr:interpolation` values handled: `odd`, `even`, `all`, `1` (any
/// step). `alphabetic` is skipped (no integer arithmetic).
fn interpolate_way_addresses(
    way: &Way<'_>,
    node_coords: &NodeCoords,
    node_addrs: &NodeAddrs,
    local_counters: &mut HashMap<(u8, u32), u64>,
    counters: &mut Counters,
    places: &mut Vec<Place>,
) {
    let tags = collect_tags(way.tags());
    let interpolation = match tag_value(&tags, "addr:interpolation") {
        Some(v) => v,
        None => return,
    };
    let way_street = tag_value(&tags, "addr:street").map(str::to_string);
    let refs: Vec<i64> = way.refs().collect();
    let synth = interpolate_addresses(
        interpolation,
        &refs,
        node_coords,
        node_addrs,
        way_street.as_deref(),
    );
    for s in synth {
        let level = Level::L2;
        let tile = TileCoord::from_coord(level, s.centroid);
        let key = (level.as_u8(), tile.id());
        let local = local_counters.entry(key).or_insert(0);
        let local_id = *local;
        *local += 1;
        let id = match PlaceId::new(level.as_u8(), tile.id(), local_id) {
            Ok(id) => id,
            Err(err) => {
                debug!(?err, "PlaceId overflow on interpolation; skipping");
                continue;
            }
        };
        let mut tags: Vec<(String, String)> = vec![
            ("source".into(), "osm-interpolation".into()),
            ("addr:housenumber".into(), s.housenumber.clone()),
        ];
        if let Some(street) = s.street.as_deref() {
            tags.push(("addr:street".into(), street.to_string()));
        }
        places.push(Place {
            id,
            kind: PlaceKind::Address,
            names: vec![LocalizedName {
                lang: "default".into(),
                value: s.display_name,
            }],
            centroid: s.centroid,
            admin_path: vec![],
            tags,
        });
        counters.interpolated_addresses += 1;
    }
}

/// Synthetic address generated from an interpolation way.
#[derive(Clone, Debug, PartialEq)]
struct InterpolatedAddress {
    housenumber: String,
    street: Option<String>,
    display_name: String,
    centroid: Coord,
}

/// Pure logic for `addr:interpolation` expansion. Separated from the
/// `Way` reader so it's unit-testable without an osmpbf fixture.
fn interpolate_addresses(
    interpolation: &str,
    refs: &[i64],
    node_coords: &NodeCoords,
    node_addrs: &NodeAddrs,
    way_street: Option<&str>,
) -> Vec<InterpolatedAddress> {
    if refs.len() != 2 {
        return Vec::new();
    }
    let (start_id, end_id) = (refs[0], refs[1]);
    let start_addr = match node_addrs.get(&start_id) {
        Some(a) => a,
        None => return Vec::new(),
    };
    let end_addr = match node_addrs.get(&end_id) {
        Some(a) => a,
        None => return Vec::new(),
    };
    let start_num: i64 = match start_addr.housenumber.parse() {
        Ok(n) => n,
        Err(_) => return Vec::new(),
    };
    let end_num: i64 = match end_addr.housenumber.parse() {
        Ok(n) => n,
        Err(_) => return Vec::new(),
    };
    if start_num == end_num {
        return Vec::new();
    }
    let step: i64 = match interpolation {
        "odd" | "even" => 2,
        "all" | "1" => 1,
        _ => return Vec::new(),
    };
    let start_coord = match node_coords.get(&start_id) {
        Some(c) => *c,
        None => return Vec::new(),
    };
    let end_coord = match node_coords.get(&end_id) {
        Some(c) => *c,
        None => return Vec::new(),
    };
    let (lo, hi, lo_coord, hi_coord) = if start_num <= end_num {
        (start_num, end_num, start_coord, end_coord)
    } else {
        (end_num, start_num, end_coord, start_coord)
    };
    let total_span = (hi - lo) as f64;
    if total_span <= 0.0 {
        return Vec::new();
    }
    let street = way_street
        .map(str::to_string)
        .or_else(|| start_addr.street.clone())
        .or_else(|| end_addr.street.clone());

    let first_synth = lo + step;
    let last_synth = hi - step;
    let mut out = Vec::new();
    let mut n = first_synth;
    while n <= last_synth {
        if step == 2 && (n % 2) != (lo % 2) {
            n += 1;
            continue;
        }
        let t = (n - lo) as f64 / total_span;
        let lon = lo_coord[0] + t * (hi_coord[0] - lo_coord[0]);
        let lat = lo_coord[1] + t * (hi_coord[1] - lo_coord[1]);
        let display_name = match street.as_deref() {
            Some(s) => format!("{n} {s}"),
            None => n.to_string(),
        };
        out.push(InterpolatedAddress {
            housenumber: n.to_string(),
            street: street.clone(),
            display_name,
            centroid: Coord { lon, lat },
        });
        n += step;
    }
    out
}

/// Build an `AdminFeature` from an OSM admin-boundary relation by stitching
/// outer-role member ways into closed rings. Returns `None` if the relation
/// isn't admin, doesn't have a usable name + admin_level, or none of its
/// outer members close into a ring.
fn relation_to_admin(
    relation: &Relation<'_>,
    way_nodes: &WayNodes,
    node_coords: &NodeCoords,
    local_counters: &mut HashMap<(u8, u32), u64>,
    counters: &mut Counters,
) -> Option<AdminFeature> {
    let tags = collect_tags(relation.tags());
    if !is_admin_boundary(&tags) {
        return None;
    }
    let names = collect_names(&tags);
    if names.is_empty() {
        counters.skipped_no_name += 1;
        return None;
    }
    let kind = match admin_level_kind(&tags) {
        Some(k) => k,
        None => {
            counters.skipped_unknown_kind += 1;
            return None;
        }
    };

    let outer_ways: Vec<i64> = relation
        .members()
        .filter(|m| matches!(m.member_type, osmpbf::RelMemberType::Way))
        .filter(|m| {
            m.role()
                .ok()
                .map(|r| r == "outer" || r.is_empty())
                .unwrap_or(false)
        })
        .map(|m| m.member_id)
        .collect();
    if outer_ways.is_empty() {
        counters.skipped_relation_no_outer += 1;
        return None;
    }

    let rings = assemble_rings(&outer_ways, way_nodes);
    if rings.is_empty() {
        counters.skipped_relation_open_ring += 1;
        debug!(rel_id = relation.id(), "no closed outer ring; dropping");
        return None;
    }

    let polygons: Vec<Polygon<f64>> = rings
        .into_iter()
        .filter_map(|ring| ring_to_polygon(&ring, node_coords))
        .collect();
    if polygons.is_empty() {
        counters.skipped_relation_open_ring += 1;
        return None;
    }
    let multipolygon = MultiPolygon(polygons);
    let centroid = multipolygon_centroid(&multipolygon)?;

    let level = level_for_kind(kind);
    let tile = TileCoord::from_coord(level, centroid);
    let key = (level.as_u8(), tile.id());
    let local = local_counters.entry(key).or_insert(0);
    let local_id = *local;
    *local += 1;
    let place_id = match PlaceId::new(level.as_u8(), tile.id(), local_id) {
        Ok(id) => id,
        Err(err) => {
            debug!(?err, "PlaceId overflow on admin relation");
            return None;
        }
    };

    let default_name = names
        .iter()
        .find(|n| n.lang == "default")
        .or_else(|| names.first())
        .map(|n| n.value.clone())
        .unwrap_or_default();
    counters.relations_emitted += 1;
    Some(AdminFeature {
        place_id: place_id.0,
        level: level.as_u8(),
        kind: kind_str(kind).into(),
        name: default_name,
        centroid,
        admin_path: vec![],
        polygon: multipolygon,
    })
}

fn is_admin_boundary(tags: &[(String, String)]) -> bool {
    let boundary = tag_value(tags, "boundary");
    let typ = tag_value(tags, "type");
    boundary == Some("administrative")
        || (typ == Some("multipolygon") && boundary == Some("administrative"))
        || (typ == Some("boundary") && boundary == Some("administrative"))
}

fn admin_level_kind(tags: &[(String, String)]) -> Option<PlaceKind> {
    let lvl = tag_value(tags, "admin_level")?.parse::<u8>().ok()?;
    Some(match lvl {
        1..=2 => PlaceKind::Country,
        3..=4 => PlaceKind::Region,
        5..=6 => PlaceKind::County,
        7..=8 => PlaceKind::City,
        9..=12 => PlaceKind::Neighborhood,
        _ => return None,
    })
}

fn kind_str(kind: PlaceKind) -> &'static str {
    match kind {
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
    }
}

/// Stitch a multi-set of outer ways into closed rings via endpoint matching.
/// Each output ring is a `Vec<NodeId>` whose first and last entries match
/// (geographically the same node).
fn assemble_rings(outer_way_ids: &[i64], way_nodes: &WayNodes) -> Vec<Vec<i64>> {
    let mut available: HashMap<i64, Vec<i64>> = outer_way_ids
        .iter()
        .filter_map(|id| way_nodes.get(id).cloned().map(|v| (*id, v)))
        .collect();
    let mut rings: Vec<Vec<i64>> = Vec::new();

    while let Some(&seed_id) = available.keys().next().copied().as_ref() {
        let mut chain = available.remove(&seed_id).unwrap();
        if chain.len() < 2 {
            continue;
        }
        // Try to extend the chain at its tail end until it closes or we
        // run out of matching ways.
        let mut extended = true;
        while extended {
            extended = false;
            if chain.first() == chain.last() {
                break;
            }
            let tail = *chain.last().unwrap();
            // Find a way that starts or ends at `tail`.
            let next_id = available.iter().find_map(|(id, nodes)| {
                if nodes.first() == Some(&tail) || nodes.last() == Some(&tail) {
                    Some(*id)
                } else {
                    None
                }
            });
            if let Some(id) = next_id {
                let mut nodes = available.remove(&id).unwrap();
                if nodes.first() != Some(&tail) {
                    nodes.reverse();
                }
                // Skip the duplicated joining node.
                chain.extend(nodes.into_iter().skip(1));
                extended = true;
            }
        }
        if chain.first() == chain.last() && chain.len() >= 4 {
            rings.push(chain);
        }
    }
    rings
}

fn ring_to_polygon(ring: &[i64], node_coords: &NodeCoords) -> Option<Polygon<f64>> {
    let coords: Vec<(f64, f64)> = ring
        .iter()
        .filter_map(|id| node_coords.get(id).map(|c| (c[0], c[1])))
        .collect();
    if coords.len() < 4 {
        return None;
    }
    Some(Polygon::new(LineString::from(coords), vec![]))
}

fn multipolygon_centroid(mp: &MultiPolygon<f64>) -> Option<Coord> {
    use geo::Centroid;
    let p = mp.centroid()?;
    Some(Coord {
        lon: p.x(),
        lat: p.y(),
    })
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
    fn interpolation_odd_2_to_10() {
        let mut coords: NodeCoords = HashMap::new();
        coords.insert(1, [9.0, 47.0]);
        coords.insert(2, [9.0, 47.5]);
        let mut addrs: NodeAddrs = HashMap::new();
        addrs.insert(
            1,
            NodeAddr {
                housenumber: "1".into(),
                street: Some("Main St".into()),
            },
        );
        addrs.insert(
            2,
            NodeAddr {
                housenumber: "11".into(),
                street: Some("Main St".into()),
            },
        );
        let synth = interpolate_addresses("odd", &[1, 2], &coords, &addrs, None);
        let nums: Vec<&str> = synth.iter().map(|s| s.housenumber.as_str()).collect();
        assert_eq!(nums, vec!["3", "5", "7", "9"]);
        // Linear interpolation: 5 sits at t = (5-1)/(11-1) = 0.4
        let mid = synth.iter().find(|s| s.housenumber == "5").unwrap();
        assert!((mid.centroid.lat - (47.0 + 0.4 * 0.5)).abs() < 1e-9);
        assert_eq!(mid.display_name, "5 Main St");
    }

    #[test]
    fn interpolation_even_with_swapped_endpoints() {
        let mut coords: NodeCoords = HashMap::new();
        coords.insert(1, [10.0, 50.0]);
        coords.insert(2, [10.0, 50.0]);
        let mut addrs: NodeAddrs = HashMap::new();
        addrs.insert(
            1,
            NodeAddr {
                housenumber: "12".into(),
                street: None,
            },
        );
        addrs.insert(
            2,
            NodeAddr {
                housenumber: "4".into(),
                street: None,
            },
        );
        let synth = interpolate_addresses("even", &[1, 2], &coords, &addrs, None);
        let nums: Vec<&str> = synth.iter().map(|s| s.housenumber.as_str()).collect();
        assert_eq!(nums, vec!["6", "8", "10"]);
    }

    #[test]
    fn interpolation_all() {
        let mut coords: NodeCoords = HashMap::new();
        coords.insert(1, [0.0, 0.0]);
        coords.insert(2, [0.0, 0.0]);
        let mut addrs: NodeAddrs = HashMap::new();
        addrs.insert(
            1,
            NodeAddr {
                housenumber: "1".into(),
                street: None,
            },
        );
        addrs.insert(
            2,
            NodeAddr {
                housenumber: "5".into(),
                street: None,
            },
        );
        let synth = interpolate_addresses("all", &[1, 2], &coords, &addrs, None);
        let nums: Vec<&str> = synth.iter().map(|s| s.housenumber.as_str()).collect();
        assert_eq!(nums, vec!["2", "3", "4"]);
    }

    #[test]
    fn interpolation_unsupported_kind() {
        let mut coords: NodeCoords = HashMap::new();
        coords.insert(1, [0.0, 0.0]);
        coords.insert(2, [0.0, 0.0]);
        let mut addrs: NodeAddrs = HashMap::new();
        addrs.insert(
            1,
            NodeAddr {
                housenumber: "1A".into(),
                street: None,
            },
        );
        addrs.insert(
            2,
            NodeAddr {
                housenumber: "1F".into(),
                street: None,
            },
        );
        let synth = interpolate_addresses("alphabetic", &[1, 2], &coords, &addrs, None);
        assert!(synth.is_empty());
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
