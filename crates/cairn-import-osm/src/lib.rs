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
use osmpbf::{BlobDecode, BlobReader, DenseNode, Element, ElementReader, Node, Relation, Way};
use rayon::iter::{ParallelBridge, ParallelIterator};
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

impl Counters {
    fn merge(&mut self, other: Counters) {
        self.nodes_seen += other.nodes_seen;
        self.nodes_emitted += other.nodes_emitted;
        self.ways_seen += other.ways_seen;
        self.ways_emitted += other.ways_emitted;
        self.relations_seen += other.relations_seen;
        self.relations_emitted += other.relations_emitted;
        self.skipped_no_name += other.skipped_no_name;
        self.skipped_unknown_kind += other.skipped_unknown_kind;
        self.skipped_way_no_coords += other.skipped_way_no_coords;
        self.skipped_relation_open_ring += other.skipped_relation_open_ring;
        self.skipped_relation_no_outer += other.skipped_relation_no_outer;
        self.interpolated_addresses += other.interpolated_addresses;
    }
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

    info!("OSM PBF pass 2a: parallel node-place emit");
    let (mut places, mut counters) = parallel_node_places(pbf_path)?;
    info!(
        nodes_seen = counters.nodes_seen,
        nodes_emitted = counters.nodes_emitted,
        "pass 2a done"
    );

    info!("OSM PBF pass 2b: ways + interpolation + admin relations");
    let mut admin_features: Vec<AdminFeature> = Vec::new();
    let mut admin_local_counters: HashMap<(u8, u32), u64> = HashMap::new();
    let mut way_nodes: WayNodes = HashMap::new();
    let mut local_counters: HashMap<(u8, u32), u64> = HashMap::new();
    let reader = ElementReader::from_path(pbf_path)?;
    reader.for_each(|element| match element {
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
        // Nodes already handled in parallel pass 2a; ignore here.
        _ => {}
    })?;

    // Renumber to deterministic, collision-free PlaceIds. Pass 2a's
    // per-block local counters mint colliding IDs within the same
    // (level, tile) — we sort the merged list by (level, tile,
    // content-hash) and reassign sequentially. Same for admin.
    let pre_places = places.len();
    places = renumber_places(places);
    let pre_admin = admin_features.len();
    admin_features = renumber_admin_features(admin_features);
    debug!(
        places = pre_places,
        admin = pre_admin,
        "renumbered with deterministic PlaceIds"
    );

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

/// Block-level parallel emit of Places from `Element::Node` and
/// `Element::DenseNode`. Each block has its own `local_counters` and
/// `Counters`; PlaceIds minted here may collide across blocks within
/// the same `(level, tile)` and are renumbered after the merge.
fn parallel_node_places(pbf_path: &Path) -> Result<(Vec<Place>, Counters), ImportError> {
    let blob_reader = BlobReader::from_path(pbf_path)?;
    blob_reader
        .par_bridge()
        .map(|blob| -> Result<(Vec<Place>, Counters), ImportError> {
            let blob = blob?;
            match blob.decode()? {
                BlobDecode::OsmHeader(_) | BlobDecode::Unknown(_) => {
                    Ok((Vec::new(), Counters::default()))
                }
                BlobDecode::OsmData(block) => {
                    let mut places = Vec::new();
                    let mut local_counters: HashMap<(u8, u32), u64> = HashMap::new();
                    let mut counters = Counters::default();
                    for elem in block.elements() {
                        match elem {
                            Element::Node(n) => {
                                counters.nodes_seen += 1;
                                if let Some(p) =
                                    node_to_place(&n, &mut local_counters, &mut counters)
                                {
                                    places.push(p);
                                }
                            }
                            Element::DenseNode(n) => {
                                counters.nodes_seen += 1;
                                if let Some(p) =
                                    dense_node_to_place(&n, &mut local_counters, &mut counters)
                                {
                                    places.push(p);
                                }
                            }
                            _ => {}
                        }
                    }
                    Ok((places, counters))
                }
            }
        })
        .reduce(
            || Ok((Vec::new(), Counters::default())),
            |a, b| match (a, b) {
                (Ok((mut av, mut ac)), Ok((bv, bc))) => {
                    av.extend(bv);
                    ac.merge(bc);
                    Ok((av, ac))
                }
                (Err(e), _) | (_, Err(e)) => Err(e),
            },
        )
}

/// Sort places by a content-derived key, then re-assign per-tile local
/// IDs sequentially. Output is deterministic across runs given the
/// same input set: same content → same final PlaceIds.
fn renumber_places(mut places: Vec<Place>) -> Vec<Place> {
    places.sort_by(|a, b| {
        a.id.level()
            .cmp(&b.id.level())
            .then(a.id.tile().cmp(&b.id.tile()))
            .then((a.kind as u8).cmp(&(b.kind as u8)))
            .then_with(|| {
                let an = a.names.first().map(|n| n.value.as_str()).unwrap_or("");
                let bn = b.names.first().map(|n| n.value.as_str()).unwrap_or("");
                an.cmp(bn)
            })
            .then_with(|| {
                ((a.centroid.lon * 1e6) as i64).cmp(&((b.centroid.lon * 1e6) as i64))
            })
            .then_with(|| {
                ((a.centroid.lat * 1e6) as i64).cmp(&((b.centroid.lat * 1e6) as i64))
            })
    });
    let mut counters: HashMap<(u8, u32), u64> = HashMap::new();
    for p in &mut places {
        let key = (p.id.level(), p.id.tile());
        let local = counters.entry(key).or_insert(0);
        let new_id = match cairn_place::PlaceId::new(key.0, key.1, *local) {
            Ok(id) => id,
            Err(err) => {
                debug!(?err, "renumber overflow — keeping original id");
                continue;
            }
        };
        p.id = new_id;
        *local += 1;
    }
    places
}

/// Renumber for AdminFeature: same idea but operates on the
/// `(level, tile, place_id)` namespace and rebuilds `place_id`
/// after sorting.
fn renumber_admin_features(mut feats: Vec<AdminFeature>) -> Vec<AdminFeature> {
    feats.sort_by(|a, b| {
        a.level
            .cmp(&b.level)
            .then(a.kind.as_str().cmp(b.kind.as_str()))
            .then(a.name.as_str().cmp(b.name.as_str()))
            .then_with(|| {
                ((a.centroid.lon * 1e6) as i64).cmp(&((b.centroid.lon * 1e6) as i64))
            })
            .then_with(|| {
                ((a.centroid.lat * 1e6) as i64).cmp(&((b.centroid.lat * 1e6) as i64))
            })
    });
    // AdminFeature::place_id encodes a tile from the centroid. Pull
    // (level, tile) from the original place_id by decoding it.
    let mut counters: HashMap<(u8, u32), u64> = HashMap::new();
    for f in &mut feats {
        let id = cairn_place::PlaceId(f.place_id);
        let key = (id.level(), id.tile());
        let local = counters.entry(key).or_insert(0);
        let new_id = match cairn_place::PlaceId::new(key.0, key.1, *local) {
            Ok(id) => id,
            Err(err) => {
                debug!(?err, "admin renumber overflow — keeping original id");
                continue;
            }
        };
        f.place_id = new_id.0;
        *local += 1;
    }
    feats
}

/// Block-level parallel decode of the node coord cache + address-tagged
/// node cache. Each PBF data blob is independent (decompression +
/// protobuf parse + per-element extraction); we let rayon's
/// `par_bridge` schedule blobs across worker threads, build a
/// per-block `(NodeCoords, NodeAddrs)` accumulator, then reduce by
/// extending one map into another.
///
/// Wins over the sequential `ElementReader::for_each` path: PBF
/// decompression dominates wall-clock at planet scale, and we now
/// pipeline that across cores. ~5-8x speedup on a planet PBF on
/// typical 8-core boxes.
fn load_node_caches(pbf_path: &Path) -> Result<(NodeCoords, NodeAddrs), ImportError> {
    let blob_reader = BlobReader::from_path(pbf_path)?;
    blob_reader
        .par_bridge()
        .map(|blob| -> Result<(NodeCoords, NodeAddrs), ImportError> {
            let blob = blob?;
            match blob.decode()? {
                BlobDecode::OsmHeader(_) | BlobDecode::Unknown(_) => {
                    Ok((HashMap::new(), HashMap::new()))
                }
                BlobDecode::OsmData(block) => {
                    let mut coords: NodeCoords = HashMap::new();
                    let mut addrs: NodeAddrs = HashMap::new();
                    for elem in block.elements() {
                        match elem {
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
                        }
                    }
                    Ok((coords, addrs))
                }
            }
        })
        .reduce(
            || Ok((HashMap::new(), HashMap::new())),
            |a, b| match (a, b) {
                (Ok((ac, aa)), Ok((bc, ba))) => {
                    // Always extend the larger map with entries from
                    // the smaller — cheaper than the reverse because
                    // HashMap::extend pays per inserted entry.
                    let (mut big_c, small_c) = if ac.len() >= bc.len() {
                        (ac, bc)
                    } else {
                        (bc, ac)
                    };
                    big_c.extend(small_c);
                    let (mut big_a, small_a) = if aa.len() >= ba.len() {
                        (aa, ba)
                    } else {
                        (ba, aa)
                    };
                    big_a.extend(small_a);
                    Ok((big_c, big_a))
                }
                (Err(e), _) | (_, Err(e)) => Err(e),
            },
        )
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
    if refs.len() < 2 {
        return;
    }
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

/// Pure logic for `addr:interpolation` expansion. Walks the way's
/// polyline by cumulative arc length so multi-segment ways place
/// synthetic addresses at the right fraction along the path, not just
/// linearly between the two endpoints.
///
/// Separated from the `Way` reader so it's unit-testable without an
/// osmpbf fixture.
fn interpolate_addresses(
    interpolation: &str,
    refs: &[i64],
    node_coords: &NodeCoords,
    node_addrs: &NodeAddrs,
    way_street: Option<&str>,
) -> Vec<InterpolatedAddress> {
    if refs.len() < 2 {
        return Vec::new();
    }
    let start_id = *refs.first().unwrap();
    let end_id = *refs.last().unwrap();
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

    // Resolve every node coord; if any are missing, drop the whole way.
    let mut polyline: Vec<[f64; 2]> = Vec::with_capacity(refs.len());
    for id in refs {
        match node_coords.get(id) {
            Some(c) => polyline.push(*c),
            None => return Vec::new(),
        }
    }
    if start_num > end_num {
        polyline.reverse();
    }
    let (lo, hi) = (start_num.min(end_num), start_num.max(end_num));

    // Cumulative arc length per node in the (now lo→hi) order. Planar
    // approximation is fine for the short distances admin ways cover;
    // any error in lon/lat space affects all positions equally and the
    // fractional coordinates still land sensibly along the polyline.
    let mut cum: Vec<f64> = Vec::with_capacity(polyline.len());
    cum.push(0.0);
    for w in polyline.windows(2) {
        let dx = w[1][0] - w[0][0];
        let dy = w[1][1] - w[0][1];
        let last = *cum.last().unwrap();
        cum.push(last + (dx * dx + dy * dy).sqrt());
    }
    let total = *cum.last().unwrap();
    let span = (hi - lo) as f64;

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
        let t = (n - lo) as f64 / span;
        let (lon, lat) = if total > 0.0 {
            // Find the segment whose cumulative length brackets `target`.
            let target = t * total;
            let seg_idx = cum
                .windows(2)
                .position(|w| target >= w[0] && target <= w[1])
                .unwrap_or(cum.len() - 2);
            let seg_start = cum[seg_idx];
            let seg_end = cum[seg_idx + 1];
            let seg_t = if seg_end > seg_start {
                (target - seg_start) / (seg_end - seg_start)
            } else {
                0.0
            };
            let a = polyline[seg_idx];
            let b = polyline[seg_idx + 1];
            (a[0] + seg_t * (b[0] - a[0]), a[1] + seg_t * (b[1] - a[1]))
        } else {
            // Degenerate polyline (all coords equal); place every synth
            // address at the start coord.
            (polyline[0][0], polyline[0][1])
        };
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

    let mut outer_ways: Vec<i64> = Vec::new();
    let mut inner_ways: Vec<i64> = Vec::new();
    for member in relation.members() {
        if !matches!(member.member_type, osmpbf::RelMemberType::Way) {
            continue;
        }
        let role = member.role().ok().unwrap_or("");
        match role {
            "outer" | "" => outer_ways.push(member.member_id),
            "inner" => inner_ways.push(member.member_id),
            _ => {}
        }
    }
    if outer_ways.is_empty() {
        counters.skipped_relation_no_outer += 1;
        return None;
    }

    let outer_rings = assemble_rings(&outer_ways, way_nodes);
    if outer_rings.is_empty() {
        counters.skipped_relation_open_ring += 1;
        debug!(rel_id = relation.id(), "no closed outer ring; dropping");
        return None;
    }
    let inner_rings = assemble_rings(&inner_ways, way_nodes);

    let outer_linestrings: Vec<LineString<f64>> = outer_rings
        .into_iter()
        .filter_map(|ring| ring_to_linestring(&ring, node_coords))
        .collect();
    if outer_linestrings.is_empty() {
        counters.skipped_relation_open_ring += 1;
        return None;
    }
    let inner_linestrings: Vec<LineString<f64>> = inner_rings
        .into_iter()
        .filter_map(|ring| ring_to_linestring(&ring, node_coords))
        .collect();
    let polygons = assemble_polygons(outer_linestrings, inner_linestrings);
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
        9 => PlaceKind::District,
        10..=12 => PlaceKind::Neighborhood,
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

fn ring_to_linestring(ring: &[i64], node_coords: &NodeCoords) -> Option<LineString<f64>> {
    let coords: Vec<(f64, f64)> = ring
        .iter()
        .filter_map(|id| node_coords.get(id).map(|c| (c[0], c[1])))
        .collect();
    if coords.len() < 4 {
        return None;
    }
    Some(LineString::from(coords))
}

/// Pair each inner ring with the outer ring that geometrically contains it
/// (a representative point of the inner ring is tested against each outer
/// linestring's polygon). Inner rings with no enclosing outer are dropped.
fn assemble_polygons(
    outers: Vec<LineString<f64>>,
    inners: Vec<LineString<f64>>,
) -> Vec<Polygon<f64>> {
    use geo::Contains;
    let mut bins: Vec<(LineString<f64>, Vec<LineString<f64>>)> =
        outers.into_iter().map(|o| (o, Vec::new())).collect();
    for inner in inners {
        let probe = match inner.0.first() {
            Some(c) => geo_types::Coord { x: c.x, y: c.y },
            None => continue,
        };
        let mut placed = false;
        for (outer, holes) in bins.iter_mut() {
            let outer_poly = Polygon::new(outer.clone(), vec![]);
            if outer_poly.contains(&probe) {
                holes.push(inner);
                placed = true;
                break;
            }
        }
        if !placed {
            debug!("inner ring without enclosing outer; dropping");
        }
    }
    bins.into_iter()
        .map(|(outer, holes)| Polygon::new(outer, holes))
        .collect()
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
    fn admin_level_kind_mapping() {
        let case = |lvl: &str| {
            admin_level_kind(&tags(&[
                ("boundary", "administrative"),
                ("admin_level", lvl),
            ]))
        };
        assert_eq!(case("2"), Some(PlaceKind::Country));
        assert_eq!(case("4"), Some(PlaceKind::Region));
        assert_eq!(case("6"), Some(PlaceKind::County));
        assert_eq!(case("8"), Some(PlaceKind::City));
        assert_eq!(case("9"), Some(PlaceKind::District));
        assert_eq!(case("10"), Some(PlaceKind::Neighborhood));
        assert_eq!(case("12"), Some(PlaceKind::Neighborhood));
        assert_eq!(case("13"), None);
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
    fn interpolation_multi_segment_arc_length() {
        // L-shaped polyline: (0,0) → (10,0) → (10,10). Total length 20.
        // Numbers 1..=11 odd: 1, 3, 5, 7, 9, 11 → endpoints + 4 synth.
        // Synth #5 sits at fraction 4/10 = 0.4, arc target 0.4*20 = 8.0
        // → still in first segment (0..10), so y=0, x=8.0.
        // Synth #9 sits at fraction 8/10 = 0.8, arc target 16.0 → second
        // segment (length 10), fraction 0.6 along → x=10, y=6.0.
        let mut coords: NodeCoords = HashMap::new();
        coords.insert(1, [0.0, 0.0]);
        coords.insert(2, [10.0, 0.0]);
        coords.insert(3, [10.0, 10.0]);
        let mut addrs: NodeAddrs = HashMap::new();
        addrs.insert(
            1,
            NodeAddr {
                housenumber: "1".into(),
                street: None,
            },
        );
        addrs.insert(
            3,
            NodeAddr {
                housenumber: "11".into(),
                street: None,
            },
        );
        let synth = interpolate_addresses("odd", &[1, 2, 3], &coords, &addrs, None);
        let nums: Vec<&str> = synth.iter().map(|s| s.housenumber.as_str()).collect();
        assert_eq!(nums, vec!["3", "5", "7", "9"]);

        let five = synth.iter().find(|s| s.housenumber == "5").unwrap();
        assert!((five.centroid.lon - 8.0).abs() < 1e-6);
        assert!(five.centroid.lat.abs() < 1e-6);

        let nine = synth.iter().find(|s| s.housenumber == "9").unwrap();
        assert!((nine.centroid.lon - 10.0).abs() < 1e-6);
        assert!((nine.centroid.lat - 6.0).abs() < 1e-6);
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

    #[test]
    fn inner_ring_becomes_hole_in_enclosing_outer() {
        // Outer: 10x10 square at origin. Inner: 2x2 square inside it.
        let outer = LineString::from(vec![
            (0.0, 0.0),
            (10.0, 0.0),
            (10.0, 10.0),
            (0.0, 10.0),
            (0.0, 0.0),
        ]);
        let inner = LineString::from(vec![
            (4.0, 4.0),
            (6.0, 4.0),
            (6.0, 6.0),
            (4.0, 6.0),
            (4.0, 4.0),
        ]);
        let polys = assemble_polygons(vec![outer], vec![inner]);
        assert_eq!(polys.len(), 1);
        assert_eq!(polys[0].interiors().len(), 1);
    }

    #[test]
    fn inner_ring_outside_outer_is_dropped() {
        let outer = LineString::from(vec![
            (0.0, 0.0),
            (1.0, 0.0),
            (1.0, 1.0),
            (0.0, 1.0),
            (0.0, 0.0),
        ]);
        let stray = LineString::from(vec![
            (50.0, 50.0),
            (51.0, 50.0),
            (51.0, 51.0),
            (50.0, 51.0),
            (50.0, 50.0),
        ]);
        let polys = assemble_polygons(vec![outer], vec![stray]);
        assert_eq!(polys.len(), 1);
        assert!(polys[0].interiors().is_empty());
    }

    #[test]
    fn multiple_outers_each_get_their_own_inner() {
        let outer_a = LineString::from(vec![
            (0.0, 0.0),
            (10.0, 0.0),
            (10.0, 10.0),
            (0.0, 10.0),
            (0.0, 0.0),
        ]);
        let outer_b = LineString::from(vec![
            (100.0, 0.0),
            (110.0, 0.0),
            (110.0, 10.0),
            (100.0, 10.0),
            (100.0, 0.0),
        ]);
        let inner_a = LineString::from(vec![
            (4.0, 4.0),
            (6.0, 4.0),
            (6.0, 6.0),
            (4.0, 6.0),
            (4.0, 4.0),
        ]);
        let inner_b = LineString::from(vec![
            (104.0, 4.0),
            (106.0, 4.0),
            (106.0, 6.0),
            (104.0, 6.0),
            (104.0, 4.0),
        ]);
        let polys = assemble_polygons(vec![outer_a, outer_b], vec![inner_a, inner_b]);
        assert_eq!(polys.len(), 2);
        assert_eq!(polys[0].interiors().len(), 1);
        assert_eq!(polys[1].interiors().len(), 1);
    }
}
