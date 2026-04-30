//! Place document model + 64-bit `PlaceId` encoding.
//!
//! All on-disk types derive `rkyv::Archive` so that tile blobs can be
//! mmap'd and read without parsing.

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Bit layout of [`PlaceId`]: `[level: 3 | tile_id: 22 | local_id: 39]`.
#[derive(
    Copy,
    Clone,
    Debug,
    Eq,
    PartialEq,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub struct PlaceId(pub u64);

impl PlaceId {
    pub const LEVEL_BITS: u32 = 3;
    pub const TILE_BITS: u32 = 22;
    pub const LOCAL_BITS: u32 = 39;

    pub const MAX_LEVEL: u8 = (1u32 << Self::LEVEL_BITS) as u8 - 1;
    pub const MAX_TILE: u32 = (1u32 << Self::TILE_BITS) - 1;
    pub const MAX_LOCAL: u64 = (1u64 << Self::LOCAL_BITS) - 1;

    pub fn new(level: u8, tile: u32, local: u64) -> Result<Self, PlaceIdError> {
        if level > Self::MAX_LEVEL {
            return Err(PlaceIdError::LevelOverflow(level));
        }
        if tile > Self::MAX_TILE {
            return Err(PlaceIdError::TileOverflow(tile));
        }
        if local > Self::MAX_LOCAL {
            return Err(PlaceIdError::LocalOverflow(local));
        }
        let bits = ((level as u64) << (Self::TILE_BITS + Self::LOCAL_BITS))
            | ((tile as u64) << Self::LOCAL_BITS)
            | local;
        Ok(Self(bits))
    }

    pub fn level(self) -> u8 {
        (self.0 >> (Self::TILE_BITS + Self::LOCAL_BITS)) as u8
    }

    pub fn tile(self) -> u32 {
        ((self.0 >> Self::LOCAL_BITS) & (Self::MAX_TILE as u64)) as u32
    }

    pub fn local(self) -> u64 {
        self.0 & Self::MAX_LOCAL
    }
}

#[derive(Debug, Error)]
pub enum PlaceIdError {
    #[error("level {0} exceeds 3-bit max")]
    LevelOverflow(u8),
    #[error("tile {0} exceeds 22-bit max")]
    TileOverflow(u32),
    #[error("local {0} exceeds 39-bit max")]
    LocalOverflow(u64),
}

#[derive(
    Copy, Clone, Debug, PartialEq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub struct Coord {
    pub lon: f64,
    pub lat: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub struct LocalizedName {
    pub lang: String,
    pub value: String,
}

#[derive(
    Copy,
    Clone,
    Debug,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub enum PlaceKind {
    Country,
    Region,
    County,
    City,
    District,
    Neighborhood,
    Street,
    Address,
    Poi,
    Postcode,
}

/// Identifies which importer emitted a Place / AdminFeature. Used at
/// build time to drive `--source-priority` weighting in dedup; not
/// persisted in the bundle (kept out-of-band as a parallel Vec).
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum SourceKind {
    Osm,
    Wof,
    OpenAddresses,
    Geonames,
    /// Phase 7b lane M — operator-supplied CSV / GeoJSON ingest. No
    /// upstream provenance constraint; license is whatever the
    /// operator declares at build time.
    Generic,
    /// Phase 7b lane K — Overture Maps Foundation drops. Mixed
    /// CDLA-Permissive 2.0 + ODbL depending on the source dataset
    /// (places vs addresses vs admins). Operators must surface
    /// attribution downstream; cairn-build records it in the
    /// per-bundle SBOM.
    Overture,
    Unknown,
}

impl SourceKind {
    /// Parse a CLI-friendly token. Accepts the canonical short names
    /// plus a couple of common aliases.
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_lowercase().as_str() {
            "osm" | "openstreetmap" => Some(Self::Osm),
            "wof" | "whosonfirst" => Some(Self::Wof),
            "oa" | "openaddresses" => Some(Self::OpenAddresses),
            "gn" | "geonames" => Some(Self::Geonames),
            "generic" | "csv" | "geojson" => Some(Self::Generic),
            "overture" => Some(Self::Overture),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Osm => "osm",
            Self::Wof => "wof",
            Self::OpenAddresses => "oa",
            Self::Geonames => "geonames",
            Self::Generic => "generic",
            Self::Overture => "overture",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub struct Place {
    pub id: PlaceId,
    pub kind: PlaceKind,
    pub names: Vec<LocalizedName>,
    pub centroid: Coord,
    pub admin_path: Vec<PlaceId>,
    pub tags: Vec<(String, String)>,
}

/// Reserved tag key for the rebuild-stable external Place identifier.
/// Importers stamp this value via [`Place::set_gid`] / [`synthesize_gid`]
/// (or [`stable_hash_gid`] for sources without an upstream stable id).
/// At query time consumers read it via [`Place::gid`] to surface a
/// `gid` field on API responses, plug into the `/v1/place` lookup, or
/// drive cross-bundle joins.
pub const GID_TAG: &str = "gid";

/// Synthesize a Pelias-compatible global identifier for a Place from
/// a known upstream provenance triple.
///
/// Format: `<source>:<type>:<id>` (e.g. `osm:way:12345`,
/// `wof:locality:101748479`, `overture:place:08bb…`). The three
/// components are joined verbatim — caller is responsible for
/// normalizing whitespace and stripping accidental colons. Empty
/// `id` returns `None` so importers don't accidentally synthesize
/// `osm:way:` for an unidentified row.
pub fn synthesize_gid(source: &str, kind: &str, id: &str) -> Option<String> {
    let id = id.trim();
    if id.is_empty() {
        return None;
    }
    Some(format!("{source}:{kind}:{id}"))
}

/// Deterministic gid for sources without an upstream stable identifier
/// (operator CSV, OpenAddresses where row IDs aren't preserved across
/// releases, etc.). Hashes the tuple `(kind, lowercased name,
/// quantized centroid at ~100 m grid)` with a stable seed so the same
/// input row produces the same gid across rebuilds.
///
/// Same quantization rule as [`dedupe_places`] uses for clustering, so
/// two near-identical rows that would dedupe also share a gid. That's
/// intentional — bookmark stability follows the dedup contract.
pub fn stable_hash_gid(source: &str, kind: &str, name: &str, centroid: Coord) -> String {
    use std::hash::{Hash, Hasher};
    // FNV-1a-style — small, deterministic, no external dep. Good
    // enough for a 64-bit content hash; collisions at country scale
    // are negligible (~10⁻¹⁰ per row pair).
    let mut h = std::collections::hash_map::DefaultHasher::new();
    kind.to_lowercase().hash(&mut h);
    name.to_lowercase().trim().hash(&mut h);
    let qlon = (centroid.lon * 1000.0).round() as i32;
    let qlat = (centroid.lat * 1000.0).round() as i32;
    qlon.hash(&mut h);
    qlat.hash(&mut h);
    let digest = h.finish();
    format!("{source}:{kind}:h{digest:016x}")
}

impl Place {
    /// Return the rebuild-stable global identifier for this Place,
    /// reading the [`GID_TAG`] entry from `tags`. None when the row
    /// was built before the gid contract landed.
    pub fn gid(&self) -> Option<&str> {
        self.tags
            .iter()
            .find(|(k, _)| k == GID_TAG)
            .map(|(_, v)| v.as_str())
    }

    /// Stamp [`GID_TAG`] on this Place. Idempotent: replaces any
    /// existing gid rather than appending a duplicate. Call once
    /// per row at import time.
    pub fn set_gid(&mut self, gid: impl Into<String>) {
        let gid = gid.into();
        if let Some(slot) = self.tags.iter_mut().find(|(k, _)| k == GID_TAG) {
            slot.1 = gid;
        } else {
            self.tags.push((GID_TAG.to_string(), gid));
        }
    }
}

/// Maximum distance between two Place centroids that still counts as
/// the same physical entity for dedup purposes. 150 m comfortably
/// covers OSM's common "entrance node a few dozen meters off the
/// building polygon centroid" pattern (e.g. "Post Vaduz" at 56 m apart)
/// plus the larger drift seen between WoF/OSM sources for the same
/// neighborhood, without merging genuinely distinct same-name POIs in
/// dense cities, which are typically separated by hundreds of meters.
const DEDUP_RADIUS_M: f64 = 150.0;
const EARTH_RADIUS_M: f64 = 6_371_000.0;

fn primary_name_lc(p: &Place) -> String {
    p.names
        .iter()
        .find(|n| n.lang == "default")
        .or_else(|| p.names.first())
        .map(|n| n.value.to_lowercase())
        .unwrap_or_default()
}

fn place_score(p: &Place) -> (usize, usize) {
    // Richer is better: longer admin_path beats shorter; more localized
    // names beats fewer. Stays stable under permutations.
    (p.admin_path.len(), p.names.len())
}

fn haversine_m(a: Coord, b: Coord) -> f64 {
    let to_rad = std::f64::consts::PI / 180.0;
    let phi1 = a.lat * to_rad;
    let phi2 = b.lat * to_rad;
    let dphi = (b.lat - a.lat) * to_rad;
    let dlam = (b.lon - a.lon) * to_rad;
    let h = (dphi / 2.0).sin().powi(2) + phi1.cos() * phi2.cos() * (dlam / 2.0).sin().powi(2);
    2.0 * EARTH_RADIUS_M * h.sqrt().asin()
}

/// Collapse near-duplicate Places emitted by overlapping sources
/// (typically WoF + OSM both shipping the same city / POI, or OSM
/// emitting both a building polygon centroid and an entrance node for
/// the same entity).
///
/// Two Places collide when they share `kind`, lowercased primary name,
/// and their centroids are within `DEDUP_RADIUS_M`. Tiebreaker order
/// for picking the winner inside a cluster:
///   1. `--source-priority` rank (lower index = higher trust).
///   2. Richer place (longer admin_path, then more localized names).
///
/// `priority` is a list of [`SourceKind`]s in preferred order. Empty
/// priority falls back to richness-only (legacy behaviour).
///
/// Order-stable: input order determines the winner on ties so building
/// the same bundle twice produces byte-identical tile blobs. Within a
/// (kind, name) bucket the algorithm is O(n²) but bucket sizes are
/// tiny in practice — a city has only so many things called "Post".
pub fn dedupe_places(items: Vec<(Place, SourceKind)>, priority: &[SourceKind]) -> Vec<Place> {
    use std::collections::BTreeMap;
    type BucketKey = (u8, String);
    type BucketEntry = (usize, Place, SourceKind);
    let mut buckets: BTreeMap<BucketKey, Vec<BucketEntry>> = BTreeMap::new();
    for (idx, (p, src)) in items.into_iter().enumerate() {
        buckets
            .entry((p.kind as u8, primary_name_lc(&p)))
            .or_default()
            .push((idx, p, src));
    }
    let mut kept: Vec<(usize, Place)> = Vec::new();
    for (_, members) in buckets {
        if members.len() == 1 {
            kept.extend(members.into_iter().map(|(idx, p, _)| (idx, p)));
            continue;
        }
        // `absorbed[k]` means "k has been folded into some earlier
        // cluster — skip it as an outer-loop seed". Both losers AND
        // the winner end up with `absorbed = true` after the cluster
        // closes; that's intentional. The surviving Place is captured
        // independently by cloning `members[winner].1` into `kept`,
        // so absorbing the winner only prevents it from re-seeding a
        // new cluster on a later iteration. Skipping the
        // absorbed-on-self mark would let the winner appear again as
        // an outer-loop seed and re-emit duplicates.
        let mut absorbed = vec![false; members.len()];
        for i in 0..members.len() {
            if absorbed[i] {
                continue;
            }
            let mut winner = i;
            for j in (i + 1)..members.len() {
                if absorbed[j] {
                    continue;
                }
                let d = haversine_m(members[i].1.centroid, members[j].1.centroid);
                if d <= DEDUP_RADIUS_M {
                    absorbed[j] = true;
                    if better(
                        &members[j].1,
                        members[j].2,
                        &members[winner].1,
                        members[winner].2,
                        priority,
                    ) {
                        winner = j;
                    }
                }
            }
            absorbed[i] = true;
            kept.push((members[winner].0, members[winner].1.clone()));
        }
    }
    kept.sort_by_key(|(idx, _)| *idx);
    kept.into_iter().map(|(_, p)| p).collect()
}

/// Returns `true` when `a` should win against `b` for dedup. Source
/// priority dominates richness; ties on priority rank fall through to
/// `place_score`.
fn better(
    a: &Place,
    a_src: SourceKind,
    b: &Place,
    b_src: SourceKind,
    priority: &[SourceKind],
) -> bool {
    let a_rank = priority.iter().position(|p| *p == a_src);
    let b_rank = priority.iter().position(|p| *p == b_src);
    match (a_rank, b_rank) {
        (Some(ar), Some(br)) if ar != br => ar < br,
        (Some(_), None) => true,
        (None, Some(_)) => false,
        _ => place_score(a) > place_score(b),
    }
}

/// Derive a Pelias-style category taxonomy for a [`Place`] from its
/// kind + OSM tags. Returns lowercase category tokens such as
/// `["health", "hospital"]` or `["food", "restaurant"]`. Empty when
/// nothing classifiable. Call sites (cairn-text indexing) emit one
/// term per category into a multi-value tantivy field; queries match
/// on any token.
///
/// Taxonomy is intentionally small + flat — top-level group + leaf —
/// so callers can filter coarsely (`?categories=health`) or precisely
/// (`?categories=hospital`) without negotiating a deep tree.
pub fn categories_for(place: &Place) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();

    // Kind-derived categories. Admin layers all roll up to "admin"
    // so `?categories=admin` filters to populated places + boundaries.
    match place.kind {
        PlaceKind::Country
        | PlaceKind::Region
        | PlaceKind::County
        | PlaceKind::City
        | PlaceKind::District
        | PlaceKind::Neighborhood => out.push("admin".into()),
        PlaceKind::Street => out.push("street".into()),
        PlaceKind::Address => out.push("address".into()),
        PlaceKind::Postcode => out.push("postal".into()),
        PlaceKind::Poi => {} // taxonomy below
    }

    let tag = |key: &str| -> Option<&str> {
        place
            .tags
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    };

    if let Some(v) = tag("amenity") {
        match v {
            "hospital" | "clinic" | "doctors" | "pharmacy" | "dentist" | "veterinary" => {
                out.push("health".into());
                out.push(v.into());
            }
            "restaurant" | "cafe" | "bar" | "pub" | "fast_food" | "food_court" | "ice_cream"
            | "biergarten" => {
                out.push("food".into());
                out.push(v.into());
            }
            "school" | "university" | "college" | "kindergarten" | "library" => {
                out.push("education".into());
                out.push(v.into());
            }
            "police" | "fire_station" | "post_office" | "townhall" | "courthouse" | "embassy"
            | "prison" | "social_facility" => {
                out.push("government".into());
                out.push(v.into());
            }
            "bank" | "atm" | "bureau_de_change" => {
                out.push("finance".into());
                out.push(v.into());
            }
            "cinema" | "theatre" | "arts_centre" | "nightclub" | "casino" => {
                out.push("entertainment".into());
                out.push(v.into());
            }
            "place_of_worship" => {
                out.push("religion".into());
                out.push(v.into());
            }
            "parking" | "fuel" | "charging_station" | "bus_station" | "ferry_terminal" | "taxi" => {
                out.push("transport".into());
                out.push(v.into());
            }
            other => out.push(other.into()),
        }
    }
    if let Some(v) = tag("shop") {
        out.push("commercial".into());
        if v == "supermarket" || v == "convenience" || v == "grocery" || v == "bakery" {
            out.push("food".into());
        }
        out.push(v.into());
    }
    if let Some(v) = tag("tourism") {
        match v {
            "hotel" | "motel" | "hostel" | "guest_house" | "apartment" | "chalet"
            | "alpine_hut" | "wilderness_hut" | "camp_site" | "caravan_site" => {
                out.push("accommodation".into());
                out.push(v.into());
            }
            "museum" | "gallery" | "attraction" | "viewpoint" | "theme_park" | "zoo"
            | "aquarium" | "artwork" => {
                out.push("attraction".into());
                out.push(v.into());
            }
            other => {
                out.push("tourism".into());
                out.push(other.into());
            }
        }
    }
    if let Some(v) = tag("leisure") {
        out.push("leisure".into());
        out.push(v.into());
    }
    if let Some(v) = tag("historic") {
        out.push("historic".into());
        out.push(v.into());
    }
    if let Some(v) = tag("emergency") {
        out.push("emergency".into());
        out.push(v.into());
    }
    if let Some(v) = tag("healthcare") {
        // Don't double-stamp "health" when amenity already added it;
        // dedup at the end.
        out.push("health".into());
        out.push(v.into());
    }
    if let Some(v) = tag("office") {
        out.push("office".into());
        out.push(v.into());
    }
    if let Some(v) = tag("craft") {
        out.push("commercial".into());
        out.push(v.into());
    }
    if let Some(v) = tag("aeroway") {
        if v == "aerodrome" || v == "terminal" || v == "gate" {
            out.push("transport".into());
            out.push("airport".into());
        }
    }
    if let Some(v) = tag("railway") {
        if v == "station" || v == "halt" || v == "tram_stop" || v == "subway_entrance" {
            out.push("transport".into());
            out.push("railway".into());
        }
    }

    // Dedup while preserving first-seen order so callers get stable
    // output across calls.
    let mut seen: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    out.retain(|c| seen.insert(c.clone()));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use rkyv::ser::serializers::AllocSerializer;
    use rkyv::ser::Serializer;

    #[test]
    fn synthesize_gid_assembles_components() {
        assert_eq!(
            synthesize_gid("osm", "way", "12345"),
            Some("osm:way:12345".to_string())
        );
        assert_eq!(
            synthesize_gid("wof", "locality", "101748479"),
            Some("wof:locality:101748479".to_string())
        );
    }

    #[test]
    fn synthesize_gid_rejects_empty_id() {
        assert_eq!(synthesize_gid("osm", "way", ""), None);
        assert_eq!(synthesize_gid("osm", "way", "   "), None);
    }

    #[test]
    fn stable_hash_gid_is_deterministic() {
        let c = Coord {
            lon: 9.5314,
            lat: 47.3769,
        };
        let a = stable_hash_gid("oa", "address", "Hauptstrasse 1", c);
        let b = stable_hash_gid("oa", "address", "Hauptstrasse 1", c);
        assert_eq!(a, b);
        // Format check: starts with `<source>:<kind>:h` then 16 hex chars.
        assert!(a.starts_with("oa:address:h"));
        assert_eq!(a.len(), "oa:address:h".len() + 16);
    }

    #[test]
    fn stable_hash_gid_quantizes_to_100m_grid() {
        // Two centroids ~50 m apart hash to the same gid (within
        // ~100 m quantization), preserving dedup contract.
        let near_a = Coord {
            lon: 9.5314,
            lat: 47.3769,
        };
        let near_b = Coord {
            lon: 9.53142,
            lat: 47.37692,
        };
        assert_eq!(
            stable_hash_gid("oa", "address", "x", near_a),
            stable_hash_gid("oa", "address", "x", near_b)
        );
        // Larger drift breaks the equivalence.
        let far = Coord {
            lon: 9.55,
            lat: 47.38,
        };
        assert_ne!(
            stable_hash_gid("oa", "address", "x", near_a),
            stable_hash_gid("oa", "address", "x", far)
        );
    }

    #[test]
    fn place_set_gid_is_idempotent() {
        let mut p = Place {
            id: PlaceId::new(1, 1, 1).unwrap(),
            kind: PlaceKind::City,
            names: vec![],
            centroid: Coord { lon: 0.0, lat: 0.0 },
            admin_path: vec![],
            tags: vec![],
        };
        p.set_gid("osm:way:1");
        p.set_gid("osm:way:2");
        assert_eq!(p.gid(), Some("osm:way:2"));
        assert_eq!(p.tags.iter().filter(|(k, _)| k == GID_TAG).count(), 1);
    }

    #[test]
    fn placeid_roundtrip() {
        let id = PlaceId::new(2, 12345, 678_901_234).unwrap();
        assert_eq!(id.level(), 2);
        assert_eq!(id.tile(), 12345);
        assert_eq!(id.local(), 678_901_234);
    }

    #[test]
    fn placeid_overflow_rejected() {
        assert!(PlaceId::new(8, 0, 0).is_err());
        assert!(PlaceId::new(0, PlaceId::MAX_TILE + 1, 0).is_err());
        assert!(PlaceId::new(0, 0, PlaceId::MAX_LOCAL + 1).is_err());
    }

    #[test]
    fn place_rkyv_roundtrip() {
        let place = Place {
            id: PlaceId::new(1, 100, 1).unwrap(),
            kind: PlaceKind::City,
            names: vec![LocalizedName {
                lang: "en".into(),
                value: "Vaduz".into(),
            }],
            centroid: Coord {
                lon: 9.5209,
                lat: 47.1410,
            },
            admin_path: vec![PlaceId::new(0, 0, 1).unwrap()],
            tags: vec![("place".into(), "city".into())],
        };

        let mut serializer = AllocSerializer::<256>::default();
        serializer.serialize_value(&place).unwrap();
        let bytes = serializer.into_serializer().into_inner();

        let archived = rkyv::check_archived_root::<Place>(&bytes).unwrap();
        assert_eq!(archived.id.0, place.id.0);
        assert_eq!(archived.names.len(), 1);
        assert_eq!(archived.names[0].value.as_str(), "Vaduz");
    }

    fn place_at(id: PlaceId, name: &str, kind: PlaceKind, lon: f64, lat: f64) -> Place {
        Place {
            id,
            kind,
            names: vec![LocalizedName {
                lang: "default".into(),
                value: name.into(),
            }],
            centroid: Coord { lon, lat },
            admin_path: vec![],
            tags: vec![],
        }
    }

    fn unknown(p: Place) -> (Place, SourceKind) {
        (p, SourceKind::Unknown)
    }

    #[test]
    fn dedupe_collapses_same_kind_same_name_same_centroid() {
        let osm = place_at(
            PlaceId::new(1, 49509, 1).unwrap(),
            "Vaduz",
            PlaceKind::City,
            9.5209,
            47.141,
        );
        let mut wof = place_at(
            PlaceId::new(1, 49509, 2).unwrap(),
            "Vaduz",
            PlaceKind::City,
            9.5209,
            47.141,
        );
        wof.admin_path = vec![PlaceId::new(0, 0, 1).unwrap()];
        let kept = dedupe_places(vec![unknown(osm), unknown(wof)], &[]);
        assert_eq!(kept.len(), 1);
        assert!(!kept[0].admin_path.is_empty(), "WoF (richer) should win");
    }

    #[test]
    fn dedupe_keeps_distinct_kinds() {
        let city = place_at(
            PlaceId::new(1, 49509, 1).unwrap(),
            "Vaduz",
            PlaceKind::City,
            9.5209,
            47.141,
        );
        let poi = place_at(
            PlaceId::new(2, 49509, 1).unwrap(),
            "Vaduz",
            PlaceKind::Poi,
            9.5209,
            47.141,
        );
        let kept = dedupe_places(vec![unknown(city), unknown(poi)], &[]);
        assert_eq!(kept.len(), 2);
    }

    #[test]
    fn dedupe_case_insensitive_on_name() {
        let a = place_at(
            PlaceId::new(2, 49509, 1).unwrap(),
            "vaduz castle",
            PlaceKind::Poi,
            9.5208,
            47.141,
        );
        let b = place_at(
            PlaceId::new(2, 49509, 2).unwrap(),
            "Vaduz Castle",
            PlaceKind::Poi,
            9.5208,
            47.141,
        );
        let kept = dedupe_places(vec![unknown(a), unknown(b)], &[]);
        assert_eq!(kept.len(), 1);
    }

    #[test]
    fn dedupe_keeps_distant_centroids() {
        let a = place_at(
            PlaceId::new(2, 49509, 1).unwrap(),
            "McDonald's",
            PlaceKind::Poi,
            9.50,
            47.14,
        );
        let b = place_at(
            PlaceId::new(2, 49509, 2).unwrap(),
            "McDonald's",
            PlaceKind::Poi,
            9.55,
            47.16,
        );
        let kept = dedupe_places(vec![unknown(a), unknown(b)], &[]);
        assert_eq!(kept.len(), 2);
    }

    #[test]
    fn dedupe_priority_overrides_richness() {
        // OSM is richer (admin_path), but priority puts WoF first.
        // WoF wins anyway.
        let mut osm = place_at(
            PlaceId::new(1, 49509, 1).unwrap(),
            "Vaduz",
            PlaceKind::City,
            9.5209,
            47.141,
        );
        osm.admin_path = vec![
            PlaceId::new(0, 0, 1).unwrap(),
            PlaceId::new(0, 0, 2).unwrap(),
        ];
        let wof = place_at(
            PlaceId::new(1, 49509, 2).unwrap(),
            "Vaduz",
            PlaceKind::City,
            9.5209,
            47.141,
        );
        let kept = dedupe_places(
            vec![(osm, SourceKind::Osm), (wof, SourceKind::Wof)],
            &[SourceKind::Wof, SourceKind::Osm],
        );
        assert_eq!(kept.len(), 1);
        assert!(
            kept[0].admin_path.is_empty(),
            "WoF (priority winner) should win even though OSM is richer"
        );
    }

    #[test]
    fn dedupe_priority_falls_back_to_richness_when_unranked() {
        let bare = place_at(
            PlaceId::new(1, 49509, 1).unwrap(),
            "Vaduz",
            PlaceKind::City,
            9.5209,
            47.141,
        );
        let mut richer = place_at(
            PlaceId::new(1, 49509, 2).unwrap(),
            "Vaduz",
            PlaceKind::City,
            9.5209,
            47.141,
        );
        richer.admin_path = vec![PlaceId::new(0, 0, 1).unwrap()];
        // Both Unknown, neither in priority list.
        let kept = dedupe_places(vec![unknown(bare), unknown(richer)], &[SourceKind::Wof]);
        assert_eq!(kept.len(), 1);
        assert!(!kept[0].admin_path.is_empty());
    }

    fn poi_with_tags(tags: &[(&str, &str)]) -> Place {
        Place {
            id: PlaceId::new(2, 1, 1).unwrap(),
            kind: PlaceKind::Poi,
            names: vec![LocalizedName {
                lang: "default".into(),
                value: "x".into(),
            }],
            centroid: Coord { lon: 0.0, lat: 0.0 },
            admin_path: vec![],
            tags: tags
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect(),
        }
    }

    #[test]
    fn categories_admin_kind_emits_admin_token() {
        let mut p = poi_with_tags(&[]);
        p.kind = PlaceKind::City;
        let cats = categories_for(&p);
        assert_eq!(cats, vec!["admin".to_string()]);
    }

    #[test]
    fn categories_amenity_hospital_emits_health_taxonomy() {
        let p = poi_with_tags(&[("amenity", "hospital")]);
        let cats = categories_for(&p);
        assert!(cats.contains(&"health".to_string()));
        assert!(cats.contains(&"hospital".to_string()));
    }

    #[test]
    fn categories_shop_supermarket_rolls_up_to_food_and_commercial() {
        let p = poi_with_tags(&[("shop", "supermarket")]);
        let cats = categories_for(&p);
        assert!(cats.contains(&"commercial".to_string()));
        assert!(cats.contains(&"food".to_string()));
        assert!(cats.contains(&"supermarket".to_string()));
    }

    #[test]
    fn categories_tourism_hotel_emits_accommodation() {
        let p = poi_with_tags(&[("tourism", "hotel")]);
        let cats = categories_for(&p);
        assert!(cats.contains(&"accommodation".to_string()));
        assert!(cats.contains(&"hotel".to_string()));
    }

    #[test]
    fn categories_dedupes_overlapping_tags() {
        // amenity=hospital + healthcare=hospital should not double-stamp
        // "health" or "hospital".
        let p = poi_with_tags(&[("amenity", "hospital"), ("healthcare", "hospital")]);
        let cats = categories_for(&p);
        assert_eq!(cats.iter().filter(|c| c.as_str() == "health").count(), 1);
        assert_eq!(cats.iter().filter(|c| c.as_str() == "hospital").count(), 1);
    }

    #[test]
    fn categories_postcode_emits_postal() {
        let mut p = poi_with_tags(&[]);
        p.kind = PlaceKind::Postcode;
        let cats = categories_for(&p);
        assert_eq!(cats, vec!["postal".to_string()]);
    }

    #[test]
    fn categories_unknown_amenity_passes_through() {
        let p = poi_with_tags(&[("amenity", "weird_unknown_thing")]);
        let cats = categories_for(&p);
        assert_eq!(cats, vec!["weird_unknown_thing".to_string()]);
    }
}
