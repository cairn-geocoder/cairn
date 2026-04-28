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

/// Maximum distance between two Place centroids that still counts as
/// the same physical entity for dedup purposes. 100 m comfortably
/// covers OSM's common "entrance node a few dozen meters off the
/// building polygon centroid" pattern (e.g. "Post Vaduz" at 56 m apart)
/// without merging genuinely distinct same-name POIs in dense cities,
/// which are typically separated by hundreds of meters.
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
/// and their centroids are within `DEDUP_RADIUS_M`. The richer Place
/// (longer admin_path, then more names) wins; the loser is dropped.
///
/// Order-stable: input order determines the winner on ties so building
/// the same bundle twice produces byte-identical tile blobs. Within a
/// (kind, name) bucket the algorithm is O(n²) but bucket sizes are
/// tiny in practice — a city has only so many things called "Post".
pub fn dedupe_places(places: Vec<Place>) -> Vec<Place> {
    use std::collections::BTreeMap;
    let mut buckets: BTreeMap<(u8, String), Vec<(usize, Place)>> = BTreeMap::new();
    for (idx, p) in places.into_iter().enumerate() {
        buckets
            .entry((p.kind as u8, primary_name_lc(&p)))
            .or_default()
            .push((idx, p));
    }
    let mut kept: Vec<(usize, Place)> = Vec::new();
    for (_, members) in buckets {
        if members.len() == 1 {
            kept.extend(members);
            continue;
        }
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
                    if place_score(&members[j].1) > place_score(&members[winner].1) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use rkyv::ser::serializers::AllocSerializer;
    use rkyv::ser::Serializer;

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
        let kept = dedupe_places(vec![osm, wof]);
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
        let kept = dedupe_places(vec![city, poi]);
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
        let kept = dedupe_places(vec![a, b]);
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
        let kept = dedupe_places(vec![a, b]);
        assert_eq!(kept.len(), 2);
    }
}
