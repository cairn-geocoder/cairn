//! Place document model + 64-bit `PlaceId` encoding.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Bit layout of [`PlaceId`]: `[level: 3 | tile_id: 22 | local_id: 39]`.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
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

#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Coord {
    pub lon: f64,
    pub lat: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LocalizedName {
    pub lang: String,
    pub value: String,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Place {
    pub id: PlaceId,
    pub kind: PlaceKind,
    pub names: Vec<LocalizedName>,
    pub centroid: Coord,
    pub admin_path: Vec<PlaceId>,
    pub tags: Vec<(String, String)>,
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
