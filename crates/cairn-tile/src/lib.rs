//! Tile coordinate scheme + bundle manifest schema.
//!
//! Three-level grid inspired by Valhalla:
//! - L0: 4° × 4° (countries / regions)
//! - L1: 1° × 1° (cities / postcodes)
//! - L2: 0.25° × 0.25° (streets / addresses / POIs)

use cairn_place::Coord;
use serde::{Deserialize, Serialize};

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Level {
    L0,
    L1,
    L2,
}

impl Level {
    pub fn cell_deg(self) -> f64 {
        match self {
            Level::L0 => 4.0,
            Level::L1 => 1.0,
            Level::L2 => 0.25,
        }
    }

    pub fn columns(self) -> u32 {
        (360.0 / self.cell_deg()) as u32
    }

    pub fn rows(self) -> u32 {
        (180.0 / self.cell_deg()) as u32
    }

    pub fn as_u8(self) -> u8 {
        match self {
            Level::L0 => 0,
            Level::L1 => 1,
            Level::L2 => 2,
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TileCoord {
    pub level: Level,
    pub row: u32,
    pub col: u32,
}

impl TileCoord {
    pub fn from_coord(level: Level, c: Coord) -> Self {
        let cell = level.cell_deg();
        let cols = level.columns();
        let rows = level.rows();
        let col = (((c.lon + 180.0) / cell).floor() as u32).min(cols - 1);
        let row = (((c.lat + 90.0) / cell).floor() as u32).min(rows - 1);
        TileCoord { level, row, col }
    }

    pub fn id(self) -> u32 {
        self.row * self.level.columns() + self.col
    }

    pub fn relative_path(self) -> String {
        let id = self.id();
        format!(
            "tiles/{lvl}/{a:03}/{b:03}/{id}.bin",
            lvl = self.level.as_u8(),
            a = id / 1000 / 1000 % 1000,
            b = id / 1000 % 1000,
            id = id,
        )
    }
}

/// Top-level bundle manifest. Serialized as `manifest.toml` at bundle root.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Manifest {
    pub schema_version: u32,
    pub built_at: String,
    pub bundle_id: String,
    pub sources: Vec<SourceVersion>,
    pub tiles: Vec<TileEntry>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SourceVersion {
    pub name: String,
    pub version: String,
    pub blake3: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TileEntry {
    pub level: u8,
    pub tile_id: u32,
    pub blake3: String,
    pub byte_size: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tile_from_zurich() {
        let c = Coord {
            lon: 8.5417,
            lat: 47.3769,
        };
        let t = TileCoord::from_coord(Level::L1, c);
        assert_eq!(t.level, Level::L1);
        assert_eq!(t.col, 188);
        assert_eq!(t.row, 137);
    }

    #[test]
    fn tile_id_within_bounds() {
        let c = Coord {
            lon: 179.99,
            lat: 89.99,
        };
        let t = TileCoord::from_coord(Level::L2, c);
        assert!(t.id() < Level::L2.columns() * Level::L2.rows());
    }
}
