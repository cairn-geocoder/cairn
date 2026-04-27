//! WhosOnFirst SQLite bundle → admin `Place` stream.
//!
//! Phase 0 stub. Phase 1 wires `rusqlite` + `geozero`.

#![allow(dead_code)]

use cairn_place::Place;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ImportError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

pub fn import(_sqlite_path: &std::path::Path) -> Result<Vec<Place>, ImportError> {
    Ok(Vec::new())
}
