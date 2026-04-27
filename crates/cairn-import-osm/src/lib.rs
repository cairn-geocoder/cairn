//! OpenStreetMap PBF → `Place` stream.
//!
//! Phase 0 stub. Phase 1 wires `osmpbf`.

#![allow(dead_code)]

use cairn_place::Place;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ImportError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

pub fn import(_pbf_path: &std::path::Path) -> Result<Vec<Place>, ImportError> {
    Ok(Vec::new())
}
