//! Geonames TSV → populated-place `Place` stream.
//!
//! Phase 0 stub. Phase 1 wires `csv`.

#![allow(dead_code)]

use cairn_place::Place;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ImportError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

pub fn import(_tsv_path: &std::path::Path) -> Result<Vec<Place>, ImportError> {
    Ok(Vec::new())
}
