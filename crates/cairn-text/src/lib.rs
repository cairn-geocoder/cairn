//! Text indexing (tantivy) + admin-prefix FST for autocomplete.
//!
//! Phase 0 stubs only. Concrete schema lands in Phase 2.

#![allow(dead_code)]

use thiserror::Error;

#[derive(Debug, Error)]
pub enum TextError {
    #[error("tantivy: {0}")]
    Tantivy(String),
    #[error("fst: {0}")]
    Fst(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

pub struct TextIndex;

impl TextIndex {
    pub fn open(_path: &std::path::Path) -> Result<Self, TextError> {
        Ok(Self)
    }
}
