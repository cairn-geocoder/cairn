//! Address parsing + normalization.
//!
//! Phase 0 stub. Phase 4 wires libpostal via FFI behind the `libpostal` feature.

#![allow(dead_code)]

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("libpostal not initialized")]
    NotInitialized,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ParsedAddress {
    pub house_number: Option<String>,
    pub road: Option<String>,
    pub unit: Option<String>,
    pub postcode: Option<String>,
    pub city: Option<String>,
    pub state: Option<String>,
    pub country: Option<String>,
}

pub fn parse(_input: &str) -> Result<ParsedAddress, ParseError> {
    Err(ParseError::NotInitialized)
}

pub fn expand(_input: &str) -> Vec<String> {
    Vec::new()
}
