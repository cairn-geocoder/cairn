//! Cairn — offline, airgap-ready geocoder.
//!
//! This umbrella crate re-exports the workspace surface for downstream
//! library consumers. Most users will instead invoke the `cairn-build`
//! and `cairn-serve` binaries.

pub use cairn_api as api;
pub use cairn_parse as parse;
pub use cairn_place as place;
pub use cairn_spatial as spatial;
pub use cairn_text as text;
pub use cairn_tile as tile;
