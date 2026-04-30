//! Overture Maps Foundation drops → `Place` stream.
//!
//! Phase 7b lane K. Wraps [`cairn_import_parquet`] with the column
//! mapping conventions Overture's monthly drops use, and stamps
//! [`cairn_place::SourceKind::Overture`] on every emitted record so downstream
//! dedup + license attribution stays correct.
//!
//! ## Supported themes
//!
//! Overture publishes several thematic datasets per release. v1 of
//! this crate targets:
//!
//! - **places** — points of interest with categories + confidence.
//! - **addresses** — house-number / street / postcode points.
//!
//! The **divisions** (admin polygons) and **transportation** themes
//! ship Polygon / LineString geometries that the v1 parquet loader
//! still skips. Tracked as follow-ups.
//!
//! ## Schema flavors
//!
//! Overture's "official" parquet files use Arrow nested structs
//! (e.g. `names: Struct<primary: String, common: Map<String, String>>`).
//! Cairn's parquet loader currently expects flat columns; operators
//! who want the "official" drops must pre-flatten via DuckDB:
//!
//! ```sql
//! COPY (
//!   SELECT
//!     id,
//!     geometry,
//!     names.primary AS name,
//!     categories.primary AS category,
//!     confidence
//!   FROM 'theme=places/type=place/*'
//! ) TO 'places-flat.parquet' (FORMAT 'parquet');
//! ```
//!
//! Once the parquet loader gains nested-struct support, this wrapper
//! will switch its `ColumnMap` to point at the nested paths and drop
//! the flatten requirement.
//!
//! ## License
//!
//! Overture data is dual-licensed: most themes under
//! CDLA-Permissive-2.0, the addresses theme partly under ODbL. The
//! per-bundle SBOM records the source-version for every input so
//! downstream attribution stays auditable.

use cairn_import_parquet::{ColumnMap, Config, Defaults, ImportError as ParquetError, TagPolicy};
use cairn_place::{stable_hash_gid, synthesize_gid, Place};
use std::path::Path;
use thiserror::Error;
use tracing::info;

#[derive(Debug, Error)]
pub enum ImportError {
    #[error("parquet: {0}")]
    Parquet(#[from] ParquetError),
}

/// Theme-aware preset for the Overture parquet drop the operator is
/// loading. Picks the right column mapping + default Place kind so
/// callers don't have to re-author a TOML config per theme.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Theme {
    /// `theme=places` — points of interest. Default kind = poi.
    /// CLI aliases parsed by [`Theme::parse`]: `places`, `place`,
    /// `poi`, `pois` (case-insensitive).
    Places,
    /// `theme=addresses` — house-number / street / postcode points.
    /// Default kind = address. CLI aliases parsed by
    /// [`Theme::parse`]: `addresses`, `address` (case-insensitive).
    Addresses,
}

impl Theme {
    /// Parse a CLI-friendly token. Whitespace-trimmed, lowercased, then
    /// matched against:
    /// - [`Theme::Places`] → `places`, `place`, `poi`, `pois`
    /// - [`Theme::Addresses`] → `addresses`, `address`
    ///
    /// Returns `None` for any other input — callers surface that as a
    /// usage error rather than guessing.
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_lowercase().as_str() {
            "places" | "place" | "poi" | "pois" => Some(Self::Places),
            "addresses" | "address" => Some(Self::Addresses),
            _ => None,
        }
    }

    /// Construct the parquet [`Config`] that flattened Overture
    /// exports of this theme should be read with. Operators who use
    /// non-default column names override these via TOML and pass it
    /// to [`cairn_import_parquet::import`] directly.
    pub fn parquet_config(self) -> Config {
        match self {
            Theme::Places => Config {
                map: ColumnMap {
                    geometry: "geometry".into(),
                    name: "name".into(),
                    kind: Some("category".into()),
                    ..ColumnMap::default()
                },
                defaults: Defaults {
                    kind: "poi".into(),
                    ..Defaults::default()
                },
                tags: TagPolicy {
                    keep: vec![
                        "id".into(),
                        "category".into(),
                        "confidence".into(),
                        "phone".into(),
                        "websites".into(),
                        "brand".into(),
                    ],
                },
            },
            Theme::Addresses => Config {
                map: ColumnMap {
                    geometry: "geometry".into(),
                    // Addresses theme has no `name` column directly;
                    // the operator's flatten step typically materializes
                    // `name` as `concat_ws(' ', number, street)`.
                    name: "name".into(),
                    ..ColumnMap::default()
                },
                defaults: Defaults {
                    kind: "address".into(),
                    ..Defaults::default()
                },
                tags: TagPolicy {
                    keep: vec![
                        "id".into(),
                        "number".into(),
                        "street".into(),
                        "postcode".into(),
                        "country".into(),
                        "locality".into(),
                    ],
                },
            },
        }
    }
}

/// Read a flattened Overture parquet drop into a `Vec<Place>` per
/// the supplied theme preset. Caller stamps `SourceKind::Overture`
/// downstream. Each emitted Place receives a Pelias-compatible
/// `gid` tag derived from the row's `id` column when present
/// (Overture publishes a stable per-feature id), or a deterministic
/// hash of `(kind, name, centroid)` as a fallback for sources whose
/// flattening dropped the column.
pub fn import(path: &Path, theme: Theme) -> Result<Vec<Place>, ImportError> {
    info!(path = %path.display(), ?theme, "importing Overture drop");
    let cfg = theme.parquet_config();
    let mut places = cairn_import_parquet::import(path, &cfg)?;
    let gid_kind = match theme {
        Theme::Places => "place",
        Theme::Addresses => "address",
    };
    stamp_gids(&mut places, gid_kind);
    Ok(places)
}

/// Like [`import`] but lets the caller override individual fields of
/// the theme's default column mapping (e.g. point at a custom
/// `display_name` column instead of `name`). Overlay rules are
/// centralized in [`Config::fill_defaults_from`] so adding a new
/// `Config` field doesn't require editing this wrapper.
pub fn import_with(path: &Path, theme: Theme, mut cfg: Config) -> Result<Vec<Place>, ImportError> {
    cfg.fill_defaults_from(theme.parquet_config());
    let mut places = cairn_import_parquet::import(path, &cfg)?;
    let gid_kind = match theme {
        Theme::Places => "place",
        Theme::Addresses => "address",
    };
    stamp_gids(&mut places, gid_kind);
    Ok(places)
}

/// Stamp a Pelias-style `gid` tag on every Place. Prefers the
/// upstream Overture row id (parquet `id` column folded into tags
/// by the generic loader); falls back to the centroid-quantized
/// hash so flattened drops missing the column still produce stable
/// gids across rebuilds.
fn stamp_gids(places: &mut [Place], gid_kind: &str) {
    for p in places.iter_mut() {
        let upstream = p
            .tags
            .iter()
            .find(|(k, _)| k == "id")
            .map(|(_, v)| v.clone());
        let gid = match upstream.as_deref().and_then(|id| synthesize_gid("overture", gid_kind, id))
        {
            Some(g) => g,
            None => {
                let primary = p
                    .names
                    .iter()
                    .find(|n| n.lang == "default")
                    .or_else(|| p.names.first())
                    .map(|n| n.value.as_str())
                    .unwrap_or("");
                stable_hash_gid("overture", gid_kind, primary, p.centroid)
            }
        };
        p.set_gid(gid);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn theme_parse_canonicals() {
        assert_eq!(Theme::parse("places"), Some(Theme::Places));
        assert_eq!(Theme::parse("Place"), Some(Theme::Places));
        assert_eq!(Theme::parse("ADDRESSES"), Some(Theme::Addresses));
        assert_eq!(Theme::parse("addr"), None);
    }

    #[test]
    fn places_preset_targets_category_kind_column() {
        let cfg = Theme::Places.parquet_config();
        assert_eq!(cfg.map.geometry, "geometry");
        assert_eq!(cfg.map.name, "name");
        assert_eq!(cfg.map.kind.as_deref(), Some("category"));
        assert_eq!(cfg.defaults.kind, "poi");
        assert!(cfg.tags.keep.iter().any(|k| k == "confidence"));
    }

    #[test]
    fn addresses_preset_keeps_postcode_country_in_tags() {
        let cfg = Theme::Addresses.parquet_config();
        assert_eq!(cfg.defaults.kind, "address");
        assert!(cfg.tags.keep.iter().any(|k| k == "postcode"));
        assert!(cfg.tags.keep.iter().any(|k| k == "country"));
    }
}
