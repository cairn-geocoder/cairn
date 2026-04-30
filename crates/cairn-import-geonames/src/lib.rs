//! Geonames TSV → populated-place `Place` stream.
//!
//! Phase 6a scope:
//! - Parse the standard Geonames `cities*.txt` / `allCountries.txt`
//!   tab-separated dump.
//! - Keep `feature_class = 'P'` (populated places). Skip historic and
//!   abandoned variants.
//! - Map `feature_code` to `PlaceKind` (City / Neighborhood).
//! - Use the canonical `name` plus `asciiname` (when different) as
//!   localized name variants.
//!
//! Multilingual alternate names are not loaded from this file (the
//! comma-separated `alternatenames` column has no language tags). The
//! richer `alternateNamesV2.zip` dump is a follow-up.

use cairn_place::{synthesize_gid, Coord, LocalizedName, Place, PlaceId, PlaceKind, GID_TAG};
use cairn_tile::{Level, TileCoord};
use std::collections::HashMap;
use std::path::Path;
use thiserror::Error;
use tracing::{debug, info};

#[derive(Debug, Error)]
pub enum ImportError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("csv: {0}")]
    Csv(#[from] csv::Error),
    #[error("placeid: {0}")]
    PlaceId(#[from] cairn_place::PlaceIdError),
}

#[derive(Default)]
struct Counters {
    rows_seen: u64,
    rows_emitted: u64,
    skipped_not_populated: u64,
    skipped_no_centroid: u64,
    skipped_unknown_code: u64,
}

const COL_GEONAMEID: usize = 0;
const COL_NAME: usize = 1;
const COL_ASCII: usize = 2;
const COL_LAT: usize = 4;
const COL_LON: usize = 5;
const COL_FEATURE_CLASS: usize = 6;
const COL_FEATURE_CODE: usize = 7;
const COL_COUNTRY: usize = 8;
const COL_POPULATION: usize = 14;
const TOTAL_COLS: usize = 19;

pub fn import(tsv_path: &Path) -> Result<Vec<Place>, ImportError> {
    info!(path = %tsv_path.display(), "opening Geonames TSV");
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .delimiter(b'\t')
        .flexible(true)
        .quoting(false)
        .from_path(tsv_path)?;

    let mut places = Vec::new();
    let mut counters = Counters::default();
    let mut local_counters: HashMap<(u8, u32), u64> = HashMap::new();

    for record in reader.records() {
        let row = record?;
        counters.rows_seen += 1;
        if row.len() < TOTAL_COLS {
            continue;
        }

        let feature_class = row.get(COL_FEATURE_CLASS).unwrap_or("");
        if feature_class != "P" {
            counters.skipped_not_populated += 1;
            continue;
        }
        let feature_code = row.get(COL_FEATURE_CODE).unwrap_or("");
        let kind = match map_feature_code(feature_code) {
            Some(k) => k,
            None => {
                counters.skipped_unknown_code += 1;
                continue;
            }
        };

        let lat: f64 = row
            .get(COL_LAT)
            .and_then(|s| s.parse().ok())
            .unwrap_or(f64::NAN);
        let lon: f64 = row
            .get(COL_LON)
            .and_then(|s| s.parse().ok())
            .unwrap_or(f64::NAN);
        if !lat.is_finite() || !lon.is_finite() {
            counters.skipped_no_centroid += 1;
            continue;
        }

        let name = row.get(COL_NAME).unwrap_or("").trim().to_string();
        if name.is_empty() {
            counters.skipped_no_centroid += 1;
            continue;
        }
        let ascii = row.get(COL_ASCII).unwrap_or("").trim().to_string();
        let mut names = vec![LocalizedName {
            lang: "default".into(),
            value: name.clone(),
        }];
        if !ascii.is_empty() && ascii != name {
            names.push(LocalizedName {
                lang: "ascii".into(),
                value: ascii,
            });
        }

        let centroid = Coord { lon, lat };
        let level = level_for_kind(kind);
        let tile = TileCoord::from_coord(level, centroid);
        let key = (level.as_u8(), tile.id());
        let local = local_counters.entry(key).or_insert(0);
        let local_id = *local;
        *local += 1;
        let id = match PlaceId::new(level.as_u8(), tile.id(), local_id) {
            Ok(id) => id,
            Err(err) => {
                debug!(?err, "PlaceId overflow on Geonames row; skipping");
                continue;
            }
        };

        let mut tags: Vec<(String, String)> = vec![
            ("source".into(), "geonames".into()),
            ("feature_code".into(), feature_code.to_string()),
        ];
        if let Some(country) = row
            .get(COL_COUNTRY)
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            tags.push(("ISO3166-1".into(), country.to_string()));
        }
        if let Some(pop) = row
            .get(COL_POPULATION)
            .and_then(|s| s.trim().parse::<u64>().ok())
        {
            if pop > 0 {
                tags.push(("population".into(), pop.to_string()));
            }
        }
        if let Some(geonameid) = row
            .get(COL_GEONAMEID)
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            tags.push(("geonameid".into(), geonameid.to_string()));
            // Pelias ships geonames features as `geonames:venue:<id>`
            // for POIs and `geonames:locality:<id>` for cities. Use
            // the importer's already-resolved PlaceKind for the type
            // slot so the gid carries the same semantic as the row's
            // place classification.
            if let Some(gid) = synthesize_gid("geonames", kind_slug(kind), geonameid) {
                tags.push((GID_TAG.into(), gid));
            }
        }

        places.push(Place {
            id,
            kind,
            names,
            centroid,
            admin_path: vec![],
            tags,
        });
        counters.rows_emitted += 1;
    }

    info!(
        rows_seen = counters.rows_seen,
        emitted = counters.rows_emitted,
        skipped_not_populated = counters.skipped_not_populated,
        skipped_no_centroid = counters.skipped_no_centroid,
        skipped_unknown_code = counters.skipped_unknown_code,
        "Geonames import done"
    );
    Ok(places)
}

/// Geonames postcode dump (`<CC>.txt` from
/// `download.geonames.org/export/zip/`) → `Place(kind=Postcode)`
/// stream.
///
/// Format: 12 tab-separated columns —
///   `country | postal_code | place_name | admin1_name | admin1_code |
///    admin2_name | admin2_code | admin3_name | admin3_code |
///    latitude | longitude | accuracy`
///
/// Each row becomes one Place: name = postal code, ASCII-folded
/// duplicate added if place_name carries diacritics. Country and
/// optional accuracy are stored in tags so reverse lookups can
/// surface them. Centroid uses (lon, lat); rows with non-numeric
/// coords are skipped.
pub fn import_postcodes(tsv_path: &Path) -> Result<Vec<Place>, ImportError> {
    info!(path = %tsv_path.display(), "opening Geonames postcode TSV");
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .delimiter(b'\t')
        .flexible(true)
        .quoting(false)
        .from_path(tsv_path)?;

    const COL_COUNTRY: usize = 0;
    const COL_POSTAL: usize = 1;
    const COL_PLACE: usize = 2;
    const COL_ADMIN1_NAME: usize = 3;
    const COL_LAT: usize = 9;
    const COL_LON: usize = 10;
    const COL_ACCURACY: usize = 11;
    const POSTCODE_TOTAL_COLS: usize = 12;

    let mut places = Vec::new();
    let mut local_counters: HashMap<(u8, u32), u64> = HashMap::new();
    let mut rows_seen = 0u64;
    let mut emitted = 0u64;
    let mut skipped_no_centroid = 0u64;
    let mut skipped_no_postal = 0u64;

    for record in reader.records() {
        let row = record?;
        rows_seen += 1;
        if row.len() < POSTCODE_TOTAL_COLS {
            continue;
        }

        let postal = row.get(COL_POSTAL).unwrap_or("").trim();
        if postal.is_empty() {
            skipped_no_postal += 1;
            continue;
        }
        let lat: f64 = row
            .get(COL_LAT)
            .and_then(|s| s.parse().ok())
            .unwrap_or(f64::NAN);
        let lon: f64 = row
            .get(COL_LON)
            .and_then(|s| s.parse().ok())
            .unwrap_or(f64::NAN);
        if !lat.is_finite() || !lon.is_finite() {
            skipped_no_centroid += 1;
            continue;
        }
        let centroid = Coord { lon, lat };
        let level = level_for_kind(PlaceKind::Postcode);
        let tile = TileCoord::from_coord(level, centroid);
        let key = (level.as_u8(), tile.id());
        let local = local_counters.entry(key).or_insert(0);
        let local_id = *local;
        *local += 1;
        let id = match PlaceId::new(level.as_u8(), tile.id(), local_id) {
            Ok(id) => id,
            Err(err) => {
                debug!(?err, "PlaceId overflow on postcode row; skipping");
                continue;
            }
        };

        let mut names = vec![LocalizedName {
            lang: "default".into(),
            value: postal.to_string(),
        }];
        let place_name = row.get(COL_PLACE).unwrap_or("").trim();
        if !place_name.is_empty() {
            // Composite "<postal> <place>" gives autocomplete a
            // searchable variant — typing the postal code OR the
            // bound town finds the row. tantivy multi-value field
            // means both terms tokenize independently.
            names.push(LocalizedName {
                lang: "default".into(),
                value: format!("{postal} {place_name}"),
            });
        }

        let mut tags: Vec<(String, String)> = vec![("source".into(), "geonames".into())];
        let country = row
            .get(COL_COUNTRY)
            .map(str::trim)
            .filter(|s| !s.is_empty());
        if let Some(c) = country {
            tags.push(("ISO3166-1".into(), c.to_string()));
        }
        if !place_name.is_empty() {
            tags.push(("addr:city".into(), place_name.to_string()));
        }
        if let Some(admin1) = row
            .get(COL_ADMIN1_NAME)
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            tags.push(("addr:state".into(), admin1.to_string()));
        }
        if let Some(accuracy) = row
            .get(COL_ACCURACY)
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            tags.push(("accuracy".into(), accuracy.to_string()));
        }
        // Postcode TSV has no upstream stable id; the stable
        // composite key is `<country>-<postal_code>` (geonames keeps
        // postal codes country-scoped). Synthesize a Pelias-style
        // gid from that — same shape Pelias's Geonames postal
        // importer produces.
        if let Some(c) = country {
            if let Some(gid) = synthesize_gid("geonames", "postalcode", &format!("{c}-{postal}")) {
                tags.push((GID_TAG.into(), gid));
            }
        }

        places.push(Place {
            id,
            kind: PlaceKind::Postcode,
            names,
            centroid,
            admin_path: vec![],
            tags,
        });
        emitted += 1;
    }

    info!(
        rows_seen,
        emitted, skipped_no_centroid, skipped_no_postal, "Geonames postcode import done"
    );
    Ok(places)
}

/// PlaceKind slug used in [`cairn_place::GID_TAG`] values for
/// Geonames rows. Picks Pelias-style layer names so a Cairn-emitted
/// gid is interchangeable with a Pelias-emitted one for the same
/// underlying record.
fn kind_slug(kind: PlaceKind) -> &'static str {
    match kind {
        PlaceKind::Country => "country",
        PlaceKind::Region => "region",
        PlaceKind::County => "county",
        PlaceKind::City => "locality",
        PlaceKind::District => "borough",
        PlaceKind::Neighborhood => "neighbourhood",
        PlaceKind::Street => "street",
        PlaceKind::Address => "address",
        PlaceKind::Postcode => "postalcode",
        PlaceKind::Poi => "venue",
    }
}

fn map_feature_code(code: &str) -> Option<PlaceKind> {
    Some(match code {
        // Capital, administrative seats, generic populated places.
        "PPLC" | "PPLA" | "PPLA2" | "PPLA3" | "PPLA4" | "PPLA5" | "PPLG" | "PPL" | "PPLS"
        | "PPLF" | "PPLR" | "PPLL" | "STLMT" => PlaceKind::City,
        // Sections of populated place + abandoned/destroyed variants we want.
        "PPLX" => PlaceKind::Neighborhood,
        // Historical / abandoned: skip.
        "PPLH" | "PPLW" | "PPLQ" | "PPLCH" => return None,
        _ => return None,
    })
}

fn level_for_kind(kind: PlaceKind) -> Level {
    match kind {
        PlaceKind::Country | PlaceKind::Region => Level::L0,
        PlaceKind::County | PlaceKind::City | PlaceKind::Postcode => Level::L1,
        PlaceKind::District
        | PlaceKind::Neighborhood
        | PlaceKind::Street
        | PlaceKind::Address
        | PlaceKind::Poi => Level::L2,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn tmp_tsv(content: &str) -> std::path::PathBuf {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "cairn-geonames-test-{}-{}-{}.txt",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            COUNTER.fetch_add(1, Ordering::Relaxed),
        ));
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(content.as_bytes()).unwrap();
        path
    }

    #[test]
    fn imports_capital_with_population() {
        // 19 tab-separated columns per Geonames spec.
        let tsv = "3042030\tVaduz\tVaduz\t\t47.14151\t9.52154\tP\tPPLC\tLI\t\t04\t\t\t\t5450\t460\t460\tEurope/Vaduz\t2024-01-01\n";
        let path = tmp_tsv(tsv);
        let places = import(&path).unwrap();
        assert_eq!(places.len(), 1);
        let p = &places[0];
        assert_eq!(p.kind, PlaceKind::City);
        assert_eq!(p.names[0].value, "Vaduz");
        assert!(p.tags.iter().any(|(k, v)| k == "population" && v == "5450"));
        assert!(p.tags.iter().any(|(k, v)| k == "ISO3166-1" && v == "LI"));
    }

    #[test]
    fn skips_non_populated_classes() {
        let tsv = concat!(
            "1\tMt Foo\tMt Foo\t\t10.0\t10.0\tT\tMT\tCH\t\t\t\t\t\t0\t\t\t\t\n",
            "2\tFoo Park\tFoo Park\t\t10.0\t10.1\tL\tPRK\tCH\t\t\t\t\t\t0\t\t\t\t\n",
            "3\tFoo City\tFoo City\t\t10.0\t10.2\tP\tPPL\tCH\t\t\t\t\t\t100\t\t\t\t\n",
        );
        let path = tmp_tsv(tsv);
        let places = import(&path).unwrap();
        assert_eq!(places.len(), 1);
        assert_eq!(places[0].names[0].value, "Foo City");
    }

    #[test]
    fn imports_postcode_row() {
        // Real Geonames postcode dump format. Vaduz, LI, 9490.
        let tsv = "LI\t9490\tVaduz\tVaduz\t\t\t\t\t\t47.1410\t9.5215\t1\n";
        let path = tmp_tsv(tsv);
        let places = import_postcodes(&path).unwrap();
        assert_eq!(places.len(), 1);
        let p = &places[0];
        assert_eq!(p.kind, PlaceKind::Postcode);
        assert_eq!(p.names[0].value, "9490");
        // Composite alias with bound place name.
        assert!(p.names.iter().any(|n| n.value == "9490 Vaduz"));
        assert!(p.tags.iter().any(|(k, v)| k == "ISO3166-1" && v == "LI"));
        assert!(p.tags.iter().any(|(k, v)| k == "addr:city" && v == "Vaduz"));
        assert_eq!(p.centroid.lon, 9.5215);
        assert_eq!(p.centroid.lat, 47.141);
    }

    #[test]
    fn skips_postcode_with_no_postal_code() {
        let tsv = "LI\t\tVaduz\tVaduz\t\t\t\t\t\t47.14\t9.52\t1\n";
        let path = tmp_tsv(tsv);
        let places = import_postcodes(&path).unwrap();
        assert!(places.is_empty());
    }

    #[test]
    fn skips_postcode_with_bad_coords() {
        let tsv = "LI\t9490\tVaduz\tVaduz\t\t\t\t\t\tNaN\tNaN\t1\n";
        let path = tmp_tsv(tsv);
        let places = import_postcodes(&path).unwrap();
        assert!(places.is_empty());
    }

    #[test]
    fn ascii_alias_added_when_distinct() {
        let tsv = "1\tZürich\tZurich\t\t47.37\t8.55\tP\tPPLA\tCH\t\t\t\t\t\t400000\t\t\t\t\n";
        let path = tmp_tsv(tsv);
        let places = import(&path).unwrap();
        assert_eq!(places.len(), 1);
        assert_eq!(places[0].names.len(), 2);
        assert!(places[0]
            .names
            .iter()
            .any(|n| n.lang == "default" && n.value == "Zürich"));
        assert!(places[0]
            .names
            .iter()
            .any(|n| n.lang == "ascii" && n.value == "Zurich"));
    }
}
