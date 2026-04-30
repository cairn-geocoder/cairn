//! OpenAddresses CSV → address `Place` stream.
//!
//! Phase 4 scope:
//! - Read OA-style CSVs (`LON,LAT,NUMBER,STREET,UNIT,CITY,DISTRICT,REGION,
//!   POSTCODE,ID,HASH`) with case-insensitive headers.
//! - Emit one `Place` per row with `kind = Address`, level L2,
//!   `name = "{number} {street}"`.
//! - Skip rows without coordinates, without a number, or without a street.
//!
//! Interpolation along OSM ways arrives in a follow-up phase.

use cairn_place::{
    stable_hash_gid, Coord, LocalizedName, Place, PlaceId, PlaceKind, GID_TAG,
};
use cairn_tile::{Level, TileCoord};
use serde::Deserialize;
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

#[derive(Debug, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
struct OaRow {
    #[serde(default)]
    lon: Option<f64>,
    #[serde(default)]
    lat: Option<f64>,
    #[serde(default)]
    number: Option<String>,
    #[serde(default)]
    street: Option<String>,
    #[serde(default)]
    unit: Option<String>,
    #[serde(default)]
    city: Option<String>,
    #[serde(default)]
    district: Option<String>,
    #[serde(default)]
    region: Option<String>,
    #[serde(default)]
    postcode: Option<String>,
}

#[derive(Default)]
struct Counters {
    rows_seen: u64,
    rows_emitted: u64,
    skipped_no_coords: u64,
    skipped_no_number: u64,
    skipped_no_street: u64,
}

pub fn import(csv_path: &Path) -> Result<Vec<Place>, ImportError> {
    info!(path = %csv_path.display(), "opening OpenAddresses CSV");
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_path(csv_path)?;

    let mut places = Vec::new();
    let mut counters = Counters::default();
    let mut local_counters: HashMap<(u8, u32), u64> = HashMap::new();

    for record in reader.deserialize() {
        let row: OaRow = record?;
        counters.rows_seen += 1;
        let (lon, lat) = match (row.lon, row.lat) {
            (Some(lon), Some(lat)) if !(lon == 0.0 && lat == 0.0) => (lon, lat),
            _ => {
                counters.skipped_no_coords += 1;
                continue;
            }
        };
        let number = match row
            .number
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            Some(n) => n.to_string(),
            None => {
                counters.skipped_no_number += 1;
                continue;
            }
        };
        let street = match row
            .street
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            Some(s) => s.to_string(),
            None => {
                counters.skipped_no_street += 1;
                continue;
            }
        };

        let mut display = format!("{number} {street}");
        if let Some(unit) = row.unit.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
            display.push_str(", ");
            display.push_str(unit);
        }

        let centroid = Coord { lon, lat };
        let level = Level::L2;
        let tile = TileCoord::from_coord(level, centroid);
        let key = (level.as_u8(), tile.id());
        let local = local_counters.entry(key).or_insert(0);
        let local_id = *local;
        *local += 1;
        let id = match PlaceId::new(level.as_u8(), tile.id(), local_id) {
            Ok(id) => id,
            Err(err) => {
                debug!(?err, "PlaceId overflow on OA row; skipping");
                continue;
            }
        };

        let mut tags: Vec<(String, String)> = vec![("source".into(), "openaddresses".into())];
        for (k, v) in [
            ("addr:housenumber", Some(number.clone())),
            ("addr:street", Some(street.clone())),
            ("addr:unit", row.unit.clone()),
            ("addr:city", row.city.clone()),
            ("addr:district", row.district.clone()),
            ("addr:region", row.region.clone()),
            ("addr:postcode", row.postcode.clone()),
        ] {
            if let Some(value) = v.and_then(|s| {
                let t = s.trim().to_string();
                if t.is_empty() {
                    None
                } else {
                    Some(t)
                }
            }) {
                tags.push((k.into(), value));
            }
        }

        // OpenAddresses rows don't carry a stable upstream id —
        // each per-municipality CSV decides its own scheme and the
        // values churn across releases. Use the deterministic hash
        // of (kind, normalized name, ~100m centroid quantize) so a
        // bookmark survives rebuilds at least when the underlying
        // address point doesn't move.
        tags.push((
            GID_TAG.into(),
            stable_hash_gid("oa", "address", &display, centroid),
        ));
        places.push(Place {
            id,
            kind: PlaceKind::Address,
            names: vec![LocalizedName {
                lang: "default".into(),
                value: display,
            }],
            centroid,
            admin_path: vec![],
            tags,
        });
        counters.rows_emitted += 1;
    }

    info!(
        rows_seen = counters.rows_seen,
        emitted = counters.rows_emitted,
        skipped_no_coords = counters.skipped_no_coords,
        skipped_no_number = counters.skipped_no_number,
        skipped_no_street = counters.skipped_no_street,
        "OpenAddresses import done"
    );
    Ok(places)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn tempfile_with_content(content: &str) -> std::path::PathBuf {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "cairn-oa-test-{}-{}-{}.csv",
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
    fn imports_addresses_with_unit_postcode() {
        let csv = "LON,LAT,NUMBER,STREET,UNIT,CITY,DISTRICT,REGION,POSTCODE,ID,HASH\n\
                   9.5209,47.1410,12,Aeulestrasse,4B,Vaduz,,Liechtenstein,9490,abc,xyz\n\
                   9.5095,47.1650,33,Landstrasse,,Schaan,,Liechtenstein,9494,def,uvw\n";
        let path = tempfile_with_content(csv);
        let places = import(&path).unwrap();
        assert_eq!(places.len(), 2);

        let p = &places[0];
        assert_eq!(p.kind, PlaceKind::Address);
        assert_eq!(p.id.level(), 2);
        assert_eq!(p.names[0].value, "12 Aeulestrasse, 4B");
        assert!(p
            .tags
            .iter()
            .any(|(k, v)| k == "addr:postcode" && v == "9490"));
        assert!(p.tags.iter().any(|(k, v)| k == "addr:city" && v == "Vaduz"));
    }

    #[test]
    fn skips_invalid_rows() {
        let csv = "LON,LAT,NUMBER,STREET\n\
                   ,,12,Some St\n\
                   1.0,1.0,,Empty Number St\n\
                   1.0,1.0,5,\n\
                   1.0,1.0,5,Real St\n";
        let path = tempfile_with_content(csv);
        let places = import(&path).unwrap();
        assert_eq!(places.len(), 1);
        assert_eq!(places[0].names[0].value, "5 Real St");
    }
}
