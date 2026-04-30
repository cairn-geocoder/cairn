//! Wikidata enrichment for an existing Cairn bundle.
//!
//! v0.3 lane I. **Augmenter**, not importer — runs over a built
//! bundle and rewrites tile blobs in place. Adds:
//!
//! - **Multilingual labels.** A Place tagged `wikidata=Q42` gains
//!   every label in the dump's `labels` map as a [`LocalizedName`].
//!   Operators get free 200+ language coverage on POIs that already
//!   carry a Q-id (typical for landmark POIs imported from OSM).
//! - **Cross-references.** Selected claims (P1566 GeoNames ID, P984
//!   ISO 3166-2, P901 FIPS code, P131 admin parent) flatten into
//!   the Place's `tags` so downstream consumers can join against
//!   external datasets.
//!
//! ## Two-pass design
//!
//! Passing the full Wikidata dump (~120 GB compressed JSONL) is
//! infeasible for memory; we filter aggressively up front.
//!
//! 1. **Walk the bundle** and collect every Q-id referenced in
//!    Place tags (`wikidata` key from OSM imports). Typically
//!    100k–10M Q-ids per planet bundle.
//! 2. **Stream the dump** line by line. Parse only Q-ids in our set;
//!    keep label/claim slices in a `HashMap<qid, WikidataEntry>`.
//!    Discard everything else without allocating.
//!
//! With ~3M tracked Q-ids, the resident set is ~500 MB — comfortably
//! below desktop memory budgets.
//!
//! ## License
//!
//! Wikidata is **CC0**. No attribution constraint, but the
//! per-bundle SBOM still records the source dump version for
//! reproducibility.

use cairn_place::{LocalizedName, Place, PlaceKind};
use flate2::read::GzDecoder;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;
use thiserror::Error;
use tracing::{debug, info, warn};

#[derive(Debug, Error)]
pub enum WikidataError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
}

/// Selected fields extracted per Wikidata entity. Everything else is
/// dropped at parse time to keep the resident set small.
#[derive(Clone, Debug, Default)]
pub struct WikidataEntry {
    pub qid: String,
    pub labels: Vec<(String, String)>,
    pub aliases: Vec<(String, String)>,
    pub geonames_id: Option<String>,
    pub iso_3166_2: Option<String>,
    pub fips_10_4: Option<String>,
    /// Q-id of the immediate admin parent (P131 — "located in the
    /// administrative territorial entity"). When the entity has
    /// multiple, the first non-deprecated one wins.
    pub p131_parent: Option<String>,
    /// Every Q-id from P31 ("instance of") in declaration order,
    /// non-deprecated only. Used to refine [`PlaceKind`] when the
    /// importer was unable to classify the row precisely (e.g. an
    /// OSM POI tagged with `wikidata=Q34442` is actually a road).
    pub p31_types: Vec<String>,
}

/// Map a P31 "instance of" Q-id onto a refined [`PlaceKind`].
/// Conservative table — only entries that map cleanly onto cairn's
/// 10-element kind enum are listed. Wikidata's P31 graph is huge;
/// adding entries blindly would let typo-Q-ids reclassify Places.
const P31_PLACEKIND_TABLE: &[(&str, PlaceKind)] = &[
    // Country / sovereign-state-ish.
    ("Q6256", PlaceKind::Country),
    ("Q3624078", PlaceKind::Country),
    ("Q7275", PlaceKind::Country), // state
    // Region / first-order admin.
    ("Q35657", PlaceKind::Region),  // U.S. state
    ("Q34876", PlaceKind::Region),  // province
    ("Q34772", PlaceKind::Region),  // department of France
    ("Q12046105", PlaceKind::Region), // territory
    ("Q1907114", PlaceKind::Region), // metropolitan area
    // County-tier.
    ("Q484170", PlaceKind::County), // commune of France
    ("Q47168", PlaceKind::County),  // county of the United States
    ("Q1549591", PlaceKind::County), // big city — leave as County until lane J adds population check
    // Cities + settlements.
    ("Q515", PlaceKind::City),
    ("Q486972", PlaceKind::City), // human settlement
    ("Q3957", PlaceKind::City),   // town
    ("Q532", PlaceKind::City),    // village
    ("Q15284", PlaceKind::City),  // municipality
    ("Q5119", PlaceKind::City),   // capital
    ("Q1093829", PlaceKind::City), // city of the United States
    ("Q22746", PlaceKind::City),  // borough of NYC
    // Districts + neighborhoods.
    ("Q174841", PlaceKind::District),
    ("Q123705", PlaceKind::Neighborhood),
    ("Q12076836", PlaceKind::Neighborhood), // suburb
    // Streets + roads.
    ("Q34442", PlaceKind::Street),
    ("Q83620", PlaceKind::Street),
    ("Q25934", PlaceKind::Street), // trail
    // Postcode-ish.
    ("Q37447", PlaceKind::Postcode),
];

fn p31_to_placekind(qid: &str) -> Option<PlaceKind> {
    P31_PLACEKIND_TABLE
        .iter()
        .find_map(|(k, kind)| (*k == qid).then_some(*kind))
}

#[derive(Default, Debug)]
pub struct AugmentStats {
    pub places_examined: u64,
    pub places_with_qid: u64,
    pub qids_found_in_dump: u64,
    pub places_enriched: u64,
    pub labels_added: u64,
    pub aliases_added: u64,
    pub crossrefs_added: u64,
    /// Number of Places whose `kind` was refined by a P31 lookup
    /// (e.g. promoted from Poi to City). Conservative: only Places
    /// currently classified as [`PlaceKind::Poi`] are eligible —
    /// upstream importers that already produced a more specific
    /// kind are trusted.
    pub kind_promotions: u64,
}

/// Walk a `Vec<Place>` (one tile's contents) and extract every Q-id
/// from `tags`. The OSM importer stamps `wikidata=Q1234` on POIs +
/// admin areas that carry the upstream tag.
pub fn collect_qids(places: &[Place]) -> HashSet<String> {
    let mut out = HashSet::new();
    for p in places {
        for (k, v) in &p.tags {
            if k == "wikidata" && v.starts_with('Q') {
                out.insert(v.clone());
            }
        }
    }
    out
}

/// Stream a Wikidata JSONL dump (gzip or plain) and extract entries
/// whose Q-id is in `wanted`. Returns a `HashMap<qid, WikidataEntry>`.
///
/// The streaming reader handles the canonical Wikidata JSON dump
/// format: one entity per line, plus optional opening `[` and
/// trailing `]` lines. Parse errors on any single line are warned
/// and skipped — partial dumps are common in operator workflows.
pub fn stream_dump(
    path: &Path,
    wanted: &HashSet<String>,
) -> Result<HashMap<String, WikidataEntry>, WikidataError> {
    info!(path = %path.display(), wanted = wanted.len(), "streaming Wikidata dump");
    let f = File::open(path)?;
    let reader: Box<dyn Read> = if path
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.eq_ignore_ascii_case("gz"))
        .unwrap_or(false)
    {
        Box::new(GzDecoder::new(f))
    } else {
        Box::new(f)
    };
    let buffered = BufReader::with_capacity(8 * 1024 * 1024, reader);

    let mut out: HashMap<String, WikidataEntry> = HashMap::with_capacity(wanted.len().min(1 << 20));
    let mut lines_seen: u64 = 0;
    let mut lines_kept: u64 = 0;
    for line in buffered.lines() {
        lines_seen += 1;
        let mut s = match line {
            Ok(s) => s,
            Err(err) => {
                warn!(?err, "wikidata line read error");
                continue;
            }
        };
        // Trim wrapping array commas / brackets that the canonical
        // dump emits. After this, `s` is either empty (skip) or a
        // standalone JSON object.
        let trimmed = s.trim_end_matches(&[',', ' ', '\r', '\t'][..]);
        if trimmed.is_empty() || trimmed == "[" || trimmed == "]" {
            continue;
        }
        if !trimmed.starts_with('{') {
            continue;
        }
        s.truncate(trimmed.len());

        // Cheap pre-filter: every entity line carries `"id":"Qxxx"`
        // near the front. Skip the JSON parse if the Q-id isn't in
        // the wanted set.
        if !line_qid_in_set(&s, wanted) {
            continue;
        }

        let raw: RawEntity = match serde_json::from_str(&s) {
            Ok(e) => e,
            Err(err) => {
                debug!(?err, "wikidata entity parse failed");
                continue;
            }
        };
        if !wanted.contains(&raw.id) {
            continue;
        }
        let entry = raw.distill();
        out.insert(entry.qid.clone(), entry);
        lines_kept += 1;
    }
    info!(
        lines_seen,
        lines_kept,
        kept_unique = out.len(),
        "Wikidata stream done"
    );
    Ok(out)
}

/// Scan a JSONL line for `"id":"Q…"` and return true iff the
/// substring is in `wanted`. Avoids parsing the (often >100 KB)
/// entity body when the Q-id is uninteresting.
fn line_qid_in_set(line: &str, wanted: &HashSet<String>) -> bool {
    let needle = "\"id\":\"";
    let i = match line.find(needle) {
        Some(i) => i + needle.len(),
        None => return false,
    };
    let rest = &line[i..];
    let end = match rest.find('"') {
        Some(e) => e,
        None => return false,
    };
    wanted.contains(&rest[..end])
}

/// Apply enrichments to a place list in place. Returns the number of
/// individual edits the caller can sum into `AugmentStats`.
pub fn apply_to_places(
    places: &mut [Place],
    entries: &HashMap<String, WikidataEntry>,
    stats: &mut AugmentStats,
) {
    for p in places.iter_mut() {
        stats.places_examined += 1;
        let qid = p
            .tags
            .iter()
            .find(|(k, _)| k == "wikidata")
            .map(|(_, v)| v.clone());
        let qid = match qid {
            Some(q) => q,
            None => continue,
        };
        stats.places_with_qid += 1;
        let entry = match entries.get(&qid) {
            Some(e) => e,
            None => continue,
        };
        stats.qids_found_in_dump += 1;
        let mut touched = false;
        // Labels: append unique (lang, value) combinations only.
        for (lang, value) in &entry.labels {
            if value.trim().is_empty() {
                continue;
            }
            let already = p
                .names
                .iter()
                .any(|n| &n.lang == lang && &n.value == value);
            if already {
                continue;
            }
            p.names.push(LocalizedName {
                lang: lang.clone(),
                value: value.clone(),
            });
            stats.labels_added += 1;
            touched = true;
        }
        // Aliases: also feed Wikidata's `aliases` map into Place.names
        // under `<lang>_alt`. The text indexer searches every name field
        // verbatim, so an alias becomes a hit even though it doesn't
        // displace the canonical label. The lang-suffix isolates them
        // from the primary label so future per-language ranking can
        // weight aliases lower if needed.
        for (lang, value) in &entry.aliases {
            if value.trim().is_empty() {
                continue;
            }
            let alt_lang = format!("{lang}_alt");
            let already = p
                .names
                .iter()
                .any(|n| n.lang == alt_lang && &n.value == value);
            if already {
                continue;
            }
            p.names.push(LocalizedName {
                lang: alt_lang,
                value: value.clone(),
            });
            stats.aliases_added += 1;
            touched = true;
        }
        // Cross-refs: append on the place's tags. Skip duplicates so
        // re-running the augmenter is idempotent.
        let mut push_tag = |tags: &mut Vec<(String, String)>, k: &str, v: &str| {
            if v.is_empty() {
                return;
            }
            if tags.iter().any(|(ek, ev)| ek == k && ev == v) {
                return;
            }
            tags.push((k.to_string(), v.to_string()));
            stats.crossrefs_added += 1;
            touched = true;
        };
        if let Some(g) = &entry.geonames_id {
            push_tag(&mut p.tags, "geonames_id", g);
        }
        if let Some(iso) = &entry.iso_3166_2 {
            push_tag(&mut p.tags, "iso_3166_2", iso);
        }
        if let Some(fips) = &entry.fips_10_4 {
            push_tag(&mut p.tags, "fips_10_4", fips);
        }
        if let Some(parent) = &entry.p131_parent {
            push_tag(&mut p.tags, "wikidata_parent", parent);
        }
        // P31 → PlaceKind refinement. Only promote when the current
        // kind is `Poi` (the importer's "I don't know" bucket); any
        // more specific classification from upstream is trusted as-is.
        if matches!(p.kind, PlaceKind::Poi) {
            for q in &entry.p31_types {
                if let Some(refined) = p31_to_placekind(q) {
                    if refined != p.kind {
                        p.kind = refined;
                        stats.kind_promotions += 1;
                        touched = true;
                    }
                    break;
                }
            }
        }
        if touched {
            stats.places_enriched += 1;
        }
    }
}

// ============================================================
// JSON wire types — minimal projection of the Wikidata schema
// ============================================================

#[derive(Debug, Deserialize)]
struct RawEntity {
    id: String,
    #[serde(default)]
    labels: HashMap<String, RawTerm>,
    #[serde(default)]
    aliases: HashMap<String, Vec<RawTerm>>,
    #[serde(default)]
    claims: HashMap<String, Vec<RawClaim>>,
}

#[derive(Debug, Deserialize)]
struct RawTerm {
    #[serde(default)]
    language: Option<String>,
    value: String,
}

#[derive(Debug, Deserialize)]
struct RawClaim {
    #[serde(default)]
    mainsnak: Option<RawSnak>,
    #[serde(default)]
    rank: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawSnak {
    #[serde(default)]
    datavalue: Option<RawDataValue>,
}

#[derive(Debug, Deserialize)]
struct RawDataValue {
    #[serde(rename = "type", default)]
    kind: Option<String>,
    #[serde(default)]
    value: serde_json::Value,
}

impl RawEntity {
    fn distill(self) -> WikidataEntry {
        let labels: Vec<(String, String)> = self
            .labels
            .into_iter()
            .map(|(lang, t)| (t.language.unwrap_or(lang), t.value))
            .collect();
        let aliases: Vec<(String, String)> = self
            .aliases
            .into_iter()
            .flat_map(|(lang, ts)| {
                ts.into_iter()
                    .map(move |t| (t.language.clone().unwrap_or_else(|| lang.clone()), t.value))
            })
            .collect();
        let geonames_id = first_string_claim(&self.claims, "P1566");
        let iso_3166_2 = first_string_claim(&self.claims, "P300");
        let fips_10_4 = first_string_claim(&self.claims, "P901");
        let p131_parent = first_entity_claim(&self.claims, "P131");
        let p31_types = all_entity_claims(&self.claims, "P31");
        WikidataEntry {
            qid: self.id,
            labels,
            aliases,
            geonames_id,
            iso_3166_2,
            fips_10_4,
            p131_parent,
            p31_types,
        }
    }
}

fn first_string_claim(claims: &HashMap<String, Vec<RawClaim>>, prop: &str) -> Option<String> {
    let cs = claims.get(prop)?;
    for c in cs {
        if c.rank.as_deref() == Some("deprecated") {
            continue;
        }
        let dv = c.mainsnak.as_ref()?.datavalue.as_ref()?;
        if dv.kind.as_deref() == Some("string") {
            if let Some(s) = dv.value.as_str() {
                return Some(s.to_string());
            }
        }
    }
    None
}

fn first_entity_claim(claims: &HashMap<String, Vec<RawClaim>>, prop: &str) -> Option<String> {
    let cs = claims.get(prop)?;
    for c in cs {
        if c.rank.as_deref() == Some("deprecated") {
            continue;
        }
        let dv = c.mainsnak.as_ref()?.datavalue.as_ref()?;
        if dv.kind.as_deref() == Some("wikibase-entityid") {
            if let Some(qid) = dv.value.get("id").and_then(|v| v.as_str()) {
                return Some(qid.to_string());
            }
            if let Some(num) = dv.value.get("numeric-id").and_then(|v| v.as_u64()) {
                return Some(format!("Q{num}"));
            }
        }
    }
    None
}

/// Like [`first_entity_claim`] but returns every non-deprecated
/// Q-id for `prop`, in declaration order. Used for P31 because an
/// entity can be an instance of several types; the apply step
/// iterates and picks the first one our table maps onto a kind.
fn all_entity_claims(claims: &HashMap<String, Vec<RawClaim>>, prop: &str) -> Vec<String> {
    let mut out = Vec::new();
    let cs = match claims.get(prop) {
        Some(c) => c,
        None => return out,
    };
    for c in cs {
        if c.rank.as_deref() == Some("deprecated") {
            continue;
        }
        let Some(dv) = c.mainsnak.as_ref().and_then(|s| s.datavalue.as_ref()) else {
            continue;
        };
        if dv.kind.as_deref() != Some("wikibase-entityid") {
            continue;
        }
        if let Some(qid) = dv.value.get("id").and_then(|v| v.as_str()) {
            out.push(qid.to_string());
        } else if let Some(num) = dv.value.get("numeric-id").and_then(|v| v.as_u64()) {
            out.push(format!("Q{num}"));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_place::{Coord, PlaceId, PlaceKind};

    fn place_with_qid(qid: &str) -> Place {
        Place {
            id: PlaceId::new(2, 1, 0).unwrap(),
            kind: PlaceKind::Poi,
            names: vec![LocalizedName {
                lang: "default".into(),
                value: "Test".into(),
            }],
            centroid: Coord { lon: 0.0, lat: 0.0 },
            admin_path: vec![],
            tags: vec![("wikidata".into(), qid.into())],
        }
    }

    #[test]
    fn collect_qids_picks_up_wikidata_tag() {
        let p = place_with_qid("Q42");
        let s = collect_qids(&[p]);
        assert!(s.contains("Q42"));
    }

    #[test]
    fn line_qid_in_set_finds_id_quickly() {
        let mut wanted = HashSet::new();
        wanted.insert("Q42".to_string());
        let line = r#"{"id":"Q42","labels":{"en":{"language":"en","value":"x"}}}"#;
        assert!(line_qid_in_set(line, &wanted));
        let other = r#"{"id":"Q99","labels":{}}"#;
        assert!(!line_qid_in_set(other, &wanted));
    }

    #[test]
    fn distill_extracts_label_and_geonames_id() {
        let json = r#"{
            "id": "Q42",
            "labels": {
                "en": {"language": "en", "value": "Adams"},
                "fr": {"language": "fr", "value": "Adams"}
            },
            "claims": {
                "P1566": [
                    {"mainsnak": {"datavalue": {"type": "string", "value": "12345"}}}
                ]
            }
        }"#;
        let raw: RawEntity = serde_json::from_str(json).unwrap();
        let entry = raw.distill();
        assert_eq!(entry.qid, "Q42");
        assert_eq!(entry.geonames_id.as_deref(), Some("12345"));
        assert_eq!(entry.labels.len(), 2);
    }

    #[test]
    fn apply_adds_labels_and_crossrefs_idempotently() {
        let p = place_with_qid("Q42");
        let entry = WikidataEntry {
            qid: "Q42".into(),
            labels: vec![
                ("en".into(), "Adams".into()),
                ("fr".into(), "Adams".into()),
            ],
            aliases: vec![
                ("en".into(), "Douglas Adams".into()),
                ("en".into(), "DNA".into()),
            ],
            geonames_id: Some("12345".into()),
            iso_3166_2: None,
            fips_10_4: None,
            p131_parent: Some("Q5".into()),
            p31_types: vec![],
        };
        let mut entries = HashMap::new();
        entries.insert("Q42".to_string(), entry);
        let mut stats = AugmentStats::default();
        let mut places = vec![p.clone()];
        apply_to_places(&mut places, &entries, &mut stats);
        // default + en + fr labels + 2 en_alt aliases = 5 names total.
        assert_eq!(places[0].names.len(), 5);
        assert_eq!(stats.labels_added, 2);
        assert_eq!(stats.aliases_added, 2);
        assert!(places[0].names.iter().any(|n| n.lang == "en_alt" && n.value == "Douglas Adams"));
        assert!(places[0].tags.iter().any(|(k, _)| k == "geonames_id"));
        assert!(places[0].tags.iter().any(|(k, _)| k == "wikidata_parent"));
        // Re-run is idempotent: same counts.
        let pre_names = places[0].names.len();
        let pre_tags = places[0].tags.len();
        apply_to_places(&mut places, &entries, &mut stats);
        assert_eq!(places[0].names.len(), pre_names);
        assert_eq!(places[0].tags.len(), pre_tags);
        let _ = p;
    }

    #[test]
    fn p31_promotes_poi_to_city() {
        let p = place_with_qid("Q42");
        // Place is currently Poi (importer's "I don't know" default).
        assert!(matches!(p.kind, PlaceKind::Poi));

        let entry = WikidataEntry {
            qid: "Q42".into(),
            labels: vec![],
            aliases: vec![],
            geonames_id: None,
            iso_3166_2: None,
            fips_10_4: None,
            p131_parent: None,
            // First non-mapped Q-id ignored, second matches the table.
            p31_types: vec!["Q9999999".into(), "Q515".into()], // Q515 = city
        };
        let mut entries = HashMap::new();
        entries.insert("Q42".to_string(), entry);
        let mut stats = AugmentStats::default();
        let mut places = vec![p.clone()];
        apply_to_places(&mut places, &entries, &mut stats);
        assert!(matches!(places[0].kind, PlaceKind::City));
        assert_eq!(stats.kind_promotions, 1);
        // Re-run does nothing — already a City, no longer eligible.
        apply_to_places(&mut places, &entries, &mut stats);
        assert_eq!(stats.kind_promotions, 1);
        let _ = p;
    }

    #[test]
    fn p31_does_not_demote_specific_kinds() {
        let mut p = place_with_qid("Q42");
        p.kind = PlaceKind::Country;
        let entry = WikidataEntry {
            qid: "Q42".into(),
            labels: vec![],
            aliases: vec![],
            geonames_id: None,
            iso_3166_2: None,
            fips_10_4: None,
            p131_parent: None,
            p31_types: vec!["Q515".into()], // city — would demote Country to City
        };
        let mut entries = HashMap::new();
        entries.insert("Q42".to_string(), entry);
        let mut stats = AugmentStats::default();
        let mut places = vec![p];
        apply_to_places(&mut places, &entries, &mut stats);
        assert!(matches!(places[0].kind, PlaceKind::Country));
        assert_eq!(stats.kind_promotions, 0);
    }

    #[test]
    fn first_entity_claim_handles_id_and_numeric_id() {
        let mut claims = HashMap::new();
        let json = r#"{"mainsnak":{"datavalue":{"type":"wikibase-entityid","value":{"id":"Q5","numeric-id":5}}}}"#;
        let c: RawClaim = serde_json::from_str(json).unwrap();
        claims.insert("P131".to_string(), vec![c]);
        assert_eq!(first_entity_claim(&claims, "P131").as_deref(), Some("Q5"));
    }
}
