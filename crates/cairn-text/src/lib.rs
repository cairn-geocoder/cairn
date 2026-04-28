//! Text indexing (tantivy) + autocomplete + geo-bias + fuzzy.
//!
//! Phase 2.5 scope:
//! - Single doc per Place, multi-value name field carries every localized name.
//! - `name_prefix` carries prefix-ngram terms for autocomplete.
//! - Stored fields hydrate hits: place_id, level, kind, lon, lat, admin_path.
//! - Search supports a layer filter (kind allowlist), fuzzy edit distance
//!   (forward mode), and a focus point that re-ranks top candidates by
//!   distance.

use cairn_place::{Coord, Place, PlaceKind};
use serde::{Deserialize, Serialize};
use std::path::Path;
use tantivy::collector::TopDocs;
use tantivy::query::{BooleanQuery, FuzzyTermQuery, Occur, Query, QueryParser, TermQuery};
use tantivy::schema::{
    Field, IndexRecordOption, Schema, TextFieldIndexing, TextOptions, Value, FAST, INDEXED, STORED,
    STRING, TEXT,
};
use tantivy::tokenizer::{LowerCaser, NgramTokenizer, RemoveLongFilter, TextAnalyzer};
use tantivy::{Index, IndexReader, IndexWriter, ReloadPolicy, TantivyDocument, Term};
use thiserror::Error;
use tracing::debug;

const PREFIX_TOKENIZER: &str = "cairn_prefix";
const CJK_TOKENIZER: &str = "cairn_cjk";
const PREFIX_MIN: usize = 1;
const PREFIX_MAX: usize = 25;
const CJK_NGRAM_MIN: usize = 1;
const CJK_NGRAM_MAX: usize = 2;
const WRITER_HEAP: usize = 64 * 1024 * 1024;
const RERANK_MULTIPLIER: usize = 5;
const MAX_FUZZY_DISTANCE: u8 = 2;
/// Multiplier applied to a Hit's BM25 score when the lowercased,
/// deunicoded query string equals one of its stored names. 4.0 lifts a
/// "Vaduz" search past a noisy POI containing "Vaduz" in its name —
/// without nuking BM25 entirely, so well-ranked partial matches still
/// surface.
const EXACT_MATCH_BOOST: f32 = 4.0;
/// Per-hit popularity weighting. Score is multiplied by
/// `1 + log10(1+population) * POPULATION_BOOST_RATE`. With rate 0.1,
/// pop 100 → +0.2x; pop 10k → +0.4x; pop 1M → +0.6x; pop 10M → +0.7x.
/// Modest enough not to overwhelm exact-match (4×) or BM25, large
/// enough to break ties — Berlin beats a 200-population hamlet that
/// happens to match the query.
const POPULATION_BOOST_RATE: f32 = 0.1;

/// ASCII-fold + script transliterate a string via `deunicode`.
/// Returns `None` when the result equals the input (already ASCII)
/// — there's no point indexing it again. Used at index time to add
/// "moskva" alongside "Москва", "athena" alongside "Αθήνα", and
/// "Munchen" alongside "München", so a Latin-keyboard query finds
/// non-Latin records and vice versa.
pub fn ascii_fold(s: &str) -> Option<String> {
    let folded = deunicode::deunicode(s);
    if folded == s {
        None
    } else {
        Some(folded)
    }
}

/// Returns true if the input contains at least one character in the
/// CJK script ranges. Picks up CJK Unified Ideographs, Hiragana,
/// Katakana, Hangul, and the common compatibility blocks. Used to
/// decide whether to OR the `name_cjk` analyzer in at query time.
pub fn has_cjk(s: &str) -> bool {
    s.chars().any(|c| {
        let cp = c as u32;
        (0x3040..=0x30FF).contains(&cp)        // Hiragana + Katakana
            || (0x3400..=0x4DBF).contains(&cp) // CJK Ext A
            || (0x4E00..=0x9FFF).contains(&cp) // CJK Unified
            || (0xAC00..=0xD7AF).contains(&cp) // Hangul Syllables
            || (0xF900..=0xFAFF).contains(&cp) // CJK Compat
    })
}

#[derive(Debug, Error)]
pub enum TextError {
    #[error("tantivy: {0}")]
    Tantivy(#[from] tantivy::TantivyError),
    #[error("query: {0}")]
    Query(#[from] tantivy::query::QueryParserError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Clone, Debug, Serialize)]
pub struct Hit {
    pub place_id: u64,
    pub name: String,
    pub kind: String,
    pub level: u64,
    pub lon: f64,
    pub lat: f64,
    pub score: f32,
    pub admin_path: Vec<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub distance_km: Option<f64>,
    /// Place population when available (Geonames stamps it; WoF
    /// occasionally; OSM rarely). 0 = unknown.
    #[serde(skip_serializing_if = "is_zero_u64")]
    pub population: u64,
}

fn is_zero_u64(n: &u64) -> bool {
    *n == 0
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum SearchMode {
    #[default]
    Search,
    Autocomplete,
}

#[derive(Clone, Debug)]
pub struct SearchOptions {
    pub mode: SearchMode,
    pub limit: usize,
    /// Maximum edit distance for fuzzy matching. 0 disables. Capped at 2.
    /// Only honored when `mode == Search`.
    pub fuzzy: u8,
    /// Restrict results to these `kind` values (empty = no filter).
    pub layers: Vec<String>,
    /// Focus point used to re-rank top candidates by distance.
    pub focus: Option<Coord>,
    /// Weight for the distance penalty in the geo-bias re-rank step.
    /// final_score = bm25 / (1 + focus_weight * km).
    pub focus_weight: f64,
}

impl Default for SearchOptions {
    fn default() -> Self {
        Self {
            mode: SearchMode::Search,
            limit: 10,
            fuzzy: 0,
            layers: Vec::new(),
            focus: None,
            focus_weight: 0.5,
        }
    }
}

struct TextSchema {
    schema: Schema,
    name: Field,
    name_prefix: Field,
    name_cjk: Field,
    name_translit: Field,
    place_id: Field,
    level: Field,
    kind: Field,
    lon: Field,
    lat: Field,
    admin_path: Field,
    /// Optional population (`tags["population"]`). 0 = unknown.
    population: Field,
}

impl TextSchema {
    fn build() -> Self {
        let mut sb = Schema::builder();
        let prefix_indexing = TextFieldIndexing::default()
            .set_tokenizer(PREFIX_TOKENIZER)
            .set_index_option(IndexRecordOption::WithFreqsAndPositions);
        let prefix_options = TextOptions::default().set_indexing_options(prefix_indexing);

        let cjk_indexing = TextFieldIndexing::default()
            .set_tokenizer(CJK_TOKENIZER)
            .set_index_option(IndexRecordOption::WithFreqsAndPositions);
        let cjk_options = TextOptions::default().set_indexing_options(cjk_indexing);

        let name = sb.add_text_field("name", TEXT | STORED);
        let name_prefix = sb.add_text_field("name_prefix", prefix_options);
        let name_cjk = sb.add_text_field("name_cjk", cjk_options);
        // ASCII-folded variant for cross-script search ("moskva" →
        // "Москва"). Default analyzer is fine here — the input has
        // already been romanized so whitespace/lowercase tokenization
        // works as expected.
        let name_translit = sb.add_text_field("name_translit", TEXT);
        let place_id = sb.add_u64_field("place_id", FAST | STORED | INDEXED);
        let level = sb.add_u64_field("level", FAST | STORED | INDEXED);
        let kind = sb.add_text_field("kind", STRING | STORED);
        let lon = sb.add_f64_field("lon", STORED);
        let lat = sb.add_f64_field("lat", STORED);
        let admin_path = sb.add_u64_field("admin_path", STORED);
        let population = sb.add_u64_field("population", FAST | STORED);
        let schema = sb.build();
        Self {
            schema,
            name,
            name_prefix,
            name_cjk,
            name_translit,
            place_id,
            level,
            kind,
            lon,
            lat,
            admin_path,
            population,
        }
    }
}

fn register_prefix_tokenizer(index: &Index) -> Result<(), TextError> {
    let tokenizer = TextAnalyzer::builder(
        NgramTokenizer::prefix_only(PREFIX_MIN, PREFIX_MAX)
            .map_err(|e| tantivy::TantivyError::SystemError(format!("{e:?}")))?,
    )
    .filter(LowerCaser)
    .filter(RemoveLongFilter::limit(64))
    .build();
    index.tokenizers().register(PREFIX_TOKENIZER, tokenizer);
    Ok(())
}

/// CJK languages aren't whitespace-segmented; the standard QueryParser
/// with the default tokenizer over `name` matches a 3-kanji query
/// against a 5-kanji document only by luck. A character-bigram
/// analyzer indexes every adjacent pair so any 2-char sub-string of
/// the document is findable. Bigrams over romanized text would
/// over-recall, so we route only CJK-bearing names into this field at
/// index time and only route CJK-bearing queries against it at search
/// time.
fn register_cjk_tokenizer(index: &Index) -> Result<(), TextError> {
    let tokenizer = TextAnalyzer::builder(
        NgramTokenizer::all_ngrams(CJK_NGRAM_MIN, CJK_NGRAM_MAX)
            .map_err(|e| tantivy::TantivyError::SystemError(format!("{e:?}")))?,
    )
    .filter(LowerCaser)
    .filter(RemoveLongFilter::limit(64))
    .build();
    index.tokenizers().register(CJK_TOKENIZER, tokenizer);
    Ok(())
}

pub fn kind_str(kind: PlaceKind) -> &'static str {
    match kind {
        PlaceKind::Country => "country",
        PlaceKind::Region => "region",
        PlaceKind::County => "county",
        PlaceKind::City => "city",
        PlaceKind::District => "district",
        PlaceKind::Neighborhood => "neighborhood",
        PlaceKind::Street => "street",
        PlaceKind::Address => "address",
        PlaceKind::Poi => "poi",
        PlaceKind::Postcode => "postcode",
    }
}

/// Build a fresh tantivy index from a stream of [`Place`] values.
pub fn build_index<I>(dir: &Path, places: I) -> Result<usize, TextError>
where
    I: IntoIterator<Item = Place>,
{
    if dir.exists() {
        std::fs::remove_dir_all(dir)?;
    }
    std::fs::create_dir_all(dir)?;

    let schema = TextSchema::build();
    let index = Index::create_in_dir(dir, schema.schema.clone())?;
    register_prefix_tokenizer(&index)?;
    register_cjk_tokenizer(&index)?;
    let mut writer: IndexWriter = index.writer(WRITER_HEAP)?;

    let mut doc_count = 0usize;
    for place in places {
        if place.names.is_empty() {
            continue;
        }
        let mut doc = TantivyDocument::default();
        for n in &place.names {
            doc.add_text(schema.name, &n.value);
            doc.add_text(schema.name_prefix, &n.value);
            if has_cjk(&n.value) {
                doc.add_text(schema.name_cjk, &n.value);
            }
            if let Some(folded) = ascii_fold(&n.value) {
                doc.add_text(schema.name_translit, &folded);
            }
        }
        doc.add_u64(schema.place_id, place.id.0);
        doc.add_u64(schema.level, place.id.level() as u64);
        doc.add_text(schema.kind, kind_str(place.kind));
        doc.add_f64(schema.lon, place.centroid.lon);
        doc.add_f64(schema.lat, place.centroid.lat);
        for ancestor in &place.admin_path {
            doc.add_u64(schema.admin_path, ancestor.0);
        }
        // Pull population out of tags. Geonames stamps it natively;
        // 0 = unknown (no boost). Cap at u64::MAX / 2 so log scaling
        // can't overflow.
        let population = place
            .tags
            .iter()
            .find_map(|(k, v)| {
                if k == "population" {
                    v.parse::<u64>().ok()
                } else {
                    None
                }
            })
            .unwrap_or(0);
        doc.add_u64(schema.population, population);
        writer.add_document(doc)?;
        doc_count += 1;
    }
    writer.commit()?;
    debug!(docs = doc_count, "tantivy index committed");
    Ok(doc_count)
}

pub struct TextIndex {
    index: Index,
    reader: IndexReader,
    schema: TextSchema,
}

impl TextIndex {
    pub fn open(dir: &Path) -> Result<Self, TextError> {
        let index = Index::open_in_dir(dir)?;
        register_prefix_tokenizer(&index)?;
        register_cjk_tokenizer(&index)?;
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;
        let schema = TextSchema::build();
        Ok(Self {
            index,
            reader,
            schema,
        })
    }

    pub fn search(&self, query: &str, opts: &SearchOptions) -> Result<Vec<Hit>, TextError> {
        let trimmed = query.trim();
        if trimmed.is_empty() {
            return Ok(Vec::new());
        }

        let text_q = self.build_text_query(trimmed, opts)?;
        let combined = self.apply_layer_filter(text_q, &opts.layers);

        // Always over-fetch when we'll re-rank — focus, exact-name, or
        // both — so the BM25 top-N doesn't truncate matches the rerank
        // would have promoted.
        let needs_rerank = opts.focus.is_some() || matches!(opts.mode, SearchMode::Search);
        let candidate_limit = if needs_rerank {
            opts.limit
                .saturating_mul(RERANK_MULTIPLIER)
                .clamp(opts.limit, 200)
        } else {
            opts.limit
        };

        let searcher = self.reader.searcher();
        let raw = searcher.search(&combined, &TopDocs::with_limit(candidate_limit))?;

        let mut hits: Vec<Hit> = Vec::with_capacity(raw.len());
        for (score, addr) in raw {
            let doc: TantivyDocument = searcher.doc(addr)?;
            hits.push(self.hit_from_doc(score, &doc));
        }

        // Exact-name match boost: case + diacritic-folded equality
        // between the (trimmed) query and a Hit's stored name promotes
        // the hit's score by `EXACT_MATCH_BOOST`. Plain exact-but-
        // diacritic-mismatch ("munchen" vs "München") still counts
        // because we fold both sides via deunicode.
        //
        // Applies in Search mode only; Autocomplete is prefix-driven
        // and a literal full-name match isn't a meaningful signal at
        // typing time.
        if matches!(opts.mode, SearchMode::Search) {
            apply_exact_name_boost(&mut hits, trimmed);
        }
        apply_population_boost(&mut hits);

        if let Some(focus) = opts.focus {
            apply_geo_bias(&mut hits, focus, opts.focus_weight);
        }
        hits.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
        hits.truncate(opts.limit);
        Ok(hits)
    }

    /// Resolve a list of `place_id` values to [`Hit`]s. Used by Pelias's
    /// `/v1/place?ids=…` endpoint; missing IDs are silently skipped.
    /// Result order mirrors `ids` so callers can correlate.
    pub fn lookup_by_ids(&self, ids: &[u64]) -> Result<Vec<Hit>, TextError> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let searcher = self.reader.searcher();
        let mut hits_by_id: std::collections::HashMap<u64, Hit> =
            std::collections::HashMap::with_capacity(ids.len());
        for &id in ids {
            let term = tantivy::Term::from_field_u64(self.schema.place_id, id);
            let q: Box<dyn Query> = Box::new(TermQuery::new(term, IndexRecordOption::Basic));
            let raw = searcher.search(&q, &TopDocs::with_limit(1))?;
            if let Some((score, addr)) = raw.into_iter().next() {
                let doc: TantivyDocument = searcher.doc(addr)?;
                hits_by_id.insert(id, self.hit_from_doc(score, &doc));
            }
        }
        Ok(ids.iter().filter_map(|id| hits_by_id.remove(id)).collect())
    }

    fn build_text_query(
        &self,
        query: &str,
        opts: &SearchOptions,
    ) -> Result<Box<dyn Query>, TextError> {
        let field = match opts.mode {
            SearchMode::Search => self.schema.name,
            SearchMode::Autocomplete => self.schema.name_prefix,
        };

        let fuzzy = opts.fuzzy.min(MAX_FUZZY_DISTANCE);
        if fuzzy == 0 || matches!(opts.mode, SearchMode::Autocomplete) {
            // Default to the QueryParser path so users keep tantivy's
            // boolean / phrase syntax. Autocomplete also stays exact-prefix
            // because mixing fuzzy + ngram explodes the term space.
            //
            // CJK queries also hit the bigram-tokenized `name_cjk` field so
            // that whitespace-less scripts find sub-string matches the
            // default analyzer would miss. Latin/ASCII queries also OR
            // against `name_translit` so "moskva" finds "Москва".
            let mut fields = vec![field];
            if matches!(opts.mode, SearchMode::Search) {
                if has_cjk(query) {
                    fields.push(self.schema.name_cjk);
                } else {
                    fields.push(self.schema.name_translit);
                }
            }
            let parser = QueryParser::for_index(&self.index, fields);
            return Ok(parser.parse_query(query)?);
        }

        // Forward search with fuzzy distance.
        //
        // Per-token edit distance is clamped by token length so that
        // 3-char tokens don't mass-match the index. Damerau is enabled
        // (transposition_cost_one = true) so swapped-letter typos like
        // "Vauzd" → "Vaduz" cost 1 instead of 2.
        //
        // For multi-token queries, switch from SHOULD to MUST so that
        // every token must fuzzy-match. SHOULD over-recalls when the
        // user types a real address ("Vaduz Liechtenstein" should match
        // both, not either).
        let lowered = query.to_lowercase();
        let tokens: Vec<&str> = lowered.split_whitespace().collect();
        if tokens.is_empty() {
            let parser = QueryParser::for_index(&self.index, vec![field]);
            return Ok(parser.parse_query(query)?);
        }
        let occur = if tokens.len() >= 2 {
            Occur::Must
        } else {
            Occur::Should
        };
        let mut clauses: Vec<(Occur, Box<dyn Query>)> = Vec::with_capacity(tokens.len());
        for tok in tokens {
            let per_token = effective_fuzzy_distance(tok, fuzzy);
            let term = Term::from_field_text(field, tok);
            let q: Box<dyn Query> = if per_token == 0 {
                Box::new(TermQuery::new(term, IndexRecordOption::WithFreqs))
            } else {
                Box::new(FuzzyTermQuery::new(term, per_token, true))
            };
            clauses.push((occur, q));
        }
        Ok(Box::new(BooleanQuery::new(clauses)))
    }

    fn apply_layer_filter(&self, text_q: Box<dyn Query>, layers: &[String]) -> Box<dyn Query> {
        if layers.is_empty() {
            return text_q;
        }
        let mut layer_clauses: Vec<(Occur, Box<dyn Query>)> = Vec::with_capacity(layers.len());
        for layer in layers {
            let term = Term::from_field_text(self.schema.kind, layer);
            let q = TermQuery::new(term, IndexRecordOption::Basic);
            layer_clauses.push((Occur::Should, Box::new(q) as Box<dyn Query>));
        }
        let layer_q: Box<dyn Query> = Box::new(BooleanQuery::new(layer_clauses));
        Box::new(BooleanQuery::new(vec![
            (Occur::Must, text_q),
            (Occur::Must, layer_q),
        ]))
    }

    fn hit_from_doc(&self, score: f32, doc: &TantivyDocument) -> Hit {
        let admin_path: Vec<u64> = doc
            .get_all(self.schema.admin_path)
            .filter_map(|v| v.as_u64())
            .collect();
        Hit {
            place_id: doc
                .get_first(self.schema.place_id)
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
            name: doc
                .get_first(self.schema.name)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            kind: doc
                .get_first(self.schema.kind)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            level: doc
                .get_first(self.schema.level)
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
            lon: doc
                .get_first(self.schema.lon)
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0),
            lat: doc
                .get_first(self.schema.lat)
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0),
            score,
            admin_path,
            distance_km: None,
            population: doc
                .get_first(self.schema.population)
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
        }
    }
}

/// Multiply each hit's score by [`EXACT_MATCH_BOOST`] when the query
/// (lowercased, ASCII-folded via deunicode) equals the hit's name
/// after the same fold. Sort happens later in the pipeline.
fn apply_exact_name_boost(hits: &mut [Hit], query: &str) {
    let q = fold_for_compare(query);
    if q.is_empty() {
        return;
    }
    for h in hits.iter_mut() {
        if fold_for_compare(&h.name) == q {
            h.score *= EXACT_MATCH_BOOST;
        }
    }
}

/// Multiply each hit's score by `1 + log10(1+pop) * POPULATION_BOOST_RATE`.
/// Hits with `population == 0` (unknown) are unchanged. Logarithmic
/// scaling means a city of 10M doesn't crush a clean BM25 match for a
/// 10k-population village.
fn apply_population_boost(hits: &mut [Hit]) {
    for h in hits.iter_mut() {
        if h.population == 0 {
            continue;
        }
        let factor = 1.0 + ((h.population as f32 + 1.0).log10()) * POPULATION_BOOST_RATE;
        h.score *= factor;
    }
}

fn fold_for_compare(s: &str) -> String {
    deunicode::deunicode(s).trim().to_lowercase()
}

fn apply_geo_bias(hits: &mut [Hit], focus: Coord, weight: f64) {
    let weight = weight.max(0.0);
    for h in hits.iter_mut() {
        let km = haversine_km(focus.lat, focus.lon, h.lat, h.lon);
        h.distance_km = Some(km);
        let blended = (h.score as f64) / (1.0 + weight * km);
        h.score = blended as f32;
    }
    hits.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
}

/// Pick an edit distance for a token given the user-requested maximum.
///
/// Short tokens (3 chars or fewer) get exact match — at distance 1, a
/// 3-char token matches half the dictionary. Longer tokens scale up
/// linearly, capped at the user's requested max and the index-wide
/// fuzzy limit.
fn effective_fuzzy_distance(token: &str, requested_max: u8) -> u8 {
    let len = token.chars().count();
    let cap = match len {
        0..=3 => 0,
        4..=5 => 1,
        _ => MAX_FUZZY_DISTANCE,
    };
    requested_max.min(cap)
}

fn haversine_km(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const EARTH_KM: f64 = 6371.0088;
    let to_rad = std::f64::consts::PI / 180.0;
    let phi1 = lat1 * to_rad;
    let phi2 = lat2 * to_rad;
    let dphi = (lat2 - lat1) * to_rad;
    let dlam = (lon2 - lon1) * to_rad;
    let a = (dphi / 2.0).sin().powi(2) + phi1.cos() * phi2.cos() * (dlam / 2.0).sin().powi(2);
    2.0 * EARTH_KM * a.sqrt().asin()
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_place::{Coord, LocalizedName, PlaceId};

    fn vaduz() -> Place {
        Place {
            id: PlaceId::new(1, 49509, 1).unwrap(),
            kind: PlaceKind::City,
            names: vec![
                LocalizedName {
                    lang: "default".into(),
                    value: "Vaduz".into(),
                },
                LocalizedName {
                    lang: "de".into(),
                    value: "Vaduz".into(),
                },
            ],
            centroid: Coord {
                lon: 9.5209,
                lat: 47.1410,
            },
            admin_path: vec![PlaceId::new(0, 49509, 1).unwrap()],
            tags: vec![],
        }
    }

    fn schaan() -> Place {
        Place {
            id: PlaceId::new(1, 49509, 2).unwrap(),
            kind: PlaceKind::City,
            names: vec![LocalizedName {
                lang: "default".into(),
                value: "Schaan".into(),
            }],
            centroid: Coord {
                lon: 9.5095,
                lat: 47.1650,
            },
            admin_path: vec![PlaceId::new(0, 49509, 1).unwrap()],
            tags: vec![],
        }
    }

    fn liechtenstein_country() -> Place {
        Place {
            id: PlaceId::new(0, 49509, 1).unwrap(),
            kind: PlaceKind::Country,
            names: vec![LocalizedName {
                lang: "default".into(),
                value: "Liechtenstein".into(),
            }],
            centroid: Coord {
                lon: 9.5594,
                lat: 47.1114,
            },
            admin_path: vec![],
            tags: vec![],
        }
    }

    fn tempdir_for_test() -> std::path::PathBuf {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let d = std::env::temp_dir().join(format!(
            "cairn-text-test-{}-{}-{}",
            std::process::id(),
            nanos,
            n
        ));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn build_and_search() {
        let dir = tempdir_for_test();
        let docs = build_index(&dir, vec![vaduz(), schaan()]).unwrap();
        assert_eq!(docs, 2);

        let idx = TextIndex::open(&dir).unwrap();
        let hits = idx.search("vaduz", &SearchOptions::default()).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].name, "Vaduz");
        assert_eq!(hits[0].admin_path.len(), 1);
    }

    #[test]
    fn autocomplete_prefix() {
        let dir = tempdir_for_test();
        build_index(&dir, vec![vaduz(), schaan()]).unwrap();
        let idx = TextIndex::open(&dir).unwrap();
        let opts = SearchOptions {
            mode: SearchMode::Autocomplete,
            ..Default::default()
        };

        let hits = idx.search("Vad", &opts).unwrap();
        assert!(hits.iter().any(|h| h.name == "Vaduz"));
    }

    #[test]
    fn fuzzy_recovers_typo() {
        let dir = tempdir_for_test();
        build_index(&dir, vec![vaduz(), schaan()]).unwrap();
        let idx = TextIndex::open(&dir).unwrap();
        let opts = SearchOptions {
            fuzzy: 2,
            ..Default::default()
        };
        let hits = idx.search("vaaduz", &opts).unwrap();
        assert!(hits.iter().any(|h| h.name == "Vaduz"));
    }

    #[test]
    fn layer_filter_excludes_other_kinds() {
        let dir = tempdir_for_test();
        build_index(&dir, vec![vaduz(), liechtenstein_country()]).unwrap();
        let idx = TextIndex::open(&dir).unwrap();

        let only_country = SearchOptions {
            layers: vec!["country".into()],
            ..Default::default()
        };
        let hits = idx.search("liechtenstein", &only_country).unwrap();
        assert!(!hits.is_empty());
        assert!(hits.iter().all(|h| h.kind == "country"));

        let only_city = SearchOptions {
            layers: vec!["city".into()],
            ..Default::default()
        };
        let hits = idx.search("liechtenstein", &only_city).unwrap();
        assert!(hits.is_empty(), "country must not leak into city layer");
    }

    #[test]
    fn fuzzy_clamped_for_short_tokens() {
        assert_eq!(effective_fuzzy_distance("v", 2), 0);
        assert_eq!(effective_fuzzy_distance("vad", 2), 0);
        assert_eq!(effective_fuzzy_distance("vad", 1), 0);
        assert_eq!(effective_fuzzy_distance("vadu", 2), 1);
        assert_eq!(effective_fuzzy_distance("vaduz", 2), 1);
        assert_eq!(effective_fuzzy_distance("vaduzz", 2), 2);
        // requested distance is the upper bound, not the floor
        assert_eq!(effective_fuzzy_distance("vaduzz", 1), 1);
    }

    #[test]
    fn fuzzy_recovers_transposition() {
        let dir = tempdir_for_test();
        build_index(&dir, vec![vaduz(), schaan()]).unwrap();
        let idx = TextIndex::open(&dir).unwrap();
        // "Vadzu" is "Vaduz" with the trailing u/z swapped → a single
        // adjacent transposition. Damerau-Levenshtein with
        // transposition_cost_one=true counts this as distance 1.
        let opts = SearchOptions {
            fuzzy: 1,
            ..Default::default()
        };
        let hits = idx.search("Vadzu", &opts).unwrap();
        assert!(
            hits.iter().any(|h| h.name == "Vaduz"),
            "expected Vaduz via Damerau transposition, got {hits:?}"
        );
    }

    #[test]
    fn fuzzy_multi_token_requires_all() {
        let dir = tempdir_for_test();
        build_index(&dir, vec![vaduz(), schaan(), liechtenstein_country()]).unwrap();
        let idx = TextIndex::open(&dir).unwrap();
        // "vaduz schaan" → Vaduz alone shouldn't match because "schaan"
        // doesn't appear in its names. Only docs containing both tokens
        // (none here) come back.
        let opts = SearchOptions {
            fuzzy: 2,
            ..Default::default()
        };
        let hits = idx.search("vaduz schaan", &opts).unwrap();
        assert!(
            hits.is_empty(),
            "multi-token AND must not return docs missing one of the tokens"
        );
    }

    #[test]
    fn focus_reranks_nearer_first() {
        let dir = tempdir_for_test();
        build_index(&dir, vec![vaduz(), schaan()]).unwrap();
        let idx = TextIndex::open(&dir).unwrap();

        // Focus on Schaan's centroid; without bias both score equally on
        // an ambiguous prefix, but Schaan should win on distance.
        let opts = SearchOptions {
            mode: SearchMode::Autocomplete,
            focus: Some(Coord {
                lon: 9.5095,
                lat: 47.1650,
            }),
            focus_weight: 5.0,
            limit: 5,
            ..Default::default()
        };
        let hits = idx.search("S", &opts).unwrap();
        assert!(hits.iter().all(|h| h.distance_km.is_some()));
        assert!(
            hits.first().map(|h| h.name == "Schaan").unwrap_or(false),
            "expected Schaan first, got {:?}",
            hits
        );
    }

    #[test]
    fn exact_name_match_outranks_partial() {
        let dir = tempdir_for_test();
        // Two records: a city literally called "Vaduz" and a POI whose
        // name contains "Vaduz" as a substring. BM25 alone might tie or
        // even rank the longer-name POI higher because of term-frequency
        // quirks. Exact-match boost must lift the literal city.
        let city = vaduz();
        let mut poi = Place {
            id: PlaceId::new(2, 49509, 1).unwrap(),
            kind: PlaceKind::Poi,
            names: vec![LocalizedName {
                lang: "default".into(),
                value: "Vaduz Castle Tea Room".into(),
            }],
            centroid: Coord {
                lon: 9.5208,
                lat: 47.141,
            },
            admin_path: vec![],
            tags: vec![],
        };
        let _ = &mut poi;
        build_index(&dir, vec![city, poi]).unwrap();
        let idx = TextIndex::open(&dir).unwrap();
        let hits = idx.search("Vaduz", &SearchOptions::default()).unwrap();
        assert!(
            hits.first().map(|h| h.name == "Vaduz").unwrap_or(false),
            "exact 'Vaduz' must outrank a partial match, got {hits:?}"
        );
    }

    #[test]
    fn exact_name_boost_is_diacritic_insensitive() {
        let dir = tempdir_for_test();
        let munich = Place {
            id: PlaceId::new(1, 23456, 1).unwrap(),
            kind: PlaceKind::City,
            names: vec![LocalizedName {
                lang: "default".into(),
                value: "München".into(),
            }],
            centroid: Coord {
                lon: 11.58,
                lat: 48.14,
            },
            admin_path: vec![],
            tags: vec![],
        };
        let other = Place {
            id: PlaceId::new(2, 23456, 1).unwrap(),
            kind: PlaceKind::Poi,
            names: vec![LocalizedName {
                lang: "default".into(),
                value: "Münchener Freiheit S-Bahn".into(),
            }],
            centroid: Coord {
                lon: 11.58,
                lat: 48.14,
            },
            admin_path: vec![],
            tags: vec![],
        };
        build_index(&dir, vec![munich, other]).unwrap();
        let idx = TextIndex::open(&dir).unwrap();
        // Type "munchen" (no umlaut) — exact-match boost should still
        // promote München via the deunicode-folded compare.
        let hits = idx.search("munchen", &SearchOptions::default()).unwrap();
        assert!(
            hits.first().map(|h| h.name == "München").unwrap_or(false),
            "ASCII 'munchen' must boost München over partial-name PoI: {hits:?}"
        );
    }

    #[test]
    fn population_boost_breaks_ties_in_favor_of_bigger() {
        let dir = tempdir_for_test();
        let big = Place {
            id: PlaceId::new(1, 1, 1).unwrap(),
            kind: PlaceKind::City,
            names: vec![LocalizedName {
                lang: "default".into(),
                value: "Springfield".into(),
            }],
            centroid: Coord {
                lon: 0.0,
                lat: 0.0,
            },
            admin_path: vec![],
            tags: vec![("population".into(), "200000".into())],
        };
        let small = Place {
            id: PlaceId::new(1, 2, 1).unwrap(),
            kind: PlaceKind::City,
            names: vec![LocalizedName {
                lang: "default".into(),
                value: "Springfield".into(),
            }],
            centroid: Coord {
                lon: 50.0,
                lat: 50.0,
            },
            admin_path: vec![],
            tags: vec![("population".into(), "150".into())],
        };
        build_index(&dir, vec![small, big]).unwrap();
        let idx = TextIndex::open(&dir).unwrap();
        let hits = idx
            .search("Springfield", &SearchOptions::default())
            .unwrap();
        assert_eq!(hits.len(), 2);
        // Both exact-match boosted; population differentiates.
        assert_eq!(hits[0].population, 200000);
        assert_eq!(hits[1].population, 150);
    }

    fn tokyo() -> Place {
        Place {
            id: PlaceId::new(1, 12345, 1).unwrap(),
            kind: PlaceKind::City,
            names: vec![
                LocalizedName {
                    lang: "default".into(),
                    value: "Tokyo".into(),
                },
                LocalizedName {
                    lang: "ja".into(),
                    value: "東京都".into(),
                },
            ],
            centroid: Coord {
                lon: 139.69,
                lat: 35.68,
            },
            admin_path: vec![],
            tags: vec![],
        }
    }

    fn beijing() -> Place {
        Place {
            id: PlaceId::new(1, 23456, 1).unwrap(),
            kind: PlaceKind::City,
            names: vec![
                LocalizedName {
                    lang: "default".into(),
                    value: "Beijing".into(),
                },
                LocalizedName {
                    lang: "zh".into(),
                    value: "北京市".into(),
                },
            ],
            centroid: Coord {
                lon: 116.40,
                lat: 39.90,
            },
            admin_path: vec![],
            tags: vec![],
        }
    }

    #[test]
    fn cjk_substring_matches_full_name() {
        let dir = tempdir_for_test();
        build_index(&dir, vec![tokyo(), beijing()]).unwrap();
        let idx = TextIndex::open(&dir).unwrap();

        // Bigram tokenizer over CJK names: a 2-char sub-string of "東京都"
        // must surface Tokyo.
        let hits = idx.search("東京", &SearchOptions::default()).unwrap();
        assert!(
            hits.iter().any(|h| h.name == "Tokyo"),
            "CJK bigram match failed: {hits:?}"
        );

        let hits = idx.search("北京", &SearchOptions::default()).unwrap();
        assert!(
            hits.iter().any(|h| h.name == "Beijing"),
            "CJK bigram match failed: {hits:?}"
        );
    }

    #[test]
    fn cjk_does_not_pollute_latin_search() {
        let dir = tempdir_for_test();
        build_index(&dir, vec![vaduz(), tokyo()]).unwrap();
        let idx = TextIndex::open(&dir).unwrap();
        // A latin query must still hit only the latin name, not bigram-fold
        // it across CJK.
        let hits = idx.search("Vaduz", &SearchOptions::default()).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].name, "Vaduz");
    }

    #[test]
    fn has_cjk_classifier() {
        assert!(has_cjk("東京"));
        assert!(has_cjk("ソウル"));
        assert!(has_cjk("서울"));
        assert!(!has_cjk("Vaduz"));
        assert!(!has_cjk("München"));
    }

    #[test]
    fn ascii_fold_skips_already_ascii() {
        assert_eq!(ascii_fold("Vaduz"), None);
        assert_eq!(ascii_fold("New York"), None);
        assert_eq!(ascii_fold(""), None);
    }

    #[test]
    fn ascii_fold_romanizes_non_latin() {
        assert_eq!(ascii_fold("München").as_deref(), Some("Munchen"));
        assert!(ascii_fold("Москва")
            .as_deref()
            .unwrap_or("")
            .contains("Mosk"));
        assert!(!ascii_fold("Αθήνα").as_deref().unwrap_or("").is_empty());
    }

    fn moscow() -> Place {
        Place {
            id: PlaceId::new(1, 12345, 1).unwrap(),
            kind: PlaceKind::City,
            names: vec![LocalizedName {
                lang: "default".into(),
                value: "Москва".into(),
            }],
            centroid: Coord {
                lon: 37.62,
                lat: 55.75,
            },
            admin_path: vec![],
            tags: vec![],
        }
    }

    fn munich() -> Place {
        Place {
            id: PlaceId::new(1, 23456, 1).unwrap(),
            kind: PlaceKind::City,
            names: vec![LocalizedName {
                lang: "default".into(),
                value: "München".into(),
            }],
            centroid: Coord {
                lon: 11.58,
                lat: 48.14,
            },
            admin_path: vec![],
            tags: vec![],
        }
    }

    #[test]
    fn translit_finds_cyrillic_via_latin_query() {
        let dir = tempdir_for_test();
        build_index(&dir, vec![moscow()]).unwrap();
        let idx = TextIndex::open(&dir).unwrap();
        let hits = idx.search("Moskva", &SearchOptions::default()).unwrap();
        assert!(
            hits.iter().any(|h| h.name == "Москва"),
            "Latin 'Moskva' should match Cyrillic 'Москва', got {hits:?}"
        );
    }

    #[test]
    fn translit_finds_diacritic_via_ascii_query() {
        let dir = tempdir_for_test();
        build_index(&dir, vec![munich()]).unwrap();
        let idx = TextIndex::open(&dir).unwrap();
        let hits = idx.search("Munchen", &SearchOptions::default()).unwrap();
        assert!(
            hits.iter().any(|h| h.name == "München"),
            "ASCII 'Munchen' should match 'München', got {hits:?}"
        );
    }

    #[test]
    fn translit_does_not_swallow_cjk_path() {
        // CJK queries still go through name_cjk (bigram analyzer), not
        // name_translit. Confirm Tokyo still resolves via 東京.
        let dir = tempdir_for_test();
        build_index(&dir, vec![tokyo()]).unwrap();
        let idx = TextIndex::open(&dir).unwrap();
        let hits = idx.search("東京", &SearchOptions::default()).unwrap();
        assert!(hits.iter().any(|h| h.name == "Tokyo"));
    }
}
