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
use rphonetic::DoubleMetaphone;
use rustc_hash::FxHashSet;
use serde::{Deserialize, Serialize};

pub mod edit;
pub mod semantic;
#[cfg(feature = "semantic-onnx")]
pub mod semantic_v2;
pub mod stopwords;
pub mod trigram;
use std::path::Path;
use tantivy::collector::TopDocs;
use tantivy::query::{
    BooleanQuery, FuzzyTermQuery, Occur, Query, QueryParser, RangeQuery, TermQuery,
};
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
/// Build-time tantivy IndexWriter heap. 256 MiB instead of the
/// default 50 MiB cuts segment flush count + merge churn at country
/// scale (DE: previously ~120 in-build segments; bumped to a single
/// digit). Per-bundle build is the only allocator hit, so spending
/// 256 MiB during build buys multi-second wall-clock at no serve-side
/// cost.
/// Phase 7 memory work — drop the tantivy writer heap from 256 MiB
/// to 64 MiB. Larger heaps trade per-segment memory for fewer segment
/// flushes during build; on Europe-scale corpora (25.45 M docs) the
/// 256 MiB heap held a transient ~3-4 GB across parallel
/// segment-builder threads at peak. Tantivy's `LogMergePolicy` still
/// consolidates segments after commit, so the smaller heap costs
/// build-time disk I/O (more segment flushes + merges) rather than
/// final index quality. Trade is favourable when the host is the
/// memory-constrained one — and on planet that's always the case.
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
/// Multiplier applied when a hit ships a localized name in the
/// `?lang=` preferred language. Modest — language match alone
/// shouldn't override a much stronger BM25 / exact-match signal,
/// but should cleanly break ties between two equivalently-matching
/// hits (one in German, one in French) for a German-preferring user.
const LANG_PREFERENCE_BOOST: f32 = 1.5;
/// Minimum query token length before the phonetic OR clause kicks
/// in. DoubleMetaphone produces overly-broad codes on 1-2 char
/// tokens — "a" and "an" collapse to single-letter codes that
/// match thousands of corpus docs, polluting the rerank set.
/// 3 is the empirical floor where the encoder produces
/// specific-enough codes for the OR-widen to be net-positive on
/// precision/recall.
const PHONETIC_MIN_TOKEN_LEN: usize = 3;

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
    /// Canonical Pelias-style label, e.g. "Vaduz Castle, Vaduz,
    /// Liechtenstein". Built by joining the hit's name with the
    /// resolved admin_path names (root → leaf, deduplicated). Empty
    /// when the bundle has no admin_names sidecar.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub label: String,
    /// Language codes this hit carries a localized name for
    /// (e.g. `["default", "de", "fr"]`). Populates the `?lang=`
    /// preference boost. Empty for older bundles that didn't index
    /// the field.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub langs: Vec<String>,
    /// Pelias-style category taxonomy this hit belongs to (e.g.
    /// `["health", "hospital"]`, `["food", "restaurant"]`,
    /// `["admin"]`). Empty for older bundles that didn't index the
    /// field.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub categories: Vec<String>,
    /// Pelias-compatible global identifier (`<source>:<type>:<id>`,
    /// e.g. `osm:way:12345`). Stable across rebuilds when the
    /// underlying upstream data is unchanged. Suitable for
    /// bookmark URLs and the `/v1/place?ids=…` lookup. Empty for
    /// pre-v0.4 bundles that didn't carry the tag.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub gid: String,
    /// Per-stage scoring breakdown. Populated only when
    /// `SearchOptions.explain == true`. Shows the BM25 baseline plus
    /// every multiplier applied — exact-name, population, language
    /// preference, geo-bias — and the final post-rerank score.
    /// Useful for debugging ranking surprises and for clients that
    /// need to surface "why this hit ranked here" to end users. No
    /// other geocoder ships this today.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub explain: Option<HitExplain>,
    /// Phase 6e B1 — name embedding bytes captured at the BM25 pass so
    /// `apply_semantic_boost` can rerank without re-resolving every
    /// hit's doc by `place_id`. Populated by `hit_from_doc` when the
    /// stored field is present; cleared before the response is
    /// serialized so the wire format stays unchanged.
    #[serde(skip)]
    pub name_vec_bytes: Option<Vec<u8>>,
}

/// Score-contribution breakdown for one [`Hit`]. All multiplier fields
/// default to 1.0 (no effect); the final value is the score returned
/// after every stage runs.
#[derive(Clone, Debug, Default, Serialize)]
pub struct HitExplain {
    /// Raw BM25 score returned by tantivy before any rerank.
    pub bm25: f32,
    /// Multiplier applied when the (folded) query equals one of the
    /// hit's stored names. 1.0 = no exact-name match.
    pub exact_match_boost: f32,
    /// Multiplier from `1 + log10(1+population) * rate`.
    /// 1.0 = unknown population (no boost).
    pub population_boost: f32,
    /// Multiplier when a `?lang=` preferred language matches one of
    /// the hit's stored language tags. 1.0 = no language match.
    pub language_boost: f32,
    /// Geo-bias divisor `1 / (1 + focus_weight * km)`. 1.0 = no
    /// focus point set.
    pub geo_bias: f32,
    /// Final post-rerank score (all multipliers applied, sorted by
    /// this value).
    pub final_score: f32,
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
    /// Optional preferred language code (`"de"`, `"fr"`, `"en"`, …).
    /// Hits with a localized name in this language get a modest score
    /// boost so multi-lingual users see results in their preferred
    /// tongue when ties are close. Empty / `None` disables the boost.
    pub prefer_lang: Option<String>,
    /// Restrict results to documents whose `categories` field contains
    /// at least one of these tokens (`"health"`, `"hospital"`,
    /// `"food"`, …). Empty = no filter. OR semantics across the list:
    /// `categories=hospital,clinic` returns docs tagged with EITHER
    /// hospital OR clinic.
    pub categories: Vec<String>,
    /// Optional axis-aligned bounding box. Hits whose centroid falls
    /// outside the rect are dropped after BM25 retrieval (post-filter).
    /// Layout: `(min_lon, min_lat, max_lon, max_lat)`. Antimeridian
    /// crossing (min_lon > max_lon) currently treated as no-op since
    /// it would require splitting into two spans; callers spanning
    /// the antimeridian should issue two requests for now.
    pub bbox: Option<Bbox>,
    /// When true, OR an extra clause against the `name_phonetic` field
    /// (DoubleMetaphone-encoded). Catches misspellings the fuzzy edit
    /// distance can't reach: `"Mueller"` ↔ `"Müller"`,
    /// `"Smyth"` ↔ `"Smith"`, `"Katherine"` ↔ `"Catherine"`.
    pub phonetic: bool,
    /// When true, post-rerank the candidate set by lexical-vector
    /// (character-trigram BoW) cosine similarity to the query.
    /// Boosts hits with morphologically similar names — `"Vienna"`
    /// query bumps `"Viennese"`, `"Trisenberg"` rescues
    /// `"Triesenberg"` past plain BM25. See `cairn_text::semantic`.
    pub semantic: bool,
    /// When true, populate `Hit::explain` with the BM25 baseline plus
    /// each rerank multiplier so callers can surface "why this hit
    /// ranked here". Tiny per-hit cost; off by default to keep the
    /// JSON payload small.
    pub explain: bool,
    /// Phase 7a-Q — temporal validity filter. When `Some(year)`, only
    /// places whose `start_year ≤ year ≤ end_year` window covers the
    /// requested year are returned. Places without OSM date tags
    /// have sentinel bounds (`i64::MIN`..=`i64::MAX`) and match every
    /// `valid_at`. Use `0` to disable.
    pub valid_at: Option<i64>,
}

/// Axis-aligned bounding box, lon/lat degrees. Inclusive on all sides.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct Bbox {
    pub min_lon: f64,
    pub min_lat: f64,
    pub max_lon: f64,
    pub max_lat: f64,
}

impl Bbox {
    pub fn contains(&self, lon: f64, lat: f64) -> bool {
        lon >= self.min_lon && lon <= self.max_lon && lat >= self.min_lat && lat <= self.max_lat
    }
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
            prefer_lang: None,
            categories: Vec::new(),
            bbox: None,
            phonetic: false,
            semantic: false,
            explain: false,
            valid_at: None,
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
    /// One token per localized-name language code that this Place
    /// carries (`"default"`, `"de"`, `"fr"`, etc). Multi-value STORED;
    /// drives the `?lang=` boost at query time.
    lang_codes: Field,
    /// Pelias-style category taxonomy (`"health"`, `"hospital"`,
    /// `"food"`, `"restaurant"`, …). Multi-value STRING + STORED;
    /// derived from `Place.kind` + OSM tags via
    /// `cairn_place::categories_for`. Drives the `?categories=` filter.
    categories: Field,
    /// DoubleMetaphone-encoded name tokens. STRING (whole-token, no
    /// folding) so the encoded form matches verbatim at query time.
    /// Multi-value: each localized name produces both a primary and
    /// optional alternate code. Drives `?phonetic=true` recall when
    /// `Müller` ↔ `Mueller`, `Smith` ↔ `Smyth`, `Catherine` ↔
    /// `Katherine` would otherwise miss.
    name_phonetic: Field,
    /// Phase 7a-Q — temporal validity start year (i64). Indexed as
    /// FAST + INDEXED for RangeQuery filtering on `?valid_at=YYYY`.
    /// Sentinel `i64::MIN` means "always-before" (open-ended).
    start_year: Field,
    /// Phase 7a-Q — temporal validity end year (i64). Sentinel
    /// `i64::MAX` means "always-after" (still valid today).
    end_year: Field,
    /// Phase 7a — character-trigram pre-filter for fuzzy matching.
    /// STRING multi-value field (one term per distinct boundary-padded
    /// trigram of every name variant). Used as a `Should`-OR clause
    /// `Must`-AND'd with the `FuzzyTermQuery` so docs sharing zero
    /// trigrams with the query never reach the BM25 scoring stage.
    name_trigrams: Field,
    /// Packed `DIM*4` little-endian f32 lexical-vector embedding
    /// of the place's primary name. STORED only — never indexed,
    /// since cosine ranking happens on the candidate set after
    /// BM25. See `semantic.rs` for layout.
    name_vec: Field,
    /// Pelias-compatible global identifier (`<source>:<type>:<id>`).
    /// STRING (whole-token) + STORED + INDEXED — whole-token so the
    /// colon-delimited form matches verbatim, INDEXED so the
    /// `/v1/place?ids=…` resolver can hit it via `TermQuery`.
    /// Pulled from the row's `gid` tag at index time; older bundles
    /// without the tag leave the field empty.
    gid: Field,
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
        // STRING analyzer = whole-token, no folding — language codes
        // ('default', 'de', 'fr-CH') stay verbatim for exact match.
        let lang_codes = sb.add_text_field("lang_codes", STRING | STORED);
        // Same treatment for category tokens — exact match on the
        // canonical lowercase name. STRING + STORED so we can both
        // filter on it AND echo it back in the JSON response.
        let categories = sb.add_text_field("categories", STRING | STORED);
        // Phonetic codes: STRING (whole token), no STORED — we don't
        // need to echo encoded codes back; only used as a query
        // pivot. Indexed to support TermQuery lookups.
        let name_phonetic = sb.add_text_field("name_phonetic", STRING);
        // Phase 7a-Q — temporal validity bounds. FAST + INDEXED so
        // RangeQuery can filter at search time.
        let start_year = sb.add_i64_field("start_year", FAST | INDEXED);
        let end_year = sb.add_i64_field("end_year", FAST | INDEXED);
        // Phase 7a — char-trigram pre-filter. STRING (whole-token,
        // no analyzer); the indexer feeds pre-extracted trigrams.
        let name_trigrams = sb.add_text_field("name_trigrams", STRING);
        // Lexical-vector embedding: DIM*4 packed bytes per place,
        // STORED only (no indexing). The cosine rerank reads it on
        // the candidate set after BM25, so this stays out of the
        // hot inverted-index path.
        let name_vec = sb.add_bytes_field(
            "name_vec",
            tantivy::schema::BytesOptions::default().set_stored(),
        );
        // gid: whole-token, indexed for TermQuery lookups, stored so
        // search hits round-trip the value into the JSON response.
        // STRING already implies indexed; no extra flag needed.
        let gid = sb.add_text_field("gid", STRING | STORED);
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
            lang_codes,
            categories,
            name_phonetic,
            start_year,
            end_year,
            name_trigrams,
            name_vec,
            gid,
        }
    }
}

/// Encode a single name into its DoubleMetaphone primary + optional
/// alternate codes, returning a deduplicated vec of non-empty codes.
/// Used at index time AND query time so encoder symmetry is preserved.
fn phonetic_codes(name: &str) -> Vec<String> {
    let dm = DoubleMetaphone::default();
    let res = dm.double_metaphone(name);
    let primary = res.primary();
    let alternate = res.alternate();
    let mut out: Vec<String> = Vec::with_capacity(2);
    if !primary.is_empty() {
        out.push(primary);
    }
    if !alternate.is_empty() && !out.contains(&alternate) {
        out.push(alternate);
    }
    out
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
        // Per-place dedup sets. Switched from BTreeSet to FxHashSet
        // here: BTreeSet allocates a heap node per unique key
        // (24-byte overhead) and the order it preserves isn't used
        // anywhere downstream — tantivy stamps each value as a
        // separate token regardless of insertion order. FxHashSet
        // amortizes via a single contiguous buffer + faster hashing
        // on small string keys. Capacity hints sized to typical
        // per-place fan-out (1-5 names × ~8 langs / phonetic codes /
        // trigrams).
        let mut langs_seen: FxHashSet<String> =
            FxHashSet::with_capacity_and_hasher(8, Default::default());
        let mut phonetic_seen: FxHashSet<String> =
            FxHashSet::with_capacity_and_hasher(8, Default::default());
        let mut trigrams_seen: FxHashSet<String> =
            FxHashSet::with_capacity_and_hasher(64, Default::default());
        for n in &place.names {
            doc.add_text(schema.name, &n.value);
            doc.add_text(schema.name_prefix, &n.value);
            if has_cjk(&n.value) {
                doc.add_text(schema.name_cjk, &n.value);
            }
            if let Some(folded) = ascii_fold(&n.value) {
                doc.add_text(schema.name_translit, &folded);
            }
            // Skip CJK for phonetic — DoubleMetaphone is Latin-script
            // and produces empty / nonsense codes for non-Latin input.
            if !has_cjk(&n.value) {
                let phonetic_input = ascii_fold(&n.value).unwrap_or_else(|| n.value.clone());
                for code in phonetic_codes(&phonetic_input) {
                    phonetic_seen.insert(code);
                }
            }
            // Phase 7a — char-trigram pre-filter. Aggregate distinct
            // trigrams across every name variant so the fuzzy
            // pre-filter survives translation drift (`Munich` ↔
            // `München` ↔ `Monaco di Baviera`).
            for tg in trigram::extract_indexed(&n.value) {
                trigrams_seen.insert(tg);
            }
            if !n.lang.is_empty() {
                // Wikidata aliases land here as `<lang>_alt` (e.g.
                // `en_alt` for an English alias of a French Place).
                // Normalize to the canonical lang so a `?lang=en`
                // query still boosts Places that only carry English
                // via aliases. The `_alt` form itself is dropped from
                // `lang_codes`; the alias *value* is still indexed in
                // every name field above, so it remains searchable.
                let canonical = n.lang.strip_suffix("_alt").unwrap_or(n.lang.as_str());
                langs_seen.insert(canonical.to_string());
            }
        }
        // Sort each dedup set before adding tokens to tantivy so
        // the index is byte-deterministic across rebuilds (FxHashSet
        // iteration order is unspecified). The collect+sort costs
        // microseconds; bundle reproducibility is non-negotiable
        // for diff/apply + signature workflows.
        let mut phonetic_v: Vec<String> = phonetic_seen.into_iter().collect();
        phonetic_v.sort_unstable();
        for code in phonetic_v {
            doc.add_text(schema.name_phonetic, &code);
        }
        let mut trigrams_v: Vec<String> = trigrams_seen.into_iter().collect();
        trigrams_v.sort_unstable();
        for tg in trigrams_v {
            doc.add_text(schema.name_trigrams, &tg);
        }
        let mut langs_v: Vec<String> = langs_seen.into_iter().collect();
        langs_v.sort_unstable();
        for lang in langs_v {
            doc.add_text(schema.lang_codes, &lang);
        }
        for cat in cairn_place::categories_for(&place) {
            doc.add_text(schema.categories, &cat);
        }
        // Lexical-vector embedding of the primary (default-language)
        // name. Skipped when the place ships no usable name string —
        // the unpack helper already returns the zero vector for
        // missing data, so reranking is a no-op on those.
        let primary = place
            .names
            .iter()
            .find(|n| n.lang == "default")
            .or_else(|| place.names.first())
            .map(|n| n.value.as_str())
            .unwrap_or("");
        if !primary.is_empty() {
            let v = semantic::embed(primary);
            // tantivy 0.26 add_bytes takes &[u8] (was Vec<u8> in 0.22).
            let packed = semantic::pack(&v);
            doc.add_bytes(schema.name_vec, &packed);
        }
        doc.add_u64(schema.place_id, place.id.0);
        doc.add_u64(schema.level, place.id.level() as u64);
        doc.add_text(schema.kind, kind_str(place.kind));
        if let Some(gid) = place.gid() {
            doc.add_text(schema.gid, gid);
        }
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

        // Phase 7a-Q — extract `start_date` / `end_date` from OSM
        // tags. Year-only parse (we don't try to be ISO-8601-strict;
        // OSM's date tags are notoriously inconsistent — "1939",
        // "1939-09-01", "1234 BC", "before 1989" all show up). The
        // sentinels (i64::MIN / i64::MAX) get indexed when no parse
        // succeeds, so RangeQuery on `valid_at` matches the place
        // unconditionally — same semantics as "always valid".
        let start_year = place
            .tags
            .iter()
            .find_map(|(k, v)| {
                if k == "start_date" {
                    Some(v.as_str())
                } else {
                    None
                }
            })
            .and_then(parse_year_loose)
            .unwrap_or(i64::MIN);
        let end_year = place
            .tags
            .iter()
            .find_map(|(k, v)| {
                if k == "end_date" {
                    Some(v.as_str())
                } else {
                    None
                }
            })
            .and_then(parse_year_loose)
            .unwrap_or(i64::MAX);
        doc.add_i64(schema.start_year, start_year);
        doc.add_i64(schema.end_year, end_year);

        writer.add_document(doc)?;
        doc_count += 1;
    }
    writer.commit()?;
    // Block until tantivy's background merge threads quiesce before
    // returning. Otherwise cairn-build's blake3 hashing race with
    // post-commit merges produces manifest hashes that don't match
    // the file bytes once the merge thread finishes its last write.
    //
    // We previously also called `writer.merge(searchable_segment_ids())`
    // here to force a single-segment final state (Phase 6d C1) — the
    // single-segment shape cuts BM25 cross-segment overhead 20-40%
    // on small bundles. On Europe-scale corpora (25.45 M docs) the
    // explicit merge raced against tantivy's own LogMergePolicy:
    // background merges started consuming some segment_ids before our
    // call ran, then the explicit `merge(...)` saw orphan ids and
    // returned `'The segments that were merged could not be found in
    // the SegmentManager'`. Bundle build wall-clock at the failure
    // point: 7 h 9 m on Europe.
    //
    // The natural LogMergePolicy already collapses small / medium
    // corpora to a single segment; large corpora may end with 2-4
    // segments, which is the same shape every other tantivy-backed
    // geocoder ships. Trade the build-time race for the per-query
    // overhead and let tantivy do its thing.
    writer.wait_merging_threads()?;
    debug!(docs = doc_count, "tantivy index committed");
    Ok(doc_count)
}

pub struct TextIndex {
    index: Index,
    reader: IndexReader,
    schema: TextSchema,
    /// Optional admin-id → name map for label rendering. Loaded from
    /// `<dir>/admin_names.json` at open time; empty when the sidecar
    /// is missing.
    admin_names: std::sync::Arc<std::collections::HashMap<u64, String>>,
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
        let admin_names = load_admin_names(dir);
        Ok(Self {
            index,
            reader,
            schema,
            admin_names: std::sync::Arc::new(admin_names),
        })
    }

    /// Read-only view of the admin-id → name map. Empty when the
    /// bundle was built without the sidecar (older bundles).
    pub fn admin_names(&self) -> &std::collections::HashMap<u64, String> {
        &self.admin_names
    }

    pub fn search(&self, query: &str, opts: &SearchOptions) -> Result<Vec<Hit>, TextError> {
        let trimmed = query.trim();
        if trimmed.is_empty() {
            return Ok(Vec::new());
        }
        // Phase 7a-E — drop multi-token interior stop-words ("de",
        // "la", "der", "the"…) before BM25 weighting so a "Place de la
        // Bastille" query doesn't get crushed by POIs that just
        // contain "de" + "la". Single- and two-token queries pass
        // through untouched; head + tail tokens always preserved.
        let stop_filtered = stopwords::filter(trimmed);
        let trimmed = stop_filtered.as_str();

        let text_q = self.build_text_query(trimmed, opts)?;
        let text_q = self.apply_phonetic_orclause(text_q, trimmed, opts);
        let combined = self.apply_layer_filter(text_q, &opts.layers);
        let combined = self.apply_categories_filter(combined, &opts.categories);
        let combined = self.apply_valid_at_filter(combined, opts.valid_at);

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
        let raw = searcher.search(
            &combined,
            &TopDocs::with_limit(candidate_limit).order_by_score(),
        )?;

        let mut hits: Vec<Hit> = Vec::with_capacity(raw.len());
        for (score, addr) in raw {
            let doc: TantivyDocument = searcher.doc(addr)?;
            hits.push(self.hit_from_doc(score, &doc));
        }

        // Hard-clip on the optional viewport rect. Done post-fetch so
        // that BM25 / fuzzy / focus all stay independent of geometry.
        // Rejecting only on the extracted (lon, lat) keeps this O(n)
        // over the candidate set.
        if let Some(bbox) = opts.bbox {
            // Skip degenerate / antimeridian-crossing rects so we don't
            // accidentally drop everything; treating those as a no-op
            // surfaces the bad input via empty results downstream.
            if bbox.min_lon <= bbox.max_lon && bbox.min_lat <= bbox.max_lat {
                hits.retain(|h| bbox.contains(h.lon, h.lat));
            }
        }

        // Seed explain payloads with BM25 baseline; downstream rerank
        // steps overwrite their multiplier slot when they fire.
        if opts.explain {
            for h in hits.iter_mut() {
                h.explain = Some(HitExplain {
                    bm25: h.score,
                    exact_match_boost: 1.0,
                    population_boost: 1.0,
                    language_boost: 1.0,
                    geo_bias: 1.0,
                    final_score: h.score,
                });
            }
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
        // Phase 7a-N — Myers bit-parallel Levenshtein boost. Only
        // fires when fuzzy matching produced > 1 hit AND the query
        // length is in the Myers fast-path range. Pulls candidates
        // closer to the query in edit-distance space without
        // disturbing exact / population / lang scoring.
        if matches!(opts.mode, SearchMode::Search) && opts.fuzzy > 0 && hits.len() > 1 {
            apply_edit_distance_boost(&mut hits, trimmed);
        }
        apply_population_boost(&mut hits);
        if let Some(lang) = opts.prefer_lang.as_deref() {
            apply_lang_preference_boost(&mut hits, lang);
        }
        if opts.semantic {
            self.apply_semantic_boost(&mut hits, trimmed)?;
        }

        if let Some(focus) = opts.focus {
            apply_geo_bias(&mut hits, focus, opts.focus_weight);
        }
        hits.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        hits.truncate(opts.limit);
        // Stamp the post-rerank score back into explain so callers see
        // the same number used for the sort key.
        if opts.explain {
            for h in hits.iter_mut() {
                if let Some(ex) = h.explain.as_mut() {
                    ex.final_score = h.score;
                }
            }
        }
        // Populate label after truncation — only the surviving hits
        // pay the format_label cost.
        if !self.admin_names.is_empty() {
            for h in hits.iter_mut() {
                h.label = format_label(h, &self.admin_names);
            }
        }
        Ok(hits)
    }

    /// Resolve a list of Pelias-style `gid` strings (e.g.
    /// `osm:way:12345`) to [`Hit`]s. Stable across rebuilds when the
    /// underlying upstream data is unchanged — suitable for the
    /// `/v1/place?ids=…` endpoint and for client-side bookmark
    /// resolution. Missing gids are silently skipped; result order
    /// mirrors `gids` so callers can correlate.
    pub fn lookup_by_gids(&self, gids: &[String]) -> Result<Vec<Hit>, TextError> {
        if gids.is_empty() {
            return Ok(Vec::new());
        }
        let searcher = self.reader.searcher();
        let mut hits_by_gid: std::collections::HashMap<String, Hit> =
            std::collections::HashMap::with_capacity(gids.len());
        for gid in gids {
            if gid.is_empty() {
                continue;
            }
            let term = tantivy::Term::from_field_text(self.schema.gid, gid);
            let q: Box<dyn Query> = Box::new(TermQuery::new(term, IndexRecordOption::Basic));
            let raw = searcher.search(&q, &TopDocs::with_limit(1).order_by_score())?;
            if let Some((score, addr)) = raw.into_iter().next() {
                let doc: TantivyDocument = searcher.doc(addr)?;
                hits_by_gid.insert(gid.clone(), self.hit_from_doc(score, &doc));
            }
        }
        Ok(gids.iter().filter_map(|g| hits_by_gid.remove(g)).collect())
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
            let raw = searcher.search(&q, &TopDocs::with_limit(1).order_by_score())?;
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
        let fuzzy_query: Box<dyn Query> = Box::new(BooleanQuery::new(clauses));

        // Phase 7a — char-trigram pre-filter. Adds a `Must` clause
        // requiring at least one query trigram to match the doc's
        // `name_trigrams` field, so docs sharing zero trigrams with
        // the query never reach BM25 scoring. Bypassed for queries
        // shorter than `MIN_QUERY_LEN_FOR_FILTER` (too few trigrams
        // to avoid false negatives on edit-distance-1 typos).
        if let Some(filter) = self.build_trigram_prefilter(query) {
            return Ok(Box::new(BooleanQuery::new(vec![
                (Occur::Must, fuzzy_query),
                (Occur::Must, filter),
            ])));
        }
        Ok(fuzzy_query)
    }

    /// Build the trigram pre-filter clause for a query string.
    /// Returns `None` when the query is too short to filter safely
    /// or when no trigrams could be extracted.
    fn build_trigram_prefilter(&self, query: &str) -> Option<Box<dyn Query>> {
        if query.chars().count() < trigram::MIN_QUERY_LEN_FOR_FILTER {
            return None;
        }
        let trigrams = trigram::extract_query(query);
        if trigrams.is_empty() {
            return None;
        }
        let mut clauses: Vec<(Occur, Box<dyn Query>)> = Vec::with_capacity(trigrams.len());
        for tg in trigrams {
            let term = Term::from_field_text(self.schema.name_trigrams, &tg);
            clauses.push((
                Occur::Should,
                Box::new(TermQuery::new(term, IndexRecordOption::Basic)),
            ));
        }
        Some(Box::new(BooleanQuery::new(clauses)))
    }

    /// When `phonetic=true` and the query produces non-empty
    /// DoubleMetaphone codes, OR an extra clause against the
    /// `name_phonetic` field. Each query token's primary + alternate
    /// codes become Should-terms, then the encoded clause is unioned
    /// (Should / Should) with the original text query so phonetic
    /// matches widen recall without dropping BM25 hits. Multi-token
    /// queries OR every token's codes — order doesn't matter for
    /// catching variant spellings.
    /// Rerank `hits` by lexical-vector cosine similarity to the
    /// query. Reads each hit's stored `name_vec` blob, decodes it,
    /// computes cosine vs the query embedding, and applies a
    /// monotonic multiplicative boost via [`semantic::boost_for`].
    /// Hits with no vector (older bundles) get the zero vector and
    /// no boost.
    fn apply_semantic_boost(&self, hits: &mut [Hit], query: &str) -> Result<(), TextError> {
        let qv = semantic::embed(query);
        // Skip if query produced the zero vector — too short to embed.
        if qv.iter().all(|x| *x == 0.0) {
            return Ok(());
        }
        // Phase 6e B1 — read each Hit's preloaded `name_vec_bytes` (set
        // by `hit_from_doc` in the BM25 pass) instead of doing N
        // TermQuery + searcher.doc lookups per hit. Cuts -5-15ms off
        // p95 on `semantic=true` queries by removing the per-hit
        // round-trip into the inverted index.
        for h in hits.iter_mut() {
            if let Some(bytes) = h.name_vec_bytes.as_deref() {
                let v = semantic::unpack(bytes);
                let sim = semantic::cosine(&qv, &v);
                let boost = semantic::boost_for(sim);
                h.score *= boost;
            }
        }
        Ok(())
    }

    fn apply_phonetic_orclause(
        &self,
        text_q: Box<dyn Query>,
        query: &str,
        opts: &SearchOptions,
    ) -> Box<dyn Query> {
        if !opts.phonetic {
            return text_q;
        }
        // Skip CJK on the query side too — the encoder produces
        // empty / nonsense for non-Latin input.
        if has_cjk(query) {
            return text_q;
        }
        let folded = ascii_fold(query).unwrap_or_else(|| query.to_string());
        let tokens: Vec<&str> = folded.split_whitespace().collect();
        let mut code_clauses: Vec<(Occur, Box<dyn Query>)> = Vec::new();
        let mut seen: FxHashSet<String> = FxHashSet::default();
        for tok in tokens {
            // DoubleMetaphone produces noisy codes on short tokens
            // (1-2 chars often collapse to a single letter that
            // matches a wide swath of the corpus). Skip them on the
            // query side — the index still carries the codes; we
            // just don't OR them into the rerank widening clause for
            // these tokens. ≥ 3 chars threshold is the empirical
            // floor where the encoder produces specific-enough
            // codes for the precision/recall tradeoff to favor
            // including the clause.
            if tok.chars().count() < PHONETIC_MIN_TOKEN_LEN {
                continue;
            }
            for code in phonetic_codes(tok) {
                if seen.insert(code.clone()) {
                    let term = Term::from_field_text(self.schema.name_phonetic, &code);
                    let q = TermQuery::new(term, IndexRecordOption::Basic);
                    code_clauses.push((Occur::Should, Box::new(q) as Box<dyn Query>));
                }
            }
        }
        if code_clauses.is_empty() {
            return text_q;
        }
        let phonetic_q: Box<dyn Query> = Box::new(BooleanQuery::new(code_clauses));
        Box::new(BooleanQuery::new(vec![
            (Occur::Should, text_q),
            (Occur::Should, phonetic_q),
        ]))
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

    /// Phase 7a-Q — temporal validity filter. Adds two `Must` range
    /// clauses requiring `start_year <= valid_at <= end_year`. Places
    /// without OSM date tags carry sentinel bounds (`i64::MIN`..=
    /// `i64::MAX`) and pass every range, so this filter is opt-in
    /// per-query and never excludes rows that simply lack date tags.
    fn apply_valid_at_filter(
        &self,
        text_q: Box<dyn Query>,
        valid_at: Option<i64>,
    ) -> Box<dyn Query> {
        let year = match valid_at {
            Some(y) => y,
            None => return text_q,
        };
        let start_term_lo = Term::from_field_i64(self.schema.start_year, i64::MIN);
        let start_term_hi = Term::from_field_i64(self.schema.start_year, year);
        let start_q: Box<dyn Query> = Box::new(RangeQuery::new(
            std::ops::Bound::Included(start_term_lo),
            std::ops::Bound::Included(start_term_hi),
        ));
        let end_term_lo = Term::from_field_i64(self.schema.end_year, year);
        let end_term_hi = Term::from_field_i64(self.schema.end_year, i64::MAX);
        let end_q: Box<dyn Query> = Box::new(RangeQuery::new(
            std::ops::Bound::Included(end_term_lo),
            std::ops::Bound::Included(end_term_hi),
        ));
        Box::new(BooleanQuery::new(vec![
            (Occur::Must, text_q),
            (Occur::Must, start_q),
            (Occur::Must, end_q),
        ]))
    }

    /// OR-of-categories filter, AND-joined with the rest of the
    /// query. Categories are case-insensitive on the lookup side
    /// (lowercased to match indexed form). Empty `categories` skips
    /// the filter.
    fn apply_categories_filter(
        &self,
        text_q: Box<dyn Query>,
        categories: &[String],
    ) -> Box<dyn Query> {
        if categories.is_empty() {
            return text_q;
        }
        let mut clauses: Vec<(Occur, Box<dyn Query>)> = Vec::with_capacity(categories.len());
        for cat in categories {
            let normalized = cat.trim().to_lowercase();
            if normalized.is_empty() {
                continue;
            }
            let term = Term::from_field_text(self.schema.categories, &normalized);
            let q = TermQuery::new(term, IndexRecordOption::Basic);
            clauses.push((Occur::Should, Box::new(q) as Box<dyn Query>));
        }
        if clauses.is_empty() {
            return text_q;
        }
        let cat_q: Box<dyn Query> = Box::new(BooleanQuery::new(clauses));
        Box::new(BooleanQuery::new(vec![
            (Occur::Must, text_q),
            (Occur::Must, cat_q),
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
            label: String::new(),
            langs: doc
                .get_all(self.schema.lang_codes)
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect(),
            categories: doc
                .get_all(self.schema.categories)
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect(),
            gid: doc
                .get_first(self.schema.gid)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            explain: None,
            name_vec_bytes: doc
                .get_first(self.schema.name_vec)
                .and_then(|v| v.as_bytes())
                .map(|b| b.to_vec()),
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
            if let Some(ex) = h.explain.as_mut() {
                ex.exact_match_boost = EXACT_MATCH_BOOST;
            }
        }
    }
}

/// Phase 7a-N — multiplicative score boost based on Myers bit-parallel
/// Levenshtein edit distance between (folded) query and (folded) hit
/// name. The boost decays linearly from `EDIT_DIST_BOOST_MAX` at zero
/// distance down to `1.0` (no boost) at `EDIT_DIST_NEUTRAL` and beyond.
///
/// Only fires when the candidate set is fuzzy-matched (otherwise the
/// pre-existing exact-match boost already covers the relevant cases).
/// `fuzzy=0` skips the rerank entirely; for short queries we also bail
/// because edit-distance scaling on 2-3 char names is degenerate.
const EDIT_DIST_BOOST_MAX: f32 = 1.6;
const EDIT_DIST_NEUTRAL: f32 = 4.0;

fn apply_edit_distance_boost(hits: &mut [Hit], query: &str) {
    let q = fold_for_compare(query);
    if q.chars().count() < 4 {
        return;
    }
    for h in hits.iter_mut() {
        let folded_name = fold_for_compare(&h.name);
        if folded_name.is_empty() {
            continue;
        }
        let dist = edit::edit_distance(&q, &folded_name) as f32;
        if dist >= EDIT_DIST_NEUTRAL {
            continue;
        }
        let t = (EDIT_DIST_NEUTRAL - dist) / EDIT_DIST_NEUTRAL;
        let boost = 1.0 + (EDIT_DIST_BOOST_MAX - 1.0) * t;
        h.score *= boost;
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
        if let Some(ex) = h.explain.as_mut() {
            ex.population_boost = factor;
        }
    }
}

/// Multiply each hit's score by [`LANG_PREFERENCE_BOOST`] when the hit
/// has a localized name in `lang`. Case-insensitive on the language
/// code. No-op when `lang` is empty or no hit carries it (so a query
/// for `?lang=xx` won't reshuffle results when nothing matches).
fn apply_lang_preference_boost(hits: &mut [Hit], lang: &str) {
    let needle = lang.trim().to_lowercase();
    if needle.is_empty() {
        return;
    }
    for h in hits.iter_mut() {
        if h.langs.iter().any(|l| l.to_lowercase() == needle) {
            h.score *= LANG_PREFERENCE_BOOST;
            if let Some(ex) = h.explain.as_mut() {
                ex.language_boost = LANG_PREFERENCE_BOOST;
            }
        }
    }
}

/// Phase 7a-Q — loose year parser for OSM `start_date` / `end_date`
/// tag values. Handles plain years (`"1939"`), ISO dates
/// (`"1939-09-01"`), BC dates (`"1234 BC"`), and "before/after Y"
/// prefixes. Returns `None` for unparseable input — caller falls
/// back to the sentinel bounds so the place still matches every
/// `valid_at` filter.
pub fn parse_year_loose(raw: &str) -> Option<i64> {
    let s = raw.trim();
    if s.is_empty() {
        return None;
    }
    // BC suffix → negate.
    let lower = s.to_ascii_lowercase();
    let (digits, sign) = if let Some(stripped) = lower
        .strip_suffix(" bc")
        .or_else(|| lower.strip_suffix("bc"))
    {
        (stripped.trim().to_string(), -1i64)
    } else {
        (lower, 1i64)
    };
    // Strip "before " / "after " / "circa " / "ca." prefixes; these
    // give an approximate bound, we ignore the qualifier.
    let cleaned = digits
        .trim_start_matches("before ")
        .trim_start_matches("after ")
        .trim_start_matches("circa ")
        .trim_start_matches("ca. ")
        .trim_start_matches("ca.")
        .trim_start_matches("c. ")
        .trim();
    // ISO date: take the leading 4-digit year.
    let leading: String = cleaned.chars().take_while(|c| c.is_ascii_digit()).collect();
    if leading.is_empty() {
        return None;
    }
    leading.parse::<i64>().ok().map(|y| y * sign)
}

fn fold_for_compare(s: &str) -> String {
    deunicode::deunicode(s).trim().to_lowercase()
}

fn load_admin_names(dir: &Path) -> std::collections::HashMap<u64, String> {
    let path = dir.join("admin_names.json");
    let raw = match std::fs::read_to_string(&path) {
        Ok(s) => s,
        Err(_) => return std::collections::HashMap::new(),
    };
    // Sidecar is `{ "u64-as-string": "Name" }`. JSON object keys are
    // always strings, so we parse to BTreeMap<String, String> and
    // convert keys.
    let parsed: std::collections::BTreeMap<String, String> =
        serde_json::from_str(&raw).unwrap_or_default();
    let mut out = std::collections::HashMap::with_capacity(parsed.len());
    for (k, v) in parsed {
        if let Ok(id) = k.parse::<u64>() {
            out.insert(id, v);
        }
    }
    out
}

/// Build a Pelias-style label by joining the hit's name with the
/// resolved admin chain (e.g. "Vaduz Castle, Vaduz, Liechtenstein").
///
/// Order: name first, then admin_path traversed leaf → root.
/// Deduplication runs over the WHOLE chain (not just adjacent) using
/// a parenthetical-suffix-stripped fold key, so "Vaduz" + "Vaduz (Li)"
/// collapse to a single entry. Empty when the bundle has no
/// admin_names sidecar.
fn format_label(hit: &Hit, admin_names: &std::collections::HashMap<u64, String>) -> String {
    if admin_names.is_empty() {
        return String::new();
    }
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut parts: Vec<String> = Vec::with_capacity(1 + hit.admin_path.len());
    let leaf_key = label_key(&hit.name);
    seen.insert(leaf_key);
    parts.push(hit.name.clone());
    for id in hit.admin_path.iter().rev() {
        if let Some(name) = admin_names.get(id) {
            let key = label_key(name);
            if !key.is_empty() && seen.insert(key) {
                parts.push(name.clone());
            }
        }
    }
    parts.join(", ")
}

/// Normalize a label component for dedup: lowercased, deunicoded, and
/// stripped of parenthetical suffixes ("Vaduz (Li)" → "vaduz") so
/// admin-tier disambiguation noise doesn't double-print in the label.
fn label_key(s: &str) -> String {
    let folded = deunicode::deunicode(s).to_lowercase();
    let trimmed = folded.split('(').next().unwrap_or("").trim();
    trimmed.to_string()
}

fn apply_geo_bias(hits: &mut [Hit], focus: Coord, weight: f64) {
    let weight = weight.max(0.0);
    for h in hits.iter_mut() {
        let km = haversine_km(focus.lat, focus.lon, h.lat, h.lon);
        h.distance_km = Some(km);
        let divisor = 1.0 + weight * km;
        let blended = (h.score as f64) / divisor;
        h.score = blended as f32;
        if let Some(ex) = h.explain.as_mut() {
            // Record the divisor's reciprocal so all explain fields
            // stay multipliers — easier for callers to reason about.
            ex.geo_bias = (1.0 / divisor) as f32;
        }
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

    // ── Phase 7a-Q: parse_year_loose tests ────────────────────────

    #[test]
    fn parse_year_plain_digits() {
        assert_eq!(parse_year_loose("1939"), Some(1939));
        assert_eq!(parse_year_loose("  2024  "), Some(2024));
    }

    #[test]
    fn parse_year_iso_date() {
        assert_eq!(parse_year_loose("1939-09-01"), Some(1939));
        assert_eq!(parse_year_loose("2024-01-15"), Some(2024));
    }

    #[test]
    fn parse_year_bc_negation() {
        assert_eq!(parse_year_loose("44 BC"), Some(-44));
        assert_eq!(parse_year_loose("753bc"), Some(-753));
    }

    #[test]
    fn parse_year_qualifier_prefixes() {
        assert_eq!(parse_year_loose("before 1989"), Some(1989));
        assert_eq!(parse_year_loose("circa 1850"), Some(1850));
        assert_eq!(parse_year_loose("ca. 1700"), Some(1700));
        assert_eq!(parse_year_loose("c. 1500"), Some(1500));
    }

    #[test]
    fn parse_year_unparseable_returns_none() {
        assert_eq!(parse_year_loose(""), None);
        assert_eq!(parse_year_loose("yesterday"), None);
        assert_eq!(parse_year_loose("Q1 2024"), None); // letter at start
    }

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
    fn label_joins_admin_chain_root_to_leaf() {
        let mut admin_names: std::collections::HashMap<u64, String> =
            std::collections::HashMap::new();
        admin_names.insert(100, "Liechtenstein".into());
        admin_names.insert(200, "Oberland".into());
        admin_names.insert(300, "Vaduz (Li)".into());
        let hit = Hit {
            place_id: 999,
            name: "Vaduz Castle".into(),
            kind: "poi".into(),
            level: 2,
            lon: 9.5,
            lat: 47.1,
            score: 1.0,
            admin_path: vec![100, 200, 300],
            distance_km: None,
            population: 0,
            label: String::new(),
            langs: Vec::new(),
            categories: Vec::new(),
            gid: String::new(),
            explain: None,
            name_vec_bytes: None,
        };
        let label = super::format_label(&hit, &admin_names);
        // leaf → root order, parenthetical suffix dedup against
        // "Vaduz Castle" (different stem). Country last.
        assert_eq!(label, "Vaduz Castle, Vaduz (Li), Oberland, Liechtenstein");
    }

    #[test]
    fn label_dedup_strips_parenthetical_suffix() {
        let mut admin_names: std::collections::HashMap<u64, String> =
            std::collections::HashMap::new();
        admin_names.insert(50, "Vaduz (Li)".into());
        admin_names.insert(60, "Liechtenstein".into());
        let hit = Hit {
            place_id: 1,
            name: "Vaduz".into(),
            kind: "city".into(),
            level: 1,
            lon: 9.5,
            lat: 47.1,
            score: 1.0,
            admin_path: vec![60, 50],
            distance_km: None,
            population: 0,
            label: String::new(),
            langs: Vec::new(),
            categories: Vec::new(),
            gid: String::new(),
            explain: None,
            name_vec_bytes: None,
        };
        let label = super::format_label(&hit, &admin_names);
        // 'Vaduz (Li)' folds to 'vaduz' which equals leaf 'Vaduz' →
        // dropped. Only Liechtenstein survives the chain.
        assert_eq!(label, "Vaduz, Liechtenstein");
    }

    #[test]
    fn label_empty_when_admin_names_missing() {
        let admin_names: std::collections::HashMap<u64, String> = std::collections::HashMap::new();
        let hit = Hit {
            place_id: 1,
            name: "Vaduz".into(),
            kind: "city".into(),
            level: 1,
            lon: 9.5,
            lat: 47.1,
            score: 1.0,
            admin_path: vec![1, 2, 3],
            distance_km: None,
            population: 0,
            label: String::new(),
            langs: Vec::new(),
            categories: Vec::new(),
            gid: String::new(),
            explain: None,
            name_vec_bytes: None,
        };
        assert_eq!(super::format_label(&hit, &admin_names), "");
    }

    #[test]
    fn phonetic_finds_mueller_for_muller() {
        // Indexed name "Müller" — DoubleMetaphone should give the same
        // codes as query "Mueller" after ASCII-folding.
        let dir = tempdir_for_test();
        let muller = Place {
            id: PlaceId::new(2, 1, 1).unwrap(),
            kind: PlaceKind::Poi,
            names: vec![LocalizedName {
                lang: "default".into(),
                value: "Müller".into(),
            }],
            centroid: Coord {
                lon: 11.0,
                lat: 48.0,
            },
            admin_path: vec![],
            tags: vec![],
        };
        build_index(&dir, vec![muller]).unwrap();
        let idx = TextIndex::open(&dir).unwrap();

        // Without phonetic flag: deunicode already rescues this case
        // via `name_translit` (Müller → Muller). Sanity check.
        let baseline = idx.search("Mueller", &SearchOptions::default()).unwrap();
        // baseline may or may not match depending on transliteration;
        // we don't assert here — the phonetic path is what we test.

        // With phonetic on: must find the record even if the translit
        // path missed.
        let opts = SearchOptions {
            phonetic: true,
            ..Default::default()
        };
        let hits = idx.search("Mueller", &opts).unwrap();
        assert!(
            !hits.is_empty(),
            "phonetic Mueller→Müller failed (baseline {} hits): {hits:?}",
            baseline.len()
        );
    }

    #[test]
    fn phonetic_finds_smith_for_smyth() {
        let dir = tempdir_for_test();
        let smith = Place {
            id: PlaceId::new(2, 1, 1).unwrap(),
            kind: PlaceKind::Poi,
            names: vec![LocalizedName {
                lang: "default".into(),
                value: "Smith Plaza".into(),
            }],
            centroid: Coord { lon: 0.0, lat: 0.0 },
            admin_path: vec![],
            tags: vec![],
        };
        build_index(&dir, vec![smith]).unwrap();
        let idx = TextIndex::open(&dir).unwrap();
        let opts = SearchOptions {
            phonetic: true,
            ..Default::default()
        };
        // "Smyth Plaza" should phonetically match "Smith Plaza".
        let hits = idx.search("Smyth Plaza", &opts).unwrap();
        assert!(!hits.is_empty(), "phonetic Smyth→Smith failed: {hits:?}");
    }

    #[test]
    fn phonetic_off_misses_misspelled_query() {
        let dir = tempdir_for_test();
        let smith = Place {
            id: PlaceId::new(2, 1, 1).unwrap(),
            kind: PlaceKind::Poi,
            names: vec![LocalizedName {
                lang: "default".into(),
                value: "Smithsonian".into(),
            }],
            centroid: Coord { lon: 0.0, lat: 0.0 },
            admin_path: vec![],
            tags: vec![],
        };
        build_index(&dir, vec![smith]).unwrap();
        let idx = TextIndex::open(&dir).unwrap();
        let hits = idx
            .search("Smythsonian", &SearchOptions::default())
            .unwrap();
        // Without phonetic and without fuzzy, the misspelled token
        // doesn't match — confirms the phonetic path delivers signal.
        assert!(hits.is_empty(), "non-phonetic shouldn't match: {hits:?}");

        // Phonetic on: codes for "Smythsonian" / "Smithsonian" align.
        let opts = SearchOptions {
            phonetic: true,
            ..Default::default()
        };
        let hits = idx.search("Smythsonian", &opts).unwrap();
        assert!(!hits.is_empty(), "phonetic should rescue: {hits:?}");
    }

    #[test]
    fn bbox_filter_drops_outside_centroids() {
        let dir = tempdir_for_test();
        // Vaduz at 9.5209, 47.141; Schaan at 9.5095, 47.165.
        // Tight bbox over Vaduz only.
        build_index(&dir, vec![vaduz(), schaan()]).unwrap();
        let idx = TextIndex::open(&dir).unwrap();
        let bbox = Bbox {
            min_lon: 9.515,
            min_lat: 47.135,
            max_lon: 9.525,
            max_lat: 47.145,
        };
        let opts = SearchOptions {
            bbox: Some(bbox),
            ..Default::default()
        };
        let hits = idx.search("Vaduz", &opts).unwrap();
        assert!(!hits.is_empty(), "Vaduz must be inside its own bbox");
        for h in &hits {
            assert!(
                bbox.contains(h.lon, h.lat),
                "leaked outside-bbox hit: {h:?}"
            );
        }
        // Schaan is outside this bbox; querying for it returns nothing.
        let hits_schaan = idx.search("Schaan", &opts).unwrap();
        assert!(
            hits_schaan.is_empty(),
            "Schaan should be clipped, got {hits_schaan:?}"
        );
    }

    #[test]
    fn bbox_inverted_rect_is_noop_not_empty() {
        // Inverted rect (min_lon > max_lon) currently means
        // "antimeridian crosser" — we treat as no-op rather than
        // accidentally clipping everything. Documented in comment.
        let dir = tempdir_for_test();
        build_index(&dir, vec![vaduz(), schaan()]).unwrap();
        let idx = TextIndex::open(&dir).unwrap();
        let bad = SearchOptions {
            bbox: Some(Bbox {
                min_lon: 100.0,
                min_lat: 47.0,
                max_lon: 9.0,
                max_lat: 48.0,
            }),
            ..Default::default()
        };
        let hits = idx.search("Vaduz", &bad).unwrap();
        // Falls back to no-bbox behaviour, so we still get hits.
        assert!(!hits.is_empty(), "inverted bbox must not nuke results");
    }

    #[test]
    fn categories_filter_drops_non_matching_kinds() {
        let dir = tempdir_for_test();
        // Two POIs with the same searchable name but different
        // amenities. Filter on `categories=hospital` must keep only
        // the hospital, drop the bakery.
        let hospital = Place {
            id: PlaceId::new(2, 1, 1).unwrap(),
            kind: PlaceKind::Poi,
            names: vec![LocalizedName {
                lang: "default".into(),
                value: "Riva Center".into(),
            }],
            centroid: Coord {
                lon: 11.0,
                lat: 46.0,
            },
            admin_path: vec![],
            tags: vec![("amenity".into(), "hospital".into())],
        };
        let bakery = Place {
            id: PlaceId::new(2, 1, 2).unwrap(),
            kind: PlaceKind::Poi,
            names: vec![LocalizedName {
                lang: "default".into(),
                value: "Riva Center".into(),
            }],
            centroid: Coord {
                lon: 11.1,
                lat: 46.0,
            },
            admin_path: vec![],
            tags: vec![("shop".into(), "bakery".into())],
        };
        build_index(&dir, vec![hospital, bakery]).unwrap();
        let idx = TextIndex::open(&dir).unwrap();

        // No filter: both come back.
        let all = idx
            .search("Riva Center", &SearchOptions::default())
            .unwrap();
        assert_eq!(all.len(), 2, "no-filter sanity: got {all:?}");

        // Filter to hospital category: only the hospital.
        let opts = SearchOptions {
            categories: vec!["hospital".into()],
            ..Default::default()
        };
        let hits = idx.search("Riva Center", &opts).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].place_id, PlaceId::new(2, 1, 1).unwrap().0);
        assert!(hits[0].categories.contains(&"hospital".into()));
    }

    #[test]
    fn categories_filter_or_semantics_across_list() {
        let dir = tempdir_for_test();
        let hospital = Place {
            id: PlaceId::new(2, 1, 1).unwrap(),
            kind: PlaceKind::Poi,
            names: vec![LocalizedName {
                lang: "default".into(),
                value: "Salus Place".into(),
            }],
            centroid: Coord { lon: 0.0, lat: 0.0 },
            admin_path: vec![],
            tags: vec![("amenity".into(), "hospital".into())],
        };
        let school = Place {
            id: PlaceId::new(2, 1, 2).unwrap(),
            kind: PlaceKind::Poi,
            names: vec![LocalizedName {
                lang: "default".into(),
                value: "Salus Place".into(),
            }],
            centroid: Coord { lon: 0.1, lat: 0.0 },
            admin_path: vec![],
            tags: vec![("amenity".into(), "school".into())],
        };
        let bar = Place {
            id: PlaceId::new(2, 1, 3).unwrap(),
            kind: PlaceKind::Poi,
            names: vec![LocalizedName {
                lang: "default".into(),
                value: "Salus Place".into(),
            }],
            centroid: Coord { lon: 0.2, lat: 0.0 },
            admin_path: vec![],
            tags: vec![("amenity".into(), "bar".into())],
        };
        build_index(&dir, vec![hospital, school, bar]).unwrap();
        let idx = TextIndex::open(&dir).unwrap();
        let opts = SearchOptions {
            // hospital OR school — bar must drop out.
            categories: vec!["hospital".into(), "school".into()],
            ..Default::default()
        };
        let hits = idx.search("Salus Place", &opts).unwrap();
        let ids: Vec<u64> = hits.iter().map(|h| h.place_id).collect();
        assert_eq!(ids.len(), 2, "expected 2 hits, got {hits:?}");
        assert!(ids.contains(&PlaceId::new(2, 1, 1).unwrap().0));
        assert!(ids.contains(&PlaceId::new(2, 1, 2).unwrap().0));
        assert!(!ids.contains(&PlaceId::new(2, 1, 3).unwrap().0));
    }

    #[test]
    fn lang_preference_boosts_matching_language_hit() {
        let dir = tempdir_for_test();
        // Two records with the same name string. One ships only a
        // 'default' language tag, the other adds 'de'. Asking for
        // ?lang=de boosts the second.
        let plain = Place {
            id: PlaceId::new(1, 1, 1).unwrap(),
            kind: PlaceKind::City,
            names: vec![LocalizedName {
                lang: "default".into(),
                value: "Brixen".into(),
            }],
            centroid: Coord {
                lon: 11.0,
                lat: 46.7,
            },
            admin_path: vec![],
            tags: vec![],
        };
        let german = Place {
            id: PlaceId::new(1, 1, 2).unwrap(),
            kind: PlaceKind::City,
            names: vec![
                LocalizedName {
                    lang: "default".into(),
                    value: "Brixen".into(),
                },
                LocalizedName {
                    lang: "de".into(),
                    value: "Brixen".into(),
                },
            ],
            centroid: Coord {
                lon: 11.6,
                lat: 46.7,
            },
            admin_path: vec![],
            tags: vec![],
        };
        build_index(&dir, vec![plain, german]).unwrap();
        let idx = TextIndex::open(&dir).unwrap();
        let opts = SearchOptions {
            prefer_lang: Some("de".into()),
            ..Default::default()
        };
        let hits = idx.search("Brixen", &opts).unwrap();
        assert_eq!(hits.len(), 2);
        assert_eq!(
            hits[0].place_id,
            PlaceId::new(1, 1, 2).unwrap().0,
            "lang=de must promote the German-tagged record, got {hits:?}"
        );
        assert!(hits[0].langs.iter().any(|l| l == "de"));
    }

    #[test]
    fn gid_round_trips_through_search_results() {
        // A Place tagged with `gid=osm:way:12345` (set by the OSM
        // importer) must surface that identifier verbatim on every
        // matching hit — both via free-text search and via the
        // direct gid resolver.
        let dir = tempdir_for_test();
        let mut place = Place {
            id: PlaceId::new(1, 1, 1).unwrap(),
            kind: PlaceKind::City,
            names: vec![LocalizedName {
                lang: "default".into(),
                value: "Vaduz".into(),
            }],
            centroid: Coord {
                lon: 9.5209,
                lat: 47.141,
            },
            admin_path: vec![],
            tags: vec![],
        };
        place.set_gid("osm:way:12345");
        build_index(&dir, vec![place]).unwrap();
        let idx = TextIndex::open(&dir).unwrap();

        // Free-text search surfaces the gid on the hit.
        let hits = idx.search("Vaduz", &SearchOptions::default()).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].gid, "osm:way:12345");

        // gid resolver finds the hit by gid alone.
        let resolved = idx.lookup_by_gids(&["osm:way:12345".to_string()]).unwrap();
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].name, "Vaduz");
        // Result order matches input order; missing gids dropped.
        let mixed = idx
            .lookup_by_gids(&[
                "osm:way:nonexistent".to_string(),
                "osm:way:12345".to_string(),
            ])
            .unwrap();
        assert_eq!(mixed.len(), 1);
        assert_eq!(mixed[0].gid, "osm:way:12345");
    }

    #[test]
    fn alias_alt_lang_normalizes_into_canonical_lang_codes() {
        // A Place tagged `wikidata=Q...` whose enrichment ships an
        // English alias must surface for `?lang=en` queries even
        // though the canonical name carries no `en` LocalizedName.
        // The wikidata augmenter writes aliases as `<lang>_alt`; the
        // indexer must strip `_alt` when populating `lang_codes`.
        let dir = tempdir_for_test();
        let place = Place {
            id: PlaceId::new(1, 1, 1).unwrap(),
            kind: PlaceKind::City,
            names: vec![
                LocalizedName {
                    lang: "default".into(),
                    value: "München".into(),
                },
                LocalizedName {
                    lang: "en_alt".into(),
                    value: "Munich".into(),
                },
            ],
            centroid: Coord {
                lon: 11.58,
                lat: 48.13,
            },
            admin_path: vec![],
            tags: vec![],
        };
        build_index(&dir, vec![place]).unwrap();
        let idx = TextIndex::open(&dir).unwrap();
        // Search hits via the alias text.
        let opts = SearchOptions {
            prefer_lang: Some("en".into()),
            ..Default::default()
        };
        let hits = idx.search("Munich", &opts).unwrap();
        assert_eq!(hits.len(), 1);
        // Crucially: lang_codes carries `en` (canonical), not `en_alt`.
        assert!(
            hits[0].langs.iter().any(|l| l == "en"),
            "expected canonical `en` in langs, got {:?}",
            hits[0].langs
        );
        assert!(
            !hits[0].langs.iter().any(|l| l == "en_alt"),
            "`_alt` suffix must be stripped before indexing"
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
            centroid: Coord { lon: 0.0, lat: 0.0 },
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
