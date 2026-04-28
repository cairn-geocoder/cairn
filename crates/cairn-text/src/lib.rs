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
const PREFIX_MIN: usize = 1;
const PREFIX_MAX: usize = 25;
const WRITER_HEAP: usize = 64 * 1024 * 1024;
const RERANK_MULTIPLIER: usize = 5;
const MAX_FUZZY_DISTANCE: u8 = 2;

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
    place_id: Field,
    level: Field,
    kind: Field,
    lon: Field,
    lat: Field,
    admin_path: Field,
}

impl TextSchema {
    fn build() -> Self {
        let mut sb = Schema::builder();
        let prefix_indexing = TextFieldIndexing::default()
            .set_tokenizer(PREFIX_TOKENIZER)
            .set_index_option(IndexRecordOption::WithFreqsAndPositions);
        let prefix_options = TextOptions::default().set_indexing_options(prefix_indexing);

        let name = sb.add_text_field("name", TEXT | STORED);
        let name_prefix = sb.add_text_field("name_prefix", prefix_options);
        let place_id = sb.add_u64_field("place_id", FAST | STORED | INDEXED);
        let level = sb.add_u64_field("level", FAST | STORED | INDEXED);
        let kind = sb.add_text_field("kind", STRING | STORED);
        let lon = sb.add_f64_field("lon", STORED);
        let lat = sb.add_f64_field("lat", STORED);
        let admin_path = sb.add_u64_field("admin_path", STORED);
        let schema = sb.build();
        Self {
            schema,
            name,
            name_prefix,
            place_id,
            level,
            kind,
            lon,
            lat,
            admin_path,
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
        }
        doc.add_u64(schema.place_id, place.id.0);
        doc.add_u64(schema.level, place.id.level() as u64);
        doc.add_text(schema.kind, kind_str(place.kind));
        doc.add_f64(schema.lon, place.centroid.lon);
        doc.add_f64(schema.lat, place.centroid.lat);
        for ancestor in &place.admin_path {
            doc.add_u64(schema.admin_path, ancestor.0);
        }
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

        let candidate_limit = if opts.focus.is_some() {
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

        if let Some(focus) = opts.focus {
            apply_geo_bias(&mut hits, focus, opts.focus_weight);
        }
        hits.truncate(opts.limit);
        Ok(hits)
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
            let parser = QueryParser::for_index(&self.index, vec![field]);
            return Ok(parser.parse_query(query)?);
        }

        // Forward search with fuzzy distance: union FuzzyTermQuery per token.
        let lowered = query.to_lowercase();
        let tokens: Vec<&str> = lowered.split_whitespace().collect();
        if tokens.is_empty() {
            let parser = QueryParser::for_index(&self.index, vec![field]);
            return Ok(parser.parse_query(query)?);
        }
        let mut clauses: Vec<(Occur, Box<dyn Query>)> = Vec::with_capacity(tokens.len());
        for tok in tokens {
            let term = Term::from_field_text(field, tok);
            let q = FuzzyTermQuery::new(term, fuzzy, true);
            clauses.push((Occur::Should, Box::new(q) as Box<dyn Query>));
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
        }
    }
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
        let d = std::env::temp_dir().join(format!(
            "cairn-text-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
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
}
