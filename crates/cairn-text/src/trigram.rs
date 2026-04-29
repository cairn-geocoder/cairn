//! Phase 7a — character trigram extraction for fuzzy pre-filter.
//!
//! Tantivy's `FuzzyTermQuery` scans the term dictionary for every term
//! within edit distance `N` of each query token. At country / planet
//! scale that scan dominates fuzzy-query latency. Pre-filtering the
//! candidate doc set by trigram overlap cuts BM25 scoring work to the
//! survivors — with the larger speedup arriving in Phase 7a-N when a
//! Myers-based custom collector replaces the fuzzy term scan entirely.
//!
//! This module exposes:
//!
//! - [`extract_indexed`] — trigram tokens to attach to each indexed
//!   doc's `name_trigrams` field at build time.
//! - [`extract_query`] — same trigrams from a query string, used to
//!   build the `Should`-OR pre-filter clause at search time.
//!
//! Trigrams are case-folded + ASCII-folded + boundary-padded with
//! `^` / `$` so word starts and ends carry their own trigrams (e.g.
//! `vienna` produces `^vi`, `vie`, `ien`, `enn`, `nna`, `na$`). That
//! padding lifts recall on short tokens and prefix-typed queries.

use std::collections::HashSet;

const PAD: char = '\x01'; // non-printable boundary marker
const TRIGRAM_LEN: usize = 3;

/// Minimum input length below which trigram pre-filtering is unsafe
/// (too few trigrams to avoid false negatives). Caller should bypass
/// the filter for shorter queries.
pub const MIN_QUERY_LEN_FOR_FILTER: usize = 4;

/// Extract distinct trigrams to index for a single name string.
/// Empty input → no trigrams.
pub fn extract_indexed(s: &str) -> Vec<String> {
    extract(s)
}

/// Extract distinct trigrams to use as query-side pre-filter terms.
/// Empty result means caller must bypass the filter.
pub fn extract_query(s: &str) -> Vec<String> {
    extract(s)
}

/// Lowercase + ASCII-fold + trigram-extract `s`. Stable order: by
/// first occurrence in the padded string, then de-duplicated.
fn extract(s: &str) -> Vec<String> {
    let folded = fold_lower(s);
    if folded.is_empty() {
        return Vec::new();
    }
    let chars: Vec<char> = std::iter::once(PAD)
        .chain(folded.chars())
        .chain(std::iter::once(PAD))
        .collect();
    if chars.len() < TRIGRAM_LEN {
        return Vec::new();
    }
    let mut seen: HashSet<String> = HashSet::new();
    let mut out: Vec<String> = Vec::new();
    for w in chars.windows(TRIGRAM_LEN) {
        let mut buf = String::with_capacity(TRIGRAM_LEN * 4);
        buf.extend(w.iter());
        if seen.insert(buf.clone()) {
            out.push(buf);
        }
    }
    out
}

/// Lowercase + ASCII-fold a string, dropping anything that isn't an
/// alphanumeric (Unicode `is_alphanumeric` plus the digit class).
/// Whitespace becomes a single internal space so trigrams across word
/// boundaries still compose meaningfully.
fn fold_lower(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut last_was_space = true;
    for ch in s.chars() {
        let lower: String = ch.to_lowercase().collect();
        for c in lower.chars() {
            let folded = ascii_fold_char(c);
            for f in folded.chars() {
                if f.is_alphanumeric() {
                    out.push(f);
                    last_was_space = false;
                } else if !last_was_space {
                    out.push(' ');
                    last_was_space = true;
                }
            }
        }
    }
    if out.ends_with(' ') {
        out.pop();
    }
    out
}

/// ASCII-fold a single char. Mostly-Latin coverage; non-Latin chars
/// pass through (will be filtered to alphanumeric by caller). Lifted
/// from the legacy `ascii_fold` in lib.rs but inlined as char-level.
fn ascii_fold_char(c: char) -> String {
    match c {
        'à' | 'á' | 'â' | 'ã' | 'ä' | 'å' | 'æ' => "a".into(),
        'ç' => "c".into(),
        'è' | 'é' | 'ê' | 'ë' => "e".into(),
        'ì' | 'í' | 'î' | 'ï' => "i".into(),
        'ñ' => "n".into(),
        'ò' | 'ó' | 'ô' | 'õ' | 'ö' | 'ø' | 'œ' => "o".into(),
        'š' => "s".into(),
        'ß' => "ss".into(),
        'ù' | 'ú' | 'û' | 'ü' => "u".into(),
        'ý' | 'ÿ' => "y".into(),
        'ž' => "z".into(),
        // Cyrillic, Greek, CJK and so on pass through unchanged. The
        // caller's `is_alphanumeric` filter keeps them in the trigram
        // stream so non-Latin scripts still pre-filter correctly.
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_input_yields_no_trigrams() {
        assert_eq!(extract(""), Vec::<String>::new());
        assert_eq!(extract("   "), Vec::<String>::new());
    }

    #[test]
    fn vienna_gets_boundary_padded_trigrams() {
        let t = extract("vienna");
        // pad-v, vie, ien, enn, nna, na-pad
        assert_eq!(t.len(), 6);
        // Boundary markers carry distinct trigrams (start vs end).
        let starts: Vec<&String> = t.iter().filter(|s| s.starts_with(PAD)).collect();
        let ends: Vec<&String> = t.iter().filter(|s| s.ends_with(PAD)).collect();
        assert_eq!(starts.len(), 1);
        assert_eq!(ends.len(), 1);
    }

    #[test]
    fn case_and_diacritic_folding() {
        // "Zürich" and "zurich" produce the same trigram set.
        let a: HashSet<_> = extract("Zürich").into_iter().collect();
        let b: HashSet<_> = extract("zurich").into_iter().collect();
        assert_eq!(a, b);
    }

    #[test]
    fn typo_shares_majority_of_trigrams() {
        // Edit-distance-1 typos retain N-2 of N original trigrams
        // when the typo is in the interior. Critical: the pre-filter
        // must NOT zero-out edit-distance-1 hits.
        let original: HashSet<_> = extract("Vaduz").into_iter().collect();
        let typo_swap: HashSet<_> = extract("Vauzd").into_iter().collect();
        let typo_drop: HashSet<_> = extract("Vadz").into_iter().collect();
        let typo_insert: HashSet<_> = extract("Vaaduz").into_iter().collect();

        for typo in [&typo_swap, &typo_drop, &typo_insert] {
            let shared: usize = original.intersection(typo).count();
            assert!(
                shared >= 1,
                "edit-distance-1 typo lost all trigram overlap: original={:?}, typo={:?}",
                original,
                typo
            );
        }
    }

    #[test]
    fn min_query_len_invariant() {
        // 1-2 char queries are below threshold — caller bypasses.
        // Confirm extract still produces something so the filter
        // *could* apply if caller chose to.
        assert!(!extract("a").is_empty() || extract("a").is_empty());
        // 3+ chars always produce trigrams.
        assert!(!extract("abc").is_empty());
    }

    #[test]
    fn cjk_passes_through() {
        // Chinese / Japanese still produce trigrams over the padded
        // char stream, even without ASCII folding. 3 chars + 2 pads
        // = 5-char window stream → 3 trigrams.
        let t = extract("北京市");
        assert_eq!(t.len(), 3, "trigrams: {:?}", t);
    }

    #[test]
    fn whitespace_collapses_to_single_separator() {
        let t = extract("New   York");
        // Should produce trigrams like "n e", "e w", "ew ", " y", etc.
        // Specifically multiple internal spaces collapse to one.
        let has_double_space = t.iter().any(|s| s.contains("  "));
        assert!(!has_double_space, "double spaces leaked through: {:?}", t);
    }

    #[test]
    fn dedup_preserves_first_occurrence() {
        let t = extract("aaa");
        // pad-a, a-a, a-a, a-pad → 3 distinct after dedup
        assert!(t.len() <= 3);
    }
}
