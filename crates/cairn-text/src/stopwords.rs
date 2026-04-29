//! Phase 7a-E — per-language stop-word filter for multi-token
//! geocoder queries.
//!
//! Function words ("de", "la", "von", "der", "of", "the") inflate
//! BM25 scoring on long-form queries: `Place de la Bastille` indexes
//! all four tokens equally, so a POI containing only "de" + "la"
//! (very common in French / Spanish / Italian street names) can
//! out-score the real intended hit. This module drops common stop-
//! words from queries before they reach tantivy.
//!
//! Conservative rules:
//!
//! 1. Single-token queries pass through untouched. "La" alone could
//!    legitimately mean "La" the place; we don't second-guess.
//!
//! 2. Stop-words are dropped only from the *interior* of multi-token
//!    queries. The first and last tokens are preserved so "La Paz"
//!    still works as a 2-token query and "Le Mans 24h" keeps "Le"
//!    when it's the head.
//!
//! 3. Case-sensitive comparison: only lowercase function words match.
//!    A user-typed `La Bastille` keeps both tokens (casing signals
//!    intent); `place de la bastille` drops `de`+`la`.
//!
//! 4. If after filtering the query becomes empty (rare — all-stopwords
//!    query like "the of"), we restore the original.

/// Compact static stop-word list spanning the major Latin-script
/// languages we expect from OSM `name:*` tags. Each language adds
/// ~30-50 entries; total <500 unique tokens.
const STOP_WORDS: &[&str] = &[
    // English
    "a", "an", "and", "as", "at", "by", "for", "from", "in", "into", "of", "on", "or", "the", "to",
    "with", // German
    "am", "an", "auf", "aus", "bei", "das", "dem", "den", "der", "des", "die", "ein", "eine", "im",
    "in", "mit", "und", "von", "vom", "zum", "zur", // French
    "à", "au", "aux", "de", "des", "du", "en", "et", "la", "le", "les", "sur", // Italian
    "al", "alla", "alle", "alto", "ai", "del", "della", "delle", "di", "il", "la", "le", "lo", "su",
    "sul", "sulla", // Spanish / Portuguese
    "a", "al", "da", "das", "de", "del", "do", "dos", "el", "en", "la", "las", "los", "para", "por",
    // Dutch
    "aan", "bij", "de", "der", "het", "in", "naar", "op", "te", "tot", "van", "voor",
];

/// Filter stop-words out of a query string per the rules in the
/// module docs. Returns the filtered string. Empty input or
/// single-token queries pass through unchanged.
pub fn filter(query: &str) -> String {
    let original_trimmed = query.trim();
    if original_trimmed.is_empty() {
        return query.to_string();
    }
    let tokens: Vec<&str> = original_trimmed.split_whitespace().collect();
    if tokens.len() < 3 {
        // Single- and two-token queries pass through. Two tokens
        // are common in `<name> <admin>` patterns where neither is a
        // stop-word in practice, and dropping one of two is too risky.
        return query.to_string();
    }

    let head = tokens[0];
    let tail = tokens[tokens.len() - 1];
    let interior = &tokens[1..tokens.len() - 1];

    let kept: Vec<&str> = std::iter::once(head)
        .chain(interior.iter().copied().filter(|tok| !is_stop_word(tok)))
        .chain(std::iter::once(tail))
        .collect();

    if kept.len() == tokens.len() {
        return query.to_string();
    }
    if kept.is_empty() {
        return query.to_string();
    }
    kept.join(" ")
}

/// True iff `tok` matches a stop-word case-sensitively. Capitalized
/// tokens like `La` are preserved on the assumption they're proper
/// nouns / place-name fragments.
#[inline]
fn is_stop_word(tok: &str) -> bool {
    if !tok
        .chars()
        .all(|c| c.is_lowercase() || c.is_ascii_punctuation())
    {
        return false;
    }
    STOP_WORDS.contains(&tok)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_passes_through() {
        assert_eq!(filter(""), "");
        assert_eq!(filter("   "), "   ");
    }

    #[test]
    fn single_token_unchanged() {
        assert_eq!(filter("Vaduz"), "Vaduz");
        assert_eq!(filter("la"), "la");
    }

    #[test]
    fn two_tokens_unchanged() {
        // "La Paz" must survive: 2 tokens never filter.
        assert_eq!(filter("La Paz"), "La Paz");
        assert_eq!(filter("le mans"), "le mans");
    }

    #[test]
    fn french_interior_stopwords_drop() {
        assert_eq!(filter("place de la bastille"), "place bastille");
        assert_eq!(filter("rue du faubourg"), "rue faubourg");
    }

    #[test]
    fn german_interior_stopwords_drop() {
        assert_eq!(filter("schloss am rhein"), "schloss rhein");
        assert_eq!(filter("haus der kunst"), "haus kunst");
    }

    #[test]
    fn capitalized_interior_tokens_preserved_lowercase_dropped() {
        // Mixed: lowercase "de" is interior + stopword → dropped;
        // capitalized "La" interior is preserved as proper-noun frag.
        assert_eq!(filter("La Place de La Bastille"), "La Place La Bastille");
    }

    #[test]
    fn head_and_tail_preserved_even_when_lowercase_stopwords() {
        // Interior here contains no stopwords ("bilt"), so head "de"
        // survives untouched.
        assert_eq!(filter("de bilt hoofdstraat"), "de bilt hoofdstraat");
        // Tail "de" — interior empty after the only middle token
        // ("bilt") survives.
        assert_eq!(filter("hoofdstraat bilt de"), "hoofdstraat bilt de");
    }

    #[test]
    fn interior_stopwords_drop_head_tail_kept() {
        // Even when the only interior token IS a stopword, the head
        // and tail anchors are preserved by design.
        assert_eq!(filter("de la et"), "de et");
    }

    #[test]
    fn no_drop_when_no_match() {
        assert_eq!(
            filter("Vaduz Liechtenstein Hauptort"),
            "Vaduz Liechtenstein Hauptort"
        );
    }

    #[test]
    fn english_stopwords() {
        assert_eq!(filter("kingdom of the netherlands"), "kingdom netherlands");
    }

    #[test]
    fn punctuation_doesnt_block_stopword_detection() {
        // Common edge case: tokenizer might leave punctuation. We
        // require pure-alpha to drop, so "de," is preserved (safer).
        assert_eq!(
            filter("kingdom of, the netherlands"),
            "kingdom of, netherlands"
        );
    }
}
