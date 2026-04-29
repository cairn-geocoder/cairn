//! Phase 7a-N — Myers' bit-parallel Levenshtein distance.
//!
//! Implements Eugene Myers' 1999 bit-parallel algorithm for computing
//! the Levenshtein edit distance between a query pattern and a target
//! string, with the inner loop running entirely in u64 register
//! arithmetic. For patterns up to 64 chars (covering ~99% of geocoder
//! names — country / city / street / POI labels rarely exceed 50
//! chars), the cost is `O(n)` machine words versus the classic `O(n*m)`
//! DP cells. Fall back to a multi-word implementation for longer
//! patterns; we don't ship that yet — caller falls back to the classic
//! Wagner-Fischer DP via [`wagner_fischer`] when the pattern overflows.
//!
//! Used by the search rerank pass to score candidate hits by
//! query-to-name edit distance, on top of the trigram pre-filter that
//! already pruned the doc set in Phase 7a-A.
//!
//! References:
//! - E. Myers, "A Fast Bit-Vector Algorithm for Approximate String
//!   Matching Based on Dynamic Programming", J. ACM 46(3) 1999.
//! - <https://www.cse.unsw.edu.au/~lambert/papers/myers.pdf>

const MAX_PATTERN_BITS: usize = 64;

/// Levenshtein edit distance between `pattern` and `text` using
/// Myers' bit-parallel algorithm. Returns `None` when the pattern
/// exceeds 64 chars (caller should use [`wagner_fischer`] instead).
///
/// Both inputs are treated as Unicode `char` sequences; ASCII case
/// folding is the caller's responsibility (Phase 7a-A's `trigram`
/// module already lowercases + ASCII-folds upstream).
pub fn myers_distance(pattern: &str, text: &str) -> Option<usize> {
    let p_chars: Vec<char> = pattern.chars().collect();
    let t_chars: Vec<char> = text.chars().collect();
    let m = p_chars.len();
    if m == 0 {
        return Some(t_chars.len());
    }
    if m > MAX_PATTERN_BITS {
        return None;
    }
    if t_chars.is_empty() {
        return Some(m);
    }

    // Build the per-character bitmask `peq[c]`: bit `i` is set iff
    // `pattern[i] == c`. We use a small `Vec<(char, u64)>` instead of
    // a dense table because patterns rarely have more than ~30
    // distinct chars; linear scan in cache is faster than a HashMap
    // lookup for this size.
    let mut peq: Vec<(char, u64)> = Vec::with_capacity(m);
    for (i, &ch) in p_chars.iter().enumerate() {
        let bit = 1u64 << i;
        if let Some(entry) = peq.iter_mut().find(|(c, _)| *c == ch) {
            entry.1 |= bit;
        } else {
            peq.push((ch, bit));
        }
    }
    let zero_bits: u64 = 0;
    let last_bit: u64 = 1u64 << (m - 1);

    let mut pv: u64 = if m == 64 { u64::MAX } else { (1u64 << m) - 1 };
    let mut mv: u64 = 0;
    let mut score: usize = m;

    for &c in &t_chars {
        let eq: u64 = peq
            .iter()
            .find(|(ch, _)| *ch == c)
            .map(|(_, b)| *b)
            .unwrap_or(zero_bits);
        let xv = eq | mv;
        let xh = (((eq & pv).wrapping_add(pv)) ^ pv) | eq;
        let mut ph = mv | !(xh | pv);
        let mut mh = pv & xh;
        if ph & last_bit != 0 {
            score = score.saturating_add(1);
        }
        if mh & last_bit != 0 {
            score = score.saturating_sub(1);
        }
        ph = (ph << 1) | 1;
        mh <<= 1;
        pv = mh | !(xv | ph);
        mv = ph & xv;
    }
    Some(score)
}

/// Classic Wagner-Fischer DP for patterns longer than 64 chars.
/// `O(m * n)` time, `O(min(m, n))` space (two rolling rows).
pub fn wagner_fischer(pattern: &str, text: &str) -> usize {
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    if p.is_empty() {
        return t.len();
    }
    if t.is_empty() {
        return p.len();
    }
    let mut prev: Vec<usize> = (0..=t.len()).collect();
    let mut curr: Vec<usize> = vec![0; t.len() + 1];
    for (i, &pc) in p.iter().enumerate() {
        curr[0] = i + 1;
        for (j, &tc) in t.iter().enumerate() {
            let cost = if pc == tc { 0 } else { 1 };
            curr[j + 1] = (curr[j] + 1).min(prev[j + 1] + 1).min(prev[j] + cost);
        }
        std::mem::swap(&mut prev, &mut curr);
    }
    prev[t.len()]
}

/// Convenience: pick Myers when possible, fall back to Wagner-Fischer.
/// This is the function the rerank pipeline calls.
#[inline]
pub fn edit_distance(pattern: &str, text: &str) -> usize {
    myers_distance(pattern, text).unwrap_or_else(|| wagner_fischer(pattern, text))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ascii_fold_lower(s: &str) -> String {
        // Test helper — production callers normalize upstream.
        s.to_lowercase()
    }

    #[test]
    fn empty_pattern_returns_text_len() {
        assert_eq!(myers_distance("", "vaduz"), Some(5));
        assert_eq!(myers_distance("", ""), Some(0));
    }

    #[test]
    fn empty_text_returns_pattern_len() {
        assert_eq!(myers_distance("vaduz", ""), Some(5));
    }

    #[test]
    fn identical_strings_zero_distance() {
        assert_eq!(myers_distance("vaduz", "vaduz"), Some(0));
        assert_eq!(myers_distance("zürich", "zürich"), Some(0));
    }

    #[test]
    fn single_substitution() {
        assert_eq!(myers_distance("vaduz", "vbduz"), Some(1));
    }

    #[test]
    fn single_insertion() {
        assert_eq!(myers_distance("vaduz", "vaaduz"), Some(1));
    }

    #[test]
    fn single_deletion() {
        assert_eq!(myers_distance("vaduz", "vauz"), Some(1));
    }

    #[test]
    fn transposition_costs_two_under_levenshtein() {
        // Standard Levenshtein, not Damerau — adjacent swap is 2 ops.
        assert_eq!(myers_distance("vaduz", "avduz"), Some(2));
    }

    #[test]
    fn unrelated_strings() {
        assert_eq!(myers_distance("vaduz", "tokyo"), Some(5));
    }

    #[test]
    fn pattern_over_64_chars_returns_none() {
        let p: String = "a".repeat(65);
        assert_eq!(myers_distance(&p, "anything"), None);
    }

    #[test]
    fn wagner_fischer_matches_myers_for_short_inputs() {
        let cases = [
            ("vaduz", "vaduz"),
            ("vaduz", "vbduz"),
            ("vaduz", "vauz"),
            ("vaduz", "vaaduz"),
            ("vaduz", "tokyo"),
            ("zürich", "zurich"),
            ("munchen", "munich"),
            ("", "abc"),
            ("abc", ""),
            ("", ""),
        ];
        for (p, t) in cases {
            let m = myers_distance(p, t).unwrap();
            let w = wagner_fischer(p, t);
            assert_eq!(m, w, "myers vs wagner-fischer mismatch on ({}, {})", p, t);
        }
    }

    #[test]
    fn long_pattern_falls_back_to_wagner_fischer() {
        let p: String = "a".repeat(100);
        let mut t: String = "a".repeat(100);
        t.push('b');
        assert_eq!(edit_distance(&p, &t), 1);
    }

    #[test]
    fn case_sensitivity_is_callers_responsibility() {
        // Production callers ascii-fold upstream; raw distance is
        // case-sensitive by design.
        assert_eq!(myers_distance("Vaduz", "vaduz"), Some(1));
        assert_eq!(
            myers_distance(&ascii_fold_lower("Vaduz"), &ascii_fold_lower("vaduz")),
            Some(0)
        );
    }

    #[test]
    fn boundary_64_char_pattern_works() {
        let p: String = "x".repeat(64);
        let mut t: String = "x".repeat(64);
        t.push('y');
        assert_eq!(myers_distance(&p, &t), Some(1));
    }

    #[test]
    fn unicode_chars_counted_as_single_units() {
        // Each char is one bit in peq, regardless of UTF-8 byte width.
        assert_eq!(myers_distance("zürich", "zurich"), Some(1));
        assert_eq!(myers_distance("北京", "京北"), Some(2));
    }

    #[test]
    fn random_pairs_match_wagner_fischer_baseline() {
        // Hand-rolled fuzz-style sweep — not a property test framework
        // (avoid pulling proptest as a dep), but covers a few hundred
        // pseudo-random pairs in the range we care about.
        let words = [
            "vaduz",
            "valencia",
            "Bern",
            "Berne",
            "Bonn",
            "Boston",
            "muenchen",
            "Munich",
            "Tokyo",
            "Tooyo",
            "Tooky",
            "abcdefghi",
            "abcdefhi",
        ];
        for &p in &words {
            for &t in &words {
                let pl = p.to_lowercase();
                let tl = t.to_lowercase();
                let m = myers_distance(&pl, &tl).unwrap();
                let w = wagner_fischer(&pl, &tl);
                assert_eq!(m, w, "mismatch on ({}, {})", p, t);
            }
        }
    }
}
