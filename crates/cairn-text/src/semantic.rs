//! Lexical-vector semantic rerank.
//!
//! Phase 1 — ships today, no external deps, works in airgap. A
//! character-trigram bag-of-words hash gives every name a fixed-
//! length L2-normalized vector. At query time the same hashing
//! function turns the query into a vector; cosine similarity blends
//! into the final score so morphological variants ("Vienna" ↔
//! "Viennese") and small misspellings ("Trieserberg" ↔ "Trisenberg")
//! get a real signal instead of relying on fuzzy edit distance
//! alone.
//!
//! Phase 2 — feature-gated upgrade path to transformer embeddings
//! (`fastembed-rs`, ONNX runtime, gte-small or similar). The vector
//! shape is fixed by [`DIM`] so swapping the embedder is a one-
//! function change in [`embed`]. The pipeline (cosine rerank,
//! multiplicative blend with BM25) stays the same.
//!
//! Why character trigrams: cheap, effective on geographic names
//! (which are heavily morphological), language-agnostic, no model
//! file to ship. The hashing trick collapses any number of unique
//! trigrams into [`DIM`] dimensions so memory cost is fixed and
//! tiny.
//!
//! On-disk: each place's vector serializes as exactly
//! `DIM * 4` bytes (little-endian f32) into the tantivy `name_vec`
//! BYTES field.

/// Vector dimensionality. 32 dims keep the per-doc storage at
/// 128 bytes — at 1 M places that's 128 MB tantivy growth, which
/// is acceptable for a Phase 1 ship. Bumping to 64 doubles storage
/// for a marginal recall lift; defer until the rerank earns it.
pub const DIM: usize = 32;

/// Multiplier applied to a hit's score when its semantic similarity
/// to the query exceeds the [`SEMANTIC_THRESHOLD`] floor. Modest by
/// design — semantic should break ties or rescue near-misses, not
/// override exact-name + population + language signals.
pub const SEMANTIC_BOOST_PEAK: f32 = 1.6;

/// Cosine similarity floor below which the semantic boost is skipped
/// entirely. Avoids rewarding hits that share a single common
/// trigram (which would happen on basically every Latin-script
/// word) — only "really similar" names get the bump.
pub const SEMANTIC_THRESHOLD: f32 = 0.35;

/// Hash a single character trigram into a `[0, DIM)` bucket via a
/// small FNV-1a variant. Deterministic across architectures so the
/// same name hashes to the same bucket on every machine.
fn hash_trigram(tri: &[char]) -> usize {
    let mut h: u32 = 0x811C_9DC5;
    for c in tri {
        let bytes = (*c as u32).to_le_bytes();
        for b in bytes {
            h ^= b as u32;
            h = h.wrapping_mul(0x0100_0193);
        }
    }
    (h as usize) % DIM
}

/// Compute a fixed-size lexical-vector embedding for `text`. Steps:
/// 1. Lowercase + strip non-alphanumeric to a Vec<char>.
/// 2. Pad with `#` boundary markers so word starts / ends carry
///    their own trigrams (`#vi`, `na#` for `vienna`).
/// 3. Slide a 3-character window; bump the hashed bucket by 1.
/// 4. L2-normalize so cosine reduces to a dot product.
///
/// Empty / single-character input yields the zero vector — caller
/// should treat it as "no semantic signal".
pub fn embed(text: &str) -> [f32; DIM] {
    let mut out = [0.0f32; DIM];
    if text.trim().is_empty() {
        return out;
    }
    let mut chars: Vec<char> = vec!['#'];
    chars.extend(
        text.to_lowercase()
            .chars()
            .filter(|c| c.is_alphanumeric() || c.is_whitespace())
            .map(|c| if c.is_whitespace() { '#' } else { c }),
    );
    chars.push('#');
    if chars.len() < 3 {
        return out;
    }
    for w in chars.windows(3) {
        let bucket = hash_trigram(w);
        out[bucket] += 1.0;
    }
    // L2 normalize so cosine = dot product downstream.
    let norm: f32 = out.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        for v in &mut out {
            *v /= norm;
        }
    }
    out
}

/// Cosine similarity for two same-length L2-normalized vectors —
/// reduces to a plain dot product. Returns `0.0` if either vector
/// is the zero vector (so empty inputs don't trigger boosts).
pub fn cosine(a: &[f32; DIM], b: &[f32; DIM]) -> f32 {
    let mut dot = 0.0f32;
    for i in 0..DIM {
        dot += a[i] * b[i];
    }
    dot
}

/// Pack a vector into `DIM * 4` little-endian f32 bytes for storage
/// in tantivy's BYTES field. Round-trips with [`unpack`].
pub fn pack(v: &[f32; DIM]) -> Vec<u8> {
    let mut out = Vec::with_capacity(DIM * 4);
    for f in v.iter() {
        out.extend_from_slice(&f.to_le_bytes());
    }
    out
}

/// Read a packed vector back from `DIM * 4` bytes. Returns the zero
/// vector when the input length doesn't match — defensive default
/// so older bundles without `name_vec` populated don't crash the
/// rerank.
pub fn unpack(bytes: &[u8]) -> [f32; DIM] {
    let mut out = [0.0f32; DIM];
    if bytes.len() != DIM * 4 {
        return out;
    }
    for i in 0..DIM {
        let mut buf = [0u8; 4];
        buf.copy_from_slice(&bytes[i * 4..i * 4 + 4]);
        out[i] = f32::from_le_bytes(buf);
    }
    out
}

/// Compute the multiplicative score boost for a hit whose name
/// embedding has cosine `sim` to the query. Below threshold = 1.0
/// (no boost). Above threshold, scales linearly to
/// [`SEMANTIC_BOOST_PEAK`] at sim=1.0.
pub fn boost_for(sim: f32) -> f32 {
    if sim < SEMANTIC_THRESHOLD {
        return 1.0;
    }
    let span = 1.0 - SEMANTIC_THRESHOLD;
    let t = ((sim - SEMANTIC_THRESHOLD) / span).clamp(0.0, 1.0);
    1.0 + (SEMANTIC_BOOST_PEAK - 1.0) * t
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embed_empty_is_zero() {
        let v = embed("");
        assert_eq!(v.iter().sum::<f32>(), 0.0);
    }

    #[test]
    fn embed_l2_unit_norm() {
        let v = embed("Vaduz");
        let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        // Tolerance for f32 rounding.
        assert!((norm - 1.0).abs() < 1e-5, "norm={norm}");
    }

    #[test]
    fn cosine_self_similarity_is_one() {
        let v = embed("Vienna");
        assert!((cosine(&v, &v) - 1.0).abs() < 1e-5);
    }

    #[test]
    fn cosine_similar_names_score_higher_than_dissimilar() {
        let q = embed("Vienna");
        let same = embed("Viennese");
        let other = embed("Tokyo");
        let s_same = cosine(&q, &same);
        let s_other = cosine(&q, &other);
        assert!(
            s_same > s_other,
            "expected Vienna~Viennese > Vienna~Tokyo; got {s_same} vs {s_other}"
        );
    }

    #[test]
    fn cosine_substring_similarity_high() {
        // "Vaduz Castle" should be highly similar to "Vaduz" — they
        // share most trigrams.
        let a = embed("Vaduz");
        let b = embed("Vaduz Castle");
        assert!(cosine(&a, &b) > 0.5);
    }

    #[test]
    fn pack_unpack_roundtrip() {
        let v = embed("Liechtenstein");
        let bytes = pack(&v);
        assert_eq!(bytes.len(), DIM * 4);
        let back = unpack(&bytes);
        for i in 0..DIM {
            assert!((v[i] - back[i]).abs() < 1e-7);
        }
    }

    #[test]
    fn unpack_returns_zero_on_wrong_len() {
        let v = unpack(&[0u8; 7]);
        assert_eq!(v.iter().sum::<f32>(), 0.0);
    }

    #[test]
    fn boost_below_threshold_is_one() {
        assert_eq!(boost_for(0.0), 1.0);
        assert_eq!(boost_for(0.10), 1.0);
        assert_eq!(boost_for(SEMANTIC_THRESHOLD - 0.001), 1.0);
    }

    #[test]
    fn boost_at_perfect_match_is_peak() {
        assert!((boost_for(1.0) - SEMANTIC_BOOST_PEAK).abs() < 1e-6);
    }

    #[test]
    fn boost_scales_monotonically() {
        let a = boost_for(0.5);
        let b = boost_for(0.7);
        let c = boost_for(0.9);
        assert!(a < b && b < c, "monotonic boost: {a} {b} {c}");
    }
}
