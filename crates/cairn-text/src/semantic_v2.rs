//! Phase 7a-B — transformer-based semantic embeddings via
//! fastembed-rs + ONNX Runtime.
//!
//! Gated behind the `semantic-onnx` feature so default builds keep
//! the airgap-friendly char-trigram embedder in `semantic.rs`. When
//! enabled, this module provides a multilingual MiniLM-class model
//! that embeds queries and place names into a 384-dimensional space
//! with real cross-language semantics ("Munich" ↔ "München" ↔
//! "Monaco di Baviera"). The vector shape is fixed by [`DIM_V2`] so
//! the cosine-rerank pipeline in `lib.rs` can swap between Phase 1
//! and Phase 2 without touching the rerank code.
//!
//! ## Wiring (deferred follow-up)
//!
//! This module ships the embedder; the cairn-build + cairn-serve
//! integration that selects the active backend based on the
//! manifest's `semantic_version` field is the next commit. For now
//! the v1 char-trigram path remains the default at index time and
//! at search time. Operators who flip on `--features semantic-onnx`
//! get the new `embed_v2` function for downstream wiring.
//!
//! ## Runtime requirements
//!
//! - `libonnxruntime` available at link time (or pulled via
//!   `fastembed`'s default features which bundle a download).
//! - First-run will pull the chosen model from HuggingFace unless
//!   `FASTEMBED_CACHE_PATH` points at a pre-staged copy. For airgap
//!   deploys, stage the model directory before booting cairn-serve.
//!
//! ## WASM
//!
//! ORT does not compile cleanly on `wasm32-unknown-unknown`. The
//! `cairn-wasm` crate stays on the Phase 1 char-trigram embedder.

#![cfg(feature = "semantic-onnx")]

use fastembed::{EmbeddingModel, InitOptions, TextEmbedding};
use std::sync::{Mutex, OnceLock};

/// Phase 7a-B vector dimensionality — fixed by the chosen model
/// (`MultilingualE5SmallQ` and similar small multilingual models
/// produce 384-dim outputs). Bumping models with a different output
/// width is a coordinated change with the cairn-build emitter.
pub const DIM_V2: usize = 384;

/// Lazily-initialized embedder. Construction downloads / loads the
/// model file once per process, then `embed` calls reuse the loaded
/// graph. Wrapped in a `Mutex` because `TextEmbedding::embed` takes
/// `&mut self` to manage the per-call ORT session state.
static EMBEDDER: OnceLock<Mutex<TextEmbedding>> = OnceLock::new();

fn embedder() -> Result<&'static Mutex<TextEmbedding>, fastembed::Error> {
    if let Some(e) = EMBEDDER.get() {
        return Ok(e);
    }
    let init = InitOptions::new(EmbeddingModel::MultilingualE5Small);
    let model = TextEmbedding::try_new(init)?;
    let _ = EMBEDDER.set(Mutex::new(model));
    Ok(EMBEDDER.get().expect("just-set EMBEDDER must be present"))
}

/// Embed a single text string into a 384-dim L2-normalized vector
/// using the current Phase 7a-B model. Returns the zero vector on
/// empty input — caller handles the no-boost case identically to
/// Phase 1's `semantic::embed`.
///
/// Errors propagate as `fastembed::Error`. Production callers wrap
/// in their own error type; this signature stays close to
/// `fastembed`'s for clarity at the experimental edge.
pub fn embed(text: &str) -> Result<Vec<f32>, fastembed::Error> {
    if text.trim().is_empty() {
        return Ok(vec![0.0; DIM_V2]);
    }
    let lock = embedder()?;
    let mut em = lock
        .lock()
        .expect("semantic_v2 embedder mutex poisoned by panicking peer");
    let mut out = em.embed(vec![text], None)?;
    Ok(out.pop().unwrap_or_else(|| vec![0.0; DIM_V2]))
}

/// Cosine similarity between two L2-normalized vectors. With unit
/// vectors this reduces to a dot product.
#[inline]
pub fn cosine(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() {
        return 0.0;
    }
    a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
}

/// Pack a `Vec<f32>` into little-endian bytes for tantivy storage.
/// Mirrors `semantic::pack` so the serialized layout stays
/// deterministic and inspectable.
pub fn pack(v: &[f32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(v.len() * 4);
    for x in v {
        out.extend_from_slice(&x.to_le_bytes());
    }
    out
}

/// Unpack a slice of `DIM_V2 * 4` bytes back into a `Vec<f32>`.
/// Returns `None` on length mismatch.
pub fn unpack(bytes: &[u8]) -> Option<Vec<f32>> {
    if bytes.len() != DIM_V2 * 4 {
        return None;
    }
    let mut out = Vec::with_capacity(DIM_V2);
    for chunk in bytes.chunks_exact(4) {
        out.push(f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    // Tests for this module run only with `--features semantic-onnx`.
    // Most CI matrices don't enable the feature (download cost +
    // ORT runtime requirement). Keep the smoke test minimal and
    // gated.
    use super::*;

    #[test]
    fn pack_unpack_roundtrip_zero_vector() {
        let v = vec![0.0_f32; DIM_V2];
        let bytes = pack(&v);
        let back = unpack(&bytes).unwrap();
        assert_eq!(v, back);
    }

    #[test]
    fn cosine_self_is_one_for_unit_vectors() {
        let mut v = vec![0.0_f32; DIM_V2];
        v[0] = 1.0;
        assert!((cosine(&v, &v) - 1.0).abs() < 1e-6);
    }

    #[test]
    fn unpack_rejects_wrong_length() {
        assert!(unpack(&[0u8; 10]).is_none());
    }
}
