//! Browser-side autocomplete for Cairn bundles.
//!
//! No incumbent geocoder ships a WASM autocomplete path today. This
//! crate gives a static site or single-page app a `~250 KB` WASM
//! blob that does prefix completion against a Cairn FST without a
//! network hop to the geocoder. Use cases:
//!
//! - **Country-bundle splash pages** that suggest cities as the user
//!   types before they hit the real `/v1/search` endpoint.
//! - **Embedded form widgets** that ship a small region's FST
//!   inline (postcodes, neighborhoods) for offline-capable forms.
//! - **PWA / offline-first apps** that want autocomplete to keep
//!   working when the network is gone.
//!
//! Tantivy is intentionally not pulled in — it isn't wasm-clean, and
//! the bundle size would balloon. FST prefix iteration is enough to
//! drive the autocomplete UX; a proper relevance score still comes
//! from the server when the user submits.
//!
//! # Loading a bundle
//!
//! At build time, copy `<bundle>/index/text/fst.bin` (or whichever
//! FST your downstream pipeline emits) into your static assets.
//! At runtime fetch it as bytes and hand them to
//! [`Autocompleter::new`].

use fst::{IntoStreamer, Set, Streamer};
use serde::Serialize;

/// Static container around an in-memory FST. The `Set` borrows the
/// backing bytes via the `Vec<u8>` it owns, so the autocompleter is
/// `'static` and safe to keep across async boundaries.
pub struct Autocompleter {
    set: Set<Vec<u8>>,
}

impl Autocompleter {
    /// Build an autocompleter from raw FST bytes (the on-disk
    /// representation produced by the `fst` crate's `SetBuilder`).
    /// Returns an error string when the bytes don't parse as a
    /// valid FST.
    pub fn new(bytes: Vec<u8>) -> Result<Self, String> {
        let set = Set::new(bytes).map_err(|e| format!("invalid FST: {e}"))?;
        Ok(Self { set })
    }

    /// Return up to `limit` keys from the FST that share the given
    /// (lowercased ASCII) prefix. Order is FST natural order
    /// (lexicographic). Empty prefix returns the first `limit` keys.
    pub fn complete(&self, prefix: &str, limit: usize) -> Vec<String> {
        let needle = prefix.to_lowercase();
        let mut stream = if needle.is_empty() {
            self.set.range().into_stream()
        } else {
            // FST has no native `prefix` constructor that returns a
            // `Stream`; range from `prefix` to the next-higher
            // sibling captures everything starting with the prefix.
            let upper = next_after(&needle);
            match upper {
                Some(u) => self
                    .set
                    .range()
                    .ge(needle.as_bytes())
                    .lt(u.as_bytes())
                    .into_stream(),
                None => self.set.range().ge(needle.as_bytes()).into_stream(),
            }
        };
        let mut out: Vec<String> = Vec::with_capacity(limit.min(64));
        while let Some(k) = stream.next() {
            if out.len() >= limit {
                break;
            }
            if let Ok(s) = std::str::from_utf8(k) {
                out.push(s.to_string());
            }
        }
        out
    }

    /// Total number of keys in the loaded FST. Useful for sanity-
    /// checking on the JS side ("did I load the right bundle?").
    pub fn len(&self) -> usize {
        self.set.len()
    }

    pub fn is_empty(&self) -> bool {
        self.set.is_empty()
    }
}

/// Compute the next byte string strictly greater than `prefix` so
/// that `[prefix, next)` covers exactly the keys that start with
/// `prefix`. Returns `None` when the prefix is at the maximum
/// possible byte string (rare — only when prefix is all 0xFF).
fn next_after(prefix: &str) -> Option<String> {
    let mut bytes = prefix.as_bytes().to_vec();
    while let Some(last) = bytes.last_mut() {
        if *last == 0xFF {
            bytes.pop();
            continue;
        }
        *last += 1;
        return Some(String::from_utf8_lossy(&bytes).into_owned());
    }
    None
}

/// JSON envelope returned to JavaScript. Single field for now; keeps
/// the wire format extensible without breaking JS consumers.
#[derive(Serialize)]
#[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
struct CompletionResponse {
    suggestions: Vec<String>,
}

// === wasm-bindgen surface ===
//
// All public WASM exports live behind the `wasm32` cfg guard so the
// crate stays buildable on native targets (for tests, native rust
// consumers, and CI sanity).

#[cfg(target_arch = "wasm32")]
mod wasm {
    use super::*;
    use wasm_bindgen::prelude::*;

    #[wasm_bindgen]
    pub struct CairnWasm {
        inner: Autocompleter,
    }

    #[wasm_bindgen]
    impl CairnWasm {
        /// Build from raw FST bytes (the `Uint8Array` you get back
        /// from `fetch().then(r => r.arrayBuffer())`). Throws on
        /// invalid bytes.
        #[wasm_bindgen(constructor)]
        pub fn new(bytes: Vec<u8>) -> Result<CairnWasm, JsValue> {
            Autocompleter::new(bytes)
                .map(|inner| CairnWasm { inner })
                .map_err(|e| JsValue::from_str(&e))
        }

        /// Return up to `limit` JSON-encoded suggestions for the
        /// given prefix. JSON shape: `{"suggestions": ["..."]}`.
        pub fn complete(&self, prefix: &str, limit: usize) -> String {
            let resp = CompletionResponse {
                suggestions: self.inner.complete(prefix, limit),
            };
            serde_json::to_string(&resp).unwrap_or_else(|_| "{\"suggestions\":[]}".to_string())
        }

        pub fn len(&self) -> usize {
            self.inner.len()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fst::SetBuilder;

    fn build_fst(words: &[&str]) -> Vec<u8> {
        let mut b = SetBuilder::memory();
        let mut sorted: Vec<&&str> = words.iter().collect();
        sorted.sort();
        for w in sorted {
            b.insert(w.to_lowercase().as_bytes()).unwrap();
        }
        b.into_inner().unwrap()
    }

    #[test]
    fn complete_returns_keys_with_prefix() {
        let bytes = build_fst(&["vaduz", "vaduz castle", "schaan", "triesen", "balzers"]);
        let a = Autocompleter::new(bytes).unwrap();
        let hits = a.complete("vad", 10);
        assert_eq!(hits, vec!["vaduz", "vaduz castle"]);
    }

    #[test]
    fn complete_is_case_insensitive_on_query() {
        let bytes = build_fst(&["vaduz"]);
        let a = Autocompleter::new(bytes).unwrap();
        let hits = a.complete("VAD", 10);
        assert_eq!(hits, vec!["vaduz"]);
    }

    #[test]
    fn complete_respects_limit() {
        let bytes = build_fst(&["vaduz", "vaduz castle", "vaduz museum", "vaduz cathedral"]);
        let a = Autocompleter::new(bytes).unwrap();
        let hits = a.complete("vad", 2);
        assert_eq!(hits.len(), 2);
    }

    #[test]
    fn empty_prefix_returns_first_n_keys() {
        let bytes = build_fst(&["a", "b", "c", "d"]);
        let a = Autocompleter::new(bytes).unwrap();
        let hits = a.complete("", 3);
        assert_eq!(hits, vec!["a", "b", "c"]);
    }

    #[test]
    fn complete_returns_empty_on_no_match() {
        let bytes = build_fst(&["alpha", "beta"]);
        let a = Autocompleter::new(bytes).unwrap();
        let hits = a.complete("zzz", 10);
        assert!(hits.is_empty());
    }

    #[test]
    fn invalid_fst_bytes_error() {
        let res = Autocompleter::new(vec![0u8; 16]);
        assert!(res.is_err());
    }

    #[test]
    fn next_after_increments_last_byte() {
        assert_eq!(next_after("vad").as_deref(), Some("vae"));
    }

    #[test]
    fn len_reports_key_count() {
        let bytes = build_fst(&["a", "b", "c"]);
        let a = Autocompleter::new(bytes).unwrap();
        assert_eq!(a.len(), 3);
        assert!(!a.is_empty());
    }
}
