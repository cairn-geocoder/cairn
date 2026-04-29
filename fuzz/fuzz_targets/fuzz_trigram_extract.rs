//! Fuzz the Phase 7a-A trigram extractor. Runs at index time and
//! at every fuzzy query — must handle pathological input
//! (zero-width chars, control codes, very long UTF-8 sequences,
//! surrogate-like bytes that arrived as text via OSM tags) without
//! panicking.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(s) = std::str::from_utf8(data) {
        let _ = cairn_text::trigram::extract_indexed(s);
        let _ = cairn_text::trigram::extract_query(s);
    }
});
