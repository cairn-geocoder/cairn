//! Fuzz Phase 7a-N's Myers bit-parallel edit distance. The unsafe-
//! free implementation should never panic, but bit shifts and
//! `wrapping_add` arithmetic are fertile ground for off-by-ones at
//! the 64-char boundary. Property: Myers and Wagner-Fischer must
//! agree for inputs Myers can handle.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if data.len() < 2 {
        return;
    }
    // Split the input arbitrarily into pattern + text.
    let split = (data[0] as usize) % data.len();
    let (a, b) = data.split_at(split);
    let pattern = match std::str::from_utf8(a) {
        Ok(s) => s,
        Err(_) => return,
    };
    let text = match std::str::from_utf8(b) {
        Ok(s) => s,
        Err(_) => return,
    };

    let myers = cairn_text::edit::myers_distance(pattern, text);
    let wf = cairn_text::edit::wagner_fischer(pattern, text);
    if let Some(m) = myers {
        // Property: Myers must agree with the reference Wagner-Fischer
        // implementation for inputs short enough to use the bit-parallel
        // path.
        assert_eq!(
            m, wf,
            "Myers/WF disagreement: pattern={:?} text={:?}",
            pattern, text
        );
    }
    // Also exercise the convenience entrypoint that picks the right
    // backend automatically.
    let _ = cairn_text::edit::edit_distance(pattern, text);
});
