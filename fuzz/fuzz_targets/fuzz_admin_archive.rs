//! Fuzz `AdminTileArchive::from_aligned` — the rkyv validation
//! gateway for admin polygon tiles. Every tile is mmap'd from disk
//! at serve time; if a malformed tile can panic the validator we
//! risk crashing cairn-serve.

#![no_main]

use cairn_spatial::archived::AdminTileArchive;
use libfuzzer_sys::fuzz_target;
use rkyv::AlignedVec;

fuzz_target!(|data: &[u8]| {
    let mut aligned = AlignedVec::with_capacity(data.len());
    aligned.extend_from_slice(data);
    // The constructor must reject malformed input via `Result`, never
    // panic. We don't care about the success branch — only that the
    // path is panic-free for arbitrary bytes.
    let _ = AdminTileArchive::from_aligned(aligned);
});
