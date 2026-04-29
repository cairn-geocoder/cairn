//! Fuzz the bundle manifest parser. The manifest is operator-supplied
//! at bundle-load time; a panic on malformed input would brick a
//! production deploy. Goal: any byte sequence parses-or-errors, never
//! panics.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(s) = std::str::from_utf8(data) {
        // toml::from_str is the inner call read_manifest makes after
        // file IO. Fuzz the deserialization path directly so we cover
        // every shape of malformed TOML without touching the disk.
        let _: Result<cairn_tile::Manifest, _> = toml::from_str(s);
    }
});
