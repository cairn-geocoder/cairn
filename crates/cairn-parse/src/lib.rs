//! Address parsing + normalization.
//!
//! Two implementations behind one façade:
//!
//! - **default**: a small heuristic parser that handles common
//!   `"<number> <road>, <postcode> <city>, <country>"` shapes for Latin
//!   scripts. Good enough for English / German / French / Italian
//!   addresses. Loses fidelity on RTL scripts, CJK, and abbreviation
//!   expansion.
//! - **`libpostal` feature**: thin FFI to the C [libpostal] library,
//!   which uses a CRF model trained on ~1B addresses across 100+
//!   languages. Requires libpostal headers at build time and the ~2 GB
//!   compiled model at runtime (`LIBPOSTAL_DATA_DIR` env var).
//!
//! [libpostal]: https://github.com/openvenues/libpostal

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("empty input")]
    Empty,
    #[error("libpostal not initialized — call init() first")]
    NotInitialized,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParsedAddress {
    pub house_number: Option<String>,
    pub road: Option<String>,
    pub unit: Option<String>,
    pub postcode: Option<String>,
    pub city: Option<String>,
    pub state: Option<String>,
    pub country: Option<String>,
}

/// Parse a free-text address into structured fields.
///
/// Default build runs the heuristic parser. With `libpostal` feature
/// enabled, dispatches to libpostal's CRF tagger.
pub fn parse(input: &str) -> Result<ParsedAddress, ParseError> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(ParseError::Empty);
    }
    #[cfg(feature = "libpostal")]
    {
        libpostal::parse(trimmed)
    }
    #[cfg(not(feature = "libpostal"))]
    {
        Ok(heuristic_parse(trimmed))
    }
}

/// Expand abbreviations and normalize whitespace + case.
///
/// Default build returns a single lowercase variant with tiny English
/// abbreviation expansion (`st.` → `street`). libpostal returns every
/// language-aware permutation.
pub fn expand(input: &str) -> Vec<String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }
    #[cfg(feature = "libpostal")]
    {
        libpostal::expand(trimmed)
    }
    #[cfg(not(feature = "libpostal"))]
    {
        vec![heuristic_expand(trimmed)]
    }
}

#[cfg_attr(feature = "libpostal", allow(dead_code))]
fn heuristic_parse(input: &str) -> ParsedAddress {
    let mut out = ParsedAddress::default();
    let parts: Vec<&str> = input
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    if parts.is_empty() {
        return out;
    }

    // First chunk: house number + road (or just road).
    if let Some(first) = parts.first() {
        let (num, road) = split_leading_number(first);
        out.house_number = num;
        if !road.is_empty() {
            out.road = Some(road);
        }
    }

    // Last chunk: country if it looks like one (short alphabetic word).
    if parts.len() >= 2 {
        let last = parts[parts.len() - 1];
        if looks_like_country(last) {
            out.country = Some(last.to_string());
        }
    }

    // Middle chunks (or last if no country): try to peel postcode + city.
    let middle_end = if out.country.is_some() {
        parts.len() - 1
    } else {
        parts.len()
    };
    for part in &parts[1..middle_end] {
        let (postcode, remainder) = split_postcode_prefix(part);
        if out.postcode.is_none() {
            if let Some(p) = postcode {
                out.postcode = Some(p);
            }
        }
        let trimmed = remainder.trim();
        if !trimmed.is_empty() {
            if out.city.is_none() {
                out.city = Some(trimmed.to_string());
            } else if out.state.is_none() {
                out.state = Some(trimmed.to_string());
            }
        }
    }

    out
}

#[cfg_attr(feature = "libpostal", allow(dead_code))]
fn split_leading_number(part: &str) -> (Option<String>, String) {
    let mut chars = part.chars().peekable();
    let mut num = String::new();
    while let Some(&c) = chars.peek() {
        if c.is_ascii_digit() {
            num.push(c);
            chars.next();
        } else {
            break;
        }
    }
    if num.is_empty() {
        return (None, part.to_string());
    }
    // Optional trailing unit letter (e.g. "12B").
    if let Some(&c) = chars.peek() {
        if c.is_ascii_alphabetic() && chars.clone().nth(1).is_none_or(|n| n == ' ') {
            num.push(c);
            chars.next();
        }
    }
    let road = chars.collect::<String>().trim().to_string();
    (Some(num), road)
}

#[cfg_attr(feature = "libpostal", allow(dead_code))]
fn split_postcode_prefix(part: &str) -> (Option<String>, &str) {
    // Match a leading run of digits (3–8 chars) optionally followed by a
    // single letter, then a space.
    let bytes = part.as_bytes();
    let mut i = 0;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        i += 1;
    }
    if !(3..=8).contains(&i) {
        return (None, part);
    }
    if i < bytes.len() && bytes[i].is_ascii_alphabetic() {
        i += 1;
    }
    if i >= bytes.len() || bytes[i] != b' ' {
        return (None, part);
    }
    let postcode = part[..i].to_string();
    let remainder = part[i..].trim_start();
    (Some(postcode), remainder)
}

#[cfg_attr(feature = "libpostal", allow(dead_code))]
fn looks_like_country(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c.is_alphabetic() || c == ' ' || c == '.') && s.len() <= 32
}

#[cfg_attr(feature = "libpostal", allow(dead_code))]
fn heuristic_expand(input: &str) -> String {
    let lowered = input.to_lowercase();
    let mut out = String::with_capacity(lowered.len());
    for token in lowered.split_whitespace() {
        let trimmed = token.trim_end_matches(',');
        let expanded = match trimmed {
            "st" | "st." => "street",
            "rd" | "rd." => "road",
            "ave" | "ave." => "avenue",
            "blvd" | "blvd." => "boulevard",
            "ln" | "ln." => "lane",
            "dr" | "dr." => "drive",
            "ct" | "ct." => "court",
            "pl" | "pl." => "place",
            "n" | "n." => "north",
            "s" | "s." => "south",
            "e" | "e." => "east",
            "w" | "w." => "west",
            other => other,
        };
        if !out.is_empty() {
            out.push(' ');
        }
        out.push_str(expanded);
    }
    out
}

/// libpostal FFI bindings.
///
/// Build prerequisites (only needed when the `libpostal` feature is on):
///   1. Install libpostal C library (`brew install libpostal` on macOS,
///      `apt install libpostal-dev` once the package lands, or build
///      from source: <https://github.com/openvenues/libpostal>).
///   2. Download the language model with
///      `libpostal_data download all $LIBPOSTAL_DATA_DIR`. ~2 GB.
///   3. Set `LIBPOSTAL_DATA_DIR` env var at runtime to point at the
///      installed model directory.
///
/// On first call, this module runs `libpostal_setup` +
/// `libpostal_setup_parser`. Calls are guarded by `Once` so concurrent
/// requests don't race.
#[cfg(feature = "libpostal")]
mod libpostal {
    use super::{ParseError, ParsedAddress};
    use libpostal_sys::{
        libpostal_address_parser_response_destroy, libpostal_expand_address,
        libpostal_expansion_array_destroy, libpostal_get_address_parser_default_options,
        libpostal_get_default_options, libpostal_parse_address, libpostal_setup,
        libpostal_setup_language_classifier, libpostal_setup_parser, libpostal_teardown,
        libpostal_teardown_language_classifier, libpostal_teardown_parser,
    };
    use std::ffi::{CStr, CString};
    use std::os::raw::{c_char, c_void};
    use std::sync::Once;

    static INIT_PARSER: Once = Once::new();
    static INIT_NORMALIZER: Once = Once::new();
    static mut PARSER_OK: bool = false;
    static mut NORMALIZER_OK: bool = false;

    fn ensure_parser() -> bool {
        // SAFETY: libpostal_setup + libpostal_setup_parser may only be
        // called once per process. `Once` guarantees that. After the
        // initial call the module-static `PARSER_OK` is read-only and
        // safe to read concurrently.
        unsafe {
            INIT_PARSER.call_once(|| {
                if libpostal_setup() && libpostal_setup_parser() {
                    PARSER_OK = true;
                    register_teardown();
                }
            });
            PARSER_OK
        }
    }

    fn ensure_normalizer() -> bool {
        unsafe {
            INIT_NORMALIZER.call_once(|| {
                if libpostal_setup() && libpostal_setup_language_classifier() {
                    NORMALIZER_OK = true;
                    register_teardown();
                }
            });
            NORMALIZER_OK
        }
    }

    fn register_teardown() {
        // libpostal_setup is reference-counted across setup_* helpers,
        // so calling teardown* on shutdown matches. We register the
        // process-exit hook once via `atexit`.
        extern "C" fn shutdown() {
            unsafe {
                libpostal_teardown_parser();
                libpostal_teardown_language_classifier();
                libpostal_teardown();
            }
        }
        unsafe {
            libc_atexit(shutdown);
        }
    }

    extern "C" {
        fn atexit(cb: extern "C" fn()) -> i32;
    }
    unsafe fn libc_atexit(cb: extern "C" fn()) {
        atexit(cb);
    }

    pub(super) fn parse(input: &str) -> Result<ParsedAddress, ParseError> {
        if !ensure_parser() {
            return Err(ParseError::NotInitialized);
        }
        let c_input = CString::new(input).map_err(|_| ParseError::Empty)?;
        let mut out = ParsedAddress::default();
        unsafe {
            let opts = libpostal_get_address_parser_default_options();
            let resp = libpostal_parse_address(c_input.as_ptr() as *mut c_char, opts);
            if resp.is_null() {
                return Err(ParseError::NotInitialized);
            }
            let r = &*resp;
            let n = r.num_components as usize;
            for i in 0..n {
                let comp = CStr::from_ptr(*r.components.add(i))
                    .to_string_lossy()
                    .into_owned();
                let label = CStr::from_ptr(*r.labels.add(i)).to_string_lossy();
                match label.as_ref() {
                    "house_number" => out.house_number = Some(comp),
                    "road" => out.road = Some(comp),
                    "unit" => out.unit = Some(comp),
                    "postcode" => out.postcode = Some(comp),
                    "city" | "city_district" => out.city = Some(comp),
                    "state" | "state_district" => out.state = Some(comp),
                    "country" | "country_region" => out.country = Some(comp),
                    _ => {}
                }
            }
            libpostal_address_parser_response_destroy(resp as *mut c_void as *mut _);
        }
        Ok(out)
    }

    pub(super) fn expand(input: &str) -> Vec<String> {
        if !ensure_normalizer() {
            return Vec::new();
        }
        let c_input = match CString::new(input) {
            Ok(c) => c,
            Err(_) => return Vec::new(),
        };
        unsafe {
            let opts = libpostal_get_default_options();
            let mut n: libpostal_sys::size_t = 0;
            let arr = libpostal_expand_address(c_input.as_ptr() as *mut c_char, opts, &mut n);
            if arr.is_null() {
                return Vec::new();
            }
            let count = n as usize;
            let mut out = Vec::with_capacity(count);
            for i in 0..count {
                let s = CStr::from_ptr(*arr.add(i)).to_string_lossy().into_owned();
                out.push(s);
            }
            libpostal_expansion_array_destroy(arr, n);
            out
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_us_style_address() {
        let p = parse("123 W Main St, Springfield, IL 62701, USA").unwrap();
        assert_eq!(p.house_number.as_deref(), Some("123"));
        assert!(p.road.as_deref().unwrap().contains("Main"));
        assert_eq!(p.country.as_deref(), Some("USA"));
    }

    #[test]
    fn parse_german_style_with_postcode() {
        let p = parse("Hauptstraße 12, 10115 Berlin, Deutschland").unwrap();
        // House number after the road in German style → heuristic only
        // catches it when number is the leading token. We accept that
        // limitation; the postcode extraction below is the win.
        assert_eq!(p.postcode.as_deref(), Some("10115"));
        assert_eq!(p.city.as_deref(), Some("Berlin"));
        assert_eq!(p.country.as_deref(), Some("Deutschland"));
    }

    #[test]
    fn parse_simple_road_no_house_number() {
        let p = parse("Aeulestrasse, Vaduz, Liechtenstein").unwrap();
        assert!(p.house_number.is_none());
        assert_eq!(p.road.as_deref(), Some("Aeulestrasse"));
        assert_eq!(p.city.as_deref(), Some("Vaduz"));
        assert_eq!(p.country.as_deref(), Some("Liechtenstein"));
    }

    #[test]
    fn parse_empty_errors() {
        assert!(matches!(parse(""), Err(ParseError::Empty)));
        assert!(matches!(parse("   "), Err(ParseError::Empty)));
    }

    #[test]
    fn expand_us_abbreviations() {
        assert_eq!(expand("123 W Main St"), vec!["123 west main street"]);
    }
}
