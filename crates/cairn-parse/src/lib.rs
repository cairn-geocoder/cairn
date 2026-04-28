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

fn looks_like_country(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c.is_alphabetic() || c == ' ' || c == '.') && s.len() <= 32
}

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

#[cfg(feature = "libpostal")]
mod libpostal {
    use super::{ParseError, ParsedAddress};
    // FFI integration is gated. The actual call sequence uses libpostal-sys
    // bindings: libpostal_setup, libpostal_setup_parser,
    // libpostal_parse_address, libpostal_address_parser_response_destroy.
    // Wiring up the unsafe blocks lands in a follow-up commit.
    pub(super) fn parse(_input: &str) -> Result<ParsedAddress, ParseError> {
        Err(ParseError::NotInitialized)
    }
    pub(super) fn expand(_input: &str) -> Vec<String> {
        Vec::new()
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
