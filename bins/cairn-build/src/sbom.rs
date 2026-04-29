//! CycloneDX 1.5 SBOM generator.
//!
//! Cairn ships an SBOM alongside every bundle so operators can audit
//! which Rust crates and which source datasets went into a given
//! geocoder shipment. The format is plain CycloneDX JSON, validated
//! against the spec by tools like `cyclonedx-cli`, `grype`, and
//! `dependency-track`.
//!
//! Two component classes are emitted:
//!
//! - **library** — every entry in the workspace's `Cargo.lock`,
//!   keyed by `pkg:cargo/<name>@<version>` purl. Source URLs (the
//!   `source = "registry+..."` line) are echoed when present.
//! - **data** — every `SourceVersion` returned by the importer
//!   (OSM PBF, WhosOnFirst SQLite, OpenAddresses CSV, Geonames
//!   TSV, Geonames postcodes). Each carries a BLAKE3 hash so the
//!   SBOM also functions as a provenance attestation.
//!
//! `Cargo.lock` is embedded into the `cairn-build` binary at
//! compile time via `include_str!` so the SBOM emitter doesn't need
//! filesystem access to the workspace at bundle-build time.

use cairn_tile::SourceVersion;
use serde::Serialize;
use std::path::Path;
use std::time::SystemTime;

/// `Cargo.lock` from the workspace root, embedded at compile time.
const CARGO_LOCK: &str = include_str!("../../../Cargo.lock");

/// Builder version stamped into the SBOM `metadata.tools` array. Uses
/// the `cairn-build` package version from cargo.
const BUILDER_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Serialize)]
struct Bom<'a> {
    #[serde(rename = "bomFormat")]
    bom_format: &'static str,
    #[serde(rename = "specVersion")]
    spec_version: &'static str,
    version: u32,
    metadata: Metadata<'a>,
    components: Vec<Component>,
}

#[derive(Serialize)]
struct Metadata<'a> {
    timestamp: String,
    tools: Vec<Tool>,
    component: TopComponent<'a>,
}

#[derive(Serialize)]
struct Tool {
    vendor: &'static str,
    name: &'static str,
    version: &'static str,
}

#[derive(Serialize)]
struct TopComponent<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    name: &'static str,
    version: &'a str,
    description: &'static str,
}

#[derive(Serialize)]
struct Component {
    #[serde(rename = "type")]
    kind: &'static str,
    #[serde(rename = "bom-ref")]
    bom_ref: String,
    name: String,
    version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    purl: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    hashes: Vec<Hash>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    properties: Vec<Property>,
}

#[derive(Serialize)]
struct Hash {
    alg: &'static str,
    content: String,
}

#[derive(Serialize)]
struct Property {
    name: String,
    value: String,
}

/// Emit a CycloneDX 1.5 JSON SBOM at `<bundle>/sbom.json`. Returns the
/// number of library components recorded (informational; data
/// components are always `sources.len()`).
///
/// `bundle_id` is stamped into the top-level component so multiple
/// bundles emitted by the same builder remain distinguishable in
/// downstream tooling.
pub fn write_sbom(
    bundle_dir: &Path,
    bundle_id: &str,
    sources: &[SourceVersion],
) -> std::io::Result<usize> {
    let library_components = parse_cargo_lock(CARGO_LOCK);
    let data_components: Vec<Component> = sources
        .iter()
        .map(|s| Component {
            kind: "data",
            bom_ref: format!("data:cairn:{}", s.name),
            name: s.name.clone(),
            version: s.version.clone(),
            purl: None,
            description: Some(format!("Cairn source dataset: {}", s.name)),
            hashes: vec![Hash {
                alg: "BLAKE3",
                content: s.blake3.clone(),
            }],
            properties: Vec::new(),
        })
        .collect();

    let lib_count = library_components.len();
    let mut components = library_components;
    components.extend(data_components);

    let bom = Bom {
        bom_format: "CycloneDX",
        spec_version: "1.5",
        version: 1,
        metadata: Metadata {
            timestamp: now_rfc3339(),
            tools: vec![Tool {
                vendor: "cairn-geocoder",
                name: "cairn-build",
                version: BUILDER_VERSION,
            }],
            component: TopComponent {
                kind: "application",
                name: "cairn-bundle",
                version: bundle_id,
                description: "Cairn airgap-ready geocoder bundle",
            },
        },
        components,
    };

    let path = bundle_dir.join("sbom.json");
    let body = serde_json::to_vec_pretty(&bom).map_err(std::io::Error::other)?;
    std::fs::write(&path, body)?;
    Ok(lib_count)
}

/// Walk every `[[package]]` block of an embedded Cargo.lock and emit
/// one CycloneDX `library` component per entry. Source URL is parsed
/// out of the `source = "..."` field so registry crates get a real
/// `purl`; path-only workspace crates get a custom bom-ref instead.
fn parse_cargo_lock(text: &str) -> Vec<Component> {
    let mut out = Vec::new();
    let mut name: Option<String> = None;
    let mut version: Option<String> = None;
    let mut source: Option<String> = None;
    let mut checksum: Option<String> = None;
    let mut in_package = false;

    let flush = |out: &mut Vec<Component>,
                 name: &mut Option<String>,
                 version: &mut Option<String>,
                 source: &mut Option<String>,
                 checksum: &mut Option<String>| {
        if let (Some(n), Some(v)) = (name.take(), version.take()) {
            let src = source.take();
            let cs = checksum.take();
            let purl = src
                .as_deref()
                .filter(|s| s.starts_with("registry+"))
                .map(|_| format!("pkg:cargo/{n}@{v}"));
            let mut props: Vec<Property> = Vec::new();
            if let Some(s) = src.as_deref() {
                props.push(Property {
                    name: "cairn:source".into(),
                    value: s.to_string(),
                });
            }
            let hashes = cs
                .map(|c| {
                    vec![Hash {
                        alg: "SHA-256",
                        content: c,
                    }]
                })
                .unwrap_or_default();
            out.push(Component {
                kind: "library",
                bom_ref: format!("pkg:cargo/{n}@{v}"),
                name: n,
                version: v,
                purl,
                description: None,
                hashes,
                properties: props,
            });
        } else {
            // Reset partials so we don't carry state into the next block.
            *name = None;
            *version = None;
            *source = None;
            *checksum = None;
        }
    };

    for raw in text.lines() {
        let line = raw.trim();
        if line == "[[package]]" {
            if in_package {
                flush(
                    &mut out,
                    &mut name,
                    &mut version,
                    &mut source,
                    &mut checksum,
                );
            }
            in_package = true;
            continue;
        }
        if !in_package {
            continue;
        }
        // Stop accumulating when we hit a non-package table header
        // (e.g., `[metadata]`).
        if line.starts_with('[') && !line.starts_with("[[package]]") {
            flush(
                &mut out,
                &mut name,
                &mut version,
                &mut source,
                &mut checksum,
            );
            in_package = false;
            continue;
        }
        if let Some(rest) = line.strip_prefix("name = ") {
            name = Some(strip_quotes(rest));
        } else if let Some(rest) = line.strip_prefix("version = ") {
            version = Some(strip_quotes(rest));
        } else if let Some(rest) = line.strip_prefix("source = ") {
            source = Some(strip_quotes(rest));
        } else if let Some(rest) = line.strip_prefix("checksum = ") {
            checksum = Some(strip_quotes(rest));
        }
    }
    if in_package {
        flush(
            &mut out,
            &mut name,
            &mut version,
            &mut source,
            &mut checksum,
        );
    }
    out
}

fn strip_quotes(s: &str) -> String {
    s.trim().trim_matches('"').to_string()
}

fn now_rfc3339() -> String {
    let secs = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    // Minimal RFC3339 in UTC without a chrono dep. Good enough for an
    // SBOM `metadata.timestamp` field; downstream tools tolerate the
    // unsubseconded form.
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    let h = rem / 3600;
    let m = (rem % 3600) / 60;
    let s = rem % 60;
    let (y, mo, d) = days_to_ymd(days);
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{m:02}:{s:02}Z")
}

/// Days-since-1970-01-01 → (year, month, day). Civil-from-days
/// algorithm by Howard Hinnant. Avoids dragging chrono in just for
/// the SBOM timestamp.
fn days_to_ymd(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_minimal_cargo_lock() {
        let lock = r#"# generated
version = 3

[[package]]
name = "foo"
version = "1.2.3"
source = "registry+https://github.com/rust-lang/crates.io-index"
checksum = "abc123"

[[package]]
name = "workspace-crate"
version = "0.0.1"
dependencies = [
 "foo",
]
"#;
        let comps = parse_cargo_lock(lock);
        assert_eq!(comps.len(), 2);
        let foo = comps.iter().find(|c| c.name == "foo").unwrap();
        assert_eq!(foo.version, "1.2.3");
        assert_eq!(foo.purl.as_deref(), Some("pkg:cargo/foo@1.2.3"));
        assert_eq!(foo.hashes.len(), 1);
        assert_eq!(foo.hashes[0].alg, "SHA-256");

        // workspace-crate has no source = registry, so no purl.
        let ws = comps.iter().find(|c| c.name == "workspace-crate").unwrap();
        assert!(ws.purl.is_none());
        assert!(ws.hashes.is_empty());
    }

    #[test]
    fn embedded_cargo_lock_has_packages() {
        let comps = parse_cargo_lock(CARGO_LOCK);
        assert!(
            !comps.is_empty(),
            "embedded Cargo.lock parse produced zero components"
        );
        // Sanity — workspace ships several cairn-* crates.
        assert!(comps.iter().any(|c| c.name.starts_with("cairn-")));
    }

    #[test]
    fn write_sbom_emits_valid_json() {
        let dir = std::env::temp_dir().join(format!(
            "cairn-sbom-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let sources = vec![SourceVersion {
            name: "osm".into(),
            version: "/path/to/li.osm.pbf".into(),
            blake3: "deadbeef".into(),
        }];
        let n = write_sbom(&dir, "test-bundle", &sources).unwrap();
        assert!(n > 0);
        let raw = std::fs::read_to_string(dir.join("sbom.json")).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(parsed["bomFormat"], "CycloneDX");
        assert_eq!(parsed["specVersion"], "1.5");
        // Data component for the OSM source must be present with
        // BLAKE3 hash.
        let comps = parsed["components"].as_array().unwrap();
        let data = comps
            .iter()
            .find(|c| c["type"] == "data" && c["name"] == "osm")
            .unwrap();
        assert_eq!(data["hashes"][0]["alg"], "BLAKE3");
        assert_eq!(data["hashes"][0]["content"], "deadbeef");
    }

    #[test]
    fn days_to_ymd_known_dates() {
        // 1970-01-01 = 0
        assert_eq!(days_to_ymd(0), (1970, 1, 1));
        // 2000-01-01 = 10957
        assert_eq!(days_to_ymd(10957), (2000, 1, 1));
        // 2024-02-29 (leap day) = 19782
        assert_eq!(days_to_ymd(19782), (2024, 2, 29));
    }
}
