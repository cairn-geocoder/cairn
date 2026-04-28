//! OSM minutely diff replication — fetcher + state tracking.
//!
//! Scope (this module): pull new `.osc.gz` diff files into the bundle's
//! `replication/` subdirectory and persist a `replication_state.toml`
//! file that records the latest fetched sequence number and the
//! upstream base URL.
//!
//! Out of scope (follow-up): applying the fetched diffs to the
//! existing tile structure. That would mean parsing each
//! `<modify>` / `<create>` / `<delete>` against the archived rkyv
//! tile blobs and rebuilding affected tiles. Multi-day work; this
//! module hands operators a known-good staging directory + state
//! file to drive that step manually for now.
//!
//! Airgap pattern: mirror upstream diffs to a local store and point
//! `--upstream file:///path/to/mirror` at it. The fetcher is happy
//! with any URL ureq can resolve.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Persisted state of bundle replication. Lives at
/// `<bundle>/replication_state.toml`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplicationState {
    /// URL prefix for the minutely replication feed, e.g.
    /// `https://planet.openstreetmap.org/replication/minute`.
    pub upstream: String,
    /// Highest sequence number successfully fetched. `None` means
    /// nothing fetched yet — first run will start from the upstream's
    /// current `state.txt` sequence.
    pub last_fetched_seq: Option<u64>,
    /// Most recent fetch wall-clock (`epoch:N`).
    pub last_fetched_at: Option<String>,
    /// Highest sequence number that has been APPLIED to the bundle.
    /// Always `<= last_fetched_seq`. Initially `None`. Application is
    /// a follow-up; this field exists so the apply pass knows where
    /// to start.
    pub last_applied_seq: Option<u64>,
}

impl ReplicationState {
    pub fn new(upstream: String) -> Self {
        Self {
            upstream,
            last_fetched_seq: None,
            last_fetched_at: None,
            last_applied_seq: None,
        }
    }
}

const STATE_FILE: &str = "replication_state.toml";
const REPLICATION_DIR: &str = "replication";
const FETCH_TIMEOUT_SECS: u64 = 60;

/// Read the bundle's replication state. Returns `None` when the file
/// hasn't been written yet (replication never initialized).
pub fn read_state(bundle: &Path) -> Result<Option<ReplicationState>> {
    let path = bundle.join(STATE_FILE);
    if !path.exists() {
        return Ok(None);
    }
    let raw = std::fs::read_to_string(&path)
        .with_context(|| format!("reading {}", path.display()))?;
    let state: ReplicationState = toml::from_str(&raw)
        .with_context(|| format!("parsing {}", path.display()))?;
    Ok(Some(state))
}

pub fn write_state(bundle: &Path, state: &ReplicationState) -> Result<()> {
    let path = bundle.join(STATE_FILE);
    let raw = toml::to_string_pretty(state).context("encoding replication state")?;
    std::fs::write(&path, raw)
        .with_context(|| format!("writing {}", path.display()))?;
    Ok(())
}

/// Map a sequence number to the `AAA/BBB/CCC.osc.gz` path layout used
/// by every OSM-style replication feed. Pads to 9 digits left-zero.
fn seq_path_components(seq: u64) -> (String, String, String) {
    let s = format!("{seq:09}");
    (s[0..3].into(), s[3..6].into(), s[6..9].into())
}

/// Fetch the upstream's `state.txt` and parse the `sequenceNumber=N`
/// line. Returns the current published seq.
pub fn fetch_current_seq(upstream: &str) -> Result<u64> {
    let url = format!("{}/state.txt", upstream.trim_end_matches('/'));
    let body = http_get_text(&url)?;
    parse_sequence_number(&body)
        .ok_or_else(|| anyhow::anyhow!("no sequenceNumber in {url}"))
}

fn parse_sequence_number(body: &str) -> Option<u64> {
    for line in body.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("sequenceNumber=") {
            return rest.trim().parse().ok();
        }
    }
    None
}

/// Download a single `<seq>.osc.gz` into `<bundle>/replication/`.
/// Returns the local path on success.
pub fn fetch_diff(bundle: &Path, upstream: &str, seq: u64) -> Result<PathBuf> {
    let (a, b, c) = seq_path_components(seq);
    let url = format!(
        "{}/{}/{}/{}.osc.gz",
        upstream.trim_end_matches('/'),
        a,
        b,
        c
    );
    let dir = bundle.join(REPLICATION_DIR);
    std::fs::create_dir_all(&dir)?;
    let dst = dir.join(format!("{seq:09}.osc.gz"));
    let bytes = http_get_bytes(&url)
        .with_context(|| format!("fetching {url}"))?;
    std::fs::write(&dst, &bytes)
        .with_context(|| format!("writing {}", dst.display()))?;
    tracing::info!(
        seq,
        size = bytes.len(),
        path = %dst.display(),
        "fetched diff"
    );
    Ok(dst)
}

/// Fetch every diff from `start_seq` to the current upstream seq,
/// inclusive on both ends. Caps at `max` files per call so a stale
/// bundle doesn't trigger a multi-GB download in one go.
pub fn fetch_pending(
    bundle: &Path,
    state: &mut ReplicationState,
    max: usize,
) -> Result<Vec<u64>> {
    let current = fetch_current_seq(&state.upstream)
        .with_context(|| format!("reading state.txt from {}", state.upstream))?;
    let start = match state.last_fetched_seq {
        Some(seq) => seq.saturating_add(1),
        None => current, // first run — anchor to the present
    };
    if start > current {
        tracing::info!(
            current,
            last = state.last_fetched_seq,
            "no new diffs"
        );
        return Ok(Vec::new());
    }

    let mut fetched = Vec::new();
    for seq in start..=current {
        if fetched.len() >= max {
            tracing::warn!(
                fetched = fetched.len(),
                max,
                next = seq,
                "hit --max cap; rerun replicate-fetch to continue"
            );
            break;
        }
        fetch_diff(bundle, &state.upstream, seq)?;
        fetched.push(seq);
    }
    if let Some(&last) = fetched.last() {
        state.last_fetched_seq = Some(last);
        state.last_fetched_at = Some(now_iso8601_local());
    }
    Ok(fetched)
}

fn http_get_text(url: &str) -> Result<String> {
    let resp = ureq::get(url)
        .timeout(Duration::from_secs(FETCH_TIMEOUT_SECS))
        .call()
        .with_context(|| format!("GET {url}"))?;
    Ok(resp.into_string()?)
}

fn http_get_bytes(url: &str) -> Result<Vec<u8>> {
    let resp = ureq::get(url)
        .timeout(Duration::from_secs(FETCH_TIMEOUT_SECS))
        .call()
        .with_context(|| format!("GET {url}"))?;
    let mut buf = Vec::new();
    let mut reader = resp.into_reader();
    std::io::copy(&mut reader, &mut buf)?;
    Ok(buf)
}

fn now_iso8601_local() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    format!("epoch:{secs}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_roundtrip_via_toml() {
        let dir = std::env::temp_dir().join(format!(
            "cairn-rep-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let mut s = ReplicationState::new("https://example.com".into());
        s.last_fetched_seq = Some(12345);
        s.last_applied_seq = Some(12300);
        s.last_fetched_at = Some("epoch:1700000000".into());
        write_state(&dir, &s).unwrap();
        let back = read_state(&dir).unwrap().unwrap();
        assert_eq!(back.upstream, s.upstream);
        assert_eq!(back.last_fetched_seq, s.last_fetched_seq);
        assert_eq!(back.last_applied_seq, s.last_applied_seq);
    }

    #[test]
    fn read_state_returns_none_when_missing() {
        let dir = std::env::temp_dir().join(format!(
            "cairn-rep-empty-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        assert!(read_state(&dir).unwrap().is_none());
    }

    #[test]
    fn seq_path_layout_matches_osm_convention() {
        assert_eq!(
            seq_path_components(123_456_789),
            ("123".into(), "456".into(), "789".into())
        );
        assert_eq!(
            seq_path_components(7),
            ("000".into(), "000".into(), "007".into())
        );
    }

    #[test]
    fn parse_state_txt() {
        let body = "#Tue Jan 02 03:04:05 UTC 2024\n\
                    txnMaxQueried=12345\n\
                    sequenceNumber=987654321\n\
                    timestamp=2024-01-02T03\\:04\\:05Z\n";
        assert_eq!(parse_sequence_number(body), Some(987654321));
    }

    #[test]
    fn parse_state_txt_missing_returns_none() {
        let body = "sequence=12345\nfoo=bar\n";
        assert_eq!(parse_sequence_number(body), None);
    }
}
