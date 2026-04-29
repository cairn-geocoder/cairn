//! OsmChange (`.osc.gz`) parser.
//!
//! Decompresses + parses one OSM minutely diff file into a flat list
//! of [`DiffOp`]s. Out of scope here: way / relation re-application
//! (those need geometry resolution against the existing bundle).
//! The parser surfaces every op so a caller can pick what it can
//! handle now and log the rest as deferred.
//!
//! XML shape (per OSM wiki):
//! ```xml
//! <osmChange version="0.6">
//!   <create><node id=".." lat=".." lon=".." version=".."> <tag k=".." v=".."/> ... </node></create>
//!   <modify><node id=".." lat=".." lon=".." version=".."> ... </node></modify>
//!   <delete><node id=".." version=".."/></delete>
//!   <create><way id=".." version="..">  <nd ref=".."/> ... </way></create>
//!   ...
//! </osmChange>
//! ```
//!
//! Multi-version creates inside one diff are normal — the parser
//! preserves declared order so the caller's apply pass sees them in
//! the order they actually occurred.

use anyhow::{Context, Result};
use flate2::read::GzDecoder;
use quick_xml::events::{BytesStart, Event};
use quick_xml::Reader;
use std::io::{BufRead, BufReader};
use std::path::Path;

/// Outer change action wrapping each operation.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum Action {
    Create,
    Modify,
    Delete,
}

/// Element kind targeted by the operation.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum OsmKind {
    Node,
    Way,
    Relation,
}

/// One create / modify / delete on one OSM element.
#[derive(Clone, Debug)]
#[allow(dead_code)] // id + version consumed by upcoming tile-mutation pass.
pub struct DiffOp {
    pub action: Action,
    pub kind: OsmKind,
    pub id: i64,
    pub version: u64,
    /// Present on `Node` ops; absent on `Way` / `Relation`.
    pub lat: Option<f64>,
    pub lon: Option<f64>,
    pub tags: Vec<(String, String)>,
}

impl DiffOp {
    /// `true` when the op carries enough signal to potentially become
    /// a `Place` — has at least one tag we'd index. Used by the apply
    /// pass to skip noise (e.g. way-node coord micro-edits with no
    /// tags).
    pub fn looks_taggable(&self) -> bool {
        if self.tags.is_empty() {
            return false;
        }
        self.tags.iter().any(|(k, _)| {
            matches!(
                k.as_str(),
                "place"
                    | "name"
                    | "amenity"
                    | "shop"
                    | "tourism"
                    | "office"
                    | "leisure"
                    | "historic"
                    | "craft"
                    | "emergency"
                    | "healthcare"
                    | "boundary"
                    | "highway"
                    | "addr:housenumber"
            )
        })
    }
}

/// Parse a `.osc.gz` (or plain `.osc`) file from disk into the full
/// list of operations. Caller is expected to bucket by tile and apply
/// in a follow-up step.
pub fn parse_file(path: &Path) -> Result<Vec<DiffOp>> {
    let f = std::fs::File::open(path)
        .with_context(|| format!("opening osc file {}", path.display()))?;
    let is_gzipped = path
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.eq_ignore_ascii_case("gz"))
        .unwrap_or(false);
    if is_gzipped {
        parse_reader(BufReader::new(GzDecoder::new(f)))
    } else {
        parse_reader(BufReader::new(f))
    }
}

/// Parser entry point that takes any [`BufRead`] — useful for tests
/// that feed XML strings directly.
pub fn parse_reader<R: BufRead>(reader: R) -> Result<Vec<DiffOp>> {
    let mut xml = Reader::from_reader(reader);
    xml.config_mut().trim_text(true);

    let mut buf = Vec::with_capacity(4096);
    let mut ops: Vec<DiffOp> = Vec::new();
    let mut cur_action: Option<Action> = None;
    let mut cur_op: Option<DiffOp> = None;

    loop {
        match xml.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) => match e.name().as_ref() {
                b"create" => cur_action = Some(Action::Create),
                b"modify" => cur_action = Some(Action::Modify),
                b"delete" => cur_action = Some(Action::Delete),
                b"node" | b"way" | b"relation" => {
                    if let Some(action) = cur_action {
                        cur_op = Some(start_op(action, e)?);
                    }
                }
                b"tag" => {
                    if let Some(op) = cur_op.as_mut() {
                        if let Some((k, v)) = read_tag(e)? {
                            op.tags.push((k, v));
                        }
                    }
                }
                _ => {}
            },
            Ok(Event::Empty(ref e)) => match e.name().as_ref() {
                // Self-closing forms: `<node ... />` (delete bodies +
                // tagless creates), `<tag k=".." v=".." />`,
                // `<nd ref=".." />`.
                b"node" | b"way" | b"relation" => {
                    if let Some(action) = cur_action {
                        ops.push(start_op(action, e)?);
                    }
                }
                b"tag" => {
                    if let Some(op) = cur_op.as_mut() {
                        if let Some((k, v)) = read_tag(e)? {
                            op.tags.push((k, v));
                        }
                    }
                }
                _ => {}
            },
            Ok(Event::End(ref e)) => match e.name().as_ref() {
                b"create" | b"modify" | b"delete" => cur_action = None,
                b"node" | b"way" | b"relation" => {
                    if let Some(op) = cur_op.take() {
                        ops.push(op);
                    }
                }
                _ => {}
            },
            Ok(Event::Eof) => break,
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "OSC XML parse error at byte {}: {e}",
                    xml.buffer_position()
                ))
            }
            _ => {}
        }
        buf.clear();
    }
    Ok(ops)
}

fn start_op(action: Action, e: &BytesStart<'_>) -> Result<DiffOp> {
    let kind = match e.name().as_ref() {
        b"node" => OsmKind::Node,
        b"way" => OsmKind::Way,
        b"relation" => OsmKind::Relation,
        other => {
            return Err(anyhow::anyhow!(
                "unexpected element <{}> in osmChange",
                String::from_utf8_lossy(other)
            ))
        }
    };
    let mut id: i64 = 0;
    let mut version: u64 = 0;
    let mut lat: Option<f64> = None;
    let mut lon: Option<f64> = None;
    for attr in e.attributes().flatten() {
        let key = attr.key.as_ref();
        let val = attr.unescape_value().unwrap_or_default();
        match key {
            b"id" => id = val.parse().unwrap_or(0),
            b"version" => version = val.parse().unwrap_or(0),
            b"lat" => lat = val.parse().ok(),
            b"lon" => lon = val.parse().ok(),
            _ => {}
        }
    }
    Ok(DiffOp {
        action,
        kind,
        id,
        version,
        lat,
        lon,
        tags: Vec::new(),
    })
}

fn read_tag(e: &BytesStart<'_>) -> Result<Option<(String, String)>> {
    let mut k: Option<String> = None;
    let mut v: Option<String> = None;
    for attr in e.attributes().flatten() {
        let val = attr.unescape_value().unwrap_or_default().into_owned();
        match attr.key.as_ref() {
            b"k" => k = Some(val),
            b"v" => v = Some(val),
            _ => {}
        }
    }
    Ok(match (k, v) {
        (Some(k), Some(v)) => Some((k, v)),
        _ => None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn parse_str(xml: &str) -> Vec<DiffOp> {
        parse_reader(Cursor::new(xml)).expect("parse")
    }

    #[test]
    fn parse_create_node_with_tags() {
        let xml = r#"<?xml version="1.0"?>
<osmChange version="0.6">
  <create>
    <node id="1" lat="47.14" lon="9.52" version="1">
      <tag k="place" v="village"/>
      <tag k="name" v="Vaduz"/>
    </node>
  </create>
</osmChange>"#;
        let ops = parse_str(xml);
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].action, Action::Create);
        assert_eq!(ops[0].kind, OsmKind::Node);
        assert_eq!(ops[0].id, 1);
        assert_eq!(ops[0].version, 1);
        assert_eq!(ops[0].lat, Some(47.14));
        assert_eq!(ops[0].lon, Some(9.52));
        assert_eq!(ops[0].tags.len(), 2);
        assert!(ops[0].looks_taggable());
    }

    #[test]
    fn parse_delete_node_self_closing() {
        let xml = r#"<osmChange><delete>
            <node id="42" version="3"/>
        </delete></osmChange>"#;
        let ops = parse_str(xml);
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].action, Action::Delete);
        assert_eq!(ops[0].id, 42);
        assert_eq!(ops[0].version, 3);
        assert!(ops[0].tags.is_empty());
    }

    #[test]
    fn parse_modify_way_with_nodes_and_tags() {
        // <nd ref=".."/> ignored — we don't need way-node lists for
        // diff apply (geometry resolution lands in the apply pass).
        let xml = r#"<osmChange><modify>
            <way id="100" version="2">
              <nd ref="1"/>
              <nd ref="2"/>
              <tag k="highway" v="residential"/>
              <tag k="name" v="Aeulestrasse"/>
            </way>
        </modify></osmChange>"#;
        let ops = parse_str(xml);
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].action, Action::Modify);
        assert_eq!(ops[0].kind, OsmKind::Way);
        assert_eq!(ops[0].id, 100);
        assert_eq!(ops[0].tags.len(), 2);
        assert!(ops[0].looks_taggable());
    }

    #[test]
    fn parse_mixed_actions_preserve_order() {
        let xml = r#"<osmChange>
          <create><node id="1" lat="0.0" lon="0.0" version="1"/></create>
          <modify><node id="2" lat="0.1" lon="0.1" version="2"><tag k="amenity" v="cafe"/></node></modify>
          <delete><node id="3" version="4"/></delete>
        </osmChange>"#;
        let ops = parse_str(xml);
        assert_eq!(ops.len(), 3);
        assert_eq!(ops[0].action, Action::Create);
        assert_eq!(ops[1].action, Action::Modify);
        assert_eq!(ops[2].action, Action::Delete);
    }

    #[test]
    fn looks_taggable_false_for_untagged_node() {
        let xml = r#"<osmChange><create>
            <node id="1" lat="0.0" lon="0.0" version="1"/>
        </create></osmChange>"#;
        let ops = parse_str(xml);
        assert!(!ops[0].looks_taggable());
    }

    #[test]
    fn looks_taggable_false_for_irrelevant_tags() {
        let xml = r#"<osmChange><create>
            <node id="1" lat="0.0" lon="0.0" version="1">
              <tag k="created_by" v="JOSM"/>
            </node>
        </create></osmChange>"#;
        let ops = parse_str(xml);
        assert!(!ops[0].looks_taggable());
    }

    #[test]
    fn parse_relation_op_collected() {
        let xml = r#"<osmChange><create>
            <relation id="500" version="1">
              <member type="way" ref="100" role="outer"/>
              <tag k="boundary" v="administrative"/>
              <tag k="admin_level" v="8"/>
            </relation>
        </create></osmChange>"#;
        let ops = parse_str(xml);
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].kind, OsmKind::Relation);
        assert_eq!(ops[0].tags.len(), 2);
    }
}
