//! `cairn-build augment` — apply v0.3 enrichers to an existing bundle.
//!
//! Two flavors, both run as **augmenters** rather than importers:
//!
//! - **Lane A — Microsoft Building Footprints.** Reads polygon
//!   GeoParquet, partitions to per-tile rkyv blobs under
//!   `spatial/buildings/`, and stamps `manifest.building_tiles`.
//!   Existing place + admin + point tiles are untouched.
//! - **Lane I — Wikidata enrichment.** Two-pass over the dump:
//!   collect every Q-id referenced by `wikidata=Qxxx` tags in
//!   the bundle's place tiles, then stream the dump to extract
//!   labels + cross-refs for those Q-ids only. Walks the tiles
//!   a second time, applies labels + cross-refs, rewrites
//!   modified tiles in place (preserving each tile's existing
//!   compression scheme), and recomputes blake3 / byte_size in
//!   the manifest entries.
//!
//! Both lanes are idempotent: re-running on the same inputs
//! produces a byte-identical manifest because the apply step
//! deduplicates labels by `(lang, value)` and cross-refs by
//! `(key, value)` before insertion.
//!
//! Optional `--key <path>` re-signs the manifest at the end.

use anyhow::{Context, Result};
use cairn_augment_wikidata as wikidata;
use cairn_import_buildings as buildings_import;
use cairn_place::Place;
use cairn_spatial::buildings as bspatial;
use cairn_tile::{
    encode_tile, read_manifest, read_tile, write_manifest, Level, Manifest, TileEntry,
};
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use tracing::{info, warn};

use crate::sign;

pub struct AugmentArgs {
    pub bundle: PathBuf,
    pub buildings: Vec<PathBuf>,
    pub wikidata: Option<PathBuf>,
    pub key: Option<PathBuf>,
}

pub fn cmd_augment(args: AugmentArgs) -> Result<()> {
    let manifest_path = args.bundle.join("manifest.toml");
    if !manifest_path.exists() {
        return Err(anyhow::anyhow!(
            "manifest missing at {} — bundle must already exist",
            manifest_path.display()
        ));
    }
    let mut manifest = read_manifest(&manifest_path)
        .with_context(|| format!("reading {}", manifest_path.display()))?;

    if !args.buildings.is_empty() {
        run_buildings(&args.bundle, &args.buildings, &mut manifest)?;
        // Bundle just gained building_tiles → bump the manifest
        // version so downstream tooling can gate on it. Existing v3
        // bundles being augmented in place jump straight to v4.
        if manifest.schema_version < 4 {
            manifest.schema_version = 4;
        }
    }
    if let Some(dump) = args.wikidata.as_deref() {
        run_wikidata(&args.bundle, dump, &mut manifest)?;
    }
    if args.buildings.is_empty() && args.wikidata.is_none() {
        warn!(
            "no augmenter selected (use --buildings <parquet> and/or --wikidata <dump>); manifest unchanged"
        );
        return Ok(());
    }

    write_manifest(&manifest_path, &manifest)
        .with_context(|| format!("writing {}", manifest_path.display()))?;
    println!(
        "augmented manifest -> {} (tiles={}, building_tiles={})",
        manifest_path.display(),
        manifest.tiles.len(),
        manifest.building_tiles.len()
    );

    if let Some(key) = args.key {
        sign::cmd_sign(&args.bundle, &key)
            .with_context(|| format!("re-signing manifest with {}", key.display()))?;
    }
    Ok(())
}

// ============================================================
// Lane A — buildings
// ============================================================

fn run_buildings(bundle: &Path, sources: &[PathBuf], manifest: &mut Manifest) -> Result<()> {
    info!(
        bundle = %bundle.display(),
        source_count = sources.len(),
        "augment: importing building footprints"
    );
    let cols = buildings_import::ColumnMap::default();
    let mut all: Vec<bspatial::Building> = Vec::new();
    for path in sources {
        let footprints = buildings_import::import(path, &cols)
            .with_context(|| format!("importing {}", path.display()))?;
        info!(
            source = %path.display(),
            count = footprints.len(),
            "imported building source"
        );
        for f in footprints {
            all.push(bspatial::Building {
                id: f.id,
                centroid: f.centroid,
                bbox: f.bbox,
                outer_ring: f.outer_ring,
                height: f.height,
            });
        }
    }
    info!(total = all.len(), "buildings collected, partitioning");

    let layer = bspatial::BuildingLayer { buildings: all };
    let entries = bspatial::write_buildings_partitioned(bundle, &layer, Level::L2)
        .with_context(|| "writing partitioned building tiles")?;
    info!(tile_count = entries.len(), "wrote building tiles");

    // Drop any pre-existing entries — we just wrote a fresh set, and
    // file paths are deterministic per (level, tile_id) so reruns are
    // safe overwrites.
    manifest.building_tiles = entries;
    Ok(())
}

// ============================================================
// Lane I — Wikidata
// ============================================================

fn run_wikidata(bundle: &Path, dump_path: &Path, manifest: &mut Manifest) -> Result<()> {
    info!(
        dump = %dump_path.display(),
        tile_count = manifest.tiles.len(),
        "augment: collecting Q-ids from existing place tiles"
    );

    // Pass 1: collect Q-ids.
    let mut wanted: HashSet<String> = HashSet::new();
    for entry in &manifest.tiles {
        let path = bundle.join(rel_tile_path(entry));
        let places = match read_tile(&path) {
            Ok(ps) => ps,
            Err(err) => {
                warn!(?err, path = %path.display(), "skipping unreadable tile during qid scan");
                continue;
            }
        };
        for q in wikidata::collect_qids(&places) {
            wanted.insert(q);
        }
    }
    info!(qids = wanted.len(), "Q-id scan complete");

    if wanted.is_empty() {
        info!("no wikidata-tagged places; skipping dump scan");
        return Ok(());
    }

    // Stream the dump, extracting only the Q-ids we care about.
    let entries = wikidata::stream_dump(dump_path, &wanted)
        .with_context(|| format!("streaming Wikidata dump {}", dump_path.display()))?;
    info!(matched = entries.len(), "Wikidata entries materialized");

    // Pass 2: rewrite affected tiles.
    let mut stats = wikidata::AugmentStats::default();
    let mut updated_tiles = 0u64;
    for entry in manifest.tiles.iter_mut() {
        let rel = rel_tile_path(entry);
        let path = bundle.join(&rel);
        let mut places: Vec<Place> = match read_tile(&path) {
            Ok(ps) => ps,
            Err(err) => {
                warn!(?err, path = %path.display(), "skipping unreadable tile during apply");
                continue;
            }
        };
        let pre_signature = signature(&places);
        wikidata::apply_to_places(&mut places, &entries, &mut stats);
        let post_signature = signature(&places);
        if pre_signature == post_signature {
            continue;
        }
        let bytes = encode_tile(&places, entry.compression)
            .with_context(|| format!("re-encoding tile {}", path.display()))?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).ok();
        }
        fs::write(&path, &bytes)
            .with_context(|| format!("rewriting tile {}", path.display()))?;
        entry.byte_size = bytes.len() as u64;
        entry.blake3 = blake3::hash(&bytes).to_hex().to_string();
        entry.place_count = places.len() as u32;
        updated_tiles += 1;
    }
    info!(
        examined = stats.places_examined,
        with_qid = stats.places_with_qid,
        matched = stats.qids_found_in_dump,
        enriched = stats.places_enriched,
        labels_added = stats.labels_added,
        crossrefs_added = stats.crossrefs_added,
        tiles_rewritten = updated_tiles,
        "wikidata augment apply done"
    );
    Ok(())
}

fn rel_tile_path(entry: &TileEntry) -> String {
    let a = entry.tile_id / 1_000_000 % 1000;
    let b = entry.tile_id / 1000 % 1000;
    format!(
        "tiles/{}/{:03}/{:03}/{}.bin",
        entry.level, a, b, entry.tile_id
    )
}

/// Cheap content signature so we can skip a re-encode + re-write when
/// the augmenter found no enrichments to apply for this tile. (Hashing
/// the full Place list would be a sounder check but `(name count, tag
/// count)` per place is enough — apply_to_places only ever inserts.)
fn signature(places: &[Place]) -> Vec<(usize, usize)> {
    places.iter().map(|p| (p.names.len(), p.tags.len())).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_place::{Coord, LocalizedName, PlaceId, PlaceKind};
    use cairn_tile::{TileCompression, TileEntry};

    #[test]
    fn rel_tile_path_matches_tilecoord_format() {
        let e = TileEntry {
            level: 2,
            tile_id: 49509,
            blake3: "x".into(),
            byte_size: 1,
            place_count: 0,
            compression: TileCompression::None,
        };
        assert_eq!(rel_tile_path(&e), "tiles/2/000/049/49509.bin");
    }

    #[test]
    fn signature_changes_when_tags_added() {
        let p1 = Place {
            id: PlaceId::new(2, 1, 0).unwrap(),
            kind: PlaceKind::Poi,
            names: vec![LocalizedName {
                lang: "default".into(),
                value: "x".into(),
            }],
            centroid: Coord { lon: 0.0, lat: 0.0 },
            admin_path: vec![],
            tags: vec![],
        };
        let s1 = signature(std::slice::from_ref(&p1));
        let mut p2 = p1;
        p2.tags.push(("k".into(), "v".into()));
        let s2 = signature(std::slice::from_ref(&p2));
        assert_ne!(s1, s2);
    }
}
