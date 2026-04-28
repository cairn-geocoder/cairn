//! `cairn-build` — offline bundle builder.
//!
//! Phase 1: read OSM PBF → bucket places into tiles → write `.bin` blobs +
//! `manifest.toml`. WhosOnFirst, OpenAddresses, Geonames land in later
//! phases.

use anyhow::{Context, Result};
use cairn_place::Place;
use cairn_spatial::{PlacePoint, PointLayer};
use cairn_tile::{
    bbox_intersects, bucket_places, read_manifest, verify_bundle, write_manifest, write_tile,
    Level, Manifest, SourceVersion, TileCompression, TileCoord, TileEntry,
};
use clap::{Parser, Subcommand};
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

#[derive(Parser, Debug)]
#[command(name = "cairn-build", version, about = "Build Cairn geocoder bundles")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Build a bundle from configured sources.
    Build {
        #[arg(long)]
        osm: Option<PathBuf>,
        #[arg(long)]
        wof: Option<PathBuf>,
        #[arg(long)]
        oa: Option<PathBuf>,
        #[arg(long)]
        geonames: Option<PathBuf>,
        #[arg(long)]
        out: PathBuf,
        #[arg(long, default_value = "alpha-bundle")]
        bundle_id: String,
        /// Compress every tile blob with zstd. Smaller bundle on disk
        /// at the cost of a tiny CPU hit on first read.
        #[arg(long)]
        zstd: bool,
    },
    /// Extract a regional bundle from an existing planet bundle.
    Extract {
        #[arg(long)]
        bundle: PathBuf,
        #[arg(long, num_args = 4, value_names = ["MIN_LON", "MIN_LAT", "MAX_LON", "MAX_LAT"])]
        bbox: Vec<f64>,
        #[arg(long)]
        out: PathBuf,
        /// After extracting, write a `<out>.tar.gz` archive of the
        /// resulting bundle directory and remove the staging directory.
        #[arg(long)]
        tar: bool,
    },
    /// Verify bundle integrity against its manifest.
    Verify {
        #[arg(long)]
        bundle: PathBuf,
    },
    /// Print summary information about a bundle.
    Info {
        #[arg(long)]
        bundle: PathBuf,
    },
    /// Compute a tile-level diff between two bundles. Writes a TOML
    /// manifest of added / changed / removed files that `apply` can use
    /// to bring `--old` up to `--new` without re-downloading the whole
    /// bundle.
    Diff {
        #[arg(long)]
        old: PathBuf,
        #[arg(long)]
        new: PathBuf,
        #[arg(long)]
        out: PathBuf,
    },
    /// Apply a previously-computed diff to a target bundle, pulling new /
    /// changed files from `--source` (a copy of the new bundle, possibly
    /// remote-mounted) and deleting removed ones.
    Apply {
        #[arg(long)]
        bundle: PathBuf,
        #[arg(long)]
        diff: PathBuf,
        #[arg(long)]
        source: PathBuf,
    },
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    match cli.command {
        Command::Build {
            osm,
            wof,
            oa,
            geonames,
            out,
            bundle_id,
            zstd,
        } => cmd_build(BuildArgs {
            osm,
            wof,
            oa,
            geonames,
            out,
            bundle_id,
            compression: if zstd {
                TileCompression::Zstd
            } else {
                TileCompression::None
            },
        }),
        Command::Extract {
            bundle,
            bbox,
            out,
            tar,
        } => cmd_extract(&bundle, &bbox, &out, tar),
        Command::Verify { bundle } => cmd_verify(&bundle),
        Command::Info { bundle } => cmd_info(&bundle),
        Command::Diff { old, new, out } => cmd_diff(&old, &new, &out),
        Command::Apply {
            bundle,
            diff,
            source,
        } => cmd_apply(&bundle, &diff, &source),
    }
}

struct BuildArgs {
    osm: Option<PathBuf>,
    wof: Option<PathBuf>,
    oa: Option<PathBuf>,
    geonames: Option<PathBuf>,
    out: PathBuf,
    bundle_id: String,
    compression: TileCompression,
}

fn cmd_build(args: BuildArgs) -> Result<()> {
    std::fs::create_dir_all(&args.out)
        .with_context(|| format!("creating bundle dir {}", args.out.display()))?;

    let mut places: Vec<Place> = Vec::new();
    let mut sources: Vec<SourceVersion> = Vec::new();

    let mut admin_layer: Option<cairn_spatial::AdminLayer> = None;
    if let Some(osm_path) = args.osm.as_ref() {
        tracing::info!(path = %osm_path.display(), "ingesting OSM PBF");
        let imported = cairn_import_osm::import(osm_path)
            .with_context(|| format!("OSM import failed: {}", osm_path.display()))?;
        tracing::info!(
            places = imported.places.len(),
            polygons = imported.admin_layer.features.len(),
            "OSM imported"
        );
        places.extend(imported.places);
        if !imported.admin_layer.features.is_empty() {
            admin_layer = Some(imported.admin_layer);
        }
        sources.push(SourceVersion {
            name: "osm".into(),
            version: osm_path.display().to_string(),
            blake3: hash_file(osm_path)?,
        });
    }

    if let Some(wof_path) = args.wof.as_ref() {
        tracing::info!(path = %wof_path.display(), "ingesting WhosOnFirst SQLite");
        let imported = cairn_import_wof::import(wof_path)
            .with_context(|| format!("WoF import failed: {}", wof_path.display()))?;
        tracing::info!(
            count = imported.places.len(),
            polygons = imported.admin_layer.features.len(),
            "WoF imported"
        );
        places.extend(imported.places);
        admin_layer = match admin_layer.take() {
            Some(mut existing) => {
                existing.features.extend(imported.admin_layer.features);
                Some(existing)
            }
            None => Some(imported.admin_layer),
        };
        sources.push(SourceVersion {
            name: "wof".into(),
            version: wof_path.display().to_string(),
            blake3: hash_file(wof_path)?,
        });
    }

    if let Some(oa_path) = args.oa.as_ref() {
        tracing::info!(path = %oa_path.display(), "ingesting OpenAddresses CSV");
        let imported = cairn_import_oa::import(oa_path)
            .with_context(|| format!("OpenAddresses import failed: {}", oa_path.display()))?;
        tracing::info!(count = imported.len(), "OA places imported");
        places.extend(imported);
        sources.push(SourceVersion {
            name: "openaddresses".into(),
            version: oa_path.display().to_string(),
            blake3: hash_file(oa_path)?,
        });
    }

    if let Some(geonames_path) = args.geonames.as_ref() {
        tracing::info!(path = %geonames_path.display(), "ingesting Geonames TSV");
        let imported = cairn_import_geonames::import(geonames_path)
            .with_context(|| format!("Geonames import failed: {}", geonames_path.display()))?;
        tracing::info!(count = imported.len(), "Geonames places imported");
        places.extend(imported);
        sources.push(SourceVersion {
            name: "geonames".into(),
            version: geonames_path.display().to_string(),
            blake3: hash_file(geonames_path)?,
        });
    }

    // Build the text index from the full place set first; tile bucketing
    // consumes the vec afterwards.
    let text_dir = args.out.join("index/text");
    let docs = cairn_text::build_index(&text_dir, places.iter().cloned())
        .with_context(|| format!("building text index at {}", text_dir.display()))?;
    tracing::info!(docs, path = %text_dir.display(), "text index written");

    // Bucket per-level using each Place's PlaceId-recorded level so admin,
    // city, and street/POI rows land in their natural tier.
    let mut by_level: HashMap<u8, Vec<Place>> = HashMap::new();
    for p in places.iter() {
        by_level.entry(p.id.level()).or_default().push(p.clone());
    }
    let mut buckets: HashMap<TileCoord, Vec<Place>> = HashMap::new();
    for (level_u8, level_places) in by_level {
        let level = Level::from_u8(level_u8).unwrap_or(Level::L1);
        for (coord, group) in bucket_places(level, level_places) {
            buckets.entry(coord).or_default().extend(group);
        }
    }
    tracing::info!(tile_count = buckets.len(), "bucketed places per-level");

    let mut entries: Vec<TileEntry> = Vec::new();
    let sorted: BTreeMap<(u8, u32), (TileCoord, Vec<Place>)> = buckets
        .into_iter()
        .map(|(coord, places)| ((coord.level.as_u8(), coord.id()), (coord, places)))
        .collect();

    for (_key, (coord, tile_places)) in sorted {
        let path = args.out.join(coord.relative_path());
        let count = tile_places.len() as u32;
        let (hash, size) = write_tile(&path, &tile_places, args.compression)?;
        entries.push(TileEntry {
            level: coord.level.as_u8(),
            tile_id: coord.id(),
            blake3: hash,
            byte_size: size,
            place_count: count,
            compression: args.compression,
        });
    }

    let mut admin_tile_entries: Vec<cairn_tile::SpatialTileEntry> = Vec::new();
    if let Some(mut layer) = admin_layer {
        let before = layer.features.len();
        layer.features = cairn_spatial::dedupe_features(layer.features);
        let after = layer.features.len();
        if before != after {
            tracing::info!(
                before,
                after,
                dropped = before - after,
                "admin layer deduplicated across sources"
            );
        }
        admin_tile_entries = cairn_spatial::write_admin_partitioned(&args.out, &layer)
            .with_context(|| {
                format!("writing partitioned admin layer to {}", args.out.display())
            })?;
        let total_bytes: u64 = admin_tile_entries.iter().map(|e| e.byte_size).sum();
        let total_features: u64 = admin_tile_entries.iter().map(|e| e.item_count).sum();
        tracing::info!(
            tiles = admin_tile_entries.len(),
            total_features,
            total_bytes,
            "admin layer written (partitioned)"
        );
    }

    let point_layer = PointLayer {
        points: places
            .iter()
            .map(|p| {
                let default_name = p
                    .names
                    .iter()
                    .find(|n| n.lang == "default")
                    .or_else(|| p.names.first())
                    .map(|n| n.value.clone())
                    .unwrap_or_default();
                PlacePoint {
                    place_id: p.id.0,
                    level: p.id.level(),
                    kind: cairn_text::kind_str(p.kind).to_string(),
                    name: default_name,
                    centroid: p.centroid,
                    admin_path: p.admin_path.iter().map(|a| a.0).collect(),
                }
            })
            .collect(),
    };
    let point_tile_entries = cairn_spatial::write_points_partitioned(&args.out, &point_layer)
        .with_context(|| format!("writing partitioned point layer to {}", args.out.display()))?;
    let total_point_bytes: u64 = point_tile_entries.iter().map(|e| e.byte_size).sum();
    let total_points: u64 = point_tile_entries.iter().map(|e| e.item_count).sum();
    tracing::info!(
        tiles = point_tile_entries.len(),
        total_points,
        total_bytes = total_point_bytes,
        "point layer written (partitioned)"
    );

    let manifest = Manifest {
        schema_version: 2,
        built_at: now_iso8601(),
        bundle_id: args.bundle_id,
        sources,
        tiles: entries,
        admin_tiles: admin_tile_entries,
        point_tiles: point_tile_entries,
    };
    let manifest_path = args.out.join("manifest.toml");
    write_manifest(&manifest_path, &manifest)?;
    tracing::info!(
        path = %manifest_path.display(),
        tiles = manifest.tiles.len(),
        "manifest written"
    );

    Ok(())
}

fn cmd_extract(bundle: &Path, bbox_arg: &[f64], out: &Path, write_tar: bool) -> Result<()> {
    if bbox_arg.len() != 4 {
        anyhow::bail!("--bbox needs 4 values: MIN_LON MIN_LAT MAX_LON MAX_LAT");
    }
    let q = (bbox_arg[0], bbox_arg[1], bbox_arg[2], bbox_arg[3]);
    if q.0 > q.2 || q.1 > q.3 {
        anyhow::bail!("bbox min must be <= max in both dimensions");
    }

    let manifest_path = bundle.join("manifest.toml");
    let src_manifest = read_manifest(&manifest_path)?;
    tracing::info!(
        src_tiles = src_manifest.tiles.len(),
        bbox = ?q,
        "starting bbox extract"
    );

    std::fs::create_dir_all(out).with_context(|| format!("creating {}", out.display()))?;

    // Tile copy: anything whose tile bbox intersects the query.
    let mut new_tiles: Vec<TileEntry> = Vec::new();
    let mut tile_count = 0u64;
    let mut tile_bytes_total = 0u64;
    for entry in &src_manifest.tiles {
        let level = Level::from_u8(entry.level)
            .ok_or_else(|| anyhow::anyhow!("unknown level {}", entry.level))?;
        let coord = TileCoord::from_id(level, entry.tile_id);
        if !bbox_intersects(coord.bbox(), q) {
            continue;
        }
        let rel = coord.relative_path();
        let src = bundle.join(&rel);
        let dst = out.join(&rel);
        if let Some(parent) = dst.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::copy(&src, &dst)
            .with_context(|| format!("copy {} → {}", src.display(), dst.display()))?;
        // Verify hash matches the source manifest (cheap correctness gate).
        let actual = hash_file(&dst)?;
        if actual != entry.blake3 {
            anyhow::bail!(
                "blake3 mismatch on {} after copy ({} vs {})",
                dst.display(),
                actual,
                entry.blake3
            );
        }
        new_tiles.push(entry.clone());
        tile_count += 1;
        tile_bytes_total += entry.byte_size;
    }
    tracing::info!(tile_count, tile_bytes = tile_bytes_total, "tiles copied");

    // Admin tiles: copy any tile whose bbox intersects the query bbox.
    let kept_admin_tiles: Vec<cairn_tile::SpatialTileEntry> = src_manifest
        .admin_tiles
        .iter()
        .filter(|e| bbox_intersects((e.min_lon, e.min_lat, e.max_lon, e.max_lat), q))
        .cloned()
        .collect();
    for entry in &kept_admin_tiles {
        copy_relative_file(bundle, out, &entry.rel_path)?;
    }
    tracing::info!(tiles = kept_admin_tiles.len(), "admin tiles copied");

    // Point tiles: copy any tile whose bbox intersects the query bbox.
    // The PIP query at runtime can land in any tile that overlaps the
    // query, so a tile with a single point inside the query bbox still
    // needs all of that tile's points to be present.
    let kept_point_tiles: Vec<cairn_tile::SpatialTileEntry> = src_manifest
        .point_tiles
        .iter()
        .filter(|e| bbox_intersects((e.min_lon, e.min_lat, e.max_lon, e.max_lat), q))
        .cloned()
        .collect();
    for entry in &kept_point_tiles {
        copy_relative_file(bundle, out, &entry.rel_path)?;
    }
    tracing::info!(tiles = kept_point_tiles.len(), "point tiles copied");

    // Text index: copy wholesale. Filtering tantivy segments by bbox would
    // require rebuilding the index from kept Places — out of scope for this
    // extract pass. The unfiltered text index over a regional bundle is
    // still functional; oversized but correct.
    let text_src = bundle.join("index/text");
    if text_src.exists() {
        let text_dst = out.join("index/text");
        copy_dir_all(&text_src, &text_dst)?;
        tracing::info!(path = %text_dst.display(), "text index copied");
    }

    let new_manifest = Manifest {
        schema_version: src_manifest.schema_version,
        built_at: now_iso8601(),
        bundle_id: format!("{}-extract", src_manifest.bundle_id),
        sources: src_manifest.sources.clone(),
        tiles: new_tiles,
        admin_tiles: kept_admin_tiles,
        point_tiles: kept_point_tiles,
    };
    let dst_manifest = out.join("manifest.toml");
    write_manifest(&dst_manifest, &new_manifest)?;
    tracing::info!(
        path = %dst_manifest.display(),
        tiles = new_manifest.tiles.len(),
        "extract manifest written"
    );

    if write_tar {
        let archive_path = out.with_extension("tar.gz");
        let bytes = write_tar_gz(out, &archive_path)
            .with_context(|| format!("writing tar archive {}", archive_path.display()))?;
        std::fs::remove_dir_all(out)
            .with_context(|| format!("removing staging dir {}", out.display()))?;
        tracing::info!(
            path = %archive_path.display(),
            bytes,
            "tar.gz archive written; staging directory removed"
        );
        println!(
            "OK: extracted {} tiles → {} ({:.1} MB)",
            new_manifest.tiles.len(),
            archive_path.display(),
            bytes as f64 / 1_048_576.0,
        );
    } else {
        println!(
            "OK: extracted {} tiles to {}",
            new_manifest.tiles.len(),
            out.display()
        );
    }
    Ok(())
}

/// Tar + gzip the given directory tree.
fn write_tar_gz(src_dir: &Path, dst: &Path) -> Result<u64> {
    use flate2::{write::GzEncoder, Compression};
    if let Some(parent) = dst.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let f = std::fs::File::create(dst)?;
    let gz = GzEncoder::new(f, Compression::default());
    let mut tar = tar::Builder::new(gz);
    let inner = src_dir
        .file_name()
        .ok_or_else(|| anyhow::anyhow!("source has no file name"))?;
    tar.append_dir_all(inner, src_dir)?;
    tar.finish()?;
    let len = std::fs::metadata(dst)?.len();
    Ok(len)
}

// =====================================================================
// Differential tile updates
// =====================================================================

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct DiffManifest {
    schema_version: u32,
    old_bundle_id: String,
    new_bundle_id: String,
    #[serde(default)]
    changed: Vec<DiffEntry>,
    #[serde(default)]
    added: Vec<DiffEntry>,
    #[serde(default)]
    removed: Vec<DiffEntry>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct DiffEntry {
    rel_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    old_blake3: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    new_blake3: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    byte_size: Option<u64>,
}

fn cmd_diff(old: &Path, new: &Path, out: &Path) -> Result<()> {
    let old_manifest = read_manifest(&old.join("manifest.toml"))
        .with_context(|| format!("reading old manifest at {}", old.display()))?;
    let new_manifest = read_manifest(&new.join("manifest.toml"))
        .with_context(|| format!("reading new manifest at {}", new.display()))?;

    let old_index: HashMap<String, FileSig> = collect_files(&old_manifest);
    let new_index: HashMap<String, FileSig> = collect_files(&new_manifest);

    let mut changed = Vec::new();
    let mut added = Vec::new();
    let mut removed = Vec::new();

    for (rel, new_sig) in &new_index {
        match old_index.get(rel) {
            Some(old_sig) if old_sig.blake3 == new_sig.blake3 => {}
            Some(old_sig) => changed.push(DiffEntry {
                rel_path: rel.clone(),
                old_blake3: Some(old_sig.blake3.clone()),
                new_blake3: Some(new_sig.blake3.clone()),
                byte_size: Some(new_sig.byte_size),
            }),
            None => added.push(DiffEntry {
                rel_path: rel.clone(),
                old_blake3: None,
                new_blake3: Some(new_sig.blake3.clone()),
                byte_size: Some(new_sig.byte_size),
            }),
        }
    }
    for (rel, old_sig) in &old_index {
        if !new_index.contains_key(rel) {
            removed.push(DiffEntry {
                rel_path: rel.clone(),
                old_blake3: Some(old_sig.blake3.clone()),
                new_blake3: None,
                byte_size: None,
            });
        }
    }

    changed.sort_by(|a, b| a.rel_path.cmp(&b.rel_path));
    added.sort_by(|a, b| a.rel_path.cmp(&b.rel_path));
    removed.sort_by(|a, b| a.rel_path.cmp(&b.rel_path));

    let diff = DiffManifest {
        schema_version: 1,
        old_bundle_id: old_manifest.bundle_id,
        new_bundle_id: new_manifest.bundle_id,
        changed,
        added,
        removed,
    };

    if let Some(parent) = out.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let toml = toml::to_string_pretty(&diff).context("encoding diff manifest")?;
    std::fs::write(out, toml)
        .with_context(|| format!("writing diff manifest to {}", out.display()))?;

    println!(
        "OK: diff {} → {}: changed={} added={} removed={} → {}",
        diff.old_bundle_id,
        diff.new_bundle_id,
        diff.changed.len(),
        diff.added.len(),
        diff.removed.len(),
        out.display()
    );
    Ok(())
}

fn cmd_apply(bundle: &Path, diff_path: &Path, source: &Path) -> Result<()> {
    let raw = std::fs::read_to_string(diff_path)
        .with_context(|| format!("reading diff manifest at {}", diff_path.display()))?;
    let diff: DiffManifest = toml::from_str(&raw)
        .with_context(|| format!("parsing diff manifest at {}", diff_path.display()))?;

    tracing::info!(
        old = %diff.old_bundle_id,
        new = %diff.new_bundle_id,
        changed = diff.changed.len(),
        added = diff.added.len(),
        removed = diff.removed.len(),
        "applying diff"
    );

    // 1. Copy added + changed files from `source` and verify their blake3
    //    against the diff manifest before overwriting the live bundle.
    for entry in diff.changed.iter().chain(diff.added.iter()) {
        let src = source.join(&entry.rel_path);
        let dst = bundle.join(&entry.rel_path);
        if let Some(parent) = dst.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::copy(&src, &dst)
            .with_context(|| format!("copy {} → {}", src.display(), dst.display()))?;
        if let Some(expected) = entry.new_blake3.as_deref() {
            let actual = hash_file(&dst)?;
            if actual != expected {
                anyhow::bail!(
                    "blake3 mismatch on {} after copy ({} vs {})",
                    dst.display(),
                    actual,
                    expected
                );
            }
        }
    }

    // 2. Remove deleted files. Best-effort — a missing file is fine.
    for entry in &diff.removed {
        let dst = bundle.join(&entry.rel_path);
        if dst.exists() {
            std::fs::remove_file(&dst).with_context(|| format!("removing {}", dst.display()))?;
        }
    }

    // 3. Replace the manifest itself wholesale from source so the bundle
    //    converges to the new schema state. The diff carried just the
    //    file-level deltas; we trust source's manifest.toml as truth.
    std::fs::copy(source.join("manifest.toml"), bundle.join("manifest.toml"))
        .context("copying new manifest")?;

    println!(
        "OK: applied diff {} → {} ({} files updated, {} removed)",
        diff.old_bundle_id,
        diff.new_bundle_id,
        diff.changed.len() + diff.added.len(),
        diff.removed.len()
    );
    Ok(())
}

#[derive(Clone)]
struct FileSig {
    blake3: String,
    byte_size: u64,
}

/// Collect every file in a manifest as `(rel_path, FileSig)`. Tile blobs
/// live under `tiles/`, spatial files under `spatial/`. The text index
/// directory isn't blake3-anchored in the manifest yet, so it's skipped.
fn collect_files(manifest: &Manifest) -> HashMap<String, FileSig> {
    let mut out = HashMap::new();
    for t in &manifest.tiles {
        let coord = TileCoord::from_id(
            cairn_tile::Level::from_u8(t.level).unwrap_or(cairn_tile::Level::L0),
            t.tile_id,
        );
        out.insert(
            coord.relative_path(),
            FileSig {
                blake3: t.blake3.clone(),
                byte_size: t.byte_size,
            },
        );
    }
    for e in &manifest.admin_tiles {
        out.insert(
            e.rel_path.clone(),
            FileSig {
                blake3: e.blake3.clone(),
                byte_size: e.byte_size,
            },
        );
    }
    for e in &manifest.point_tiles {
        out.insert(
            e.rel_path.clone(),
            FileSig {
                blake3: e.blake3.clone(),
                byte_size: e.byte_size,
            },
        );
    }
    out
}

fn copy_relative_file(src_root: &Path, dst_root: &Path, rel_path: &str) -> Result<()> {
    let src = src_root.join(rel_path);
    let dst = dst_root.join(rel_path);
    if let Some(parent) = dst.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::copy(&src, &dst)
        .with_context(|| format!("copy {} → {}", src.display(), dst.display()))?;
    Ok(())
}

fn copy_dir_all(src: &Path, dst: &Path) -> Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let ty = entry.file_type()?;
        let path = entry.path();
        let target = dst.join(entry.file_name());
        if ty.is_dir() {
            copy_dir_all(&path, &target)?;
        } else {
            std::fs::copy(&path, &target)?;
        }
    }
    Ok(())
}

fn cmd_verify(bundle: &Path) -> Result<()> {
    let report = verify_bundle(bundle)
        .with_context(|| format!("verifying bundle at {}", bundle.display()))?;
    tracing::info!(
        manifest = %report.manifest_path,
        tiles_checked = report.tiles_checked,
        failures = report.failures.len(),
        "tile verify done"
    );
    if !report.ok() {
        for f in &report.failures {
            tracing::error!(
                path = %f.path,
                expected = %f.expected,
                actual = %f.actual,
                "blake3 mismatch"
            );
        }
        anyhow::bail!("{} tiles failed integrity check", report.failures.len());
    }

    let manifest = read_manifest(&bundle.join("manifest.toml"))?;

    // Text index: openable check (no manifest hash yet because tantivy is a
    // multi-file directory; coverage upgrade lands when we walk segments).
    let text_dir = bundle.join("index/text");
    let text_status = if text_dir.exists() {
        match cairn_text::TextIndex::open(&text_dir) {
            Ok(_) => "ok",
            Err(err) => {
                tracing::error!(?err, path = %text_dir.display(), "text index broken");
                anyhow::bail!("text index at {} failed to open", text_dir.display());
            }
        }
    } else {
        "missing"
    };

    let admin_status = verify_spatial_tiles(bundle, &manifest.admin_tiles, "admin")?;
    let points_status = verify_spatial_tiles(bundle, &manifest.point_tiles, "points")?;

    println!(
        "OK: {} tiles verified, text={}, admin={}, points={} at {}",
        report.tiles_checked, text_status, admin_status, points_status, report.manifest_path
    );
    Ok(())
}

/// Recompute blake3 over each per-tile spatial file and compare to the
/// manifest. Bails on the first mismatch.
fn verify_spatial_tiles(
    bundle: &Path,
    entries: &[cairn_tile::SpatialTileEntry],
    label: &str,
) -> Result<&'static str> {
    if entries.is_empty() {
        return Ok("none");
    }
    for entry in entries {
        let abs = bundle.join(&entry.rel_path);
        let actual = hash_file(&abs)?;
        if actual != entry.blake3 {
            tracing::error!(
                path = %abs.display(),
                expected = %entry.blake3,
                actual = %actual,
                label,
                "blake3 mismatch on spatial tile"
            );
            anyhow::bail!("{} tile blake3 mismatch at {}", label, abs.display());
        }
    }
    Ok("ok")
}

fn cmd_info(bundle: &Path) -> Result<()> {
    let manifest_path = bundle.join("manifest.toml");
    let manifest = read_manifest(&manifest_path)?;
    let total_places: u64 = manifest.tiles.iter().map(|t| t.place_count as u64).sum();
    let total_bytes: u64 = manifest.tiles.iter().map(|t| t.byte_size).sum();
    println!("bundle_id      = {}", manifest.bundle_id);
    println!("built_at       = {}", manifest.built_at);
    println!("schema_version = {}", manifest.schema_version);
    println!("tiles          = {}", manifest.tiles.len());
    println!("places         = {}", total_places);
    println!("tile bytes     = {}", total_bytes);
    println!("sources:");
    for s in &manifest.sources {
        println!("  - {} :: {}", s.name, s.version);
    }
    Ok(())
}

fn hash_file(path: &Path) -> Result<String> {
    let mut hasher = blake3::Hasher::new();
    let mut f = std::fs::File::open(path).with_context(|| format!("opening {}", path.display()))?;
    std::io::copy(&mut f, &mut hasher)?;
    Ok(hasher.finalize().to_hex().to_string())
}

fn now_iso8601() -> String {
    let secs = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    format!("epoch:{}", secs)
}
