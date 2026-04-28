//! `cairn-build` — offline bundle builder.
//!
//! Phase 1: read OSM PBF → bucket places into tiles → write `.bin` blobs +
//! `manifest.toml`. WhosOnFirst, OpenAddresses, Geonames land in later
//! phases.

use anyhow::{Context, Result};
use cairn_place::Place;
use cairn_spatial::{AdminLayer, PlacePoint, PointLayer};
use cairn_tile::{
    bbox_contains, bbox_intersects, bucket_places, read_manifest, verify_bundle, write_manifest,
    write_tile, Level, Manifest, SourceVersion, TileCompression, TileCoord, TileEntry,
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

    let mut admin_artifact: Option<cairn_tile::ArtifactEntry> = None;
    if let Some(layer) = admin_layer {
        let admin_path = args.out.join("spatial/admin.bin");
        let bytes = layer
            .write_to(&admin_path)
            .with_context(|| format!("writing admin layer to {}", admin_path.display()))?;
        let blake = hash_file(&admin_path)?;
        tracing::info!(
            features = layer.features.len(),
            bytes,
            blake3 = %blake,
            path = %admin_path.display(),
            "admin layer written"
        );
        admin_artifact = Some(cairn_tile::ArtifactEntry {
            blake3: blake,
            byte_size: bytes,
            item_count: layer.features.len() as u64,
        });
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
    let points_path = args.out.join("spatial/points.bin");
    let bytes = point_layer
        .write_to(&points_path)
        .with_context(|| format!("writing point layer to {}", points_path.display()))?;
    let points_blake = hash_file(&points_path)?;
    tracing::info!(
        points = point_layer.points.len(),
        bytes,
        blake3 = %points_blake,
        path = %points_path.display(),
        "point layer written"
    );
    let points_artifact = cairn_tile::ArtifactEntry {
        blake3: points_blake,
        byte_size: bytes,
        item_count: point_layer.points.len() as u64,
    };

    let manifest = Manifest {
        schema_version: 1,
        built_at: now_iso8601(),
        bundle_id: args.bundle_id,
        sources,
        tiles: entries,
        admin: admin_artifact,
        points: Some(points_artifact),
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

    // Admin layer: filter by polygon bbox intersect.
    let admin_path_src = bundle.join("spatial/admin.bin");
    let admin_artifact = if admin_path_src.exists() {
        let layer = AdminLayer::read_from(&admin_path_src)?;
        let kept: Vec<_> = layer
            .features
            .into_iter()
            .filter(|f| {
                f.bbox()
                    .map(|r| bbox_intersects((r.min().x, r.min().y, r.max().x, r.max().y), q))
                    .unwrap_or(false)
            })
            .collect();
        tracing::info!(features = kept.len(), "admin features kept");
        let dst = out.join("spatial/admin.bin");
        let new_layer = AdminLayer { features: kept };
        let bytes = new_layer.write_to(&dst)?;
        let blake = hash_file(&dst)?;
        Some(cairn_tile::ArtifactEntry {
            blake3: blake,
            byte_size: bytes,
            item_count: new_layer.features.len() as u64,
        })
    } else {
        None
    };

    // Point layer: filter centroids inside the bbox.
    let points_path_src = bundle.join("spatial/points.bin");
    let points_artifact = if points_path_src.exists() {
        let layer = PointLayer::read_from(&points_path_src)?;
        let kept: Vec<PlacePoint> = layer
            .points
            .into_iter()
            .filter(|p| bbox_contains(q, p.centroid.lon, p.centroid.lat))
            .collect();
        tracing::info!(points = kept.len(), "points kept");
        let dst = out.join("spatial/points.bin");
        let new_layer = PointLayer { points: kept };
        let bytes = new_layer.write_to(&dst)?;
        let blake = hash_file(&dst)?;
        Some(cairn_tile::ArtifactEntry {
            blake3: blake,
            byte_size: bytes,
            item_count: new_layer.points.len() as u64,
        })
    } else {
        None
    };

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
        admin: admin_artifact,
        points: points_artifact,
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

    let admin_path = bundle.join("spatial/admin.bin");
    let admin_status = verify_artifact(&admin_path, manifest.admin.as_ref(), "admin")?;
    if admin_path.exists() {
        let layer = cairn_spatial::AdminLayer::read_from(&admin_path)
            .with_context(|| format!("parsing admin.bin at {}", admin_path.display()))?;
        tracing::info!(features = layer.features.len(), "admin layer parsed");
    }

    let points_path = bundle.join("spatial/points.bin");
    let points_status = verify_artifact(&points_path, manifest.points.as_ref(), "points")?;
    if points_path.exists() {
        let layer = cairn_spatial::PointLayer::read_from(&points_path)
            .with_context(|| format!("parsing points.bin at {}", points_path.display()))?;
        tracing::info!(points = layer.points.len(), "point layer parsed");
    }

    println!(
        "OK: {} tiles verified, text={}, admin={}, points={} at {}",
        report.tiles_checked, text_status, admin_status, points_status, report.manifest_path
    );
    Ok(())
}

fn verify_artifact(
    path: &Path,
    entry: Option<&cairn_tile::ArtifactEntry>,
    label: &str,
) -> Result<&'static str> {
    if !path.exists() {
        return Ok("missing");
    }
    let actual = hash_file(path)?;
    if let Some(e) = entry {
        if actual != e.blake3 {
            tracing::error!(
                path = %path.display(),
                expected = %e.blake3,
                actual = %actual,
                label,
                "blake3 mismatch on artifact"
            );
            anyhow::bail!("{} blake3 mismatch at {}", label, path.display());
        }
        Ok("ok")
    } else {
        // Artifact present on disk but not in manifest — older bundle or
        // out-of-band copy. Still useful, but flag.
        tracing::warn!(
            path = %path.display(),
            label,
            "artifact on disk but missing from manifest"
        );
        Ok("present-no-manifest-entry")
    }
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
