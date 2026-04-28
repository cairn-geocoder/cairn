//! `cairn-build` — offline bundle builder.
//!
//! Phase 1: read OSM PBF → bucket places into tiles → write `.bin` blobs +
//! `manifest.toml`. WhosOnFirst, OpenAddresses, Geonames land in later
//! phases.

use anyhow::{Context, Result};
use cairn_place::Place;
use cairn_spatial::{PlacePoint, PointLayer};
use cairn_tile::{
    bucket_places, read_manifest, verify_bundle, write_manifest, write_tile, Level, Manifest,
    SourceVersion, TileCoord, TileEntry,
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
    },
    /// Extract a regional bundle from an existing planet bundle.
    Extract {
        #[arg(long)]
        bundle: PathBuf,
        #[arg(long, num_args = 4, value_names = ["MIN_LON", "MIN_LAT", "MAX_LON", "MAX_LAT"])]
        bbox: Vec<f64>,
        #[arg(long)]
        out: PathBuf,
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
        } => cmd_build(BuildArgs {
            osm,
            wof,
            oa,
            geonames,
            out,
            bundle_id,
        }),
        Command::Extract { bundle, out, .. } => {
            tracing::warn!(
                src = %bundle.display(),
                dst = %out.display(),
                "extract not implemented in Phase 1"
            );
            Ok(())
        }
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
}

fn cmd_build(args: BuildArgs) -> Result<()> {
    std::fs::create_dir_all(&args.out)
        .with_context(|| format!("creating bundle dir {}", args.out.display()))?;

    let mut places: Vec<Place> = Vec::new();
    let mut sources: Vec<SourceVersion> = Vec::new();

    if let Some(osm_path) = args.osm.as_ref() {
        tracing::info!(path = %osm_path.display(), "ingesting OSM PBF");
        let imported = cairn_import_osm::import(osm_path)
            .with_context(|| format!("OSM import failed: {}", osm_path.display()))?;
        tracing::info!(count = imported.len(), "OSM places imported");
        places.extend(imported);
        sources.push(SourceVersion {
            name: "osm".into(),
            version: osm_path.display().to_string(),
            blake3: hash_file(osm_path)?,
        });
    }

    let mut admin_layer: Option<cairn_spatial::AdminLayer> = None;
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
        admin_layer = Some(imported.admin_layer);
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

    if args.geonames.is_some() {
        tracing::warn!("Geonames importer is still a stub");
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
        let (hash, size) = write_tile(&path, &tile_places)?;
        entries.push(TileEntry {
            level: coord.level.as_u8(),
            tile_id: coord.id(),
            blake3: hash,
            byte_size: size,
            place_count: count,
        });
    }

    if let Some(layer) = admin_layer {
        let admin_path = args.out.join("spatial/admin.bin");
        let bytes = layer
            .write_to(&admin_path)
            .with_context(|| format!("writing admin layer to {}", admin_path.display()))?;
        tracing::info!(
            features = layer.features.len(),
            bytes,
            path = %admin_path.display(),
            "admin layer written"
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
    let points_path = args.out.join("spatial/points.bin");
    let bytes = point_layer
        .write_to(&points_path)
        .with_context(|| format!("writing point layer to {}", points_path.display()))?;
    tracing::info!(
        points = point_layer.points.len(),
        bytes,
        path = %points_path.display(),
        "point layer written"
    );

    let manifest = Manifest {
        schema_version: 1,
        built_at: now_iso8601(),
        bundle_id: args.bundle_id,
        sources,
        tiles: entries,
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

    // Optional layers: text index + admin polygons + nearest-fallback points.
    // Each is verified by attempting to open / parse the artifact. Missing
    // artifacts are warnings, not failures.
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
    let admin_status = if admin_path.exists() {
        match cairn_spatial::AdminLayer::read_from(&admin_path) {
            Ok(layer) => {
                tracing::info!(features = layer.features.len(), "admin layer ok");
                "ok"
            }
            Err(err) => {
                tracing::error!(?err, path = %admin_path.display(), "admin layer broken");
                anyhow::bail!("admin.bin at {} failed to parse", admin_path.display());
            }
        }
    } else {
        "missing"
    };

    let points_path = bundle.join("spatial/points.bin");
    let points_status = if points_path.exists() {
        match cairn_spatial::PointLayer::read_from(&points_path) {
            Ok(layer) => {
                tracing::info!(points = layer.points.len(), "point layer ok");
                "ok"
            }
            Err(err) => {
                tracing::error!(?err, path = %points_path.display(), "point layer broken");
                anyhow::bail!("points.bin at {} failed to parse", points_path.display());
            }
        }
    } else {
        "missing"
    };

    println!(
        "OK: {} tiles verified, text={}, admin={}, points={} at {}",
        report.tiles_checked, text_status, admin_status, points_status, report.manifest_path
    );
    Ok(())
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
