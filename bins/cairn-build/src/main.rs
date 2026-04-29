//! `cairn-build` — offline bundle builder.
//!
//! Phase 1: read OSM PBF → bucket places into tiles → write `.bin` blobs +
//! `manifest.toml`. WhosOnFirst, OpenAddresses, Geonames land in later
//! phases.

use anyhow::{Context, Result};
mod osc;
mod replication;
mod sbom;
mod sign;
use cairn_place::Place;
use cairn_spatial::{PlacePoint, PointLayer};
use cairn_tile::{
    bbox_intersects, bucket_places, read_manifest, verify_bundle, write_manifest, write_tile,
    Level, Manifest, SourceVersion, TileCompression, TileCoord, TileEntry,
};
use clap::{Parser, Subcommand, ValueEnum};
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

/// Phase 6f: pluggable OSM node-coord cache strategies. Surfaced as
/// `cairn-build build --node-cache <…>`. `auto` resolves to a concrete
/// strategy at run time based on PBF size.
#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
enum NodeCacheArg {
    Auto,
    Inline,
    #[value(name = "sorted-vec")]
    SortedVec,
    Flatnode,
}

impl NodeCacheArg {
    /// Resolve `Auto` against the PBF size on disk. ≤ 5 GB uses
    /// inline, 5-30 GB sorted-vec, > 30 GB flatnode.
    fn resolve(
        self,
        pbf_path: &Path,
        flatnode_path: Option<&Path>,
    ) -> cairn_import_osm::NodeCacheStrategy {
        match self {
            NodeCacheArg::Inline => cairn_import_osm::NodeCacheStrategy::Inline,
            NodeCacheArg::SortedVec => cairn_import_osm::NodeCacheStrategy::SortedVec,
            NodeCacheArg::Flatnode => cairn_import_osm::NodeCacheStrategy::Flatnode {
                path: flatnode_path
                    .map(Path::to_path_buf)
                    .unwrap_or_else(|| default_flatnode_path(pbf_path)),
            },
            NodeCacheArg::Auto => {
                let size = std::fs::metadata(pbf_path).map(|m| m.len()).unwrap_or(0);
                const SORTED_VEC_THRESHOLD: u64 = 5 * 1024 * 1024 * 1024;
                const FLATNODE_THRESHOLD: u64 = 30 * 1024 * 1024 * 1024;
                if size > FLATNODE_THRESHOLD {
                    cairn_import_osm::NodeCacheStrategy::Flatnode {
                        path: flatnode_path
                            .map(Path::to_path_buf)
                            .unwrap_or_else(|| default_flatnode_path(pbf_path)),
                    }
                } else if size > SORTED_VEC_THRESHOLD {
                    cairn_import_osm::NodeCacheStrategy::SortedVec
                } else {
                    cairn_import_osm::NodeCacheStrategy::Inline
                }
            }
        }
    }
}

/// Where to drop the flatnode file when the operator didn't pick a
/// path. Sits next to the PBF as `<pbf-stem>.flatnode.bin`.
fn default_flatnode_path(pbf_path: &Path) -> PathBuf {
    let mut p = pbf_path.to_path_buf();
    if p.extension().is_some() {
        p.set_extension("flatnode.bin");
    } else {
        p.push("flatnode.bin");
    }
    p
}

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
        /// Geonames postcode dump path (`<CC>.txt` /
        /// `allCountries.txt` from
        /// `download.geonames.org/export/zip/`). Each row becomes a
        /// `Place(kind=Postcode)` so `?layer=postcode` /
        /// `?categories=postal` can filter to postcodes.
        #[arg(long)]
        postcodes: Option<PathBuf>,
        #[arg(long)]
        out: PathBuf,
        #[arg(long, default_value = "alpha-bundle")]
        bundle_id: String,
        /// Disable zstd compression of tile blobs. Compression is
        /// ON by default (bundle size drops ~50-70% with negligible
        /// read-time overhead — decompress runs once on tile load,
        /// reads are mmap'd after that). Use this only for
        /// differential debugging or when an embedded operator
        /// pipes blobs through their own compression layer.
        #[arg(long)]
        no_zstd: bool,
        /// Comma-separated source priority for cross-source dedup.
        /// Earlier in the list = higher trust. Tokens: osm, wof, oa,
        /// geonames. Default `wof,osm,oa,geonames` (admin polygons +
        /// parent chains from WoF preferred over OSM relations).
        #[arg(long, default_value = "wof,osm,oa,geonames")]
        source_priority: String,
        /// Douglas-Peucker simplification tolerance for admin polygons,
        /// in METERS. 0 disables. Reasonable values: 50-200m for
        /// admin boundaries; the user-visible difference is negligible
        /// while bundle size typically drops 30-60% on dense
        /// boundaries. Default 0 (off).
        #[arg(long, default_value_t = 0.0)]
        simplify_meters: f64,
        /// OSM node-coord cache strategy. `auto` (default) picks
        /// `inline` for inputs ≤ 5 GB, `sorted-vec` for 5–30 GB, and
        /// `flatnode` above 30 GB. `inline` keeps the legacy HashMap
        /// (fastest lookup, ~48 B/entry). `sorted-vec` packs i32-
        /// quantized coords into a binary-searchable Vec (~16 B/entry,
        /// 3× less RAM, lossless at OSM coord precision). `flatnode`
        /// writes a disk-backed mmap'd dense `[i32;2]` array indexed
        /// by node id; RSS stays bounded by the kernel's working set
        /// regardless of input size, at the cost of one extra
        /// max-id scan up front.
        #[arg(long, default_value = "auto")]
        node_cache: NodeCacheArg,
        /// Override where the flatnode file lands when
        /// `--node-cache flatnode` is in effect (or when `auto` picks
        /// flatnode). Defaults to `<pbf-stem>.flatnode.bin` next to
        /// the input PBF.
        #[arg(long)]
        flatnode_path: Option<PathBuf>,
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
    /// Decode a single tile and pretty-print its contents. Debugging
    /// aid for inspecting what a bundle actually shipped — kind
    /// histogram, place samples, optional grep against name.
    InspectTile {
        #[arg(long)]
        bundle: PathBuf,
        /// Tile coordinate as `LEVEL:TILE_ID` (e.g. `1:49509`).
        #[arg(long)]
        tile: String,
        /// Number of sample places to print. Default 10.
        #[arg(long, default_value_t = 10)]
        sample: usize,
        /// Filter samples to places whose default name contains this
        /// substring (case-insensitive).
        #[arg(long)]
        grep: Option<String>,
    },
    /// Decode a single admin spatial tile and pretty-print its
    /// AdminFeature list (place_id, kind, name, vertex count).
    InspectAdminTile {
        #[arg(long)]
        bundle: PathBuf,
        /// Tile coordinate as `LEVEL:TILE_ID`.
        #[arg(long)]
        tile: String,
    },
    /// Fetch new OSM minutely diff files into `<bundle>/replication/`
    /// and update `replication_state.toml`. Application of the diffs
    /// to tile blobs is a separate follow-up step; this command only
    /// stages them. Safe to run repeatedly.
    ReplicateFetch {
        #[arg(long)]
        bundle: PathBuf,
        /// Replication base URL, e.g.
        /// `https://planet.openstreetmap.org/replication/minute`.
        /// Required on first run; ignored afterwards (state file
        /// remembers it).
        #[arg(long)]
        upstream: Option<String>,
        /// Cap on number of diffs fetched per invocation. Default 60
        /// (~one hour of minutely updates). Stale bundles get caught
        /// up over multiple runs.
        #[arg(long, default_value_t = 60)]
        max: usize,
    },
    /// Print the current replication state for a bundle.
    ReplicateStatus {
        #[arg(long)]
        bundle: PathBuf,
    },
    /// Apply previously-fetched OSM minutely diffs to the bundle's
    /// place tiles. Walks `<bundle>/replication/*.osc.gz` from
    /// `last_applied_seq+1` up to `last_fetched_seq`, parses each
    /// diff, and rewrites every tile that any node-place op touched.
    /// Way / relation re-application is deferred (logged + skipped);
    /// run a full rebuild from PBF for those.
    ReplicateApply {
        #[arg(long)]
        bundle: PathBuf,
        /// Cap on number of diffs applied per invocation. Default 60.
        /// Lets a stale bundle catch up over multiple runs without
        /// holding the tile store locked for hours.
        #[arg(long, default_value_t = 60)]
        max: usize,
        /// Parse + bucket without rewriting any tile blobs. Prints
        /// the dirty-tile count + per-action histogram so operators
        /// can sanity-check before committing.
        #[arg(long)]
        dry_run: bool,
    },
    /// Generate a fresh ed25519 signing keypair into `<dir>/cairn.key`
    /// (secret, mode 0600) + `<dir>/cairn.pub` (public). Refuses to
    /// overwrite an existing key.
    Keygen {
        #[arg(long)]
        out: PathBuf,
    },
    /// Sign `<bundle>/manifest.toml` with the secret key. Writes the
    /// detached signature to `<bundle>/manifest.toml.sig`.
    Sign {
        #[arg(long)]
        bundle: PathBuf,
        #[arg(long)]
        key: PathBuf,
    },
    /// Verify `<bundle>/manifest.toml` against
    /// `<bundle>/manifest.toml.sig` using the public key. Exits
    /// non-zero on failure.
    SignVerify {
        #[arg(long)]
        bundle: PathBuf,
        #[arg(long, name = "pubkey")]
        pubkey: PathBuf,
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
            postcodes,
            out,
            bundle_id,
            no_zstd,
            source_priority,
            simplify_meters,
            node_cache,
            flatnode_path,
        } => cmd_build(BuildArgs {
            osm,
            wof,
            oa,
            geonames,
            postcodes,
            out,
            bundle_id,
            source_priority: parse_source_priority(&source_priority)?,
            simplify_tolerance_deg: meters_to_degrees(simplify_meters),
            // ZSTD is on by default. Pass `--no-zstd` to disable.
            compression: if no_zstd {
                TileCompression::None
            } else {
                TileCompression::Zstd
            },
            node_cache,
            flatnode_path,
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
        Command::InspectTile {
            bundle,
            tile,
            sample,
            grep,
        } => cmd_inspect_tile(&bundle, &tile, sample, grep.as_deref()),
        Command::InspectAdminTile { bundle, tile } => cmd_inspect_admin_tile(&bundle, &tile),
        Command::ReplicateFetch {
            bundle,
            upstream,
            max,
        } => cmd_replicate_fetch(&bundle, upstream.as_deref(), max),
        Command::ReplicateStatus { bundle } => cmd_replicate_status(&bundle),
        Command::ReplicateApply {
            bundle,
            max,
            dry_run,
        } => cmd_replicate_apply(&bundle, max, dry_run),
        Command::Keygen { out } => sign::cmd_keygen(&out),
        Command::Sign { bundle, key } => sign::cmd_sign(&bundle, &key).map(|_| ()),
        Command::SignVerify { bundle, pubkey } => sign::cmd_verify(&bundle, &pubkey),
    }
}

fn cmd_replicate_fetch(bundle: &Path, upstream_arg: Option<&str>, max: usize) -> Result<()> {
    if !bundle.exists() {
        anyhow::bail!("bundle does not exist: {}", bundle.display());
    }
    let mut state = match replication::read_state(bundle)? {
        Some(s) => {
            if let Some(new_url) = upstream_arg {
                if new_url != s.upstream {
                    tracing::info!(
                        old = %s.upstream,
                        new = new_url,
                        "upstream URL change recorded"
                    );
                    let mut s = s;
                    s.upstream = new_url.to_string();
                    s
                } else {
                    s
                }
            } else {
                s
            }
        }
        None => {
            let url = upstream_arg.ok_or_else(|| {
                anyhow::anyhow!(
                    "no replication state in this bundle yet — pass --upstream URL on the first run"
                )
            })?;
            replication::ReplicationState::new(url.to_string())
        }
    };
    let fetched = replication::fetch_pending(bundle, &mut state, max)?;
    replication::write_state(bundle, &state)?;
    println!(
        "OK: fetched {} diff(s); last_fetched_seq={:?}, last_applied_seq={:?}",
        fetched.len(),
        state.last_fetched_seq,
        state.last_applied_seq
    );
    Ok(())
}

fn cmd_replicate_status(bundle: &Path) -> Result<()> {
    match replication::read_state(bundle)? {
        Some(state) => {
            println!("upstream         = {}", state.upstream);
            println!(
                "last_fetched_seq = {}",
                state
                    .last_fetched_seq
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "(none)".into())
            );
            println!(
                "last_fetched_at  = {}",
                state.last_fetched_at.as_deref().unwrap_or("(none)")
            );
            println!(
                "last_applied_seq = {}",
                state
                    .last_applied_seq
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "(none)".into())
            );
            let lag = match (state.last_fetched_seq, state.last_applied_seq) {
                (Some(f), Some(a)) => f.saturating_sub(a),
                (Some(f), None) => f,
                _ => 0,
            };
            println!("apply_lag        = {lag} diff(s)");
        }
        None => println!("(no replication state — run replicate-fetch --upstream URL first)"),
    }
    Ok(())
}

/// Apply previously-fetched OSM minutely diffs to the bundle.
///
/// Pipeline:
///  1. Read replication state. Refuse to run without it.
///  2. Walk `replication/<seq>.osc.gz` from `last_applied_seq + 1`
///     up to `last_fetched_seq` (or until `max` is hit).
///  3. Parse each diff via [`osc::parse_file`] and tally
///     create/modify/delete totals per element kind.
///  4. Bucket node-with-lat/lon ops by their tile coord at every
///     level so operators can see which tiles need rebuilding.
///  5. Advance `last_applied_seq` to the highest seq processed
///     unless `--dry-run`.
///
/// Way / relation handling is logged + counted; actually rewriting
/// way / polygon-bearing tiles requires the original way-node graph,
/// which the bundle doesn't persist. Operators in that situation
/// should run a full `cairn-build build` from the latest PBF.
///
/// Tile-blob mutation for node-only ops is the next concrete
/// follow-up — the bucket map this command emits is exactly the set
/// of (level, tile_id) keys that need rewriting. The writer side is
/// already in `cairn-tile::write_tile`.
fn cmd_replicate_apply(bundle: &Path, max: usize, dry_run: bool) -> Result<()> {
    let mut state = match replication::read_state(bundle)? {
        Some(s) => s,
        None => {
            return Err(anyhow::anyhow!(
                "no replication state at {}/replication_state.toml — \
                 run `cairn-build replicate-fetch --upstream URL` first",
                bundle.display()
            ));
        }
    };
    let last_fetched = match state.last_fetched_seq {
        Some(s) => s,
        None => {
            tracing::info!("no diffs fetched yet; nothing to apply");
            return Ok(());
        }
    };
    let start = state
        .last_applied_seq
        .map(|s| s.saturating_add(1))
        .unwrap_or(0);
    if start > last_fetched {
        tracing::info!(
            last_applied = state.last_applied_seq,
            last_fetched,
            "apply state already at head"
        );
        return Ok(());
    }

    let mut totals = ApplyTotals::default();
    let mut dirty_tiles: std::collections::BTreeSet<(u8, u32)> = std::collections::BTreeSet::new();
    let mut last_processed: Option<u64> = None;

    for seq in start..=last_fetched {
        if totals.diffs_processed >= max {
            tracing::warn!(
                processed = totals.diffs_processed,
                next = seq,
                max,
                "hit --max cap; rerun replicate-apply to continue"
            );
            break;
        }
        let path = bundle
            .join(replication_dir())
            .join(format!("{seq:09}.osc.gz"));
        if !path.exists() {
            tracing::warn!(
                seq,
                path = %path.display(),
                "missing diff file; refetch to recover"
            );
            break;
        }
        let ops = osc::parse_file(&path)
            .with_context(|| format!("parse diff seq={seq} ({})", path.display()))?;
        totals.diffs_processed += 1;
        process_ops(&ops, &mut totals, &mut dirty_tiles);
        last_processed = Some(seq);
    }

    println!(
        "diffs processed = {}\n\
         total ops       = {}\n\
         node creates    = {}   modifies = {}   deletes = {}\n\
         way creates     = {}   modifies = {}   deletes = {}\n\
         relation creates = {}  modifies = {}   deletes = {}\n\
         taggable nodes  = {}\n\
         dirty tiles     = {}",
        totals.diffs_processed,
        totals.ops_total,
        totals.node_creates,
        totals.node_modifies,
        totals.node_deletes,
        totals.way_creates,
        totals.way_modifies,
        totals.way_deletes,
        totals.relation_creates,
        totals.relation_modifies,
        totals.relation_deletes,
        totals.taggable_nodes,
        dirty_tiles.len(),
    );

    if totals.way_creates + totals.way_modifies + totals.way_deletes > 0
        || totals.relation_creates + totals.relation_modifies + totals.relation_deletes > 0
    {
        println!(
            "note: way / relation re-application is not yet implemented;\n\
             run a full `cairn-build build` from the latest PBF to pick those up."
        );
    }

    if dry_run {
        println!("(dry-run: state file NOT updated)");
        return Ok(());
    }
    if let Some(applied) = last_processed {
        state.last_applied_seq = Some(applied);
        replication::write_state(bundle, &state)?;
        tracing::info!(last_applied_seq = applied, "replication state advanced");
    }
    Ok(())
}

#[derive(Default)]
struct ApplyTotals {
    diffs_processed: usize,
    ops_total: u64,
    node_creates: u64,
    node_modifies: u64,
    node_deletes: u64,
    way_creates: u64,
    way_modifies: u64,
    way_deletes: u64,
    relation_creates: u64,
    relation_modifies: u64,
    relation_deletes: u64,
    taggable_nodes: u64,
}

fn process_ops(
    ops: &[osc::DiffOp],
    totals: &mut ApplyTotals,
    dirty_tiles: &mut std::collections::BTreeSet<(u8, u32)>,
) {
    use osc::{Action, OsmKind};
    for op in ops {
        totals.ops_total += 1;
        match (op.kind, op.action) {
            (OsmKind::Node, Action::Create) => totals.node_creates += 1,
            (OsmKind::Node, Action::Modify) => totals.node_modifies += 1,
            (OsmKind::Node, Action::Delete) => totals.node_deletes += 1,
            (OsmKind::Way, Action::Create) => totals.way_creates += 1,
            (OsmKind::Way, Action::Modify) => totals.way_modifies += 1,
            (OsmKind::Way, Action::Delete) => totals.way_deletes += 1,
            (OsmKind::Relation, Action::Create) => totals.relation_creates += 1,
            (OsmKind::Relation, Action::Modify) => totals.relation_modifies += 1,
            (OsmKind::Relation, Action::Delete) => totals.relation_deletes += 1,
        }
        if op.kind == OsmKind::Node && op.looks_taggable() {
            totals.taggable_nodes += 1;
            if let (Some(lat), Some(lon)) = (op.lat, op.lon) {
                let coord = cairn_place::Coord { lon, lat };
                for level in [Level::L0, Level::L1, Level::L2] {
                    let tc = TileCoord::from_coord(level, coord);
                    dirty_tiles.insert((level.as_u8(), tc.id()));
                }
            }
        }
    }
}

fn replication_dir() -> &'static str {
    "replication"
}

fn parse_tile_arg(spec: &str) -> Result<(Level, u32)> {
    let (level_s, id_s) = spec
        .split_once(':')
        .ok_or_else(|| anyhow::anyhow!("--tile must be LEVEL:TILE_ID (got {spec:?})"))?;
    let level_u8: u8 = level_s
        .trim()
        .parse()
        .with_context(|| format!("parsing tile level from {level_s:?}"))?;
    let level =
        Level::from_u8(level_u8).ok_or_else(|| anyhow::anyhow!("unknown level {level_u8}"))?;
    let tile_id: u32 = id_s
        .trim()
        .parse()
        .with_context(|| format!("parsing tile id from {id_s:?}"))?;
    Ok((level, tile_id))
}

fn cmd_inspect_tile(bundle: &Path, spec: &str, sample: usize, grep: Option<&str>) -> Result<()> {
    let (level, tile_id) = parse_tile_arg(spec)?;
    let coord = TileCoord::from_id(level, tile_id);
    let path = bundle.join(coord.relative_path());
    if !path.exists() {
        anyhow::bail!("tile not present at {}", path.display());
    }
    let places = cairn_tile::read_tile(&path)
        .with_context(|| format!("decoding tile {}", path.display()))?;
    let mut kind_hist: std::collections::BTreeMap<&'static str, usize> =
        std::collections::BTreeMap::new();
    for p in &places {
        *kind_hist.entry(cairn_text::kind_str(p.kind)).or_insert(0) += 1;
    }
    let (min_lon, min_lat, max_lon, max_lat) = coord.bbox();
    println!("tile           = {}:{}", level.as_u8(), tile_id);
    println!("bbox           = lon[{min_lon:.4}..{max_lon:.4}], lat[{min_lat:.4}..{max_lat:.4}]");
    println!("place_count    = {}", places.len());
    println!("kinds:");
    for (k, n) in &kind_hist {
        println!("  {k:<14} {n}");
    }

    let needle = grep.map(|g| g.to_lowercase());
    let filtered: Vec<&cairn_place::Place> = places
        .iter()
        .filter(|p| {
            let Some(needle) = needle.as_deref() else {
                return true;
            };
            p.names
                .iter()
                .any(|n| n.value.to_lowercase().contains(needle))
        })
        .collect();
    let take = sample.min(filtered.len());
    println!("samples ({} of {} matching):", take, filtered.len());
    for p in filtered.iter().take(take) {
        let name = p
            .names
            .iter()
            .find(|n| n.lang == "default")
            .or_else(|| p.names.first())
            .map(|n| n.value.as_str())
            .unwrap_or("(no name)");
        println!(
            "  id={} kind={} ({:.5},{:.5}) name={:?} ap_len={}",
            p.id.0,
            cairn_text::kind_str(p.kind),
            p.centroid.lon,
            p.centroid.lat,
            name,
            p.admin_path.len(),
        );
    }
    Ok(())
}

fn cmd_inspect_admin_tile(bundle: &Path, spec: &str) -> Result<()> {
    let (level, tile_id) = parse_tile_arg(spec)?;
    let coord = TileCoord::from_id(level, tile_id);
    let manifest = read_manifest(&bundle.join("manifest.toml"))?;
    let entry = manifest
        .admin_tiles
        .iter()
        .find(|e| e.level == level.as_u8() && e.tile_id == tile_id)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "no admin tile {}:{} in manifest.toml",
                level.as_u8(),
                tile_id
            )
        })?;
    let abs = bundle.join(&entry.rel_path);
    let tile = cairn_spatial::archived::AdminTileArchive::from_path(&abs)
        .with_context(|| format!("opening admin tile {}", abs.display()))?;
    let layer = tile.archived();
    let (min_lon, min_lat, max_lon, max_lat) = coord.bbox();
    println!("tile           = {}:{}", level.as_u8(), tile_id);
    println!("bbox           = lon[{min_lon:.4}..{max_lon:.4}], lat[{min_lat:.4}..{max_lat:.4}]");
    println!("rel_path       = {}", entry.rel_path);
    println!("byte_size      = {}", entry.byte_size);
    println!("feature_count  = {}", layer.features.len());
    for feat in layer.features.iter() {
        let total_vertices: usize = feat
            .polygon_rings
            .iter()
            .flat_map(|p| p.iter())
            .map(|r| r.len())
            .sum();
        let polygons = feat.polygon_rings.len();
        println!(
            "  id={} kind={} name={:?} polygons={} vertices={} ap_len={}",
            feat.place_id,
            feat.kind.as_str(),
            feat.name.as_str(),
            polygons,
            total_vertices,
            feat.admin_path.len(),
        );
    }
    Ok(())
}

struct BuildArgs {
    osm: Option<PathBuf>,
    wof: Option<PathBuf>,
    oa: Option<PathBuf>,
    geonames: Option<PathBuf>,
    postcodes: Option<PathBuf>,
    out: PathBuf,
    bundle_id: String,
    compression: TileCompression,
    source_priority: Vec<cairn_place::SourceKind>,
    simplify_tolerance_deg: f64,
    node_cache: NodeCacheArg,
    flatnode_path: Option<PathBuf>,
}

/// Convert a simplification tolerance from meters into degrees of
/// lat/lon. Approximate enough — admin boundaries don't need WGS84
/// precision for a noise-floor simplification step.
fn meters_to_degrees(m: f64) -> f64 {
    if m <= 0.0 {
        0.0
    } else {
        m / 111_319.0
    }
}

/// Parse the `--source-priority` CLI value: comma-separated source
/// tokens. Unknown tokens are dropped with a warning. An empty result
/// means richness-only dedup.
fn parse_source_priority(raw: &str) -> Result<Vec<cairn_place::SourceKind>> {
    let mut out = Vec::new();
    for tok in raw.split(',') {
        let tok = tok.trim();
        if tok.is_empty() {
            continue;
        }
        match cairn_place::SourceKind::parse(tok) {
            Some(s) => {
                if !out.contains(&s) {
                    out.push(s);
                }
            }
            None => tracing::warn!(token = tok, "unknown source-priority token; ignored"),
        }
    }
    Ok(out)
}

fn cmd_build(args: BuildArgs) -> Result<()> {
    std::fs::create_dir_all(&args.out)
        .with_context(|| format!("creating bundle dir {}", args.out.display()))?;

    // Each Place / AdminFeature is tagged with the source that emitted
    // it; tags travel as a parallel Vec because Place itself doesn't
    // persist source info.
    let mut places: Vec<(Place, cairn_place::SourceKind)> = Vec::new();
    let mut admin_items: Vec<(cairn_spatial::AdminFeature, cairn_place::SourceKind)> = Vec::new();
    let mut sources: Vec<SourceVersion> = Vec::new();

    if let Some(osm_path) = args.osm.as_ref() {
        let node_cache_strategy = args
            .node_cache
            .resolve(osm_path, args.flatnode_path.as_deref());
        tracing::info!(
            path = %osm_path.display(),
            node_cache = ?node_cache_strategy,
            "ingesting OSM PBF"
        );
        let imported = cairn_import_osm::import_with(osm_path, node_cache_strategy)
            .with_context(|| format!("OSM import failed: {}", osm_path.display()))?;
        tracing::info!(
            places = imported.places.len(),
            polygons = imported.admin_layer.features.len(),
            "OSM imported"
        );
        places.extend(
            imported
                .places
                .into_iter()
                .map(|p| (p, cairn_place::SourceKind::Osm)),
        );
        admin_items.extend(
            imported
                .admin_layer
                .features
                .into_iter()
                .map(|f| (f, cairn_place::SourceKind::Osm)),
        );
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
        places.extend(
            imported
                .places
                .into_iter()
                .map(|p| (p, cairn_place::SourceKind::Wof)),
        );
        admin_items.extend(
            imported
                .admin_layer
                .features
                .into_iter()
                .map(|f| (f, cairn_place::SourceKind::Wof)),
        );
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
        places.extend(
            imported
                .into_iter()
                .map(|p| (p, cairn_place::SourceKind::OpenAddresses)),
        );
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
        places.extend(
            imported
                .into_iter()
                .map(|p| (p, cairn_place::SourceKind::Geonames)),
        );
        sources.push(SourceVersion {
            name: "geonames".into(),
            version: geonames_path.display().to_string(),
            blake3: hash_file(geonames_path)?,
        });
    }

    if let Some(postcodes_path) = args.postcodes.as_ref() {
        tracing::info!(path = %postcodes_path.display(), "ingesting Geonames postcode TSV");
        let imported =
            cairn_import_geonames::import_postcodes(postcodes_path).with_context(|| {
                format!(
                    "Geonames postcode import failed: {}",
                    postcodes_path.display()
                )
            })?;
        tracing::info!(count = imported.len(), "postcode places imported");
        places.extend(
            imported
                .into_iter()
                .map(|p| (p, cairn_place::SourceKind::Geonames)),
        );
        sources.push(SourceVersion {
            name: "geonames-postcodes".into(),
            version: postcodes_path.display().to_string(),
            blake3: hash_file(postcodes_path)?,
        });
    }

    if !args.source_priority.is_empty() {
        tracing::info!(
            priority = ?args.source_priority.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            "source-priority weighting active for cross-source dedup"
        );
    }

    // Dedupe Places across WoF + OSM. Both sources ship cities, POIs,
    // and addresses; without this pass /v1/search returns "Vaduz" twice
    // (one from each importer). Source priority breaks ties first;
    // richness (admin_path length, name count) is the fallback.
    let places_before = places.len();
    let mut places = cairn_place::dedupe_places(places, &args.source_priority);
    let places_after = places.len();
    if places_before != places_after {
        tracing::info!(
            before = places_before,
            after = places_after,
            dropped = places_before - places_after,
            "Place layer deduplicated across sources"
        );
    }

    // Dedupe admin features across WoF + OSM before any downstream pass
    // so the AdminIndex used for admin_path enrichment matches the one
    // we eventually write.
    let mut deduped_admin = if admin_items.is_empty() {
        None
    } else {
        let before = admin_items.len();
        let kept = cairn_spatial::dedupe_features(admin_items, &args.source_priority);
        let after = kept.len();
        if before != after {
            tracing::info!(
                before,
                after,
                dropped = before - after,
                "admin layer deduplicated across sources"
            );
        }
        Some(cairn_spatial::AdminLayer { features: kept })
    };

    // Enrich admin_path via PIP. WoF places already carry a parent chain
    // so we leave them alone; OSM-sourced cities, POIs, addresses, and
    // admin relation polygons all enter with admin_path=[] and come out
    // with country / region / county ancestors filled in. Same-kind and
    // self matches are skipped.
    //
    // Parallelism: both passes run PIP queries that are independent
    // per-input (the AdminIndex is read-only after construction; its
    // Mutex<LruCache> is Sync). We compute chains in parallel and write
    // them back sequentially so the input order is preserved (key for
    // reproducible builds).
    if let Some(layer) = &deduped_admin {
        use rayon::prelude::*;
        let admin_idx = cairn_spatial::AdminIndex::build(layer.clone());

        // Pass 1: enrich Place::admin_path (forward search, point fallback).
        let place_kind_strs: Vec<&'static str> = places
            .iter()
            .map(|p| cairn_text::kind_str(p.kind))
            .collect();
        let chains: Vec<Vec<cairn_place::PlaceId>> = places
            .par_iter()
            .zip(place_kind_strs.par_iter())
            .map(|(place, kind_str)| {
                if !place.admin_path.is_empty() {
                    return Vec::new();
                }
                pip_admin_chain(&admin_idx, place.centroid, kind_str, place.id.0)
            })
            .collect();
        let mut place_enriched = 0u64;
        for (place, chain) in places.iter_mut().zip(chains) {
            if place.admin_path.is_empty() && !chain.is_empty() {
                place.admin_path = chain;
                place_enriched += 1;
            }
        }
        tracing::info!(enriched = place_enriched, "Place admin_path enriched");
    }

    // Pass 2: enrich AdminFeature::admin_path (reverse PIP hits) using
    // the SAME index we just built. We collect chains in parallel, then
    // write them back sequentially.
    if let Some(layer) = deduped_admin.as_mut() {
        use rayon::prelude::*;
        let admin_idx = cairn_spatial::AdminIndex::build(layer.clone());
        let chains: Vec<Vec<cairn_place::PlaceId>> = layer
            .features
            .par_iter()
            .map(|feat| {
                if !feat.admin_path.is_empty() {
                    return feat
                        .admin_path
                        .iter()
                        .map(|id| cairn_place::PlaceId(*id))
                        .collect();
                }
                pip_admin_chain_for_feature(&admin_idx, feat)
            })
            .collect();
        let mut admin_enriched = 0u64;
        for (feat, chain) in layer.features.iter_mut().zip(chains) {
            if feat.admin_path.is_empty() && !chain.is_empty() {
                feat.admin_path = chain.into_iter().map(|p| p.0).collect();
                admin_enriched += 1;
            }
        }
        tracing::info!(
            enriched = admin_enriched,
            "AdminFeature admin_path enriched"
        );
    }

    // Build the text index from the full (now enriched) place set first;
    // tile bucketing consumes the vec afterwards.
    let text_dir = args.out.join("index/text");
    let docs = cairn_text::build_index(&text_dir, places.iter().cloned())
        .with_context(|| format!("building text index at {}", text_dir.display()))?;
    tracing::info!(docs, path = %text_dir.display(), "text index written");

    // Sidecar `index/text/admin_names.json` maps every admin-tier
    // PlaceId (AdminFeature.place_id post-renumber AND admin-kind Place.id)
    // to its primary name. Powers Pelias-style label rendering at query
    // time without a tantivy round-trip per hit. Optional — empty file
    // is fine, runtime will simply not populate the label field.
    let mut admin_names: std::collections::BTreeMap<u64, String> =
        std::collections::BTreeMap::new();
    if let Some(layer) = &deduped_admin {
        for f in &layer.features {
            if !f.name.is_empty() {
                admin_names.insert(f.place_id, f.name.clone());
            }
        }
    }
    for p in &places {
        if !matches!(
            p.kind,
            cairn_place::PlaceKind::Country
                | cairn_place::PlaceKind::Region
                | cairn_place::PlaceKind::County
                | cairn_place::PlaceKind::City
                | cairn_place::PlaceKind::District
                | cairn_place::PlaceKind::Neighborhood
        ) {
            continue;
        }
        if let Some(default_name) = p
            .names
            .iter()
            .find(|n| n.lang == "default")
            .or_else(|| p.names.first())
        {
            // Only insert if the AdminFeature pass didn't already cover
            // this id, so AdminFeature names (the canonical polygon
            // name) win over admin-tier Place names on ID overlap.
            admin_names
                .entry(p.id.0)
                .or_insert_with(|| default_name.value.clone());
        }
    }
    let admin_names_path = text_dir.join("admin_names.json");
    let admin_names_str =
        serde_json::to_string(&admin_names).context("encoding admin_names sidecar")?;
    std::fs::write(&admin_names_path, &admin_names_str)
        .with_context(|| format!("writing {}", admin_names_path.display()))?;
    tracing::info!(
        path = %admin_names_path.display(),
        entries = admin_names.len(),
        "admin_names sidecar written"
    );

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
    if let Some(mut layer) = deduped_admin {
        if args.simplify_tolerance_deg > 0.0 {
            let before_verts: usize = layer
                .features
                .iter()
                .map(|f| {
                    f.polygon
                        .0
                        .iter()
                        .map(|p| p.exterior().0.len())
                        .sum::<usize>()
                })
                .sum();
            cairn_spatial::simplify_admin_layer(&mut layer, args.simplify_tolerance_deg);
            let after_verts: usize = layer
                .features
                .iter()
                .map(|f| {
                    f.polygon
                        .0
                        .iter()
                        .map(|p| p.exterior().0.len())
                        .sum::<usize>()
                })
                .sum();
            tracing::info!(
                tolerance_deg = args.simplify_tolerance_deg,
                before_verts,
                after_verts,
                pct_kept = format!(
                    "{:.1}",
                    after_verts as f64 / before_verts.max(1) as f64 * 100.0
                ),
                "admin polygons simplified"
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

    let text_files = walk_text_files(&text_dir, &args.out)?;
    tracing::info!(count = text_files.len(), "text index files hashed");

    let manifest = Manifest {
        schema_version: 3,
        built_at: now_iso8601(),
        bundle_id: args.bundle_id,
        sources,
        tiles: entries,
        admin_tiles: admin_tile_entries,
        point_tiles: point_tile_entries,
        text_files,
    };
    let manifest_path = args.out.join("manifest.toml");
    write_manifest(&manifest_path, &manifest)?;
    tracing::info!(
        path = %manifest_path.display(),
        tiles = manifest.tiles.len(),
        "manifest written"
    );

    // CycloneDX SBOM: lists every Cargo.lock entry plus every input
    // dataset (with BLAKE3 hashes carried over from `sources`). Lets
    // operators audit "what code + what data made this bundle".
    match sbom::write_sbom(&args.out, &manifest.bundle_id, &manifest.sources) {
        Ok(libs) => tracing::info!(
            libraries = libs,
            datasets = manifest.sources.len(),
            "sbom.json written"
        ),
        Err(err) => tracing::warn!(?err, "skipping sbom.json (non-fatal)"),
    }

    Ok(())
}

/// Walk the tantivy text index directory tree, hash every file with
/// blake3, and return manifest entries with bundle-relative paths.
/// Tantivy keeps a small flat-ish set of segment files (`meta.json`,
/// per-segment `.term`, `.idx`, `.pos`, `.fast`, `.fieldnorm`,
/// `.store`, etc), so a recursive walk hashes the full index footprint.
fn walk_text_files(text_dir: &Path, bundle_root: &Path) -> Result<Vec<cairn_tile::TextFileEntry>> {
    let mut entries = Vec::new();
    let mut stack: Vec<PathBuf> = vec![text_dir.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in
            std::fs::read_dir(&dir).with_context(|| format!("reading {}", dir.display()))?
        {
            let entry = entry?;
            let path = entry.path();
            if entry.file_type()?.is_dir() {
                stack.push(path);
                continue;
            }
            let bytes = std::fs::metadata(&path)?.len();
            let hash = hash_file(&path)?;
            let rel = path
                .strip_prefix(bundle_root)
                .with_context(|| format!("{} not under bundle root", path.display()))?
                .to_string_lossy()
                .replace('\\', "/");
            entries.push(cairn_tile::TextFileEntry {
                rel_path: rel,
                byte_size: bytes,
                blake3: hash,
            });
        }
    }
    entries.sort_by(|a, b| a.rel_path.cmp(&b.rel_path));
    Ok(entries)
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

    // Text index: rebuild from the Places living in the kept tiles, filtered
    // by bbox. Tantivy segments aren't bbox-addressable on disk, so the
    // honest answer is to redo `build_index` over the in-bbox slice.
    let text_src = bundle.join("index/text");
    if text_src.exists() {
        let mut kept_places: Vec<Place> = Vec::new();
        for entry in &new_tiles {
            let level = Level::from_u8(entry.level)
                .ok_or_else(|| anyhow::anyhow!("unknown level {}", entry.level))?;
            let coord = TileCoord::from_id(level, entry.tile_id);
            let path = out.join(coord.relative_path());
            let places = cairn_tile::read_tile(&path)
                .with_context(|| format!("decoding tile {}", path.display()))?;
            for p in places {
                if p.centroid.lon >= q.0
                    && p.centroid.lon <= q.2
                    && p.centroid.lat >= q.1
                    && p.centroid.lat <= q.3
                {
                    kept_places.push(p);
                }
            }
        }
        let text_dst = out.join("index/text");
        let docs = cairn_text::build_index(&text_dst, kept_places)
            .with_context(|| format!("rebuilding text index at {}", text_dst.display()))?;
        tracing::info!(path = %text_dst.display(), docs, "text index rebuilt for bbox");
    }

    let extract_text_dir = out.join("index/text");
    let new_text_files = if extract_text_dir.exists() {
        walk_text_files(&extract_text_dir, out)?
    } else {
        Vec::new()
    };

    let new_manifest = Manifest {
        schema_version: src_manifest.schema_version,
        built_at: now_iso8601(),
        bundle_id: format!("{}-extract", src_manifest.bundle_id),
        sources: src_manifest.sources.clone(),
        tiles: new_tiles,
        admin_tiles: kept_admin_tiles,
        point_tiles: kept_point_tiles,
        text_files: new_text_files,
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
/// live under `tiles/`, spatial files under `spatial/`, and the tantivy
/// segments live under `index/text/`. All three are blake3-anchored so
/// the diff path can detect any byte-level corruption.
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
    for e in &manifest.text_files {
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

    // Text index: blake3 every segment file listed in the manifest, then
    // open the index. A missing or corrupt file fails the verify before we
    // ever hit the tantivy reader.
    let text_dir = bundle.join("index/text");
    let text_status = if text_dir.exists() {
        if !manifest.text_files.is_empty() {
            for entry in &manifest.text_files {
                let abs = bundle.join(&entry.rel_path);
                let actual = hash_file(&abs)?;
                if actual != entry.blake3 {
                    tracing::error!(
                        path = %abs.display(),
                        expected = %entry.blake3,
                        actual = %actual,
                        "blake3 mismatch on text segment"
                    );
                    anyhow::bail!("text segment blake3 mismatch at {}", abs.display());
                }
            }
        }
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

/// Numeric rank for an admin kind. Smaller = more root. Used to order
/// chains independently of bbox area, since OSM and WoF polygons for
/// the same admin level often have slightly different precision and
/// area-based ordering produces inconsistent root-leaf chains.
fn admin_kind_rank(kind: &str) -> Option<u8> {
    match kind {
        "country" => Some(0),
        "region" => Some(1),
        "county" => Some(2),
        "city" => Some(3),
        "district" => Some(4),
        "neighborhood" => Some(5),
        _ => None,
    }
}

/// Build an admin_path for a Place from a PIP query against the admin
/// index. Drop same-kind matches (a city shouldn't list a city-level
/// polygon), drop unranked matches (POIs etc that shouldn't appear in
/// admin chains), and sort root → leaf by `admin_kind_rank`.
fn pip_admin_chain(
    admin_idx: &cairn_spatial::AdminIndex,
    centroid: cairn_place::Coord,
    kind_str: &str,
    self_id: u64,
) -> Vec<cairn_place::PlaceId> {
    let mut ranked: Vec<(u8, cairn_spatial::AdminFeature)> = admin_idx
        .point_in_polygon(centroid)
        .into_iter()
        .filter(|f| f.place_id != self_id && f.kind != kind_str)
        .filter_map(|f| admin_kind_rank(&f.kind).map(|r| (r, f)))
        .collect();
    ranked.sort_by_key(|(r, _)| *r);
    ranked
        .into_iter()
        .map(|(_, f)| cairn_place::PlaceId(f.place_id))
        .collect()
}

/// Build an admin_path for an AdminFeature. Same as `pip_admin_chain`
/// but also enforces strict-parent semantics: drop any match whose kind
/// rank is >= self's rank (a country can't have a region as parent).
fn pip_admin_chain_for_feature(
    admin_idx: &cairn_spatial::AdminIndex,
    feat: &cairn_spatial::AdminFeature,
) -> Vec<cairn_place::PlaceId> {
    let self_rank = match admin_kind_rank(&feat.kind) {
        Some(r) => r,
        None => return Vec::new(),
    };
    let mut ranked: Vec<(u8, cairn_spatial::AdminFeature)> = admin_idx
        .point_in_polygon(feat.centroid)
        .into_iter()
        .filter(|f| f.place_id != feat.place_id)
        .filter_map(|f| admin_kind_rank(&f.kind).map(|r| (r, f)))
        .filter(|(r, _)| *r < self_rank)
        .collect();
    ranked.sort_by_key(|(r, _)| *r);
    ranked
        .into_iter()
        .map(|(_, f)| cairn_place::PlaceId(f.place_id))
        .collect()
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn process_ops_buckets_taggable_node_into_three_levels() {
        // Vaduz at 9.5209, 47.141 with place=village → taggable node.
        let xml = r#"<osmChange><create>
            <node id="1" lat="47.141" lon="9.5209" version="1">
              <tag k="place" v="village"/>
              <tag k="name" v="Vaduz"/>
            </node>
        </create></osmChange>"#;
        let ops = osc::parse_reader(Cursor::new(xml)).unwrap();
        let mut totals = ApplyTotals::default();
        let mut dirty: std::collections::BTreeSet<(u8, u32)> = std::collections::BTreeSet::new();
        process_ops(&ops, &mut totals, &mut dirty);
        assert_eq!(totals.node_creates, 1);
        assert_eq!(totals.taggable_nodes, 1);
        // L0, L1, L2 tile coords for the same point — three entries.
        assert_eq!(dirty.len(), 3);
        // Levels 0/1/2 represented.
        let levels: std::collections::BTreeSet<u8> = dirty.iter().map(|(l, _)| *l).collect();
        let expected: std::collections::BTreeSet<u8> = [0u8, 1, 2].into_iter().collect();
        assert_eq!(levels, expected);
    }

    #[test]
    fn process_ops_skips_untagged_node() {
        let xml = r#"<osmChange><create>
            <node id="1" lat="47.141" lon="9.5209" version="1"/>
        </create></osmChange>"#;
        let ops = osc::parse_reader(Cursor::new(xml)).unwrap();
        let mut totals = ApplyTotals::default();
        let mut dirty: std::collections::BTreeSet<(u8, u32)> = std::collections::BTreeSet::new();
        process_ops(&ops, &mut totals, &mut dirty);
        assert_eq!(totals.node_creates, 1);
        assert_eq!(totals.taggable_nodes, 0);
        assert!(dirty.is_empty(), "untagged nodes must not dirty tiles");
    }

    #[test]
    fn replicate_apply_dry_run_does_not_advance_state() {
        // Spin up a fake bundle dir with a hand-rolled state file +
        // one .osc.gz containing a node create. dry-run must
        // process it and leave last_applied_seq untouched.
        use flate2::write::GzEncoder;
        use flate2::Compression;
        use std::io::Write;

        let bundle = std::env::temp_dir().join(format!(
            "cairn-apply-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let rep_dir = bundle.join("replication");
        std::fs::create_dir_all(&rep_dir).unwrap();

        let mut state =
            replication::ReplicationState::new("https://example.com/replication".into());
        state.last_fetched_seq = Some(7);
        state.last_applied_seq = Some(6); // pretend everything before 7 already applied
        replication::write_state(&bundle, &state).unwrap();

        let xml = r#"<?xml version="1.0"?>
<osmChange version="0.6">
  <create>
    <node id="1" lat="47.141" lon="9.5209" version="1">
      <tag k="place" v="village"/>
      <tag k="name" v="X"/>
    </node>
  </create>
</osmChange>"#;
        let raw = xml.as_bytes();
        let path = rep_dir.join(format!("{:09}.osc.gz", 7));
        let mut enc = GzEncoder::new(Vec::new(), Compression::fast());
        enc.write_all(raw).unwrap();
        let gz = enc.finish().unwrap();
        std::fs::write(&path, gz).unwrap();

        cmd_replicate_apply(&bundle, 60, true).unwrap();
        let after = replication::read_state(&bundle).unwrap().unwrap();
        assert_eq!(
            after.last_applied_seq,
            Some(6),
            "dry-run must NOT advance last_applied_seq"
        );

        // Real run: state advances to 7.
        cmd_replicate_apply(&bundle, 60, false).unwrap();
        let after = replication::read_state(&bundle).unwrap().unwrap();
        assert_eq!(after.last_applied_seq, Some(7));

        // Idempotent: rerunning is a no-op.
        cmd_replicate_apply(&bundle, 60, false).unwrap();
        let after = replication::read_state(&bundle).unwrap().unwrap();
        assert_eq!(after.last_applied_seq, Some(7));
    }

    #[test]
    fn process_ops_counts_way_relation_without_dirtying_tiles() {
        let xml = r#"<osmChange>
          <modify><way id="100" version="2"><tag k="highway" v="residential"/><tag k="name" v="X"/></way></modify>
          <delete><relation id="500" version="3"/></delete>
        </osmChange>"#;
        let ops = osc::parse_reader(Cursor::new(xml)).unwrap();
        let mut totals = ApplyTotals::default();
        let mut dirty: std::collections::BTreeSet<(u8, u32)> = std::collections::BTreeSet::new();
        process_ops(&ops, &mut totals, &mut dirty);
        assert_eq!(totals.way_modifies, 1);
        assert_eq!(totals.relation_deletes, 1);
        assert!(
            dirty.is_empty(),
            "way / relation ops don't dirty tiles in node-only path"
        );
    }
}
