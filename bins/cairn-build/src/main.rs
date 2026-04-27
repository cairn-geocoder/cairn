//! `cairn-build` — offline bundle builder.
//!
//! Ingests OSM PBF, WhosOnFirst, OpenAddresses, Geonames into a
//! tile-partitioned, mmap-ready bundle.

use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

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
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();
    match cli.command {
        Command::Build { out, .. } => {
            tracing::info!(path = %out.display(), "build (stub)");
        }
        Command::Extract { bundle, out, .. } => {
            tracing::info!(src = %bundle.display(), dst = %out.display(), "extract (stub)");
        }
        Command::Verify { bundle } => {
            tracing::info!(path = %bundle.display(), "verify (stub)");
        }
    }
    Ok(())
}
