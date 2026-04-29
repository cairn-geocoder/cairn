//! `cairn-bench` — latency loadgen for the bench harness.
//!
//! Reads a NDJSON query file (one `{q,osm_id,lon,lat}` object per
//! line), fires each query against the named tool's search endpoint
//! sequentially in a single thread, and emits a JSON report with
//! p50 / p95 / p99 / p99.9 latency, mean QPS, and recall@10.
//!
//! Single-thread closed-loop on purpose. Concurrency tuning would
//! distort the per-query shape we want to compare across stacks.
//! Saturating throughput is a separate experiment.
//!
//! Tool URLs:
//!   cairn      http://127.0.0.1:8080/v1/search?q=
//!   pelias     http://127.0.0.1:4000/v1/search?text=
//!   nominatim  http://127.0.0.1:8081/search?q=&format=json
//!   photon     http://127.0.0.1:2322/api?q=

use clap::{Parser, ValueEnum};
use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, ValueEnum)]
#[value(rename_all = "kebab-case")]
enum Tool {
    Cairn,
    Pelias,
    Nominatim,
    Photon,
}

#[derive(Parser, Debug)]
#[command(name = "cairn-bench", about = "Geocoder latency benchmark")]
struct Cli {
    /// Tool under test — selects the URL template + response parser.
    #[arg(long)]
    tool: Tool,
    /// NDJSON file produced by `60-generate-queries.sh`.
    #[arg(long)]
    queries: PathBuf,
    /// Where to write the result JSON.
    #[arg(long)]
    out: PathBuf,
    /// Override the tool's base URL (defaults to localhost convention).
    #[arg(long)]
    base_url: Option<String>,
    /// Skip the first N queries from the histogram (warmup).
    #[arg(long, default_value_t = 1000)]
    warmup: usize,
    /// HTTP timeout per request, in milliseconds.
    #[arg(long, default_value_t = 5_000)]
    timeout_ms: u64,
    /// Maximum number of queries to run (0 = use full file).
    #[arg(long, default_value_t = 0)]
    max: usize,
}

#[derive(Deserialize, Clone)]
struct Query {
    q: String,
    osm_id: Option<String>,
}

#[derive(Serialize)]
struct Report {
    tool: String,
    queries_total: usize,
    queries_timed: usize,
    warmup: usize,
    elapsed_seconds: f64,
    qps: f64,
    latency_ms: LatencySummary,
    recall_at_10: f64,
    errors: usize,
}

#[derive(Serialize)]
struct LatencySummary {
    min: f64,
    p50: f64,
    p95: f64,
    p99: f64,
    p99_9: f64,
    max: f64,
    mean: f64,
}

fn main() -> std::io::Result<()> {
    let cli = Cli::parse();
    let base = cli.base_url.unwrap_or_else(|| match cli.tool {
        Tool::Cairn => "http://127.0.0.1:8080".into(),
        Tool::Pelias => "http://127.0.0.1:4000".into(),
        Tool::Nominatim => "http://127.0.0.1:8081".into(),
        Tool::Photon => "http://127.0.0.1:2322".into(),
    });

    let queries: Vec<Query> = BufReader::new(File::open(&cli.queries)?)
        .lines()
        .map_while(Result::ok)
        .filter(|l| !l.trim().is_empty())
        .filter_map(|l| serde_json::from_str(&l).ok())
        .collect();
    if queries.is_empty() {
        return Err(std::io::Error::other("no queries loaded"));
    }
    let limit = if cli.max == 0 {
        queries.len()
    } else {
        cli.max.min(queries.len())
    };
    let queries = &queries[..limit];
    eprintln!(
        "==> tool={:?} base={} queries={} warmup={}",
        cli.tool, base, queries.len(), cli.warmup
    );

    let agent = ureq::AgentBuilder::new()
        .timeout(Duration::from_millis(cli.timeout_ms))
        .build();

    // 1 µs to 60 s, 3 sig fig.
    let mut hist = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();
    let mut errors = 0usize;
    let mut recall_hits = 0usize;
    let mut recall_seen = 0usize;
    let started = Instant::now();

    for (i, q) in queries.iter().enumerate() {
        let url = build_url(cli.tool, &base, &q.q);
        let t0 = Instant::now();
        let resp = agent.get(&url).call();
        let elapsed = t0.elapsed();
        let dur_us = elapsed.as_micros() as u64;
        let body = match resp {
            Ok(r) => match r.into_string() {
                Ok(b) => b,
                Err(_) => {
                    errors += 1;
                    continue;
                }
            },
            Err(_) => {
                errors += 1;
                continue;
            }
        };

        if i >= cli.warmup {
            hist.record(dur_us.max(1)).ok();
            if let Some(want) = q.osm_id.as_deref() {
                recall_seen += 1;
                if recall_check(cli.tool, &body, want) {
                    recall_hits += 1;
                }
            }
        }
        if i % 1_000 == 0 {
            eprintln!(
                "    {} / {} done; errors={} last={}ms",
                i,
                queries.len(),
                errors,
                dur_us / 1000
            );
        }
    }

    let elapsed = started.elapsed();
    let timed = queries.len().saturating_sub(cli.warmup);
    let qps = timed as f64 / elapsed.as_secs_f64();
    let report = Report {
        tool: format!("{:?}", cli.tool).to_lowercase(),
        queries_total: queries.len(),
        queries_timed: timed,
        warmup: cli.warmup,
        elapsed_seconds: elapsed.as_secs_f64(),
        qps,
        latency_ms: LatencySummary {
            min: hist.min() as f64 / 1000.0,
            p50: hist.value_at_quantile(0.50) as f64 / 1000.0,
            p95: hist.value_at_quantile(0.95) as f64 / 1000.0,
            p99: hist.value_at_quantile(0.99) as f64 / 1000.0,
            p99_9: hist.value_at_quantile(0.999) as f64 / 1000.0,
            max: hist.max() as f64 / 1000.0,
            mean: hist.mean() / 1000.0,
        },
        recall_at_10: if recall_seen == 0 {
            f64::NAN
        } else {
            recall_hits as f64 / recall_seen as f64
        },
        errors,
    };

    let pretty = serde_json::to_string_pretty(&report).unwrap();
    println!("{pretty}");
    if let Some(parent) = cli.out.parent() {
        std::fs::create_dir_all(parent).ok();
    }
    let mut f = File::create(&cli.out)?;
    f.write_all(pretty.as_bytes())?;
    Ok(())
}

fn build_url(tool: Tool, base: &str, q: &str) -> String {
    let q_enc = url::form_urlencoded::byte_serialize(q.as_bytes()).collect::<String>();
    match tool {
        Tool::Cairn => format!("{base}/v1/search?q={q_enc}&limit=10"),
        Tool::Pelias => format!("{base}/v1/search?text={q_enc}&size=10"),
        Tool::Nominatim => format!("{base}/search?q={q_enc}&format=json&limit=10"),
        Tool::Photon => format!("{base}/api?q={q_enc}&limit=10"),
    }
}

/// Recall@10: did the expected `osm_id` show up in any result slot?
/// Each tool surfaces OSM ids in a different field, so we sniff for
/// the bare numeric id substring — good enough for a recall proxy
/// and tool-agnostic.
fn recall_check(_tool: Tool, body: &str, want_osm_id: &str) -> bool {
    // want_osm_id is "node/12345" or "way/678" etc — strip prefix to
    // a bare integer for a substring match. Tool-specific JSON shapes
    // all surface the integer id as `osm_id` / `gid` / `place_id` so
    // a substring sweep is consistent.
    let bare = want_osm_id
        .rsplit('/')
        .next()
        .unwrap_or(want_osm_id);
    body.contains(bare)
}
