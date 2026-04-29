# Cairn benchmark harness

Apples-to-apples latency + footprint comparison of Cairn against
Pelias, Nominatim, and Photon. All four geocoders ingest the same
Switzerland OSM PBF and answer the same 10 000 query set. Results
land in `results/<tool>.json` with p50 / p95 / p99 / p99.9 latency
plus disk + RAM footprint.

## Why these four

- **Pelias** — Mapzen / GeocodeEarth's stack. Multi-source, BM25 over
  Elasticsearch. Industry workhorse. https://github.com/pelias/pelias
- **Nominatim** — Official OSM geocoder. PostgreSQL + PostGIS, deep
  admin hierarchy. https://github.com/osm-search/Nominatim
- **Photon** — Komoot's autocomplete-first geocoder. Java + ES, hot-
  reloads from Nominatim dumps. https://github.com/komoot/photon
- **Cairn** — this repo. rkyv tile bundles, single static binary.

## Methodology

1. **Same input** — Switzerland OSM PBF from Geofabrik (~480 MB).
2. **Same query set** — 10 000 queries randomly sampled from named
   `place=*` and `amenity=*` nodes in the same PBF. Includes a 50 %
   typo'd subset (one random character flipped) to stress fuzzy paths.
3. **Same hardware** — single laptop run, all tools in Docker on the
   same kernel, only one tool active at a time (no competition for
   page cache or CPU).
4. **Cold + warm latency** — first 1 000 queries discarded as warmup
   so JIT / page cache / segment loaders aren't penalized. Remaining
   9 000 contribute to histograms.
5. **One thread, sequential** — single-client closed-loop loadgen.
   Concurrency tuning is out of scope; we want the per-query latency
   shape, not the saturation throughput.

## Quick start (~6 hours wall clock if all four run)

```bash
cd bench
./scripts/00-download.sh     # ~5 min : PBF + WoF SQLite + Geonames
./scripts/10-build-cairn.sh  # ~5 min : measures cairn-build wall time + bundle size
./scripts/60-generate-queries.sh  # ~30 sec
./scripts/70-bench.sh cairn  # ~3 min : 10k queries

# Optional — each takes 1-3 hours of import time:
./scripts/30-run-pelias.sh && ./scripts/70-bench.sh pelias
./scripts/40-run-nominatim.sh && ./scripts/70-bench.sh nominatim
./scripts/50-run-photon.sh && ./scripts/70-bench.sh photon

./scripts/99-teardown.sh     # docker compose down -v on every stack
./scripts/aggregate.sh       # → results/summary.md
```

## Layout

```
bench/
├── README.md
├── scripts/
│   ├── 00-download.sh        # fetch Switzerland PBF + sidecars
│   ├── 10-build-cairn.sh     # build Cairn bundle + record footprint
│   ├── 20-run-cairn.sh       # cairn-serve in background
│   ├── 30-run-pelias.sh      # pelias docker compose up
│   ├── 40-run-nominatim.sh   # nominatim/nominatim-docker
│   ├── 50-run-photon.sh      # photon jar + Nominatim dump
│   ├── 60-generate-queries.sh
│   ├── 70-bench.sh           # delegates to runner/cairn-bench
│   ├── aggregate.sh          # results/*.json → summary.md
│   └── 99-teardown.sh
├── compose/
│   ├── docker-compose.pelias.yml
│   ├── docker-compose.nominatim.yml
│   └── docker-compose.photon.yml
├── data/                     # gitignored — PBF, queries, dumps
├── results/                  # JSON per tool
└── runner/
    ├── Cargo.toml
    └── src/main.rs           # cairn-bench: the latency runner
```

## What we measure

| Field | Source | Unit |
|---|---|---|
| `import_seconds` | wall clock around `cairn-build` / `pelias-import` / `nominatim-import` / `photon-import` | seconds |
| `disk_bytes` | `du -sb <bundle-or-data-dir>` | bytes |
| `rss_kb` | `ps -o rss=` after 1 000 warmup + 1 000 timing queries | KB |
| `latency_p50` / `_p95` / `_p99` / `_p99_9` | runner histogram, post-warmup | milliseconds |
| `qps` | `9000 / total_seconds` (closed loop, sequential) | queries/s |
| `recall_at_10` | `1` if expected place_id appears in top-10, else `0`; mean over query set | 0..1 |

`recall_at_10` uses the OSM `osm_id` we recorded when sampling the
query — every loadgen target tries to find the same canonical place.
Tools that surface admin polygons but not the original POI score 0
on POI queries; that's intentional.

## Why one PBF

Pelias and Photon also typically ingest WoF + Geonames + OA. Limiting
all four to the same OSM PBF removes one variable — every tool sees
the same input rows. Pelias / Photon configured to import only OSM.
Real-world Pelias deploys are richer; that's a separate evaluation.

## Output

`results/<tool>.json` per run, plus a Markdown summary:

```
| Tool      | Import    | Disk    | RSS    | p50  | p99  | QPS    | Recall@10 |
| Cairn     |  3m12s    |  124 MB |  180MB |  2.1 |  9.4 |  470   |  0.94     |
| Pelias    | 47m05s    |  6.2 GB |  3.8GB | 18.7 | 84.3 |   53   |  0.91     |
| Nominatim | 51m22s    |  9.1 GB |  4.4GB | 24.1 | 92.8 |   41   |  0.97     |
| Photon    | 12m18s    |  1.4 GB |  1.6GB | 11.8 | 47.2 |  130   |  0.88     |
```

Numbers above are illustrative — real values land in
`results/summary.md` after a run.
