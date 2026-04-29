# Cairn vs Incumbents — Switzerland benchmark

Apples-to-apples comparison of Cairn, Pelias, Nominatim, and Photon
on the same Switzerland dataset, same query set, same hardware.

## Hardware (host)

Document exact host below before publishing numbers.

```
$ system_profiler SPHardwareDataType | grep -E "Model|Chip|Memory|Cores"
```

## Dataset

* Source: `https://download.geofabrik.de/europe/switzerland-latest.osm.pbf`
* Snapshot pinned in `data/SWITZERLAND_VERSION.txt` after first
  `download.sh` run so reruns hit the same data.
* Auxiliary: WhosOnFirst country / region SQLite (Switzerland slice),
  Geonames `CH.txt` (postcodes + cities).

All datasets get downloaded by `data/download.sh` into `data/`; they
are gitignored so we never commit copies.

## Engines

| Engine | Layout | Disk hot path | Setup |
|---|---|---|---|
| Cairn | rkyv tile blobs, mmap'd | bundle/ | `cairn/build.sh` |
| Pelias | Elasticsearch + Node services | docker volumes | `pelias/up.sh` |
| Nominatim | PostgreSQL + PostGIS + Apache + PHP | postgres data | `nominatim/up.sh` |
| Photon | Java + Lucene index | photon_data/ | `photon/up.sh` |

Each engine's directory carries its own `up.sh` / `down.sh` so the
benchmark harness can bring exactly one engine up at a time without
RAM contention skewing latency numbers.

## Query set

`queries/build.sh` synthesizes 10 000 representative queries from
Geonames Swiss data:

* 5000 city / village names (PPL feature class)
* 2500 postcode-only lookups (`8001`, `1004`, …)
* 2500 mixed `<postcode> <city>` strings (`8001 Zurich`)

Saved as one query per line in `queries/swiss-10k.txt`.

A small "noisy" set (typos + abbreviations) lives at
`queries/swiss-noisy.txt` for fuzzy / phonetic / semantic A/B.

## Running

```bash
cd benchmarks
./data/download.sh                 # ~700 MB switzerland-latest.osm.pbf
./queries/build.sh                 # 10k swiss-10k.txt
./cairn/build.sh                   # build Cairn bundle (timed)
./cairn/up.sh                      # start cairn-serve on :7100
./run.sh cairn                     # 10k queries, p50 / p95 / p99 / RPS
./cairn/down.sh

# Repeat for each engine. up.sh blocks until /healthz responds.
./pelias/up.sh && ./run.sh pelias && ./pelias/down.sh
./nominatim/up.sh && ./run.sh nominatim && ./nominatim/down.sh
./photon/up.sh && ./run.sh photon && ./photon/down.sh

./report.sh > results/report.md    # render comparison table
```

## What we measure

| Metric | How |
|---|---|
| Cold-start RAM | `ps -o rss=` on the engine PID, 30 s after `up.sh` returns |
| Steady-state RAM | same, after the 10k query run |
| Disk footprint | `du -sk` on the engine's data path |
| Build time | `time` wrapper around the engine's build step |
| Query latency p50/p95/p99 | sequential 10k query loop, recorded per request |
| Sustained throughput | 60 s closed-loop, single connection |
| Recall | top-1 contains the expected city for known queries |

## Output

`results/<engine>.json` — raw per-query timings + summary stats.
`results/report.md` — human-readable comparison table, regenerated
by `report.sh`.

## Reproducibility

* PBF version pinned in `data/SWITZERLAND_VERSION.txt`.
* Engine versions pinned in each `*/Dockerfile` / `up.sh`.
* Query set deterministic (sorted Geonames CSV → stable hash on
  re-generation).
* `run.sh` reads engine port + query path from env; everything else
  is hard-coded.

## What this is NOT

Not a marketing benchmark. The harness is open and reproducible.
Numbers will vary with disk speed, kernel, and concurrent processes
on the host — record what you ran on next to the numbers.
