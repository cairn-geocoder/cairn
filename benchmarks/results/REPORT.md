# Cairn vs incumbents — Switzerland benchmark

**Status:** Cairn run complete. Nominatim import running. Pelias and
Photon documented as multi-hour bringups; numbers TBD.

## Host

- **Model:** Apple Silicon (arm64), Darwin 25.1.0
- **RAM:** 36 GB
- **Disk free:** 1.4 TB
- **Docker:** OrbStack 29.0.2

## Dataset

- **Source:** `https://download.geofabrik.de/europe/switzerland-latest.osm.pbf`
- **PBF size:** 506 MB
- **Auxiliary:** Geonames `CH.txt` postcode dump (403 KB), Geonames
  `cities1000.txt` Latin-script city dump (29 MB).

## Query set

`queries/swiss-10k.txt` — **6 408 unique queries** synthesized from
Geonames Swiss data:
- 1 988 city / village names (PPL feature class, country=CH)
- 2 488 postcodes (`CH.txt` column 2)
- 1 932 `<postcode> <city>` composites

`queries/swiss-noisy.txt` — **1 153 noisy queries** — single-character
transpositions + ASCII-folded variants — for fuzzy / phonetic /
semantic A/B tests.

## Results

### Footprint + import

| Engine | Build wall-clock | Bundle / index disk | Peak build RAM |
|---|---:|---:|---:|
| **Cairn** | **27 s** | **212 MB** | **8.3 GB** |
| Pelias | TBD (~hours) | TBD | TBD |
| Nominatim | TBD (~30-60 min) | TBD | TBD |
| Photon | TBD (planet only — see notes) | TBD | TBD |

### Steady-state runtime footprint

| Engine | Cold RSS | Hot RSS (after 10k queries) |
|---|---:|---:|
| **Cairn** | **38 MB** | **102 MB** |
| Pelias | TBD | TBD |
| Nominatim | TBD | TBD |
| Photon | TBD | TBD |

### Latency (sequential single-client, 6 408 queries)

| Engine | p50 | p95 | p99 | max | RPS | errors |
|---|---:|---:|---:|---:|---:|---:|
| **Cairn** | **0.68 ms** | **1.08 ms** | **1.32 ms** | 6.6 ms | 40 (curl-bound) | **0** |
| Pelias | TBD | TBD | TBD | TBD | TBD | TBD |
| Nominatim | TBD | TBD | TBD | TBD | TBD | TBD |
| Photon | TBD | TBD | TBD | TBD | TBD | TBD |

### Sustained throughput (Apache Bench, keepalive)

Single host, varying concurrency, 10 000 requests per run.

| Engine | c=1 | c=8 | c=32 | c=64 | p99 @ peak |
|---|---:|---:|---:|---:|---:|
| **Cairn** | TBD RPS | **23 477 RPS** | **29 342 RPS** | 26 517 RPS | **2 ms** |
| Pelias | TBD | TBD | TBD | TBD | TBD |
| Nominatim | TBD | TBD | TBD | TBD | TBD |
| Photon | TBD | TBD | TBD | TBD | TBD |

### Recall on noisy queries (Cairn-only A/B)

The noisy-query set deliberately introduces typos and ASCII-folded
variants. Higher recall = better tolerance to user input drift.

| Variant | Hits | Recall |
|---|---:|---:|
| baseline (no flags) | 257 / 1 153 | 22.3 % |
| `?fuzzy=1` | 865 / 1 153 | 75.0 % |
| `?phonetic=true` | 1 147 / 1 153 | **99.5 %** |
| `?semantic=true` | 257 / 1 153 | 22.3 % |
| all flags on | 1 153 / 1 153 | **100.0 %** |

`?phonetic=true` (DoubleMetaphone) recovers 99.5 % of typos
single-handedly. Semantic boost is a tie-breaker for morphological
variants — it doesn't help on character-level perturbations, which
matches its design.

## Engine notes

### Pelias

Bringup is genuinely multi-hour:

1. `docker compose up elasticsearch` — ES 7.17.5, 2 GB JVM
2. `docker run pelias/schema:master ./bin/create_index` — bootstrap
3. `docker run pelias/whosonfirst:master` — admin polygon import
4. `docker run pelias/openstreetmap:master` — Switzerland PBF import
5. `docker run pelias/placeholder:master ./build` — placeholder index
6. `docker compose up api libpostal placeholder` — query layer

Steps 3 + 4 + 5 collectively take 2-4 hours on Switzerland on a Mac
with 16 GB allocated to Docker. The layout in `pelias/docker-compose.yml`
matches the Pelias canonical Docker layout
(<https://github.com/pelias/docker>); fill in the importer steps per
upstream docs and rerun `./run.sh pelias`. ES + API alone idle at
~3 GB hot RSS even before traffic.

### Nominatim

`mediagis/nominatim:4.4` does the full pipeline (PBF download → osm2pgsql
→ indices) automatically. Switzerland import wall-clock on a modern
Mac with 4 import threads: typically 30-60 min. PostgreSQL +
`flatnode` files together typically settle around 1.5-2 GB on disk.
Apache + PHP + Postgres hot RSS is 2-3 GB.

### Photon

The standard Photon distribution does NOT support country slicing
out-of-the-box: the official "search-index dump" is full planet
(~57 GB compressed, ~93 GB extracted). Country-slice indices require
running Nominatim first and exporting a slice. The `rtuszik/photon-docker`
image we tested ignores its `COUNTRY_CODE` env var and downloads the
planet dump regardless — leaving us 150 GB of disk and 1.5+ hours of
download for a number that wouldn't be apples-to-apples vs Cairn-on-CH
anyway. Documented; deferred.

## What this benchmark is NOT

- Not a multi-host distributed test. Single Mac, no network hop.
- Not a head-to-head on identical hardware budgets. Cairn fits
  in 102 MB hot RSS; the incumbents need GB-class JVM / Postgres
  budgets to even boot.
- Not a "best PR" run. Same default config for every engine; no
  hand-tuning.

## Reproducing

```bash
cd benchmarks
./data/download.sh         # ~510 MB
./queries/build.sh
./cairn/build.sh           # 27 s
./cairn/up.sh
./run.sh cairn
./run-rps.sh cairn         # apache bench
./run-recall.sh
./cairn/down.sh
```

Each engine has the same `up.sh` / `down.sh` shape. See `README.md`
for the full bringup sequence per engine.
