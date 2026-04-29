# Cairn vs Pelias vs Nominatim vs Photon — Switzerland benchmark

Reproducible apples-to-apples comparison. Same Switzerland OSM
PBF, same 6 408-query workload, same Mac.

## Host

- **Apple Silicon arm64**, Darwin 25.1.0
- **36 GB RAM**
- **1.4 TB free disk**
- **OrbStack (Docker 29.0.2)**

## Dataset

- Geofabrik `switzerland-latest.osm.pbf` — 506 MB
- Geonames `CH.txt` postcodes (403 KB) for query set
- Photon json dump (graphhopper.com) — 246 MB compressed, 4.0 GB
  decompressed

## Query set

`queries/swiss-10k.txt` — 6 408 unique queries:
- 1 988 city / village names (Geonames cities1000, country=CH)
- 2 488 postcodes (Geonames CH.txt column 2)
- 1 932 `<postcode> <city>` composites

`queries/swiss-noisy.txt` — 1 153 noisy variants (transpositions +
ASCII folds) for the recall A/B.

## Headline numbers

| Engine | Build wall-clock | Disk | Cold RSS | Hot RSS | p50 | p95 | p99 | max | RPS peak | Errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **Cairn** | **27 s** | **212 MB** | **38 MB** | **102 MB** | **0.68 ms** | **1.08 ms** | **1.32 ms** | 6.6 ms | **29 342** | **0** |
| Pelias | 3 m 46 s | 3.5 GB | 2.7 GB | 2.7 GB | 13.76 ms | 34.41 ms | 57.23 ms | 162.1 ms | 362 | 0 |
| Nominatim | 3 h 13 m | 9.2 GB pg + 103 GB sparse | 2.4 GB | 2.4 GB | 9.51 ms | 15.15 ms | 23.00 ms | 71.7 ms | 1 109 | 0 |
| Photon | 2 m 1 s | 1.3 GB | 2.1 GB | 2.1 GB | 5.88 ms | 15.26 ms | 25.18 ms | 76.5 ms | 2 406 | 0 |

(Sequential / single-client latency from `run.sh`. Peak RPS from
`ab -k` keepalive sweep at c=8/32/64 — winning concurrency
varies per engine.)

### Cairn vs each incumbent

| Metric | Cairn → Pelias | Cairn → Nominatim | Cairn → Photon |
|---|---:|---:|---:|
| p99 latency | **43× faster** | **17× faster** | **19× faster** |
| Peak RPS | **81× higher** | **26× higher** | **12× higher** |
| Hot RSS | **27× smaller** | **24× smaller** | **21× smaller** |
| Disk | **17× smaller** | **44× smaller (postgres only)** | **6× smaller** |
| Build wall-clock | **8× faster** | **430× faster** | **4.5× faster** |

## Recall on noisy queries (Cairn-only flags A/B)

| Variant | Hits | Recall |
|---|---:|---:|
| baseline (no flags) | 257 / 1 153 | 22.3 % |
| `?fuzzy=1` | 865 / 1 153 | 75.0 % |
| `?phonetic=true` | 1 147 / 1 153 | **99.5 %** |
| `?semantic=true` | 257 / 1 153 | 22.3 % |
| all flags on | 1 153 / 1 153 | **100.0 %** |

DoubleMetaphone phonetic single-handedly rescues 99.5 % of
typos. Semantic boost is for morphological variants (`Vienna →
Viennese`); doesn't fire on character-level perturbations. No
incumbent geocoder ships a `?phonetic=` toggle today.

## Per-engine notes

### Cairn

- One static binary (`cairn-serve`) reading mmap'd rkyv tile blobs.
- 520 470 places + 3 156 admin polygons indexed from a single
  PBF in 27 s (single-pass, no Postgres, no ES).
- Hot RSS settles at 102 MB after 6 408 queries — fits in cache
  on any laptop.
- Default labels include multilingual variants (`Zurich, Visp,
  Valais/Wallis, Schweiz/Suisse/Svizzera/Svizra`) without WoF
  download.

### Pelias

- ES 7.16.1 + node api + libpostal + placeholder.
- OSM importer wrote 2 585 525 docs in 3 min 46 s.
- Hot RSS dominated by ES (2.51 GB) + libpostal (113 MB).
- Library landed cleanly on arm64 once the ES image was bumped to
  7.16.1 (schema:master rejects 7.17.27 with a legacy version
  string regex). Required disabling adminLookup since we didn't
  ship WoF.

### Nominatim

- mediagis/nominatim:4.4 → Postgres 14 + PostGIS + Apache + PHP.
- osm2pgsql phase took **2 h 13 min single-thread** on arm64
  (THREADS=4 env ignored at this step). Total bringup including
  rank indexing + warming: **3 h 13 min**.
- 9.2 GB Postgres data + 103 GB sparse flatnode mmap (most of
  the flatnode file is zeros — actual allocated disk usage is
  smaller but `du` reports logical size).
- Port 8088 + 8080 collisions with kubectl port-forwards on the
  benchmark host; remapped to 9999.

### Photon

- Komoot Photon 1.1.0, Java 21 JRE, OpenSearch internal cluster.
- Built locally from graphhopper.com's CH photon-dump
  (`photon-dump-switzerland-liechtenstein-1.0-latest.jsonl.zst`,
  246 MB compressed).
- Import: 3 068 702 documents in **121 s** on host (Docker import
  step crashes mid-build on arm64 — host import + bind-mount
  works cleanly).
- Container needs `-listen-ip 0.0.0.0` to expose port outside
  loopback.

## Hardware caveats

- All engines ran inside Docker via OrbStack. Bare-metal numbers
  for incumbents would be ~10-20 % faster.
- Single-client latency is bounded by curl process spawn (~25 ms);
  the per-engine `time_total` reports server-side latency net of
  spawn overhead. RPS numbers are from `ab -k` keepalive which
  amortizes spawn cost.
- Numbers will shift on different disks / kernels / concurrent
  load. Re-run with `./run.sh <engine>` to validate on your host.

## Cairn at country scale (Germany)

To answer "does this hold up beyond a 500k-record bundle?" we ran
the same Cairn pipeline against the Geofabrik Germany PBF — **8.6×
larger input** (4.7 GB PBF, ~3 M places vs 506 MB / 520 k for CH).

| Metric | Switzerland | Germany | Ratio |
|---|---:|---:|---:|
| Input PBF | 506 MB | **4.7 GB** | 8.6× |
| Build wall-clock | 27 s | **633 s** (10 m 33 s) | 23× |
| Peak build RSS | 8.3 GB | **22 GB** | 2.7× |
| Bundle disk | 212 MB | **1.67 GB** | 7.9× |
| Cold serve RSS | 38 MB | **99 MB** | 2.6× |
| Hot serve RSS | 102 MB | **392 MB** | 3.8× |
| p50 latency | 0.68 ms | **0.81 ms** | 1.2× |
| p99 latency | 1.32 ms | **3.71 ms** | 2.8× |
| Peak RPS (c=8 ab keepalive) | 23 477 | **8 412** | 0.36× |
| Errors / 10 000 | 0 | **0** | — |

Build wall-clock scales roughly linearly with PBF size (8.6× input
→ 23× wall-clock — superlinear because the admin polygon assembly
has more relations to walk). Steady-state hot RSS scales sublinearly
(3.8×) because the rkyv tile blobs stay mmap'd; the resident set is
dominated by what's actively touched. p99 stays under 4 ms even at
3 M places. Peak RPS drops because the working set no longer fits
in CPU cache, but 8.4k RPS on country scale on a single laptop is
still well above any single-node deployment we'd expect from Pelias
or Nominatim on the same hardware budget.

Files: `cairn-build-germany.json`, `cairn-germany.json`,
`cairn-rps-germany.txt`.

### Incumbents on Germany — head-to-head

Same 4.7 GB Geofabrik PBF, same Mac, same `<country>-10k.txt`
workload (10 000 sampled DE city / postcode / composite queries),
same Docker host (OrbStack).

| Engine | Build wall-clock | Disk | Cold RSS | Hot RSS | p50 | p95 | p99 | max | RPS peak | Errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **Cairn** | **10 m 33 s** | **1.67 GB** | **99 MB** | **392 MB** | **0.81 ms** | **2.17 ms** | **3.71 ms** | 7.5 ms | **8 412** (c=8) | **0** |
| Pelias | 37 m 0 s | 5.28 GB | 5.15 GB | 5.15 GB | 10.29 ms | 17.10 ms | 23.16 ms | 62.5 ms | 506 (c=32) | 0 |
| Photon | 19 m 22 s | 9.10 GB | 2.13 GB | 2.13 GB | 9.69 ms | 43.91 ms | 92.94 ms | 357.4 ms | 1 919 (c=32) | 0 |
| Nominatim | *not run* (~28 h projected on arm64) | — | — | — | — | — | — | — | — | — |

#### Cairn vs each incumbent on DE

| Metric | Cairn → Pelias DE | Cairn → Photon DE |
|---|---:|---:|
| p99 latency | **6.2× faster** | **25× faster** |
| Peak RPS | **17× higher** | **4.4× higher** |
| Hot RSS | **13× smaller** | **5.4× smaller** |
| Disk | **3.2× smaller** | **5.9× smaller** |
| Build wall-clock | **3.5× faster** | **1.8× faster** |

Per-engine DE notes:

- **Pelias DE** — ES 7.16.1 + osm-import on `germany-latest.osm.pbf`,
  adminLookup disabled (no WoF). 24 189 231 documents indexed in
  37 min. Hot RSS dominated by ES (2.81 GB) + libpostal (1.91 GB)
  + node api (93 MB). p99 actually beats the CH run (23 ms vs
  57 ms on CH) — likely ES heap fits the working set better when
  it's pre-warmed by the bench, while CH numbers reflect first-touch.
  Peak RPS plateaus at c=32; c=64 regresses (p99 331 ms) when ES
  request queue saturates.
- **Photon DE** — Java 21 + OpenSearch internal cluster. 26 961 549
  documents imported in 19 m 22 s from graphhopper.com's DE
  photon-dump. Peak RPS 1 919 at c=32 with p99 27 ms; the 92.94 ms
  *sequential* p99 is dominated by JVM GC tail (max 357 ms is a
  young-gen pause).
- **Nominatim DE** — *not run.* On arm64 with the mediagis 4.4
  image's hardcoded `osm2pgsql --number-processes 1 --cache 0
  --flat-nodes` flags, Switzerland (506 MB) took 3 h 13 m. Germany
  is 9× larger and the bottleneck is single-thread CPU on
  ClientRead waits, so the projected wall-clock is **~28 hours**
  on this host. Running the import is straightforward
  (`./nominatim/up.sh` with `PBF_URL=…/germany-latest.osm.pbf`),
  but the wall-clock makes it impractical for the bench rig.
  Operators on x86_64 with multi-thread `osm2pgsql` should expect
  ~3–6 hours instead.

Files: `cairn-germany.json`, `cairn-rps-germany.txt`,
`pelias-germany.json`, `pelias-rps-germany.txt`,
`photon-germany.json`, `photon-rps-germany.txt`.

## Reproducing

```bash
cd benchmarks
./data/download.sh
./queries/build.sh

# Cairn
./cairn/build.sh && ./cairn/up.sh
./run.sh cairn && ./run-rps.sh cairn && ./run-recall.sh
./cairn/down.sh

# Pelias (~5 min import + boot)
./pelias/up.sh        # ES + schema + osm-import + api
./run.sh pelias && ./run-rps.sh pelias
./pelias/down.sh

# Nominatim (~3 h on arm64, ~30-60 min on x86_64 multi-thread)
./nominatim/up.sh
./run.sh nominatim && ./run-rps.sh nominatim
./nominatim/down.sh

# Photon (host import once, then container)
java -jar photon.jar import -import-file dump.jsonl
cp -r photon_data benchmarks/photon/
./photon/up.sh
./run.sh photon && ./run-rps.sh photon
./photon/down.sh

./report.sh > results/report.md
```

## Files

- `cairn.json` / `pelias.json` / `nominatim.json` / `photon.json`
  — raw per-engine summary
- `*-rps.txt` — Apache Bench keepalive sweeps
- `cairn-recall.txt` — noisy-set A/B
- `*.timings.txt` — per-query latency for histograms
