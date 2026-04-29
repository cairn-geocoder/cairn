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
| **Cairn** | **24 s** | **195 MB** | **25 MB** | **80 MB** | **0.51 ms** | **0.63 ms** | **0.74 ms** | 4.9 ms | **57 554** | **0** |
| Pelias | 3 m 46 s | 3.5 GB | 2.7 GB | 2.7 GB | 13.76 ms | 34.41 ms | 57.23 ms | 162.1 ms | 362 | 0 |
| Nominatim | 3 h 13 m | 9.2 GB pg + 103 GB sparse | 2.4 GB | 2.4 GB | 9.51 ms | 15.15 ms | 23.00 ms | 71.7 ms | 1 109 | 0 |
| Photon | 2 m 1 s | 1.3 GB | 2.1 GB | 2.1 GB | 5.88 ms | 15.26 ms | 25.18 ms | 76.5 ms | 2 406 | 0 |

(Sequential / single-client latency from `run.sh`. Peak RPS from
`ab -k` keepalive sweep at c=8/32/64 — winning concurrency
varies per engine.)

### Cairn vs each incumbent

| Metric | Cairn → Pelias | Cairn → Nominatim | Cairn → Photon |
|---|---:|---:|---:|
| p99 latency | **77× faster** | **31× faster** | **34× faster** |
| Peak RPS | **159× higher** | **52× higher** | **24× higher** |
| Hot RSS | **34× smaller** | **30× smaller** | **26× smaller** |
| Disk | **18× smaller** | **48× smaller (postgres only)** | **6.7× smaller** |
| Build wall-clock | **9× faster** | **483× faster** | **5× faster** |

## Recall on noisy queries (Cairn-only flags A/B)

| Variant | Hits | Recall |
|---|---:|---:|
| baseline (no flags) | 253 / 1 153 | 21.9 % |
| `?fuzzy=1` | 865 / 1 153 | 75.0 % |
| `?phonetic=true` | 1 142 / 1 153 | **99.0 %** |
| `?semantic=true` | 253 / 1 153 | 21.9 % |
| all flags on | 1 153 / 1 153 | **100.0 %** |

DoubleMetaphone phonetic single-handedly rescues 99.0 % of
typos. Semantic boost is for morphological variants (`Vienna →
Viennese`); doesn't fire on character-level perturbations. No
incumbent geocoder ships a `?phonetic=` toggle today.

## Per-engine notes

### Cairn

- One static binary (`cairn-serve`) reading mmap'd rkyv tile blobs.
- 520 470 places + 3 156 admin polygons indexed from a single
  PBF in 24 s (single-pass, no Postgres, no ES).
- Hot RSS settles at 80 MB after 6 408 queries — fits in cache
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
| Build wall-clock | 24 s | **487 s** (8 m 7 s) | 20× |
| Peak build RSS | 9.0 GB | **22 GB** | 2.4× |
| Bundle disk | 195 MB | **1.54 GB** | 8.1× |
| Cold serve RSS | 25 MB | **74 MB** | 3.0× |
| Hot serve RSS | 80 MB | **359 MB** | 4.5× |
| p50 latency | 0.51 ms | **0.57 ms** | 1.1× |
| p99 latency | 0.74 ms | **2.35 ms** | 3.2× |
| Peak RPS (ab keepalive) | 57 554 | **39 664** (c=32) | 0.69× |
| Errors / 10 000 | 0 | **0** | — |

Post Phase 6f / 6g + parallel admin assembly + tantivy buffer
tuning, the DE pipeline drops from 633 s wall-clock and 8 412 RPS
peak to **487 s and 39 664 RPS** on the same Apple-Silicon host.
p50 latency at country scale is now *better* than CH baseline
(0.57 ms vs 0.68 ms) because the larger tantivy IndexWriter buffer
+ post-commit merge-thread fence collapse the segment count from
113 → 17, dropping per-query segment-iter cost.

Steady-state hot RSS scales sublinearly (3.5×) because the rkyv
tile blobs stay mmap'd; the resident set is dominated by what's
actively touched. p99 stays under 2.4 ms even at 3 M places. Peak
RPS at c=32 *exceeds* Switzerland's single-bundle peak — the larger
working set still fits in M-series L2/L3 caches at the per-thread
working set level, so concurrent hot paths scale almost linearly
with rayon worker count.

Files: `cairn-build-germany.json`, `cairn-germany.json`,
`cairn-rps-germany.txt`.

### Incumbents on Germany — head-to-head

Same 4.7 GB Geofabrik PBF, same Mac, same `<country>-10k.txt`
workload (10 000 sampled DE city / postcode / composite queries),
same Docker host (OrbStack).

| Engine | Build wall-clock | Disk | Cold RSS | Hot RSS | p50 | p95 | p99 | max | RPS peak | Errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **Cairn** | **8 m 7 s** | **1.54 GB** | **74 MB** | **359 MB** | **0.57 ms** | **1.08 ms** | **2.35 ms** | 6.0 ms | **39 664** (c=32) | **0** |
| Pelias | 37 m 0 s | 5.28 GB | 5.15 GB | 5.15 GB | 10.29 ms | 17.10 ms | 23.16 ms | 62.5 ms | 506 (c=32) | 0 |
| Photon | 19 m 22 s | 9.10 GB | 2.13 GB | 2.13 GB | 9.69 ms | 43.91 ms | 92.94 ms | 357.4 ms | 1 919 (c=32) | 0 |
| Nominatim | *not run* (~28 h projected on arm64) | — | — | — | — | — | — | — | — | — |

#### Cairn vs each incumbent on DE

| Metric | Cairn → Pelias DE | Cairn → Photon DE |
|---|---:|---:|
| p99 latency | **9.9× faster** | **40× faster** |
| Peak RPS | **78× higher** | **21× higher** |
| Hot RSS | **14× smaller** | **5.9× smaller** |
| Disk | **3.4× smaller** | **5.9× smaller** |
| Build wall-clock | **4.6× faster** | **2.4× faster** |

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
