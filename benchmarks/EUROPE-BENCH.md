# Europe-scale parity benchmark

## Goal

Reproduce the Switzerland + Germany comparison numbers at continent scale.
Same hardware (Hetzner box, 48 cores, 124 GB RAM, NVMe at `/mnt/fast`),
same Geofabrik input (`/mnt/fast/cairn/europe-latest.osm.pbf`, 32 GB,
already on disk from the Cairn build).

## Run order

Sequential — the Postgres / Elasticsearch instances each want enough RAM
that running two at once on this box risks OOM mid-import. Total wall:
~36 h.

1. **Nominatim Europe** — longest pole. PG-bound osm2pgsql pass.
   ```bash
   cd benchmarks/nominatim
   ./up-europe.sh                 # 24-48 h
   ```
   Storage: `/mnt/fast/nominatim-europe/{pgdata,flatnode}` (~400 GB).
   ES heap-style cap: `mem_limit: 48g` in compose.

2. **Pelias Europe** — once Nominatim is past its initial Postgres
   import (Pelias uses ES, no Postgres dep). Run after Nominatim DB
   is fully up.
   ```bash
   cd benchmarks/pelias
   ./up-europe.sh                 # ES + sidecars
   # then operator-driven OSM importer:
   docker run --rm --network=pelias_default \
     -e LOG_LEVEL=info \
     -v "$PWD/pelias.europe.json":/etc/pelias/pelias.json:ro \
     -v /mnt/fast/pelias-europe:/data \
     pelias/openstreetmap:master ./bin/start
   # 12-18 h
   ```
   Storage: `/mnt/fast/pelias-europe/{es-data,placeholder-data}` (~80 GB ES + ~20 GB placeholder).
   ES heap: 24 GB (`ES_JAVA_OPTS=-Xms24g -Xmx24g` in compose).

3. **Photon Europe** — uses komoot's prebuilt `photon-db-eu-latest.tar.bz2`
   (~150 GB extracted), so no Nominatim dep at runtime. Image build
   downloads the dump once.
   ```bash
   cd benchmarks/photon
   ./up-europe.sh                 # ~30 min after dump downloads
   ```
   Storage: `/mnt/fast/photon-europe/data` (~150 GB).
   JVM heap: 12 GB (`JAVA_OPTS=-Xms8g -Xmx12g` in compose).

4. **Cairn Europe** — already done; bundle at `/mnt/fast/cairn/bundle-europe`.
   Just run the same `cairn-serve --bundle …` already used for the
   country-scale runs and point `ab` at it.

## Ports

| Engine | API |
|---|---|
| Nominatim | `127.0.0.1:9998` |
| Pelias | `127.0.0.1:4001` |
| Photon | `127.0.0.1:2323` |
| Cairn | `127.0.0.1:8080` (or whatever cairn-serve binds) |

## Bench harness

After each engine is ready, run the same `ab` queries used in the
country-scale tables. Capture: build wall, disk, hot RSS, p50/p95/p99,
peak RPS. Update `comparison/index.html` table cells.

## Cleanup between runs

Each engine has its own `down-europe.sh` (TODO) that takes the stack
down without wiping volumes — first-run import cost is high enough
that we want to keep the indices around for re-benching.
