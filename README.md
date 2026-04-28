# Cairn

Offline, airgap-ready geocoder written in Rust.

> Cairn (n.) — a pile of stones marking a trail.
> Each tile is a stone. Drop the pile on disk. The geocoder reads it.

## Status

Alpha. Forward search, autocomplete, fuzzy, layer filter, focus bias,
structured search, and reverse geocoding all working end-to-end on a
Liechtenstein dataset (OSM PBF + WhosOnFirst SQLite).

## Goals

- Forward + reverse geocoding, autocomplete, structured search
- Single static binary + single bundle artifact (tar)
- Zero network at runtime — full airgap deploy
- Region extracts via tile-tree subset (Valhalla-style 3-level grid)
- Single-machine commodity hardware, no cluster

## Non-goals

- Multi-tenant SaaS
- Live OSM diff replication (planned post-MVP)
- Cloud-native horizontal scaling

## Architecture

Three layers:

1. **Builder** (`cairn-build`) — ingests OSM PBF, WhosOnFirst SQLite,
   OpenAddresses CSV. Emits per-tile `rkyv` blobs, a tantivy text index, an
   admin polygon layer (bincode), and a centroid layer for nearest fallback.
   Writes a `manifest.toml` with blake3 hashes.
2. **Bundle** — flat directory of immutable mmap-ready files.
3. **Server** (`cairn-serve`) — `axum` HTTP API. Loads the bundle once at
   startup; no DB, no daemon dependencies.

### Tile model

64-bit `PlaceId`: `[level: 3 | tile_id: 22 | local_id: 39]`

| Level | Cell size | Contents |
|---|---|---|
| 0 | 4° × 4° | Countries, regions |
| 1 | 1° × 1° | Cities, counties, postcodes |
| 2 | 0.25° × 0.25° | Streets, addresses, POIs, neighborhoods |

### Workspace

```
crates/
  cairn-geocoder/         umbrella, re-exports
  cairn-place/            Place, PlaceId, schema (rkyv-archived)
  cairn-tile/             tile coords, manifest, blob IO, blake3 verify
  cairn-text/             tantivy index + autocomplete + fuzzy + geo-bias
  cairn-spatial/          R*-tree PIP for admin polygons + nearest centroids
  cairn-parse/            address parsing (libpostal FFI deferred)
  cairn-import-osm/       OSM PBF: place / POI nodes + named highway ways
  cairn-import-wof/       WhosOnFirst SPR + multilingual names + polygons
  cairn-import-oa/        OpenAddresses CSV
  cairn-import-geonames/  Geonames TSV (stub)
  cairn-api/              axum handlers
bins/
  cairn-build/            CLI build / extract / verify / info
  cairn-serve/            HTTP runtime
```

## Quick start

```bash
# 1. Fetch source data (one-time, can be mirrored offline after)
mkdir -p data
curl -fsSL -o data/liechtenstein.osm.pbf \
  https://download.geofabrik.de/europe/liechtenstein-latest.osm.pbf
curl -fsSL -o data/wof-li.db.bz2 \
  https://data.geocode.earth/wof/dist/sqlite/whosonfirst-data-admin-li-latest.db.bz2
bunzip2 data/wof-li.db.bz2

# 2. Build the workspace
cargo build --release -p cairn-build -p cairn-serve

# 3. Build a bundle
./target/release/cairn-build build \
  --osm data/liechtenstein.osm.pbf \
  --wof data/wof-li.db \
  --out bundle \
  --bundle-id liechtenstein

# 4. Verify integrity
./target/release/cairn-build verify --bundle bundle
# OK: 6 tiles verified, text=ok, admin=ok, points=ok

# 5. Inspect
./target/release/cairn-build info --bundle bundle

# 6. Serve
./target/release/cairn-serve --bundle bundle --bind 127.0.0.1:8080
```

## Endpoints

```
GET /healthz
GET /readyz                         200 ready / 503 if no text index
GET /v1/search                      forward + autocomplete
GET /v1/structured                  field-by-field search
GET /v1/reverse                     PIP + nearest fallback
```

### `/v1/search`

| Param | Type | Notes |
|---|---|---|
| `q` | string (required) | Free-text query. |
| `mode` | `search`\|`autocomplete` | Default `search`. |
| `limit` | int (1–100) | Default 10. |
| `fuzzy` | int 0–2 | Edit distance. Forward mode only. |
| `layer` | csv | Restrict to kinds (e.g. `country,city,street`). |
| `focus.lat`, `focus.lon` | float | Focus point for distance-biased rerank. |
| `focus.weight` | float | Distance penalty weight (default 0.5). |

```bash
curl 'http://localhost:8080/v1/search?q=Vaduz&layer=city&focus.lat=47.165&focus.lon=9.51'
curl 'http://localhost:8080/v1/search?q=Vad&mode=autocomplete'
curl 'http://localhost:8080/v1/search?q=vaaduz&fuzzy=2'
```

### `/v1/structured`

| Param | Type | Notes |
|---|---|---|
| `house_number` / `road` / `unit` | string | Address parts. |
| `postcode` / `city` / `district` / `region` / `country` | string | Admin parts. |
| `limit`, `focus.*` | as above | |

Builds a concatenated query, picks a layer hint based on the finest non-empty
field (address → street → city → region → country).

```bash
curl 'http://localhost:8080/v1/structured?road=Aeulestrasse&city=Vaduz'
curl 'http://localhost:8080/v1/structured?country=Liechtenstein'
```

### `/v1/reverse`

| Param | Type | Notes |
|---|---|---|
| `lat`, `lon` | float (required) | |
| `limit` | int 1–50 | Default 10. |
| `nearest` | int 0–50 | Fallback K-nearest centroids when PIP empty. |

Response includes `source: "pip" \| "nearest"`. PIP results are sorted finest
containing polygon first; admin chain available via `admin_path`.

```bash
curl 'http://localhost:8080/v1/reverse?lat=47.141&lon=9.523'
curl 'http://localhost:8080/v1/reverse?lat=48.0&lon=10.5&nearest=5'
```

## Bundle layout

```
bundle/
├── manifest.toml              schema, source hashes, per-tile blake3
├── tiles/<level>/<row>/<col>/<id>.bin     rkyv-archived Place blobs
├── index/text/                tantivy segments (mmap'd at runtime)
└── spatial/
    ├── admin.bin              bincode AdminLayer (polygons + metadata)
    └── points.bin             bincode PointLayer (centroids for nearest fallback)
```

## Build sources

| Source | Format | Coverage | Loaded by |
|---|---|---|---|
| OpenStreetMap | `*.osm.pbf` | Global | `--osm` |
| WhosOnFirst | SQLite | Per-country admin bundles | `--wof` |
| OpenAddresses | CSV | Per-region authoritative addresses | `--oa` |
| Geonames | TSV | Global populated places | `--geonames` (stub) |

## Quality gates

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

26 unit tests cover Place ID encoding, tile blob roundtrip, tile blake3
corruption detection, OSM tag classification, OA row validation, WoF
parent-chain walking, tantivy search/autocomplete/fuzzy/layer/focus, admin
PIP ordering, and nearest-K queries.

## Roadmap

See [ROADMAP.md](ROADMAP.md) for deferred phases (libpostal FFI,
address interpolation, OSM admin relations, per-tile spatial partitioning,
distribution tooling).

## License

Dual-licensed: MIT OR Apache-2.0. Pick whichever fits.

## References

- [Valhalla tile architecture](https://valhalla.github.io/valhalla/tiles/)
- [OpenStreetMap](https://www.openstreetmap.org)
- [WhosOnFirst](https://whosonfirst.org)
- [OpenAddresses](https://openaddresses.io)
- [tantivy](https://github.com/quickwit-oss/tantivy)
- [libpostal](https://github.com/openvenues/libpostal)
