# Why Cairn Is an Offline Geocoder, and What That Buys You

*A walkthrough of how Cairn ships place data — addresses, POIs, admin polygons, postcodes — as a self-contained directory of mmap-ready tiles you can drop on any disk, embed in a mobile app, or ship inside a static site.*

## The premise: geocoding without a server

Open a typical map app. You type an address. The text leaves your device, hits a hosted geocoder, and a JSON list of candidates comes back. That round-trip costs you four things: latency (50–300 ms is normal), money (every major API is metered), privacy (every search is logged on someone else's server), and availability (no network, no geocoding).

For most consumer products that's a fine trade. For some products it isn't:

- **Field tools** that go where cell coverage doesn't — survey apps, offline navigation, compliance inspectors, oil and gas, agriculture, search and rescue.
- **Privacy-sensitive UX** — clinical EMRs that classify patient addresses, journalism workflows handling sensitive sources, anything bound by a privacy regulator that flags third-party data egress.
- **Edge devices** with intermittent connectivity — kiosks, vehicles, IoT gateways.
- **Mobile apps that hate per-MAU costs** — anything where the marketing team wants to ship to a million phones without getting a six-figure invoice from the geocoding vendor.

Cairn was built for those products. It is a Rust-based offline geocoder where the entire database — every address, every administrative polygon, every point of interest, the full text index, even the spatial structures — ships as a directory you can copy onto a disk, mmap into a server, embed in a desktop app, or stream into a browser.

This article walks through why the on-disk format is shaped the way it is, what makes the tile architecture useful, and how mobile apps actually consume the bundle today.

## The bundle is the product

A Cairn deployment is a directory called a *bundle*. A small Liechtenstein bundle weighs about 50 MB; a planet bundle weighs roughly 80–120 GB depending on how aggressively you simplify admin polygons. The shape is the same at every scale:

```
bundle/
├── manifest.toml          # schema version, sources, every tile's blake3
├── tiles/                 # places at three zoom levels
│   ├── 0/…/00.bin
│   ├── 1/…/01.bin
│   └── 2/…/02.bin
├── spatial/
│   ├── admin/             # admin polygons (countries, regions, cities)
│   ├── nearest/            # k-NN point clouds
│   └── buildings/          # building footprints (optional)
├── index/text/             # tantivy full-text index
└── sbom.json               # CycloneDX bill of materials
```

The manifest is the entry point. It carries a schema version, every input dataset's hash and license, and one line per tile with that tile's blake3 digest plus byte size. A reader opens the manifest, validates the bundle, and from there every read is a deterministic file lookup — no global index to scan, no central database to keep in sync.

Two things flow from this design:

1. **Reproducibility.** Identical inputs produce identical bundles. The blake3 manifest plus the SBOM gives operators a forensic trail: "what code, what data, what license made this exact bundle." You can sign it, ship it through your CDN, and prove the byte sequence the user runs is the byte sequence you built.

2. **Atomic deploys.** Bundles are immutable. To upgrade, the runtime atomically swaps the `bundle/` symlink to a new directory; in-flight requests on the old bundle finish on its files because mmap holds them open. There is no migration, no schema-changing SQL, no dual-writing window.

## Why tiles, and why three levels

The world is not uniform. Manhattan has more addresses per square kilometer than the Sahara has features in a hundred. A flat database fights this distribution; a tiled one absorbs it.

Cairn partitions every Place into one of three resolution levels, inspired by Valhalla's tile scheme:

| Level | Cell size       | What lives there                            |
|-------|-----------------|---------------------------------------------|
| L0    | 4° × 4°         | countries, regions, large administrative areas |
| L1    | 1° × 1°         | cities, postcodes, mid-tier admin            |
| L2    | 0.25° × 0.25°   | streets, addresses, points of interest      |

A reverse-geocode query for a coordinate touches at most one L0 tile, one L1 tile, and one L2 tile — three small reads, regardless of whether the coordinate is in central Tokyo or the middle of Greenland. A forward-geocode query takes a different path (text index first, spatial filter second), but tiles still bound the working set: only the tiles that contain matching candidates ever get loaded.

The level isn't decorative. A `Place` carries a `PlaceId` whose top three bits encode the level, so a 64-bit place identifier never collides across levels and the runtime can route lookups by inspecting the id alone. A bundle full of mostly-empty L0 cells (an L0 grid is 4 050 cells globally) wastes nothing — empty cells aren't materialized as files at all. Only populated tiles exist on disk.

## The tile format itself

Each tile blob is a fixed 16-byte header (magic + format version + payload length) followed by an [rkyv](https://rkyv.org/) archive of `Vec<Place>`. rkyv is a zero-copy serialization framework: the on-disk bytes are the runtime layout, so reading a tile is just memory-mapping it and casting the payload to `&Archived<Vec<Place>>`. No deserialization, no allocation.

Two pieces of recent work make this fully usable:

**Compression by default.** Tile payloads compress with zstd. On a typical POI-heavy tile this trims size 4–5×, and we trained the writer to flip the format version flag automatically so callers don't have to opt in. The decompression cost is paid once per cache miss, not per query.

**Zero-copy reads via `PlaceTileArchive`.** Phase 6d added a `PlaceTileArchive::from_path(...)` constructor that opens a tile, validates its rkyv root once, and hands the caller a borrowed `&Archived<Vec<Place>>`. Subsequent iterations skip the deserialize-into-owned step entirely. On planet-scale serve workloads this saves both CPU and a 25.4M-place transient heap allocation per cold-tile load.

**Integrity by default.** Every tile is hashed with blake3 at build time. The manifest carries those hashes. A `cairn-build verify` pass walks the bundle, recomputes every digest, and confirms byte-for-byte that the runtime is reading what the build wrote. Tampered tiles surface immediately; this matters for signed deploys and for dataset attribution audits.

## Spatial layers ride alongside

Tiles store places — addresses, POIs, places-of-the-named variety. Two more layers handle the geometry questions that geocoders actually face in production:

**Admin polygons.** Reverse geocoding asks "what city is this point inside?" rather than "what's nearest?" The answer lives in `spatial/admin/`, where each tile carries a flat rkyv archive of `ArchivedAdminFeature`s — country / region / county / city polygons with their rings, holes, precomputed bounding boxes, and a per-ring sorted edge list that lets point-in-polygon binary-search the y-range instead of scanning every edge. On Russia or Norway-sized polygons this is the difference between O(n) and O(√n) edge tests per query.

**Nearest-point R\*-tree.** For the cases where polygons don't cover (islands missing from OSM, points off-coast, addresses interpolated between known nodes), `spatial/nearest/` stores a partitioned R\*-tree of place centroids. Each tile's bounding box is loaded eagerly into the tree at startup; the points themselves stay on disk and load lazily through an LRU cache when their tile is queried. A planet-scale nearest-fallback layer carries a few thousand tile bboxes — fits in a megabyte — and the LRU keeps the working set bounded.

**Federation.** A planet split into continental shards (`europe/`, `americas/`, `asia/`, `africa/`, `oceania/`) is a federation of bundles. The runtime fans every query — search, reverse, lookup-by-id — across every bundle in parallel and merges results. This is how you ship a planet's worth of geocoding to a single-tenant deployment without one giant 100 GB tantivy index. Phase 6e shipped the parallel fan-out via rayon, so multi-shard p95 stops growing linearly with shard count.

## How a mobile app actually uses this

Three integration shapes work today, depending on how aggressive your bundle-size budget is.

### Shape A: ship a regional bundle inside the app

For an app that operates inside one country or one metro, embed the bundle directly. A Liechtenstein-sized bundle is ~50 MB, a small-country bundle (Latvia, Costa Rica) is 200–500 MB, a single US state runs 1–2 GB. iOS App Store and Google Play both allow on-demand resource downloads, so you ship the binary lean and pull the bundle on first launch.

The runtime is `cairn-serve` linked as a library, exposing the same `/v1/search`, `/v1/reverse`, `/v1/place` endpoints you'd hit on a hosted deployment. On iOS it runs as a Swift Package wrapping the Rust library through C ABI; on Android it's a JNI binding under `android-cairn-aar`. Either way the geocoder is a function call, not an HTTP request.

```swift
// iOS (sketch, names follow the Swift Package surface)
let geocoder = try Cairn.openBundle(at: bundleURL)
let hits = geocoder.search(query: "kantonsspital aarau",
                           options: .init(limit: 10, lang: "de"))
```

```kotlin
// Android (sketch)
val geocoder = Cairn.openBundle(context.filesDir.resolve("cairn"))
val hits = geocoder.search("kantonsspital aarau",
                           SearchOptions(limit = 10, lang = "de"))
```

This is the fastest path: every query is a memory-mapped read against the local filesystem, ~1–8 ms for a forward search on a warm cache, ~0.5 ms for reverse-geocode in admin polygons. No network, no telemetry, no per-call billing.

### Shape B: ship a tiny WASM autocompleter, fall back to your server

When the full bundle is too heavy but you still want offline-feeling autocomplete, embed `cairn-wasm`. It's a ~250 KB WASM blob built on top of an FST (finite-state transducer) that ships with each Cairn bundle. The FST holds every indexed name; the WASM module does prefix iteration in the browser without round-tripping to the server.

```js
import init, { Autocompleter } from './cairn_wasm.js';

await init();
const fstBytes = await fetch('/cairn/index/text/fst.bin')
  .then(r => r.arrayBuffer());
const completer = new Autocompleter(new Uint8Array(fstBytes));

// As the user types:
const suggestions = completer.complete('zü', 8);
// → ["zürich", "zürich-altstetten", "zürich oerlikon", ...]
```

Hit the real `/v1/search` server only when the user submits or pauses typing. The user-facing latency on every keystroke drops to single-digit milliseconds because you never leave the device. This pattern works for PWAs, Capacitor / Cordova hybrid apps, React Native via `react-native-wasm`, and Flutter via `flutter_rust_bridge`.

The FST file is a small fraction of a bundle — country-scale FSTs run 5–20 MB. Pair this with Service Worker caching and your app gets autocomplete that survives the network going dark.

### Shape C: full bundle in a sidecar process

For apps that already ship a backend service alongside the mobile client (electron desktop apps, on-device LLM agents, edge gateways), run `cairn-serve` as the sidecar. The mobile UI talks to it over `localhost:8080` exactly like it would talk to a hosted geocoder — same JSON, same pagination, same geo-bias parameters — but the bundle lives on local disk.

```bash
cairn-serve \
  --bundle  /var/lib/cairn/europe-2026-01 \
  --bundle  /var/lib/cairn/americas-2026-01 \
  --bind    127.0.0.1:8080
```

This is the most surgical drop-in: any client written against Pelias' API contract (which Cairn mimics, including `gid`, `admin_path`, label rendering) works without changes. Federation lets you split a planet across continents without one gigantic index.

## Why this beats the standard offline geocoder pattern

The usual "offline geocoder" implementation is some flavor of "embed SQLite or LMDB, run a forward index over it, and call it offline". Two things break in production:

1. **The index *is* the database.** Every query parses through SQLite's planner, allocates result rows, deserializes blobs. There's no zero-copy path; mmap helps SQLite's pages but not your row deserialization. On a million-row search, the overhead is real.

2. **Schema migrations are hostile.** Add a column, you ship a migration. Add a denormalization, you ship a migration. Bundle hashes drift across rebuilds because SQLite's page layout depends on insertion order, vacuum state, and small-row packing.

Cairn dodges both. The on-disk format *is* the runtime layout — `Place`, `ArchivedAdminFeature`, `PlacePoint` are rkyv-archived structs, so reading a tile is a bounds-check plus a cast. The format is content-addressed: identical inputs produce byte-identical bundles. There's no migration story because there's no mutable state — bundles are immutable artifacts you replace, not databases you alter.

The trade-off you accept: your data update cadence is "rebuild and ship a new bundle" rather than "stream WAL deltas". For most products this is the right shape. OSM publishes weekly; WoF publishes monthly; OpenAddresses publishes per-source rolling updates. Rebuild on the same cadence, ship the diff (only changed tiles by blake3 mismatch), and you're current.

## What you don't have to think about

A handful of practical concerns that the bundle design eliminates by construction:

- **Cold-cache misses don't crash.** Tiles load lazily through bounded LRU caches per layer. A reverse-geocode query on a fresh bundle in central Asia is the same shape as one in Manhattan: open the right tile via mmap, validate, run the polygon test.
- **Concurrent processes share the page cache.** Two `cairn-serve` instances on the same host — say, blue/green deploy — read the same mmap'd tiles. The OS shares the pages between them. Memory is paid for once.
- **Backups are `rsync`.** A bundle is just files. Snapshotting it, mirroring it through a CDN, dropping it in object storage, transferring it to a phone over USB — all standard tooling, no orchestration.
- **Tampering shows up.** The blake3-per-tile manifest plus a CycloneDX SBOM means a runtime can refuse to load a corrupted or tampered bundle, and operators can prove the bundle's provenance after the fact.

## Where this is heading

A few items on the active roadmap directly extend the offline story:

- **String interning + columnar tile layout.** Today's `Place.tags: Vec<(String, String)>` allocates per-key per-place; a tile-level dictionary plus columnar layout cuts raw tile size another 25–40% before compression.
- **Per-feature sub-archives in admin tiles.** rkyv 0.7's relative pointers cap any single archive at 2 GB; chunking each admin feature into its own offset frame lifts that ceiling without resorting to 64-bit offsets, recovering the on-disk savings.
- **Tighter mobile SDKs.** First-class Swift Package and Kotlin Multiplatform bindings, with a sample iOS / Android app demonstrating the integration shapes above.

## Try it

The CLI is one Cargo install away:

```bash
cargo install --git https://github.com/cairn-geocoder/cairn cairn-build cairn-serve
cairn-build build \
  --osm liechtenstein-latest.osm.pbf \
  --out ./li-bundle \
  --bundle-id li-2026-01
cairn-serve --bundle ./li-bundle --bind 127.0.0.1:8080
curl 'http://127.0.0.1:8080/v1/search?q=vaduz&limit=3'
```

Point a mobile app at the resulting bundle, copy it to a phone, and you have offline geocoding for that region. Scale up to a country, a continent, or the planet by widening the input and adjusting the simplification budget. Same shape, same code, same on-disk format.

The code is at [github.com/cairn-geocoder/cairn](https://github.com/cairn-geocoder/cairn). The article series continues with build pipelines and replication.
