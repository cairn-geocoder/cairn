# Cairn Roadmap

Tracks shipped phases and deferred work.

## Shipped

### Phase 0 — Workspace scaffold
- 11-crate workspace, `Place` model, tile coord math, license + CI.

### Phase 1 — Tile bundle
- `rkyv`-archived tile blobs, 16-byte aligned header.
- `manifest.toml` with blake3 per-tile hashes.
- OSM PBF importer (place=* nodes only).
- `cairn-build build` + `verify` end-to-end on Liechtenstein.

### Phase 2 — Text + admin
- tantivy index per bundle (multi-value name field, prefix-ngram tokenizer).
- WhosOnFirst SPR ingestion + multilingual `names` table join.
- `/v1/search?q=` forward + autocomplete via axum.

### Phase 2.5 — Search polish
- Layer filter (`?layer=country,city`).
- Fuzzy edit distance via FuzzyTermQuery union.
- Geo-bias rerank (over-fetch + haversine blended score).
- WoF parent-chain walk → `admin_path`.

### Phase 3 — Reverse + spatial
- `cairn-spatial::AdminLayer` (bincode IO) + `AdminIndex` (R*-tree of bbox
  + `geo::Contains` PIP).
- WoF `geojson` table → `MultiPolygon`s.
- `/v1/reverse?lat=&lon=` returns finest containing polygon first.

### Phase 4 — Streets, POIs, addresses, fallback
- 2-pass OSM PBF (node coord cache → ways).
- POI keys (amenity/shop/tourism/office/leisure/historic/craft/emergency/
  healthcare).
- Named highway ways → Street places.
- OpenAddresses CSV reader → Address places.
- `cairn-spatial::NearestIndex` (R*-tree of centroids) for nearest
  fallback when PIP is empty.

### Phase 4.5 — Polish
- `/v1/structured` endpoint (field-by-field search with layer hint).
- `cairn-build verify` covers tiles + text + admin + points.
- README + ROADMAP.

### Phase 5 — bbox extract + manifest integrity
- `cairn-build extract --bundle … --bbox …` real implementation: copies
  intersecting tiles, filters AdminLayer + PointLayer by bbox, writes a
  fresh manifest with recomputed hashes.
- `Manifest.admin` / `Manifest.points` carry blake3 + size + count.
- `cairn-build verify` recomputes hashes and refuses mismatches.

### Phase 6a — Geonames + /metrics
- `cairn-import-geonames` parses the standard Geonames cities*.txt /
  allCountries.txt TSV, emits City / Neighborhood Places with population
  + ISO3166-1 tags. 3 unit tests.
- `cairn-api` `/metrics` endpoint emits Prometheus 0.0.4 text. Counters
  for search / autocomplete / structured / reverse outcomes; gauges for
  uptime, admin feature count, point count; bundle_id label so a
  scrape job can distinguish bundles. Hand-rolled (no `prometheus`
  crate dep).

### Phase 6b — PVC bundle pipeline
- Runtime image now ships cairn-build + curl + bzip2 alongside
  cairn-serve, so the same image can run a bundle-build Kubernetes Job.
- `geo_cloud/infra/kubernetes/cairn/bundle-pipeline.yaml`: PVC + Job +
  ConfigMap script. Configurable env vars (BUNDLE_ID, OSM_URL, WOF_URL,
  OA_URL, GEONAMES_URL).
- `deployment-pvc.yaml` overlay mounts the PVC RO at /bundle, shadowing
  the bundle baked into the image. Lets cairn.kaldera.dev switch to
  Switzerland / Germany / planet without an image rebuild.

## Deferred

### Phase 6c — Per-tile spatial partitioning + mmap rkyv

**Status:**
- Per-tile partitioning + lazy load via `OnceLock` + LRU eviction:
  **shipped** (commit `aa4142b`).
- Differential tile updates (`cairn-build diff`/`apply`):
  **shipped** (commit `78d8fa1`).
- libpostal FFI bindings (feature-gated unsafe calls): **shipped**
  (commit pending). Build prerequisites: `libpostal` C library +
  `libpostal_data download all` for the ~2 GB language model.
- rkyv-archived AdminLayer: **shipped** (`cairn_spatial::archived`).
  Flat `ArchivedAdminFeature` with `polygon_rings:
  Vec<Vec<Vec<[f64;2]>>>` and precomputed `polygon_bboxes` for an O(1)
  prefilter; round-trip helpers; 16-byte aligned write/read pair
  sharing cairn-tile's header layout. Custom `pip_archived` ray-casts
  directly on the flat ring vertices — diff-tested against
  `geo::Contains` across a 17×17 probe grid; benchmarks (256-vertex
  polygon): **128 ns inside (vs 172 ns geo, 25 % faster), 2.1 ns
  outside-bbox (vs 194 ns, 92× faster)**.

  As of commit `d33444d`+1 the `AdminIndex` runtime path reads rkyv
  blobs via memmap2 (`unsafe Mmap::map`) + `check_archived_root` and
  routes PIP through `pip_archived`; `geo::Contains` is no longer in
  the hot path. Manifest bumped to `schema_version = 3`. PointLayer
  stays bincode (rkyv was 68 % larger on String-heavy points; linear
  scan doesn't gain from zero-copy).

  Follow-up: full zero-copy via `AdminTileArchive` + `pip_archived_ref`
  is now wired. The slot holds a validated `Arc<AdminTileArchive>` (own
  `AlignedVec` for eager builds, mmap'd file for disk tiles, with a
  mmap-misaligned-payload fallback that copies into an `AlignedVec`).
  `archived()` returns a `&Archived<ArchivedAdminLayer>` via unchecked
  `archived_root` — sound because `check_archived_root` ran once at
  construction. PIP iterates the archived form directly; only winners
  hydrate to `AdminFeature` via `Deserialize` at return time. Bench
  shows pip_archived_ref ties pip_archived (~92 ns inside, ~1.5 ns
  outside-bbox); the real win is no per-tile heap allocation at first
  PIP touch.

Today `spatial/admin.bin` and `spatial/points.bin` are bundle-wide
single bincode blobs read whole at startup. At country scale this
costs <250 MB RAM; at planet scale it's a non-starter.

**Plan:**
1. Define a flat, rkyv-friendly mirror of `AdminFeature` —
   `polygon_rings: Vec<Vec<Vec<[f64; 2]>>>` instead of
   `MultiPolygon`. Round-trip helpers convert at write/read.
2. Build-time: bucket AdminFeatures into `(level, tile_id)` keys
   based on polygon-bbox / tile-bbox intersection (a polygon spanning
   tiles is replicated into each). Emit
   `spatial/admin/<level>/<bucket>/<id>.bin` per non-empty tile.
   Same 16-byte aligned header pattern the tile blobs use.
3. Runtime: `AdminIndex` becomes an R*-tree over per-tile bboxes
   plus a `Vec<TileSlot>` where each slot's archive is loaded lazily
   via `OnceLock<Mmap>`. PIP touches only tiles intersecting the
   query.
4. LRU eviction on a configurable byte budget once memory becomes
   relevant (planet scale only).
5. Same treatment for PointLayer.
6. Manifest gains a `[[admin_tiles]]` and `[[point_tiles]]` array
   alongside `[[tiles]]`.

Useful when the bundle exceeds ~200 MB of polygons. Skip until then.

### Phase 6d — OSM `boundary=administrative` relations → polygons

**Status:** scoped, not implemented.

WhosOnFirst supplies admin polygons today, but OSM-only deployments
(no internet to fetch WoF) lose admin reverse PIP entirely.

**Plan:**
1. Three-pass PBF (we already cache node coords in pass 1):
   - Pass 2: cache `way_id → Vec<NodeId>`.
   - Pass 3: iterate relations with `type=multipolygon` AND
     `boundary=administrative`. Group members by role (`outer` /
     `inner`).
2. Ring assembly: connect `outer` member ways into closed rings via
   endpoint matching. Open rings = drop with a warning.
3. Build `MultiPolygon`. Map `admin_level` → PlaceKind (2=country,
   4=region, 6=county, 8=city, 10=neighborhood).
4. Emit `AdminFeature`s alongside the existing OSM Place stream.

Tractable but multi-day work. Best after Phase 6c so the per-tile
admin layer can absorb the volume jump.

### Phase 6e — libpostal FFI

**Status:** feature flag scaffolding only.

`cairn-parse` exposes `parse(input) -> ParsedAddress` and
`expand(input) -> Vec<String>`. Today both are no-op stubs returning
`NotInitialized`. Free-text queries land in `/v1/search` as bag-of-words
which works for English `"Vaduz"` but degrades on
`"calle de alcalá 12, madrid"` etc.

**Plan:**
1. `cairn-parse` cargo feature `libpostal` (already declared in
   Cargo.toml).
2. Vendor `libpostal-sys` + the compiled C source via `cc`.
3. Ship the ~2 GB language model as a separate OCI image
   `ghcr.io/cairn-geocoder/libpostal-data` mounted as an init-container
   that copies into a PVC for the cairn-serve pod.
4. Wire `cairn-api/v1/structured` (today the structured endpoint
   already takes pre-parsed fields) and add a `/v1/parse` endpoint
   that exposes the parser to clients.

~2 days of focused work. Sequenced after 6c (per-tile spatial) so the
bundle layout doesn't shift mid-effort.

### libpostal FFI (legacy entry — superseded by Phase 6e above)
*(kept for reference; see Phase 6e for the current plan)*

### Address interpolation
- OSM `addr:interpolation` ways need endpoint nodes + step.
- Generates synthetic Address places on the fly in the importer.
- Useful for OA-sparse regions (most non-US territory).

### OSM admin relations
- `boundary=administrative` relations carry the canonical OSM admin
  geometry.
- Currently relies on WoF polygons only; OSM-only deployments lose admin
  PIP coverage.
- Requires relation member resolution + ring assembly (multi-step).

### Per-tile spatial partitioning
- Today `admin.bin` and `points.bin` are single bundle-wide blobs.
- For planet-scale, partition per `(level, tile_id)` so reverse only touches
  the tiles intersecting the query region.
- Drop-in replacement for `AdminLayer` / `PointLayer` IO.

### mmap-aligned spatial blob
- Currently bincode + full read at startup.
- Phase 6: rkyv-archived AdminLayer with the same 16-byte aligned header
  pattern that tile blobs use → zero-copy spatial access.

### Region extracts
- `cairn-build extract --bbox=` is stub.
- Implementation: copy tile subtree intersecting bbox + filtered tantivy
  segments + filtered AdminLayer / PointLayer.
- Output: `region.tar` deployable as a sub-bundle.

### Live OSM diff replication
- Mirror `osmosis` / `pyosmium` minutely diffs to a local file store.
- Apply diffs to tile-scoped reindex (only tiles touching changed objects).
- Out of scope for airgap-first MVP but useful for mixed-mode deployments.

### Distribution
- Reserved `cairn-*` crate names on crates.io (Phase 0 partial — 5/11
  reserved, 6 pending rate-limit window).
- Bare `cairn` name still squat-held; squat-release email drafted.
- musl static binary for `cairn-build` + `cairn-serve` for portable
  deploys.

### Hardening
- `cairn-build extract` real implementation.
- Manifest schema bump to track text/spatial blake3 hashes (currently
  only tiles do).
- `--zstd` flag on tile blobs for over-the-wire shipping.
- Differential update protocol (replace tile X.bin in place).
- Authentication (API key) middleware for `cairn-serve`.

## Out of scope

- Multi-tenant SaaS isolation.
- Cluster orchestration.
- Vector / semantic search ("near coffee shops" queries).
- Routing or isochrones (Valhalla territory).
