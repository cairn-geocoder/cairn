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

## Deferred

### libpostal FFI
- ~50 MB C source vendoring + ~2 GB compiled language model.
- Cross-compile complexity (musl, aarch64, etc.).
- Without it, address parsing is naive concatenation. Acceptable for
  structured queries; not for free-text address parsing.
- Plan: `libpostal` cargo feature, vendored sources via `libpostal-sys`,
  model files distributed as a separate `data/libpostal/` artifact.

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
