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

### Phase 6f — Build-time node-coord cache strategies

Unblocks Europe / planet builds on commodity hardware. Today
`cairn-import-osm::load_node_caches` returns a `HashMap<i64, [f64;2]>`
holding every OSM node's lon/lat. Hashbrown overhead is ~48 B/entry,
which scales linearly with node count and dominates build RSS.

Validated against DE actuals: ~430 M nodes × 48 B = ~21 GB peak,
matches the observed 22 GB. Linear projections:

| Region    | Nodes | HashMap RAM | Total projected |
|-----------|------:|------------:|----------------:|
| Europe    | ~2.1 B | ~100 GB | ~105 GB |
| Planet    | ~9 B   | ~430 GB | ~440 GB |

⇒ Mac harness (36 GB) caps out somewhere between DE and Europe
without changes; planet is unreachable on any single box without
mmap-backed lookup.

Ship as `cairn-build --node-cache <strategy>` (default chosen by
PBF size unless overridden):

1. **`inline`** *(default ≤ 5 GB PBF — current behavior).*
   `HashMap<i64, [f64;2]>`. No code change. Keeps the fast path for
   country-scale.

2. **`sorted-vec`** *(default 5–30 GB PBF — first thing to ship).*
   - Replace HashMap with `Vec<(i64, [i32;2])>` sorted by id.
   - Quantize coords f64×2 → i32×2 (~1 cm precision; lossless for
     OSM coord precision of 1e-7 degrees).
   - 16 B/entry vs 48 B → 3× smaller.
   - Pair with a reference-pre-filter pass: scan ways + relations
     first to build `HashSet<i64>` of node IDs actually referenced
     by way centroids / admin polygons / interpolation lines, then
     pass-1 only caches those. Typical OSM dataset has 50-70 % of
     nodes as decorative way vertices that are not standalone refs.
   - Combined: DE ~22 GB → ~3 GB. Europe ~105 GB → ~12 GB. Fits
     36 GB Mac with headroom.
   - ~1-2 days work, all in `cairn-import-osm`.

3. **`flatnode <path>`** *(default ≥ 30 GB PBF — required for
   planet).*
   - Dense `[i32;2]` array indexed by `node_id`. 8 B × max_node_id.
   - Planet ~72 GB on disk; RSS bounded by mmap working set (~2-4
     GB regardless of input). Kernel pages out unused regions.
   - Build becomes I/O-bound not RAM-bound. Same trick Nominatim
     uses (`osm2pgsql --flat-nodes`).
   - Crash-safety: write to `<path>.tmp`, fsync, atomic-rename on
     success. Resumable across runs.
   - ~3-5 days work, careful `unsafe` for the mmap'd write path.

**Order of work:**
1. Land sorted-vec + pre-filter first (#2). Multi-country grid
   bench gets cheaper, Europe becomes possible on rented box.
2. Land flatnode (#3) once #2 proves the build pipeline holds at
   continent scale. Unlocks planet on a $300/mo Hetzner AX102.

Bench harness gains a `node-cache-strategy` knob in
`benchmarks/cairn/build.sh` so the multi-country grid can compare
RSS curves across all three strategies.

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

**Status:** **shipped.** `crates/cairn-import-osm/src/lib.rs` pass 2b2
walks `Element::Relation`, tests `boundary=administrative` (or
`type=multipolygon|boundary` with `boundary=administrative`),
assembles outer + inner rings via endpoint matching
(`assemble_rings`, `assemble_polygons`), maps `admin_level` →
`PlaceKind` (1-2=country, 3-4=region, 5-6=county, 7-8=city,
9=district, 10-12=neighborhood), mints deterministic PlaceId, and
emits `AdminFeature` alongside Place stream. Open-ring relations are
dropped with `skipped_relation_open_ring` counter; missing-outer
relations counted via `skipped_relation_no_outer`.

OSM-only airgap deploys now have full admin reverse PIP coverage
without WoF dep.

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

### Address interpolation — **shipped**
- `interpolate_way_addresses` + `interpolate_addresses` in
  `cairn-import-osm` synthesise `Place(kind=Address)` from
  `addr:interpolation` ways. Cumulative-arc-length distribution
  along the multi-segment polyline so synthetic addresses land at
  the right fraction of the path. `odd`, `even`, `all`, `1` step
  values supported; `alphabetic` skipped. Endpoint addrs come from
  pass-1 `NodeAddrs` cache so no extra read pass is needed.
  5 unit tests (`interpolation_*` in `cairn-import-osm`).

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

## Superiority Plan — 4 Tiers (2026-04-29)

Goal: close incumbent parity gaps, then ship features no other geocoder
has. Sequenced to maximise unblock value per day of work. Estimated
total: ~3 weeks focused work for full superiority claim.

### Tier 1 — Parity (3-5 days; unblocks "drop-in for Pelias")

- **1a. OSM `boundary=administrative` relations → polygons**
  *(Phase 6d)* — **SHIPPED.** Pass 2b2 in cairn-import-osm assembles
  outer+inner rings, maps `admin_level` → `PlaceKind`, emits
  `AdminFeature` alongside Place stream. OSM-only airgap deploys
  covered.
- **1b. `?categories=` filter** — **SHIPPED.** New `categories` STRING
  multi-value tantivy field, `cairn_place::categories_for` derives
  Pelias-style taxonomy from kind + tags, `/v1/search?categories=` AND
  Pelias `?categories=` honor OR-list. 7 unit tests in cairn-place,
  2 in cairn-text, 1 in cairn-api.
- **1c. `boundary.rect.*` viewport bias** — **SHIPPED.**
  `Bbox { min/max lon/lat }` post-filter clip, `boundary.rect.*` query
  params on `/v1/search` + Pelias `/search`. Inverted rect treated
  as no-op (antimeridian crossers). 2 unit tests cairn-text, 3
  integration tests cairn-api.
- **1d. Postcode layer** — **SHIPPED.**
  `cairn_import_geonames::import_postcodes` parses 12-column Geonames
  postcode TSV, emits `Place(kind=Postcode)` with composite "code +
  city" alias for autocomplete. `cairn-build --postcodes` flag wired.
  Pelias-style `postalcode` / `zip` / `localadmin` layer aliases
  normalize to canonical kind tokens. 3 unit tests
  cairn-import-geonames.
- **1e. NDJSON output** — **SHIPPED.** `Accept: application/x-ndjson`
  on `/v1/search` returns one Hit per line with
  `Content-Type: application/x-ndjson`. Default Accept still returns
  the wrapped envelope. 1 integration test.

### Tier 2 — Quality lead (5-7 days; "superior" claim)

- **2a. Phonetic match** — **SHIPPED.** `rphonetic` crate's
  DoubleMetaphone via `phonetic_codes(name)`. Indexed `name_phonetic`
  STRING multi-value field at build time (primary + alternate codes
  per name, ASCII-folded first, CJK skipped). `?phonetic=true` ORs
  encoded query codes against the field. Recovers `Smyth → Smith`,
  `Mueller → Müller`, `Smythsonian → Smithsonian`. 3 unit tests in
  cairn-text.
- **2b. Address interpolation** — **SHIPPED.** OSM
  `addr:interpolation` ways are expanded into `Place(kind=Address)`
  by `interpolate_addresses` (cairn-import-osm). Cumulative-arc-length
  distribution across multi-segment polylines, `odd|even|all|1` step
  modes, `addr:street` resolved from way tag or endpoint nodes,
  `source=osm-interpolation` tag stamped for downstream filtering.
  Closes OA-sparse regions. 5 unit tests.
- **2c. libpostal FFI live wiring** — **SHIPPED.** `cairn-parse` ships
  a heuristic parser by default + `libpostal` cargo feature gating
  `libpostal-sys` FFI bindings (CRF parser + multilingual normalizer,
  ~2 GB model via `LIBPOSTAL_DATA_DIR`). `cairn-api` exposes
  `/v1/parse?q=` (`ParsedAddress` shape: house_number / road / unit /
  postcode / city / state / country) and `/v1/expand?q=` (Vec of
  language-aware permutations). `/v1/search?autoparse=true` runs the
  query through the parser, echoes `parsed` in the response, and
  promotes `categories=postal` when the parser surfaces a postcode
  with no road. cairn-api re-exports the `libpostal` feature so
  `cargo build --features cairn-parse/libpostal` flips the whole
  serve path to the CRF backend. 5 cairn-parse tests + 3 cairn-api
  integration tests covering autoparse and parsed-field echo paths.
- **2d. Diff apply** — **PHASE 1 SHIPPED** (parse + dispatch).
  `bins/cairn-build/src/osc.rs` parses `.osc` / `.osc.gz` minutely
  diffs into `DiffOp` records (create / modify / delete × node / way
  / relation, with tags). `cairn-build replicate-apply --dry-run`
  walks staged diffs, buckets node-place ops by `(level, tile_id)`
  via `TileCoord::from_coord`, and reports a per-action histogram +
  dirty-tile count. Real run advances `last_applied_seq` in
  `replication_state.toml` so the next invocation picks up where it
  left off. Way / relation re-application is logged + counted but
  defers to a full PBF rebuild — those need the way-node graph the
  bundle doesn't persist. 7 osc parser tests + 4 apply tests
  (process_ops bucket logic, dry-run state safety, idempotent rerun,
  way/relation no-op). Phase 2 (in-place tile blob mutation for
  node-only ops) requires OSM-id tag stamping at import + tile
  read/modify/write helpers — scoped, not yet implemented.
- **2e. Bundle federation** — **SHIPPED.** `cairn-serve --bundles
  a/,b/,c/` (or repeated `--bundle`) loads each bundle independently
  and wraps their indices in `FederatedText` / `FederatedAdmin` /
  `FederatedNearest`. Single-bundle deploys short-circuit to direct
  calls (no overhead). Multi-bundle path: text search fans out, hits
  concat-and-sort by score; PIP fans out, sorts finest-first by
  admin level; nearest-K fans out, re-sorts by haversine to query.
  Each Hit's `admin_names` label is rendered inside its own bundle
  before merge so Place-ID collisions across bundles don't pollute
  labels. `/v1/info` reports `bundle_ids: [...]` + `bundle_count`.
  Operational pattern: split a planet into continental shards
  without standing up multiple processes. 9 cairn-api unit tests
  (federated.rs) + 2 integration tests (cross-bundle merge,
  bundle_ids array shape).

### Tier 3 — Ops polish

- **3a. Auth middleware** — **SHIPPED.** `require_api_key` axum
  middleware honors `X-API-Key` header or `?api_key=` query.
  `CAIRN_API_KEY` env var on cairn-serve; absent = open. Health /
  readyz / info bypass the gate so probes don't need a key.
- **3b. ZSTD default for tile blobs** — **SHIPPED.** `cairn-build`
  now writes ZSTD-compressed tile blobs by default. Pass
  `--no-zstd` to opt out (debugging / external pipelines). Bundle
  size drops ~50-70 % with negligible decompress cost at tile load.
- **3c. Tile differential update protocol** — **SHIPPED.**
  `cairn-build diff --old A/ --new B/ --out diff.toml` writes a
  per-file added/changed/removed manifest with blake3 hashes;
  `cairn-build apply --bundle A/ --diff diff.toml --source B/`
  pulls deltas without re-downloading the whole bundle.
- **3d. CHANGELOG.md** — **SHIPPED.** New top-level `CHANGELOG.md`
  documenting Tier 1 + Tier 2 + Tier 3 + Tier 4e batches plus a
  pre-history pointer to the git log.
- **3e. crates.io publish prep** — **SHIPPED.** Every publishable
  crate (`cairn-place`, `-tile`, `-text`, `-spatial`, `-parse`,
  `-api`, `-import-{osm,wof,oa,geonames}`, `-geocoder`) has
  `keywords`, `categories`, and `readme = "../../README.md"` set.
  `cargo package --no-verify` cleanly produces 11 tarballs.
  `scripts/publish-all.sh` walks them in dependency order with
  rate-limit-aware retry. Operator runs `cargo login <token>` then
  the script when the squat-release on bare `cairn` clears.

### Tier 4 — Differentiators ("nobody else has this") — **all SHIPPED**

- **4a. Vector / semantic search** — **SHIPPED (Phase 1).**
  `cairn_text::semantic` provides a deterministic 32-dim character-
  trigram BoW lexical-vector embedding (FNV-1a hashing trick, L2-
  normalized). Pre-computed at index time into a packed `name_vec`
  BYTES field on every Place. `?semantic=true` cosine-reranks the
  candidate set with a thresholded multiplicative boost (≥ 0.35 sim
  → up to 1.6× score). Catches morphological variants (`Vienna →
  Viennese`) and partial matches (`Trisenberg → Triesenberg`) that
  fuzzy + phonetic don't reach. Phase 2 (transformer embeddings via
  `fastembed-rs` + ONNX) is a one-function swap in `embed()` —
  vector shape and rerank pipeline stay the same. 9 unit tests in
  `cairn-text/semantic` + 1 cairn-api integration test.
- **4b. WASM build** — **SHIPPED.** New crate `cairn-wasm` builds
  to `cdylib + rlib` (native tests pass; `wasm32-unknown-unknown`
  target produces a ~250 KB blob via `wasm-pack`). Exposes
  `Autocompleter::new(Vec<u8>)` + `complete(prefix, limit) -> Vec<String>`
  driven by FST prefix iteration — no tantivy in the wasm path so
  the bundle stays small + clean. wasm-bindgen / js-sys deps are
  cfg-gated to `target_arch = "wasm32"` so native consumers stay
  zero-deps. Use cases: country-bundle splash autocomplete, embedded
  form widgets, PWA / offline-first apps. 8 unit tests.
- **4c. Reproducible bundle attestation** — **SHIPPED.** ed25519
  detached signatures over `<bundle>/manifest.toml` (which already
  carries blake3 of every tile / admin / point / text artifact).
  `cairn-build keygen --out keys/` mints a fresh keypair (cairn.key
  mode 0600 + cairn.pub); `cairn-build sign --bundle B/ --key
  keys/cairn.key` writes `manifest.toml.sig` (64 bytes raw);
  `cairn-build sign-verify --bundle B/ --pubkey keys/cairn.pub`
  fails non-zero on tamper / wrong key / missing sig. Pure-Rust
  ed25519-dalek so static-musl stays self-contained. 6 unit tests.
- **4d. SBOM in bundle** — **SHIPPED.** `cairn-build` emits a
  CycloneDX 1.5 SBOM at `<bundle>/sbom.json` listing every
  `Cargo.lock` package (purl + SHA-256) plus every input dataset
  (BLAKE3 hash from `SourceVersion`). `Cargo.lock` is `include_str!`'d
  at compile time so the emitter needs no filesystem access at
  bundle-build time. `/v1/sbom` serves the file with
  `Content-Type: application/vnd.cyclonedx+json` so dependency-track
  / grype / cyclonedx-cli pick it up. 4 unit tests.
- **4e. Query explain endpoint** — **SHIPPED.**
  `/v1/search?explain=true` populates `Hit.explain` with `bm25`,
  `exact_match_boost`, `population_boost`, `language_boost`,
  `geo_bias`, `final_score`. Each rerank stage records its multiplier
  in place. Off by default;
  `skip_serializing_if = Option::is_none` keeps payloads lean for
  normal callers. 2 integration tests in cairn-api. No incumbent
  ships this.

### Recommended sequence

1. **1a** — admin relations (biggest deployment gap)
2. **2a** — phonetic match (quick differentiator)
3. **1b + 1c + 1d** — categories + viewport + postcode (Pelias parity
   batch)
4. **2c** — libpostal (quality jump on non-English)
5. **4e** — explain (debugging + marketing win)
6. **4a** — semantic (only-one-doing-this claim)
7. **4b** — WASM (only-one-doing-this claim)

### Risks

- **HIGH** — libpostal C dep complicates static-musl build. Mitigation:
  feature-flagged + alt static linkage path.
- **HIGH** — semantic embeddings inflate bundle ~30-50 %. Mitigation:
  optional bundle artifact, lazy-loaded.
- **MEDIUM** — OSM relation ring assembly bugs on broken multipolygons.
  Mitigation: drop-with-warn + counter in `/metrics`.
- **MEDIUM** — WASM tantivy port — tantivy isn't wasm-clean. Fallback:
  lightweight FST + ngram only (no full BM25).
- **LOW** — crates.io squat-release email blocks public crate publish.

## Out of scope

- Multi-tenant SaaS isolation.
- Cluster orchestration.
- Routing or isochrones (Valhalla territory).
