# Changelog

All notable changes to Cairn are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the
versions follow [Semantic Versioning](https://semver.org/) once we
hit `0.1`.

The bundle on-disk layout is governed by `Manifest.schema_version`
(currently `3`); any breaking change to that schema is called out
below.

## 0.1.0 — 2026-04-29

First public beta. Pelias drop-in parity (Tier 1) + quality lead
(Tier 2) + ops polish (Tier 3) + differentiators (Tier 4) all
shipped. Reproducible Switzerland + Germany benchmarks land Cairn
**6–43× faster at p99**, **4–81× higher peak RPS**, and
**5–27× smaller hot RSS** than every incumbent on the same input.
Bundle on-disk schema is `Manifest.schema_version = 3`.

Companion [`cairn-cloud`](https://github.com/cairn-geocoder/cairn-cloud)
repo ships Helm chart, Kustomize overlays, Terraform modules
(AWS / GCP / Nomad), Grafana dashboard, and Prometheus alerting
rules at chart version `0.1.0`.

### Added — Tier 1 parity batch (Pelias drop-in)
- `?categories=` filter on `/v1/search` and Pelias `/search`. Pelias-
  style taxonomy (`health`, `food`, `accommodation`, `attraction`,
  `commercial`, `transport`, …) derived from `Place.kind` + OSM tags
  by `cairn_place::categories_for`. Indexed as a multi-value
  tantivy field; OR semantics across the comma list.
- `boundary.rect.{min,max}_{lat,lon}` viewport bias on `/v1/search`
  and Pelias `/search`. All four params required together; missing
  any = ignored. Inverted rect (`min_lon > max_lon`) treated as
  no-op.
- Postcode layer via `cairn_import_geonames::import_postcodes`
  parsing the 12-column Geonames postcode TSV. `cairn-build
  --postcodes <PATH>` flag wired. Pelias-style layer aliases
  (`postalcode`, `zip`, `zipcode`, `venue`, `locality`,
  `macroregion`, `localadmin`) now normalize to canonical Cairn
  kind tokens.
- `Accept: application/x-ndjson` on `/v1/search` returns one Hit per
  line with `Content-Type: application/x-ndjson`.

### Added — Tier 2 quality lead
- DoubleMetaphone phonetic matching via `rphonetic`. New
  `name_phonetic` STRING multi-value field; `?phonetic=true` ORs
  encoded query codes against it. Recovers `Smyth → Smith`,
  `Mueller → Müller`, `Smythsonian → Smithsonian`. CJK skipped
  (Latin-script encoder).
- libpostal FFI live wiring documented as shipped. `/v1/parse?q=`
  + `/v1/expand?q=` endpoints; `?autoparse=true` on `/v1/search`
  promotes a postcode-only query to `categories=postal`. `cargo
  build --features cairn-parse/libpostal` flips the serve path to
  the CRF backend (`LIBPOSTAL_DATA_DIR` required at runtime).
- Address interpolation from OSM `addr:interpolation` ways with
  cumulative-arc-length distribution along multi-segment polylines.
  `odd | even | all | 1` step modes; `alphabetic` skipped. Synthetic
  Place(kind=Address) rows tagged `source=osm-interpolation`.
- OSC (`.osc.gz`) diff parser (`bins/cairn-build/src/osc.rs`) and
  new `cairn-build replicate-apply [--max N] [--dry-run]` CLI.
  Walks staged diffs, buckets node-place ops by `(level, tile_id)`,
  prints a per-action histogram + dirty-tile count, and advances
  `last_applied_seq` in `replication_state.toml`. Way / relation
  re-application stays out of scope (bundle doesn't persist the
  way-node graph).
- Bundle federation. `cairn-serve --bundles a/,b/,c/` (or repeated
  `--bundle`) loads each bundle independently and wraps their
  indices in `FederatedText` / `FederatedAdmin` / `FederatedNearest`.
  Single-bundle deploys short-circuit to direct calls. `/v1/info`
  reports `bundle_ids[]` + `bundle_count`. Operational pattern:
  split a planet into continental shards inside one process.

### Added — Tier 4 differentiators
- `?explain=true` on `/v1/search` populates `Hit.explain` with
  `bm25`, `exact_match_boost`, `population_boost`, `language_boost`,
  `geo_bias`, `final_score`. Each rerank stage records its
  multiplier in place. No incumbent (Pelias / Photon / Nominatim /
  Mimir) ships this.

### Added — Tier 3 ops polish
- ZSTD compression of tile blobs is now the default. Pass
  `--no-zstd` to disable (debugging / external pipelines). Bundle
  size drops ~50-70% with negligible decompress overhead at tile
  load.
- `CHANGELOG.md` added (this file).

### Verified shipped (ROADMAP cleanup)
- OSM `boundary=administrative` relations → admin polygons (Phase
  6d). Outer + inner ring assembly via endpoint matching;
  `admin_level` → `PlaceKind`; deterministic PlaceIds. OSM-only
  airgap deploys covered without WoF.
- `cairn-build diff` / `apply` differential update protocol.
- API key middleware (`require_api_key` + `CAIRN_API_KEY`).
- `?lang=` language-aware ranking (1.5× boost) with `name_phonetic`
  / `name_translit` cross-script support.
- Per-IP token-bucket rate limit + CIDR allowlist for trusted
  reverse proxies.
- rkyv mmap zero-copy admin tile archive (`AdminTileArchive` +
  `pip_archived_ref`).

## Pre-history (before this CHANGELOG)

The git log on `main` is the authoritative history; the entries
above cover the post-`39920ed` window during which the 4-tier
superiority plan (in `ROADMAP.md`) was executed end-to-end.
Highlights from earlier development:

- **Phase 0-4.5** — workspace scaffold; rkyv tile blobs; tantivy
  text index with CJK + transliteration analyzers; WhosOnFirst SPR
  ingestion + multilingual names; admin reverse PIP via R*-tree;
  `/v1/search`, `/v1/reverse`, `/v1/structured`, `/v1/place`,
  `/v1/layers`, `/v1/info`, `/healthz`, `/readyz`, `/metrics`
  endpoints; OpenAddresses + Geonames importers;
  `cairn-build verify` covers tiles + text + admin + points.
- **Phase 5-6b** — bbox extract; manifest schema integrity hashes;
  PVC bundle pipeline for Kubernetes Jobs.
- **Security** — Trivy CVE scan pipeline + SECURITY.md + README
  badges + live homepage CVE table refreshed daily.
