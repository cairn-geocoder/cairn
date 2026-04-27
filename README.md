# Cairn

Offline, airgap-ready geocoder written in Rust.

> Cairn (n.) — a pile of stones marking a trail.
> Each tile is a stone. Drop the pile on disk. The geocoder reads it.

## Status

Pre-alpha. Phase 0 scaffold.

## Goals

- Forward geocoding, reverse geocoding, autocomplete
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

1. **Builder** (`cairn build`) — ingests OSM PBF, WhosOnFirst, OpenAddresses, Geonames, Cosmogony into per-tile `rkyv` blobs + tantivy text index + FST admin lookup. Emits `bundle.tar` with `manifest.toml`.
2. **Bundle** — flat directory of immutable mmap-ready files.
3. **Server** (`cairn serve`) — `axum` HTTP API that mmaps the bundle. No DB, no daemon.

## Tile model

64-bit `PlaceId`: `[level: 3 | tile_id: 22 | local_id: 39]`

| Level | Cell size | Contents |
|---|---|---|
| 0 | 4° × 4° | Countries, big regions |
| 1 | 1° × 1° | Cities, postcodes |
| 2 | 0.25° × 0.25° | Streets, addresses, POIs |

## Workspace

```
crates/
  cairn-geocoder/         umbrella, re-exports
  cairn-place/            Place, PlaceId, schema
  cairn-tile/             tile coords, manifest
  cairn-text/             tantivy + FST
  cairn-spatial/          rstar, point-in-polygon
  cairn-parse/            libpostal FFI
  cairn-import-osm/       OSM PBF
  cairn-import-wof/       WhosOnFirst SQLite
  cairn-import-oa/        OpenAddresses
  cairn-import-geonames/  Geonames TSV
  cairn-api/              axum handlers
bins/
  cairn-build/            CLI build pipeline
  cairn-serve/            HTTP runtime
```

## Build

```bash
cargo check --workspace
cargo test --workspace
cargo build --release -p cairn-serve -p cairn-build
```

## License

Dual-licensed: MIT OR Apache-2.0. Pick whichever fits.

## References

- [Valhalla tile architecture](https://valhalla.github.io/valhalla/tiles/)
- [OpenStreetMap](https://www.openstreetmap.org)
- [WhosOnFirst](https://whosonfirst.org)
- [OpenAddresses](https://openaddresses.io)
- [libpostal](https://github.com/openvenues/libpostal)
- [tantivy](https://github.com/quickwit-oss/tantivy)
