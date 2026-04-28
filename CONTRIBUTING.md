# Contributing to Cairn

Cairn is an offline, airgap-ready geocoder written in Rust. Thanks for
considering a contribution.

## Ground rules

- Cairn is a single static binary + a flat-file bundle. Anything that
  introduces a long-running daemon, network requirement, or external
  database at query time is out of scope.
- Every feature must work without internet access once a bundle is built.
- Bundles must be reproducible: same inputs → byte-identical `manifest.toml`
  and tile blobs (modulo timestamps). PRs that change tile encoding must
  bump the manifest schema version.

## Workspace layout

| Path | Purpose |
|------|---------|
| `crates/cairn-place` | Place / Coord / PlaceId types |
| `crates/cairn-tile` | Tile coords, manifest, on-disk tile format |
| `crates/cairn-text` | tantivy-backed text + autocomplete index |
| `crates/cairn-spatial` | R*-tree + per-tile lazy admin / nearest layers |
| `crates/cairn-parse` | libpostal FFI (feature-gated) |
| `crates/cairn-import-osm` | OSM PBF reader (places, ways, relations) |
| `crates/cairn-import-wof` | WhosOnFirst SQLite reader |
| `crates/cairn-import-oa` | OpenAddresses CSV reader |
| `crates/cairn-import-geonames` | Geonames TSV reader |
| `crates/cairn-api` | axum HTTP handlers |
| `bins/cairn-build` | offline bundle builder CLI |
| `bins/cairn-serve` | runtime HTTP server |

## Local development

```sh
# Build everything
cargo build

# Run all tests
cargo test --workspace

# Run benches (skip in CI)
cargo bench -p cairn-spatial
cargo bench -p cairn-text

# Smoke-build the Liechtenstein bundle
cargo run -p cairn-build -- build \
    --osm fixtures/liechtenstein-latest.osm.pbf \
    --out /tmp/cairn-li \
    --bundle-id li-dev
```

The fixtures directory is gitignored; download a fresh
`liechtenstein-latest.osm.pbf` from Geofabrik for a small reproducible
dataset.

## Pull requests

1. Fork + branch from `main`.
2. Keep the change scoped — one feature or fix per PR.
3. Run `cargo fmt`, `cargo clippy --workspace --all-targets -- -D warnings`,
   and `cargo test --workspace` before pushing.
4. Open a PR with:
   - A one-paragraph description of the user-visible change.
   - A short note on what you tested. "Bundled Liechtenstein and ran the
     existing reverse fixture queries" is fine.
   - A roadmap pointer (`ROADMAP.md` phase / item) if applicable.

## What we will probably push back on

- Adding a runtime database (Postgres, Redis, Elastic). Cairn is flat
  files. If you need indexed queryable data, the answer is "another tile
  blob".
- New default dependencies in the hot path. tantivy and rstar are the
  only real heavyweights and that's deliberate.
- Features that only work with internet access. Cairn must boot from a
  copied bundle directory with `--airgap` semantics by default.

## Reporting bugs

Open an issue with:
- `cairn-build --version`
- The exact command that failed.
- The bundle source URL or fixture (or the smallest PBF that reproduces).
- The full error including any `RUST_LOG=debug` output if helpful.

## License

By contributing you agree your work is dual-licensed under MIT or
Apache-2.0 at the user's option, the same as the rest of the project.
