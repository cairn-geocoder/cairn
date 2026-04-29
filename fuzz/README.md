# cairn-fuzz

Fuzzing harness for Cairn's panic-sensitive paths.

## Targets

| Target | Path covered | Why it matters |
|---|---|---|
| `fuzz_manifest` | `cairn_tile::Manifest` (TOML deserialize) | Operator-supplied at bundle load; must error, never panic |
| `fuzz_admin_archive` | `AdminTileArchive::from_aligned` (rkyv validate) | Every tile is mmap'd at serve time — panic = serve crash |
| `fuzz_trigram_extract` | `cairn_text::trigram::extract_*` | Runs at every fuzzy query; pathological UTF-8 must be safe |
| `fuzz_edit_distance` | Myers + Wagner-Fischer parity | Bit shifts at 64-char boundary; differential property test |

## Local run

```bash
cargo install cargo-fuzz       # one-time
cd fuzz
cargo +nightly fuzz run fuzz_manifest -- -max_total_time=60
cargo +nightly fuzz run fuzz_admin_archive -- -max_total_time=60
cargo +nightly fuzz run fuzz_trigram_extract -- -max_total_time=60
cargo +nightly fuzz run fuzz_edit_distance -- -max_total_time=60
```

## CI integration

`.github/workflows/fuzz.yml` runs each target for 60s on push to `main`.
A nightly cron extends the run to 30 minutes per target. Crashes land
in `fuzz/artifacts/<target>/` and fail the workflow.

## Triage

When a target crashes, copy the artifact bytes into a regression test
in the relevant crate's `tests/` so the fix has a permanent guard.
