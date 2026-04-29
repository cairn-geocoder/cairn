#!/usr/bin/env bash
# Build a Switzerland Cairn bundle and record footprint to
# results/cairn-import.json:
#   - import wall-clock
#   - bundle disk size
#   - resident memory peak of cairn-build (proxy for RAM headroom needed)
set -euo pipefail
cd "$(dirname "$0")/.."

DATA=data
RESULTS=results
mkdir -p "$RESULTS"

PBF="$DATA/switzerland-latest.osm.pbf"
WOF="$DATA/whosonfirst-data-admin-ch-latest.db"
POSTCODES="$DATA/CH.txt"
OUT="$DATA/cairn-bundle-ch"

if [[ ! -f "$PBF" ]]; then
  echo "missing $PBF — run scripts/00-download.sh first" >&2
  exit 1
fi

# Build cairn-build in release for representative numbers.
echo "==> compiling cairn-build (release)"
cargo build --release -p cairn-build --quiet --manifest-path ../Cargo.toml

CAIRN=../target/release/cairn-build
test -x "$CAIRN" || CAIRN=../../target/release/cairn-build

# Optional sources — drop them silently if the WoF download was rate-
# limited so the script always produces a result.
EXTRA=()
[[ -f "$WOF" ]] && EXTRA+=(--wof "$WOF")
[[ -f "$POSTCODES" ]] && EXTRA+=(--postcodes "$POSTCODES")

rm -rf "$OUT"

# Time + RSS peak. macOS `time -l` emits "maximum resident set size"
# in bytes; Linux uses `/usr/bin/time -v` with KB. Detect + normalize.
LOG=$(mktemp)
TIME_START=$(date +%s)
if [[ "$(uname)" == "Darwin" ]]; then
  /usr/bin/time -l -- "$CAIRN" build \
    --osm "$PBF" \
    "${EXTRA[@]}" \
    --out "$OUT" \
    --bundle-id ch-bench \
    2>"$LOG"
  RSS_BYTES=$(grep "maximum resident set size" "$LOG" | awk '{print $1}')
else
  /usr/bin/time -v -- "$CAIRN" build \
    --osm "$PBF" \
    "${EXTRA[@]}" \
    --out "$OUT" \
    --bundle-id ch-bench \
    2>"$LOG"
  RSS_KB=$(grep "Maximum resident set size" "$LOG" | awk '{print $NF}')
  RSS_BYTES=$((RSS_KB * 1024))
fi
TIME_END=$(date +%s)
IMPORT_SEC=$((TIME_END - TIME_START))

DISK_BYTES=$(du -sk "$OUT" | awk '{print $1 * 1024}')

cat > "$RESULTS/cairn-import.json" <<EOF
{
  "tool": "cairn",
  "bundle_path": "$OUT",
  "import_seconds": $IMPORT_SEC,
  "disk_bytes": $DISK_BYTES,
  "build_rss_bytes_peak": $RSS_BYTES,
  "sources": {
    "osm": "$PBF",
    "wof": "$WOF",
    "postcodes": "$POSTCODES"
  }
}
EOF
echo "==> wrote $RESULTS/cairn-import.json"
cat "$RESULTS/cairn-import.json"
