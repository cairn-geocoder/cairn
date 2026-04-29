#!/usr/bin/env bash
# Build a Cairn Switzerland bundle. Times the build, captures size
# + memory usage, writes to results/cairn-build.json.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE"

ROOT="$(cd "$HERE/../.." && pwd)"
DATA="$HERE/../data"
RESULTS="$HERE/../results"
BUNDLE="$HERE/bundle"

mkdir -p "$RESULTS"

if [ ! -f "$DATA/switzerland-latest.osm.pbf" ]; then
  echo "ERROR: ../data/download.sh first" >&2
  exit 1
fi

cd "$ROOT"
echo ">> cargo build --release -p cairn-build"
START=$(date +%s)
cargo build --release -p cairn-build 2>&1 | tail -3
COMPILE_S=$(( $(date +%s) - START ))

CAIRN_BUILD="$ROOT/target/release/cairn-build"

rm -rf "$BUNDLE"
echo ">> cairn-build build (Switzerland)"
START=$(date +%s)
/usr/bin/time -l "$CAIRN_BUILD" build \
  --osm  "$DATA/switzerland-latest.osm.pbf" \
  --postcodes "$DATA/CH.txt" \
  --out  "$BUNDLE" \
  --bundle-id "switzerland-bench" \
  --simplify-meters 100 \
  2> "$RESULTS/cairn-build.time.txt" \
  || { echo "build failed"; cat "$RESULTS/cairn-build.time.txt"; exit 1; }
BUILD_S=$(( $(date +%s) - START ))

# macOS `time -l` reports peak resident set size in bytes.
PEAK_RSS=$(awk '/maximum resident set size/ {print $1}' "$RESULTS/cairn-build.time.txt" || echo 0)

DISK_BYTES=$(du -sk "$BUNDLE" | awk '{print $1*1024}')
DISK_MB=$(( DISK_BYTES / 1024 / 1024 ))

cat > "$RESULTS/cairn-build.json" <<JSON
{
  "engine": "cairn",
  "phase": "build",
  "host": "$(uname -n)",
  "compile_seconds": $COMPILE_S,
  "build_seconds": $BUILD_S,
  "peak_rss_bytes": $PEAK_RSS,
  "bundle_disk_bytes": $DISK_BYTES,
  "bundle_disk_mb": $DISK_MB,
  "bundle_id": "switzerland-bench",
  "input_pbf_bytes": $(stat -f%z "$DATA/switzerland-latest.osm.pbf" 2>/dev/null || stat -c%s "$DATA/switzerland-latest.osm.pbf")
}
JSON

echo "OK build_s=${BUILD_S}s peak_rss=${PEAK_RSS}B bundle=${DISK_MB}MB"
echo "wrote $RESULTS/cairn-build.json"
