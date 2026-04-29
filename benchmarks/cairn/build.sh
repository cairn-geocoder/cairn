#!/usr/bin/env bash
# Build a Cairn bundle for a given country. Times the build, captures
# size + memory usage, writes to results/cairn-build-<country>.json.
#
# usage: ./cairn/build.sh [country] (default: switzerland)
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE"

COUNTRY="${1:-switzerland}"
case "$COUNTRY" in
  switzerland)   CC=CH ;;
  liechtenstein) CC=LI ;;
  germany)       CC=DE ;;
  france)        CC=FR ;;
  italy)         CC=IT ;;
  austria)       CC=AT ;;
  *)             CC=$(printf '%s' "$COUNTRY" | head -c2 | tr 'a-z' 'A-Z') ;;
esac

ROOT="$(cd "$HERE/../.." && pwd)"
DATA="$HERE/../data"
RESULTS="$HERE/../results"
BUNDLE="$HERE/bundle-${COUNTRY}"

mkdir -p "$RESULTS"

PBF="$DATA/${COUNTRY}-latest.osm.pbf"
if [ ! -f "$PBF" ]; then
  echo "ERROR: $PBF missing — run ./data/download.sh $COUNTRY" >&2
  exit 1
fi

cd "$ROOT"
echo ">> cargo build --release -p cairn-build"
START=$(date +%s)
cargo build --release -p cairn-build 2>&1 | tail -3
COMPILE_S=$(( $(date +%s) - START ))

CAIRN_BUILD="$ROOT/target/release/cairn-build"
POSTCODES_ARG=""
[ -f "$DATA/${CC}.txt" ] && POSTCODES_ARG="--postcodes $DATA/${CC}.txt"

rm -rf "$BUNDLE"
echo ">> cairn-build build ($COUNTRY, postcodes=${CC})"
START=$(date +%s)
/usr/bin/time -l "$CAIRN_BUILD" build \
  --osm  "$PBF" \
  $POSTCODES_ARG \
  --out  "$BUNDLE" \
  --bundle-id "${COUNTRY}-bench" \
  --simplify-meters 100 \
  2> "$RESULTS/cairn-build-${COUNTRY}.time.txt" \
  || { echo "build failed"; cat "$RESULTS/cairn-build-${COUNTRY}.time.txt"; exit 1; }
BUILD_S=$(( $(date +%s) - START ))

PEAK_RSS=$(awk '/maximum resident set size/ {print $1}' "$RESULTS/cairn-build-${COUNTRY}.time.txt" || echo 0)
DISK_BYTES=$(du -sk "$BUNDLE" | awk '{print $1*1024}')
DISK_MB=$(( DISK_BYTES / 1024 / 1024 ))

cat > "$RESULTS/cairn-build-${COUNTRY}.json" <<JSON
{
  "engine": "cairn",
  "country": "$COUNTRY",
  "phase": "build",
  "host": "$(uname -n)",
  "arch": "$(uname -m)",
  "compile_seconds": $COMPILE_S,
  "build_seconds": $BUILD_S,
  "peak_rss_bytes": $PEAK_RSS,
  "bundle_disk_bytes": $DISK_BYTES,
  "bundle_disk_mb": $DISK_MB,
  "bundle_id": "${COUNTRY}-bench",
  "input_pbf_bytes": $(stat -f%z "$PBF" 2>/dev/null || stat -c%s "$PBF")
}
JSON

# Backwards-compat — switzerland keeps the unsuffixed file too.
if [ "$COUNTRY" = "switzerland" ]; then
  cp "$RESULTS/cairn-build-${COUNTRY}.json" "$RESULTS/cairn-build.json"
fi

echo "OK build_s=${BUILD_S}s peak_rss=${PEAK_RSS}B bundle=${DISK_MB}MB"
echo "wrote $RESULTS/cairn-build-${COUNTRY}.json"
