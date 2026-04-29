#!/usr/bin/env bash
# Pull country source data once. Idempotent. Country slug as $1
# (default switzerland). Slug → Geofabrik path + Geonames ISO2.

set -euo pipefail
cd "$(dirname "$0")"

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

PBF="${COUNTRY}-latest.osm.pbf"
GEOFABRIK_PATH="europe/${COUNTRY}-latest.osm.pbf"

if [ ! -f "$PBF" ]; then
  echo ">> downloading $PBF"
  curl -fSL --retry 3 -o "$PBF.tmp" \
    "https://download.geofabrik.de/${GEOFABRIK_PATH}"
  mv "$PBF.tmp" "$PBF"
fi
PBF_SIZE=$(stat -f%z "$PBF" 2>/dev/null || stat -c%s "$PBF")
PBF_SHA=$(shasum -a 256 "$PBF" | awk '{print $1}')
echo "$PBF size=$PBF_SIZE sha256=$PBF_SHA" > "${COUNTRY^^}_VERSION.txt" 2>/dev/null \
  || echo "$PBF size=$PBF_SIZE sha256=$PBF_SHA" > "$(printf '%s' "$COUNTRY" | tr 'a-z' 'A-Z')_VERSION.txt"

# Backwards-compat for the existing benchmarks that hardcode SWITZERLAND_VERSION.txt.
if [ "$COUNTRY" = "switzerland" ]; then
  cp "$PBF" switzerland-latest.osm.pbf 2>/dev/null || true
  echo "switzerland-latest.osm.pbf size=$PBF_SIZE sha256=$PBF_SHA" > SWITZERLAND_VERSION.txt
fi

# Geonames postcodes for the country (when available).
if [ ! -f "${CC}.zip" ]; then
  if curl -fSL --retry 3 -o "${CC}.zip.tmp" \
       "https://download.geonames.org/export/zip/${CC}.zip" 2>/dev/null; then
    mv "${CC}.zip.tmp" "${CC}.zip"
  else
    echo "WARN: Geonames doesn't host postcodes for ${CC}; skipping"
    rm -f "${CC}.zip.tmp"
  fi
fi
if [ -f "${CC}.zip" ] && [ ! -f "${CC}.txt" ]; then
  unzip -p "${CC}.zip" "${CC}.txt" > "${CC}.txt"
fi

# cities1000 for the global query set base.
if [ ! -f cities1000.zip ]; then
  curl -fSL --retry 3 -o cities1000.zip.tmp \
    "https://download.geonames.org/export/dump/cities1000.zip"
  mv cities1000.zip.tmp cities1000.zip
fi
if [ ! -f cities1000.txt ]; then
  unzip -p cities1000.zip cities1000.txt > cities1000.txt
fi

echo ">> done: $COUNTRY ($CC)"
ls -lh "$PBF" "${CC}.txt" cities1000.txt 2>&1 | head
