#!/usr/bin/env bash
# Pull Switzerland source data once. Idempotent — reruns skip
# already-downloaded files. Pins the snapshot so every engine sees
# the same input.
set -euo pipefail
cd "$(dirname "$0")"

# Switzerland PBF (Geofabrik).
PBF=switzerland-latest.osm.pbf
if [ ! -f "$PBF" ]; then
  echo ">> downloading $PBF"
  curl -fSL --retry 3 -o "$PBF.tmp" \
    "https://download.geofabrik.de/europe/switzerland-latest.osm.pbf"
  mv "$PBF.tmp" "$PBF"
fi
PBF_SIZE=$(stat -f%z "$PBF" 2>/dev/null || stat -c%s "$PBF")
PBF_SHA=$(shasum -a 256 "$PBF" | awk '{print $1}')
echo "switzerland-latest.osm.pbf size=$PBF_SIZE sha256=$PBF_SHA" > SWITZERLAND_VERSION.txt
echo ">> pinned: SWITZERLAND_VERSION.txt"

# Geonames CH (postcodes + cities).
if [ ! -f CH.zip ]; then
  echo ">> downloading Geonames CH.zip"
  curl -fSL --retry 3 -o CH.zip.tmp \
    "https://download.geonames.org/export/zip/CH.zip"
  mv CH.zip.tmp CH.zip
fi
if [ ! -f CH.txt ]; then
  unzip -p CH.zip CH.txt > CH.txt
fi

# Geonames cities1000 — for synthesizing the city-name query set.
if [ ! -f cities1000.zip ]; then
  echo ">> downloading cities1000.zip"
  curl -fSL --retry 3 -o cities1000.zip.tmp \
    "https://download.geonames.org/export/dump/cities1000.zip"
  mv cities1000.zip.tmp cities1000.zip
fi
if [ ! -f cities1000.txt ]; then
  unzip -p cities1000.zip cities1000.txt > cities1000.txt
fi

echo ">> done"
ls -lh "$PBF" CH.txt cities1000.txt
