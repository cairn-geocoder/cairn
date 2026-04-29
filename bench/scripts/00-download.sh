#!/usr/bin/env bash
# Download Switzerland OSM PBF + WhosOnFirst SQLite (admin-only) +
# Geonames postcode TSV. Cairn uses all three; Pelias / Photon /
# Nominatim configurations limit themselves to the PBF for fairness.
set -euo pipefail
cd "$(dirname "$0")/.."

mkdir -p data
cd data

PBF=switzerland-latest.osm.pbf
PBF_URL=${PBF_URL:-https://download.geofabrik.de/europe/switzerland-latest.osm.pbf}
if [[ ! -f "$PBF" ]]; then
  echo "==> downloading $PBF_URL"
  curl -L --fail --progress-bar -o "$PBF" "$PBF_URL"
else
  echo "==> $PBF already cached, skipping download"
fi
ls -la "$PBF"

# WhosOnFirst admin polygons for Switzerland (admin-only SQLite).
WOF=whosonfirst-data-admin-ch-latest.db.bz2
WOF_URL=${WOF_URL:-https://data.geocode.earth/wof/dist/sqlite/whosonfirst-data-admin-ch-latest.db.bz2}
if [[ ! -f "$WOF" ]] && [[ ! -f "${WOF%.bz2}" ]]; then
  echo "==> downloading $WOF_URL"
  curl -L --fail --progress-bar -o "$WOF" "$WOF_URL" || {
    echo "WoF download failed (Geocode Earth occasionally rate-limits) — retry later or skip"
  }
fi
if [[ -f "$WOF" ]] && [[ ! -f "${WOF%.bz2}" ]]; then
  echo "==> decompressing $WOF"
  bunzip2 -k "$WOF"
fi

# Geonames postcode TSV for CH.
POSTCODES=CH.txt
if [[ ! -f "$POSTCODES" ]]; then
  echo "==> downloading Geonames CH postcodes"
  curl -L --fail --progress-bar -o CH.zip https://download.geonames.org/export/zip/CH.zip
  unzip -o CH.zip
  rm -f CH.zip readme.txt
fi
ls -la
echo "==> done"
