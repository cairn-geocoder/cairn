#!/usr/bin/env bash
# Bring up Pelias against Geofabrik europe-latest.osm.pbf.
# Multi-stage import (ES + WoF + OSM importers + placeholder build).
# First-run wall-clock: ~12-18 h on a 48-core box with 32 GB ES heap.
# ES + placeholder volumes live on /mnt/fast NVMe.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE"

mkdir -p /mnt/fast/pelias-europe/es-data \
         /mnt/fast/pelias-europe/placeholder-data \
         /mnt/fast/pelias-europe/openstreetmap \
         /mnt/fast/pelias-europe/whosonfirst

if [ ! -f /mnt/fast/cairn/europe-latest.osm.pbf ]; then
  echo "ERROR: /mnt/fast/cairn/europe-latest.osm.pbf missing — fetch it first" >&2
  exit 1
fi
ln -sf /mnt/fast/cairn/europe-latest.osm.pbf \
       /mnt/fast/pelias-europe/openstreetmap/europe-latest.osm.pbf

docker compose -f docker-compose.europe.yml up -d elasticsearch
echo ">> waiting for ES :9201"
for i in $(seq 1 240); do
  if curl -fsS http://127.0.0.1:9201/_cluster/health 2>&1 \
     | grep -qE '"status":"(green|yellow)"'; then
    break
  fi
  sleep 2
done

if ! curl -fsS http://127.0.0.1:9201/pelias 2>&1 | grep -q '"pelias"'; then
  echo ">> running schema bootstrap"
  docker run --rm --network=pelias_default \
    -e LOG_LEVEL=info \
    -v "$HERE/pelias.europe.json":/etc/pelias/pelias.json:ro \
    pelias/schema:master ./bin/create_index
fi

echo ">> import is operator-driven; run pelias/openstreetmap importer:"
echo "   docker run --rm --network=pelias_default \\"
echo "     -e LOG_LEVEL=info \\"
echo "     -v $HERE/pelias.europe.json:/etc/pelias/pelias.json:ro \\"
echo "     -v /mnt/fast/pelias-europe:/data \\"
echo "     pelias/openstreetmap:master ./bin/start"

docker compose -f docker-compose.europe.yml up -d api libpostal placeholder
echo ">> ES + sidecars up; api on :4001 (will return 0 hits until import runs)"
