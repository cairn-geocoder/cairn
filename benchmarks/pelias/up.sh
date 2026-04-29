#!/usr/bin/env bash
# Bring up Pelias. The full canonical bringup is:
#   1. compose up elasticsearch
#   2. run pelias/schema to create indices
#   3. run pelias/openstreetmap importer against switzerland.pbf
#   4. (optional) pelias/whosonfirst importer for admin polygons
#   5. run pelias/placeholder build
#   6. compose up api libpostal placeholder
#
# This script kicks the stack but the import phase is genuinely
# hours-long. See README for the manual sequence; reuse the volumes
# on subsequent runs.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE"
docker compose up -d elasticsearch
echo ">> waiting for ES :9200"
for i in $(seq 1 120); do
  if curl -fsS http://127.0.0.1:9200/_cluster/health 2>&1 \
     | grep -qE '"status":"(green|yellow)"'; then
    break
  fi
  sleep 2
done

# Schema bootstrap (idempotent — skip when the indices exist).
if ! curl -fsS http://127.0.0.1:9200/pelias 2>&1 | grep -q '"pelias"'; then
  echo ">> running schema bootstrap"
  docker run --rm --network=pelias_default \
    -e LOG_LEVEL=info \
    -v "$HERE/pelias.json":/etc/pelias/pelias.json:ro \
    pelias/schema:master ./bin/create_index
fi

echo ">> import is operator-driven; see ../README.md for the import-and-then-bench sequence"
docker compose up -d api libpostal placeholder
echo ">> waiting for api :4000"
for i in $(seq 1 120); do
  if curl -fsS "http://127.0.0.1:4000/v1/search?text=zurich&size=1" >/dev/null 2>&1; then
    echo ">> ready"
    exit 0
  fi
  sleep 2
done
echo "ERROR: pelias api not ready" >&2
docker compose logs api --tail 60
exit 1
