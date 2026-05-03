#!/usr/bin/env bash
# Bring up Nominatim against Geofabrik europe-latest.osm.pbf.
# Expect 24-48 h on first run for full osm2pgsql import + indexing.
# Postgres volumes live on /mnt/fast NVMe to keep import I/O off
# the rotating /mnt/data spindle.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE"

mkdir -p /mnt/fast/nominatim-europe/pgdata /mnt/fast/nominatim-europe/flatnode

if [ ! -f /mnt/fast/cairn/europe-latest.osm.pbf ]; then
  echo "ERROR: /mnt/fast/cairn/europe-latest.osm.pbf missing — fetch it first" >&2
  exit 1
fi

docker compose -f docker-compose.europe.yml up -d
echo ">> nominatim-europe import started; tail logs with:"
echo "   docker compose -f docker-compose.europe.yml logs -f"
echo ">> ready check (will sit at 'still importing' for hours):"
for i in $(seq 1 17280); do        # 24 h ceiling
  if curl -fsS "http://127.0.0.1:9998/status?format=json" 2>&1 \
     | grep -q '"status":0'; then
    echo ">> ready"
    exit 0
  fi
  sleep 5
done
echo "ERROR: nominatim-europe did not become ready in 24 h" >&2
exit 1
