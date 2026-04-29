#!/usr/bin/env bash
# Stand up Nominatim 4.4 against Switzerland. First run imports the
# PBF into Postgres + builds search indexes (~45-90 min on a Mac).
# Subsequent runs reuse the persistent volume and start in ~15 s.
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ ! -f data/switzerland-latest.osm.pbf ]]; then
  echo "missing data/switzerland-latest.osm.pbf — run scripts/00-download.sh" >&2
  exit 1
fi

START=$(date +%s)
echo "==> nominatim up (first run is the slow one)"
docker compose -f compose/docker-compose.nominatim.yml up -d

# Tail the import progress so the operator knows it's alive.
docker logs -f bench_nominatim 2>&1 | grep --line-buffered -E "Postcode|Indexing|Done|Ready" &
TAIL_PID=$!

# Wait for the search endpoint to come up.
for _ in $(seq 1 1800); do  # up to 1 hour
  if curl -sf "http://127.0.0.1:8081/search?q=Vaduz&format=json" >/dev/null; then
    break
  fi
  sleep 2
done
kill "$TAIL_PID" 2>/dev/null || true

END=$(date +%s)
IMPORT_SEC=$((END - START))
DISK=$(docker system df -v | awk '/bench_nominatim_pgdata/ {print $NF}' | head -1)

mkdir -p results
cat > results/nominatim-import.json <<EOF
{
  "tool": "nominatim",
  "import_seconds": $IMPORT_SEC,
  "disk_human": "$DISK",
  "components": ["postgres", "nominatim-api"]
}
EOF
echo "==> Nominatim ready on http://127.0.0.1:8081"
cat results/nominatim-import.json
