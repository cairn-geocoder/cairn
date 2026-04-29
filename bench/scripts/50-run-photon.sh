#!/usr/bin/env bash
# Stand up Photon. The image's entrypoint will fetch the latest
# photon-db archive on first start and ingest a Switzerland-shaped
# slice if COUNTRY_CODE=ch is set in the environment. Wall clock
# is dominated by the dump download.
set -euo pipefail
cd "$(dirname "$0")/.."

START=$(date +%s)
echo "==> photon up (first run downloads photon-db archive)"
docker compose -f compose/docker-compose.photon.yml up -d

for _ in $(seq 1 1800); do
  if curl -sf "http://127.0.0.1:2322/api?q=Vaduz&limit=1" >/dev/null; then
    break
  fi
  sleep 2
done
END=$(date +%s)
IMPORT_SEC=$((END - START))

DISK=$(docker system df -v | awk '/bench_photon_data/ {print $NF}' | head -1)

mkdir -p results
cat > results/photon-import.json <<EOF
{
  "tool": "photon",
  "import_seconds": $IMPORT_SEC,
  "disk_human": "$DISK",
  "components": ["photon"]
}
EOF
echo "==> Photon ready on http://127.0.0.1:2322"
cat results/photon-import.json
