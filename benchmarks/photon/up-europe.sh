#!/usr/bin/env bash
# Bring up Photon against komoot's prebuilt Europe photon-db dump.
# First image build downloads + extracts ~150 GB; subsequent runs
# reuse the bind-mounted volume on /mnt/fast NVMe.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE"

mkdir -p /mnt/fast/photon-europe/data

docker compose -f docker-compose.europe.yml build photon
docker compose -f docker-compose.europe.yml up -d
echo ">> waiting for photon-europe :2323"
for i in $(seq 1 1800); do        # 1 h ceiling
  if curl -fsS "http://127.0.0.1:2323/api?q=zurich&limit=1" >/dev/null 2>&1; then
    echo ">> ready"
    exit 0
  fi
  sleep 2
done
echo "ERROR: photon-europe did not become ready in 1 h" >&2
docker compose -f docker-compose.europe.yml logs --tail 60
exit 1
