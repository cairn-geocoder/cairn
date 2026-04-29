#!/usr/bin/env bash
# Bring up Photon (Komoot's geocoder). First run pulls the
# Switzerland search-index dump (~few-hundred MB). Subsequent runs
# reuse the volume.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE"
docker compose up -d
echo ">> waiting for photon :2322 (first run downloads CH index)"
for i in $(seq 1 600); do
  if curl -fsS "http://127.0.0.1:2322/api?q=zurich&limit=1" >/dev/null 2>&1; then
    echo ">> ready"
    exit 0
  fi
  sleep 2
done
echo "ERROR: photon did not become ready" >&2
docker compose logs --tail 60
exit 1
