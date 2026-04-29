#!/usr/bin/env bash
# Bring up Nominatim. First run imports the Switzerland PBF —
# expect 30-60 min wall-clock. Subsequent runs reuse the postgres
# volume and boot in seconds.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE"
docker compose up -d
echo ">> waiting for nominatim :8080 (first run does full import)"
for i in $(seq 1 7200); do        # 2 hour ceiling
  if curl -fsS "http://127.0.0.1:8080/status?format=json" 2>&1 \
     | grep -q '"status":0'; then
    echo ">> ready"
    exit 0
  fi
  sleep 5
done
echo "ERROR: nominatim did not become ready in 2 h" >&2
docker compose logs --tail 80
exit 1
