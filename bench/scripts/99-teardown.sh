#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

# Stop cairn-serve.
if [[ -f data/cairn.pid ]]; then
  kill "$(cat data/cairn.pid)" 2>/dev/null || true
  rm -f data/cairn.pid
fi

# Stop docker stacks.
docker compose -f compose/docker-compose.pelias.yml down -v 2>/dev/null || true
docker compose -f compose/docker-compose.nominatim.yml down -v 2>/dev/null || true
docker compose -f compose/docker-compose.photon.yml down -v 2>/dev/null || true

echo "==> all stopped"
