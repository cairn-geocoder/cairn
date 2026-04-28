#!/usr/bin/env bash
# Publish (or republish) every Cairn crate at the current workspace version.
# Sequential because each downstream crate's verify pass needs the upstream
# version to be live in the index. Retries automatically on HTTP 429
# (crates.io new-crate rate limit) with a 11-minute cooldown.
set -euo pipefail

ORDER=(
  cairn-place
  cairn-tile
  cairn-text
  cairn-spatial
  cairn-parse
  cairn-import-osm
  cairn-import-wof
  cairn-import-oa
  cairn-import-geonames
  cairn-api
  cairn-geocoder
)

# Cooldowns are biased on the conservative side. Adjust if crates.io has
# raised your account's quota.
NEW_COOLDOWN=660       # 11 min — new-crate burst window.
INDEX_SETTLE=60        # gap to let the registry index pick up the new version.
RATE_LIMIT_BACKOFF=660 # sleep on HTTP 429.

publish_one() {
  local crate="$1"
  local attempt=1
  while true; do
    echo "=== publish $crate (attempt $attempt) ==="
    if cargo publish -p "$crate" --allow-dirty 2>&1 | tee /tmp/cairn-publish.log; then
      echo "OK $crate"
      return 0
    fi
    if grep -q "already exists" /tmp/cairn-publish.log; then
      echo ">> $crate already published at this version, skipping"
      return 0
    fi
    if grep -q "429" /tmp/cairn-publish.log; then
      echo ">> rate-limited, sleeping ${RATE_LIMIT_BACKOFF}s"
      sleep "$RATE_LIMIT_BACKOFF"
      attempt=$((attempt + 1))
      continue
    fi
    echo "FAIL $crate"
    return 1
  done
}

for c in "${ORDER[@]}"; do
  publish_one "$c"
  echo ">> waiting ${INDEX_SETTLE}s for index to settle"
  sleep "$INDEX_SETTLE"
done

echo "ALL DONE"
