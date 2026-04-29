#!/usr/bin/env bash
# Recall A/B test for Cairn-only differentiator flags. Runs the
# noisy-query set against a base configuration and three variants
# (fuzzy, phonetic, semantic) and counts how many queries return >=1
# hit. Higher recall = better tolerance to typos / spelling drift.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE"

QUERIES=queries/swiss-noisy.txt
PORT=7100

if [ ! -f "$QUERIES" ]; then
  echo "ERROR: queries/build.sh first" >&2
  exit 1
fi

probe() {
  local label="$1"; shift
  local extra="$1"; shift
  local hits=0
  local total=0
  while IFS= read -r q; do
    [ -z "$q" ] && continue
    total=$((total + 1))
    enc=$(python3 -c 'import sys,urllib.parse;print(urllib.parse.quote(sys.argv[1]))' "$q")
    n=$(curl -s "http://127.0.0.1:$PORT/v1/search?q=$enc&limit=1$extra" \
        | python3 -c 'import json,sys;d=json.load(sys.stdin);print(len(d.get("results",[])))' 2>/dev/null || echo 0)
    if [ "$n" -gt 0 ]; then
      hits=$((hits + 1))
    fi
  done < "$QUERIES"
  echo "$label	$hits/$total	$(python3 -c "print(f'{$hits/$total*100:.1f}%')")"
}

echo "variant	hits	recall"
probe "baseline"   ""
probe "fuzzy=1"    "&fuzzy=1"
probe "phonetic"   "&phonetic=true"
probe "semantic"   "&semantic=true"
probe "all-on"     "&fuzzy=2&phonetic=true&semantic=true"
