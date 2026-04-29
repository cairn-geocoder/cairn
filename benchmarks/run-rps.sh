#!/usr/bin/env bash
# Sustained throughput test using Apache Bench (ab) — much truer
# RPS than the curl-spawning sequential loop in run.sh.
# Single endpoint, repeated query, varied concurrency.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"

ENGINE="${1:-cairn}"
case "$ENGINE" in
  cairn)     URL="http://127.0.0.1:7100/v1/search?q=Zurich&limit=1" ;;
  pelias)    URL="http://127.0.0.1:4000/v1/search?text=Zurich&size=1" ;;
  nominatim) URL="http://127.0.0.1:8080/search?q=Zurich&format=json&limit=1" ;;
  photon)    URL="http://127.0.0.1:2322/api?q=Zurich&limit=1" ;;
  *) echo "unknown engine $ENGINE"; exit 2;;
esac

if ! command -v ab >/dev/null 2>&1; then
  echo "ERROR: apache bench (ab) not found — install via 'brew install httpd'" >&2
  exit 1
fi

OUT="$HERE/results/$ENGINE-rps.txt"
: > "$OUT"
for c in 1 8 32 64; do
  echo "=== $ENGINE c=$c ===" | tee -a "$OUT"
  ab -n 10000 -c "$c" -k "$URL" 2>&1 | grep -E "Requests per|Time per|Failed|95%|99%" | tee -a "$OUT"
  echo | tee -a "$OUT"
done
echo "wrote $OUT"
