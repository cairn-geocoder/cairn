#!/usr/bin/env bash
# Run 10k Swiss queries against one engine. Captures per-query
# latency, RAM, and disk; dumps JSON to results/<engine>.json.
#
# Usage: ./run.sh <engine>
#   engines: cairn | pelias | nominatim | photon
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE"

ENGINE="${1:-}"
if [ -z "$ENGINE" ]; then
  echo "usage: $0 <cairn|pelias|nominatim|photon>" >&2
  exit 2
fi

QUERIES=queries/swiss-10k.txt
if [ ! -f "$QUERIES" ]; then
  echo "ERROR: queries/build.sh first" >&2
  exit 1
fi
mkdir -p results

case "$ENGINE" in
  cairn)
    PORT=7100
    URL_TPL='http://127.0.0.1:7100/v1/search?q={Q}&limit=1'
    DATA_PATH="cairn/bundle"
    PROC_NAME="cairn-serve"
    ;;
  pelias)
    PORT=4000
    URL_TPL='http://127.0.0.1:4000/v1/search?text={Q}&size=1'
    DATA_PATH="pelias/data"
    PROC_NAME="pelias-api"
    ;;
  nominatim)
    PORT=8080
    URL_TPL='http://127.0.0.1:8080/search?q={Q}&format=json&limit=1'
    DATA_PATH="nominatim/data"
    PROC_NAME="apache2"
    ;;
  photon)
    PORT=2322
    URL_TPL='http://127.0.0.1:2322/api?q={Q}&limit=1'
    DATA_PATH="photon/photon_data"
    PROC_NAME="photon"
    ;;
  *)
    echo "unknown engine: $ENGINE" >&2
    exit 2
    ;;
esac

# Wait for /health-equivalent before starting timer.
echo ">> probing $ENGINE on :$PORT"
for i in $(seq 1 60); do
  if curl -fsS -o /dev/null "http://127.0.0.1:$PORT/" 2>&1 \
     || curl -fsS -o /dev/null "http://127.0.0.1:$PORT/healthz" 2>&1 \
     || curl -fsS -o /dev/null "http://127.0.0.1:$PORT/status" 2>&1; then
    break
  fi
  sleep 0.5
done

# Pre-flight: cold-start RSS.
COLD_RSS=$(ps -axo rss=,comm= | awk -v n="$PROC_NAME" 'index($2,n){s+=$1} END{print s}')
DISK_BYTES=0
if [ -d "$DATA_PATH" ]; then
  DISK_BYTES=$(du -sk "$DATA_PATH" | awk '{print $1*1024}')
fi

OUT="results/$ENGINE.json"
TIMINGS="results/$ENGINE.timings.txt"
: > "$TIMINGS"

START_NS=$(date +%s%N 2>/dev/null || python3 -c 'import time;print(int(time.time()*1e9))')

# Sequential closed-loop. Each query measured via curl's
# %{time_total}. 10k single requests is enough to surface the tail
# under "honest single-client" load — the public claim is "fast even
# in airgapped mode", not "max concurrency".
URLENC() { python3 -c 'import sys,urllib.parse;print(urllib.parse.quote(sys.argv[1]))' "$1"; }

COUNT=0
ERRORS=0
while IFS= read -r q; do
  [ -z "$q" ] && continue
  ENC=$(URLENC "$q")
  URL="${URL_TPL/\{Q\}/$ENC}"
  T=$(curl -s -o /dev/null -w "%{time_total}" "$URL" || echo "0")
  if [ "$T" = "0" ]; then
    ERRORS=$((ERRORS + 1))
  fi
  echo "$T" >> "$TIMINGS"
  COUNT=$((COUNT + 1))
  if [ $((COUNT % 1000)) -eq 0 ]; then
    printf "  %d/10000 (errors=%d)\r" "$COUNT" "$ERRORS" >&2
  fi
done < "$QUERIES"

END_NS=$(date +%s%N 2>/dev/null || python3 -c 'import time;print(int(time.time()*1e9))')
TOTAL_S=$(python3 -c "print(($END_NS - $START_NS) / 1e9)")

# Steady-state RSS after the run.
HOT_RSS=$(ps -axo rss=,comm= | awk -v n="$PROC_NAME" 'index($2,n){s+=$1} END{print s}')

# Aggregate p50 / p95 / p99 / mean / rps from timings.
python3 - "$TIMINGS" "$ENGINE" "$COUNT" "$ERRORS" "$TOTAL_S" "$COLD_RSS" "$HOT_RSS" "$DISK_BYTES" <<'PY' > "$OUT"
import json, sys, statistics
t_path, engine, count, errors, total_s, cold_rss_kb, hot_rss_kb, disk_b = sys.argv[1:]
count, errors = int(count), int(errors)
total_s = float(total_s)
cold = int(cold_rss_kb or 0) * 1024
hot  = int(hot_rss_kb or 0) * 1024
disk = int(disk_b or 0)
ts = []
with open(t_path) as f:
    for line in f:
        line = line.strip()
        if not line: continue
        try:
            v = float(line)
            if v > 0: ts.append(v)
        except ValueError:
            pass
ts.sort()
def pct(p):
    if not ts: return 0.0
    k = max(0, min(len(ts) - 1, int(round(p / 100.0 * (len(ts) - 1)))))
    return ts[k]
out = {
  "engine": engine,
  "queries": count,
  "errors": errors,
  "total_seconds": total_s,
  "rps": count / total_s if total_s else 0.0,
  "latency_ms": {
    "p50": pct(50) * 1000,
    "p90": pct(90) * 1000,
    "p95": pct(95) * 1000,
    "p99": pct(99) * 1000,
    "max": max(ts) * 1000 if ts else 0.0,
    "mean": statistics.mean(ts) * 1000 if ts else 0.0,
  },
  "rss_bytes": {
    "cold": cold,
    "hot": hot,
  },
  "disk_bytes": disk,
}
print(json.dumps(out, indent=2))
PY

echo
echo ">> wrote $OUT"
cat "$OUT"
