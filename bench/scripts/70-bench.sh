#!/usr/bin/env bash
# Run the latency benchmark for one tool. Usage:
#   ./scripts/70-bench.sh cairn
#   ./scripts/70-bench.sh pelias
#   ./scripts/70-bench.sh nominatim
#   ./scripts/70-bench.sh photon
#
# Assumes the tool is already running and listening on its standard
# port (cairn :8080, pelias :4000, nominatim :8081, photon :2322).
# Use the matching scripts/{20,30,40,50}-run-*.sh first.
set -euo pipefail
cd "$(dirname "$0")/.."

TOOL=${1:?"usage: $0 <cairn|pelias|nominatim|photon>"}
QUERIES=${QUERIES:-data/queries.ndjson}
OUT="results/$TOOL-bench.json"

if [[ ! -f "$QUERIES" ]]; then
  echo "missing $QUERIES — run scripts/60-generate-queries.sh first" >&2
  exit 1
fi

echo "==> compiling cairn-bench (release)"
cargo build --release --manifest-path runner/Cargo.toml --quiet
RUNNER=runner/target/release/cairn-bench

echo "==> capturing footprint snapshot"
case "$TOOL" in
  cairn)
    PID=$(cat data/cairn.pid 2>/dev/null || true)
    if [[ -n "$PID" ]]; then
      RSS_KB=$(ps -o rss= -p "$PID" | awk '{print $1}')
    fi
    ;;
  pelias|nominatim|photon)
    # Sum RSS of every container in the stack; docker stats is one
    # snapshot, fine for a footprint estimate.
    RSS_KB=$(docker stats --no-stream --format '{{.MemUsage}}' \
      $(docker ps --format '{{.Names}}' | grep -E "^$TOOL" || true) \
      | awk -F'/' '{print $1}' \
      | awk '/MiB/ {s+=$1*1024} /GiB/ {s+=$1*1024*1024} END {print s+0}')
    ;;
esac

echo "==> running bench against $TOOL"
"$RUNNER" --tool "$TOOL" --queries "$QUERIES" --out "$OUT"

# Stitch the footprint into the result JSON.
if [[ -n "${RSS_KB:-}" ]]; then
  python3 - <<EOF
import json, pathlib
p = pathlib.Path("$OUT")
d = json.loads(p.read_text())
d["rss_kb_after_warmup"] = ${RSS_KB:-0}
p.write_text(json.dumps(d, indent=2))
EOF
fi

echo "==> wrote $OUT"
cat "$OUT"
