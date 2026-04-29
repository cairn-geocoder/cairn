#!/usr/bin/env bash
# Start cairn-serve on :7100. Writes the PID to .pid so down.sh can
# kill it, blocks until /healthz responds.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
BUNDLE="$HERE/bundle"
PORT=7100

if [ ! -d "$BUNDLE" ]; then
  echo "ERROR: build.sh first" >&2
  exit 1
fi

cd "$ROOT"
cargo build --release -p cairn-serve 2>&1 | tail -1
CAIRN_SERVE="$ROOT/target/release/cairn-serve"

if [ -f "$HERE/.pid" ] && kill -0 "$(cat "$HERE/.pid")" 2>/dev/null; then
  echo ">> already running (pid $(cat "$HERE/.pid"))"
else
  "$CAIRN_SERVE" --bundle "$BUNDLE" --bind "127.0.0.1:$PORT" \
    > "$HERE/serve.log" 2>&1 &
  echo $! > "$HERE/.pid"
  echo ">> cairn-serve pid=$(cat "$HERE/.pid")"
fi

# Block until /healthz returns 200, max 30s.
for i in $(seq 1 60); do
  if curl -fsS "http://127.0.0.1:$PORT/healthz" >/dev/null 2>&1; then
    echo ">> ready on :$PORT"
    exit 0
  fi
  sleep 0.5
done
echo "ERROR: cairn-serve did not become ready in 30s" >&2
tail -20 "$HERE/serve.log" >&2 || true
exit 1
