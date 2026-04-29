#!/usr/bin/env bash
# Start cairn-serve in the background, pinned to localhost:8080.
# Writes its PID to bench/data/cairn.pid so 99-teardown.sh can kill it.
set -euo pipefail
cd "$(dirname "$0")/.."

PORT=${PORT:-8080}
BUNDLE=${BUNDLE:-data/cairn-bundle-ch}
PID_FILE=data/cairn.pid

cargo build --release -p cairn-serve --quiet --manifest-path ../Cargo.toml
SERVE=../target/release/cairn-serve
test -x "$SERVE" || SERVE=../../target/release/cairn-serve

# Stop any prior run.
[[ -f "$PID_FILE" ]] && kill "$(cat "$PID_FILE")" 2>/dev/null || true

"$SERVE" --bundle "$BUNDLE" --bind "127.0.0.1:$PORT" \
  >data/cairn-serve.log 2>&1 &
echo $! > "$PID_FILE"
echo "==> cairn-serve pid $(cat "$PID_FILE") on :$PORT"

# Wait for /healthz.
for _ in $(seq 1 30); do
  if curl -sf "http://127.0.0.1:$PORT/healthz" >/dev/null; then
    echo "==> ready"
    exit 0
  fi
  sleep 1
done
echo "cairn-serve did not become ready in 30 s" >&2
tail -20 data/cairn-serve.log >&2
exit 1
