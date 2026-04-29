#!/usr/bin/env bash
# Start cairn-serve on :7100 against bundle-<country> (default
# switzerland — backwards compat with the legacy `bundle` path).
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
COUNTRY="${1:-switzerland}"
PORT=7100

# Pick bundle-<country>; fall back to legacy `bundle` for switzerland.
BUNDLE="$HERE/bundle-${COUNTRY}"
[ -d "$BUNDLE" ] || BUNDLE="$HERE/bundle"

if [ ! -d "$BUNDLE" ]; then
  echo "ERROR: $BUNDLE missing — run ./cairn/build.sh $COUNTRY" >&2
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
  echo ">> cairn-serve pid=$(cat "$HERE/.pid") · bundle=$BUNDLE"
fi

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
