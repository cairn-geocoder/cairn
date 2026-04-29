#!/usr/bin/env bash
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
if [ -f "$HERE/.pid" ]; then
  PID=$(cat "$HERE/.pid")
  kill "$PID" 2>/dev/null || true
  rm -f "$HERE/.pid"
  echo ">> stopped pid $PID"
else
  echo ">> not running"
fi
