#!/usr/bin/env bash
# Sample 10 000 named places from the Switzerland PBF for the load
# generator. Output: data/queries.ndjson, one
# {"q": "...", "osm_id": "..."} object per line.
#
# Uses osmium-tool when available (much faster); falls back to the
# Python `osmpbf` reader baked into bench/scripts/sample_queries.py.
set -euo pipefail
cd "$(dirname "$0")/.."

PBF=${PBF:-data/switzerland-latest.osm.pbf}
OUT=${OUT:-data/queries.ndjson}
N=${N:-10000}

if ! [[ -f "$PBF" ]]; then
  echo "missing $PBF — run scripts/00-download.sh" >&2
  exit 1
fi

if command -v osmium >/dev/null 2>&1; then
  echo "==> sampling $N named places via osmium"
  # Filter to nodes with name + (place|amenity|shop|tourism) tags,
  # emit minimal CSV that scripts/sample_queries.py can consume.
  osmium tags-filter "$PBF" \
    n/name n/place n/amenity n/shop n/tourism \
    -o data/named-points.osm.pbf -O
  osmium export -f geojsonseq --add-unique-id=type_id \
    data/named-points.osm.pbf -o data/named-points.geojsonseq -O
  python3 scripts/sample_queries.py \
    --in data/named-points.geojsonseq \
    --n "$N" \
    --out "$OUT"
else
  echo "==> osmium not found; using pure-Python fallback (slower)"
  python3 scripts/sample_queries.py --pbf "$PBF" --n "$N" --out "$OUT"
fi

echo "==> wrote $(wc -l <"$OUT") queries to $OUT"
head -3 "$OUT"
