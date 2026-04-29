#!/usr/bin/env python3
"""Sample N random named-place queries from a Switzerland-shape OSM input.

Two ingestion paths so the harness works without `osmium-tool`:
  1. `--in <geojsonseq>`  — RFC 7464 line-delimited GeoJSON, the
                            shape `osmium export -f geojsonseq` emits.
  2. `--pbf <path.osm.pbf>` — direct PBF, requires the `osmpbf`
                              Python package (`pip install osmpbf`).

Filter: nodes carrying a `name` tag AND at least one of
  place / amenity / shop / tourism / historic / leisure.
That mirrors the entities Cairn / Pelias / Photon / Nominatim all
index, so query coverage is comparable across tools.

Half the sample is left untouched; the other half has a single
random character flipped (Damerau-style transposition or substitution)
to stress fuzzy + phonetic paths.
"""
from __future__ import annotations

import argparse
import json
import random
import string
import sys
from pathlib import Path

random.seed(20260429)


def maybe_typo(s: str) -> str:
    """Flip one random alpha character with 50% probability."""
    if random.random() < 0.5:
        return s
    if len(s) < 4:
        return s
    idx = random.randrange(len(s))
    if not s[idx].isalpha():
        return s
    replacement = random.choice(string.ascii_lowercase)
    return s[:idx] + replacement + s[idx + 1 :]


def sample_geojsonseq(path: Path, n: int) -> list[dict]:
    """Stream geojsonseq, reservoir-sample N entries that match the filter."""
    keep_keys = {"place", "amenity", "shop", "tourism", "historic", "leisure"}
    reservoir: list[dict] = []
    seen = 0
    with path.open() as fh:
        for raw in fh:
            raw = raw.strip()
            if not raw or raw[0] == "\x1e":
                raw = raw.lstrip("\x1e")
            if not raw:
                continue
            try:
                feat = json.loads(raw)
            except json.JSONDecodeError:
                continue
            props = feat.get("properties") or {}
            name = props.get("name")
            if not name:
                continue
            if not any(k in props for k in keep_keys):
                continue
            geom = feat.get("geometry") or {}
            coords = geom.get("coordinates")
            if not coords:
                continue
            entry = {
                "q": name,
                # osmium --add-unique-id=type_id stamps the type-prefixed
                # OSM id at the Feature root (`"id": "n12345"`). Some
                # exporters put it in properties as `@id` instead.
                "osm_id": feat.get("id") or props.get("@id") or props.get("id"),
                "lon": float(coords[0]),
                "lat": float(coords[1]),
            }
            seen += 1
            if len(reservoir) < n:
                reservoir.append(entry)
            else:
                j = random.randrange(seen)
                if j < n:
                    reservoir[j] = entry
    return reservoir


def sample_pbf(path: Path, n: int) -> list[dict]:
    """Pure-Python PBF fallback. Slow on big inputs; fine for CH (~480 MB)."""
    try:
        from osmpbf import iter_blob_data, decode_blob, decode_dense
    except ImportError as e:  # noqa: BLE001
        print(
            "scripts/sample_queries.py: install `osmpbf` "
            "(`pip install osmpbf`) or use osmium-tool",
            file=sys.stderr,
        )
        raise SystemExit(1) from e

    keep_keys = {"place", "amenity", "shop", "tourism", "historic", "leisure"}
    reservoir: list[dict] = []
    seen = 0
    with path.open("rb") as fh:
        for kind, blob in iter_blob_data(fh):
            if kind != b"OSMData":
                continue
            block = decode_blob(blob)
            for node in decode_dense(block):
                tags = dict(node.tags)
                name = tags.get("name")
                if not name:
                    continue
                if not any(k in tags for k in keep_keys):
                    continue
                entry = {
                    "q": name,
                    "osm_id": f"node/{node.id}",
                    "lon": node.lon,
                    "lat": node.lat,
                }
                seen += 1
                if len(reservoir) < n:
                    reservoir.append(entry)
                else:
                    j = random.randrange(seen)
                    if j < n:
                        reservoir[j] = entry
    return reservoir


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--in", dest="geojson", type=Path, default=None)
    p.add_argument("--pbf", dest="pbf", type=Path, default=None)
    p.add_argument("--n", type=int, default=10_000)
    p.add_argument("--out", type=Path, required=True)
    args = p.parse_args()

    if args.geojson:
        rows = sample_geojsonseq(args.geojson, args.n)
    elif args.pbf:
        rows = sample_pbf(args.pbf, args.n)
    else:
        print("need --in <geojsonseq> or --pbf <path>", file=sys.stderr)
        raise SystemExit(2)

    if len(rows) < args.n:
        print(
            f"warning: only sampled {len(rows)} rows (asked for {args.n}) — "
            f"input may be too small",
            file=sys.stderr,
        )

    with args.out.open("w") as fh:
        for row in rows:
            row["q"] = maybe_typo(row["q"])
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")
    print(f"wrote {len(rows)} queries to {args.out}")


if __name__ == "__main__":
    main()
