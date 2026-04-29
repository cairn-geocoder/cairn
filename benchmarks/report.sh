#!/usr/bin/env bash
# Render a side-by-side comparison table from results/*.json. Misses
# are silent — only engines that actually ran show up in the table.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE/results"

python3 - <<'PY'
import glob, json, os, sys
rows = []
for f in sorted(glob.glob("*.json")):
    if f.endswith(".timings.txt") or "build" in f and "cairn-build.json" not in f:
        continue
    try:
        d = json.load(open(f))
    except json.JSONDecodeError:
        continue
    if "engine" not in d or "queries" not in d:
        continue
    rows.append(d)

def mb(b): return f"{b/1024/1024:.0f}" if b else "—"
def gb(b): return f"{b/1024/1024/1024:.2f}" if b else "—"
def ms(v): return f"{v:.1f}" if v else "—"

print("# Cairn vs incumbents — Switzerland")
print()
print("Reproducible benchmark. Same dataset + same query set + same host.")
print()
print("| Engine | Disk MB | Cold RSS MB | Hot RSS MB | RPS | p50 ms | p95 ms | p99 ms | max ms | errors |")
print("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
for r in rows:
    print("| {engine} | {disk} | {cold} | {hot} | {rps:.0f} | {p50} | {p95} | {p99} | {mx} | {err} |".format(
        engine=r["engine"],
        disk=mb(r.get("disk_bytes", 0)),
        cold=mb(r.get("rss_bytes", {}).get("cold", 0)),
        hot=mb(r.get("rss_bytes", {}).get("hot", 0)),
        rps=r.get("rps", 0),
        p50=ms(r["latency_ms"]["p50"]),
        p95=ms(r["latency_ms"]["p95"]),
        p99=ms(r["latency_ms"]["p99"]),
        mx=ms(r["latency_ms"]["max"]),
        err=r.get("errors", 0),
    ))
print()
# Build numbers (Cairn only — incumbents have multi-step setups).
if os.path.exists("cairn-build.json"):
    b = json.load(open("cairn-build.json"))
    print("## Cairn build")
    print()
    print(f"- Bundle disk: **{b['bundle_disk_mb']} MB**")
    print(f"- Build wall-clock: **{b['build_seconds']} s**")
    print(f"- Peak build RSS: **{b['peak_rss_bytes']/1024/1024:.0f} MB**")
    print(f"- Input PBF: {b['input_pbf_bytes']/1024/1024:.0f} MB")
PY
