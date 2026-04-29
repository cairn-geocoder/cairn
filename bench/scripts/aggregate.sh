#!/usr/bin/env bash
# Read every results/<tool>-{import,bench}.json pair and emit
# results/summary.md as a single comparison table.
set -euo pipefail
cd "$(dirname "$0")/.."

python3 <<'PY'
import json, pathlib, statistics

R = pathlib.Path("results")
rows = []
for tool in ("cairn", "pelias", "nominatim", "photon"):
    imp = R / f"{tool}-import.json"
    bench = R / f"{tool}-bench.json"
    if not bench.exists():
        continue
    bd = json.loads(bench.read_text())
    id_ = json.loads(imp.read_text()) if imp.exists() else {}
    disk = id_.get("disk_bytes")
    disk_h = f"{disk/1_000_000:.1f} MB" if disk else id_.get("disk_human", "—")
    rss = bd.get("rss_kb_after_warmup")
    rss_h = f"{rss/1024:.0f} MB" if rss else "—"
    import_s = id_.get("import_seconds")
    import_h = "—" if import_s is None else f"{import_s//60}m{import_s%60:02d}s"
    lat = bd["latency_ms"]
    rows.append({
        "tool": tool,
        "import": import_h,
        "disk": disk_h,
        "rss": rss_h,
        "p50": f"{lat['p50']:.1f}",
        "p95": f"{lat['p95']:.1f}",
        "p99": f"{lat['p99']:.1f}",
        "p99_9": f"{lat['p99_9']:.1f}",
        "qps": f"{bd['qps']:.0f}",
        "recall": f"{bd['recall_at_10']:.2f}" if bd['recall_at_10'] == bd['recall_at_10'] else "—",
        "errors": bd["errors"],
    })

if not rows:
    print("no results yet — run the benches first")
    raise SystemExit(0)

out = ["# Benchmark Results", ""]
out.append("| Tool | Import | Disk | RSS | p50 ms | p95 ms | p99 ms | p99.9 ms | QPS | Recall@10 | Errors |")
out.append("|------|--------|------|-----|--------|--------|--------|----------|-----|-----------|--------|")
for r in rows:
    out.append(
        f"| **{r['tool']}** | {r['import']} | {r['disk']} | {r['rss']} | "
        f"{r['p50']} | {r['p95']} | {r['p99']} | {r['p99_9']} | {r['qps']} | "
        f"{r['recall']} | {r['errors']} |"
    )

out.append("")
out.append("Methodology: see [bench/README.md](README.md). Single-thread "
           "closed-loop loadgen, 1 000-query warmup discarded, "
           "10 000 queries total, hdrhistogram percentiles.")

(R / "summary.md").write_text("\n".join(out) + "\n")
print((R / "summary.md").read_text())
PY
