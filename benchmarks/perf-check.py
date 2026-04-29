#!/usr/bin/env python3
"""
Phase 7a — perf regression gate.

Walks `target/criterion/<group>/<bench_id>/new/estimates.json` for every
benchmark result that the latest `cargo bench` produced, looks up the
corresponding entry in `benchmarks/perf-baseline.json`, and exits non-
zero if any benchmark mean exceeds the baseline by more than the
configured threshold (default 15%).

Baseline format (JSON):

    {
        "myers/0":          { "mean_ns": 96.3 },
        "wagner_fischer/0": { "mean_ns": 105.1 },
        ...
    }

The dictionary key is `<criterion-group>/<bench-id>` exactly as it
appears in the criterion target directory tree. New benchmarks
without a baseline entry pass-through silently — operator commits
the freshly-recorded value to ratchet.

Update workflow:

    cargo bench -p cairn-text -p cairn-spatial
    python3 benchmarks/perf-check.py --record   # writes new baseline
    git diff benchmarks/perf-baseline.json      # review
    git add benchmarks/perf-baseline.json && git commit

CI workflow:

    cargo bench -p cairn-text -p cairn-spatial
    python3 benchmarks/perf-check.py            # exits 1 on regression
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

DEFAULT_THRESHOLD_PCT = 15.0
REPO_ROOT = Path(__file__).resolve().parent.parent
CRITERION_DIR = REPO_ROOT / "target" / "criterion"
BASELINE_PATH = REPO_ROOT / "benchmarks" / "perf-baseline.json"


def collect_current() -> dict[str, float]:
    """Walk target/criterion looking for `new/estimates.json` files.
    Returns a dict mapping `<group>/<id>` to mean_ns."""
    out: dict[str, float] = {}
    if not CRITERION_DIR.exists():
        return out
    for est in CRITERION_DIR.rglob("new/estimates.json"):
        # Path: target/criterion/<group>/<id>/new/estimates.json
        try:
            id_dir = est.parent.parent
            group_dir = id_dir.parent
            key = f"{group_dir.name}/{id_dir.name}"
        except IndexError:
            continue
        try:
            data = json.loads(est.read_text())
            mean_ns = float(data["mean"]["point_estimate"])
        except (KeyError, ValueError, json.JSONDecodeError):
            continue
        out[key] = mean_ns
    return out


def load_baseline() -> dict[str, dict[str, float]]:
    if not BASELINE_PATH.exists():
        return {}
    try:
        return json.loads(BASELINE_PATH.read_text())
    except json.JSONDecodeError:
        return {}


def write_baseline(current: dict[str, float]) -> None:
    payload = {key: {"mean_ns": round(ns, 1)} for key, ns in sorted(current.items())}
    BASELINE_PATH.write_text(json.dumps(payload, indent=2) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument(
        "--record",
        action="store_true",
        help="Overwrite the baseline with current measurements (do not gate)",
    )
    parser.add_argument(
        "--threshold-pct",
        type=float,
        default=DEFAULT_THRESHOLD_PCT,
        help="Maximum tolerated regression as a percentage (default 15)",
    )
    args = parser.parse_args()

    current = collect_current()
    if not current:
        print(
            "perf-check: no criterion results under "
            f"{CRITERION_DIR.relative_to(REPO_ROOT)} — run `cargo bench` first",
            file=sys.stderr,
        )
        return 2

    if args.record:
        write_baseline(current)
        print(f"perf-check: wrote {len(current)} entries to {BASELINE_PATH.relative_to(REPO_ROOT)}")
        return 0

    baseline = load_baseline()
    if not baseline:
        print(
            "perf-check: empty baseline — pass-through. Record one with --record.",
            file=sys.stderr,
        )
        return 0

    fail = 0
    for key, current_ns in sorted(current.items()):
        entry = baseline.get(key)
        if not entry or "mean_ns" not in entry:
            print(f"  [new]  {key}: {current_ns:.1f} ns (no baseline)")
            continue
        baseline_ns = float(entry["mean_ns"])
        delta_pct = (current_ns - baseline_ns) / baseline_ns * 100.0
        marker = "OK   " if delta_pct <= args.threshold_pct else "FAIL "
        print(
            f"  [{marker}] {key}: {current_ns:.1f} ns "
            f"(baseline {baseline_ns:.1f} ns, delta {delta_pct:+.1f} %)"
        )
        if delta_pct > args.threshold_pct:
            fail += 1

    if fail:
        print(
            f"\nperf-check: {fail} benchmark(s) regressed beyond +"
            f"{args.threshold_pct} %.",
            file=sys.stderr,
        )
        return 1
    print("\nperf-check: no regressions.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
