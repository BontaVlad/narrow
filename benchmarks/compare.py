#!/usr/bin/env python3
"""Compare two Narrow criterion benchmark JSON outputs.

Usage:
    python3 benchmarks/compare.py benchmarks/results/baseline.json benchmarks/results/new.json
"""

import argparse
import json
import math
import statistics
import sys
from typing import Any


def load_results(path: str) -> list[dict[str, Any]]:
    with open(path) as f:
        return json.load(f)


def mean_time_us(entry: dict[str, Any]) -> float:
    """Return mean per-iteration time in microseconds."""
    times = [
        t / n for t, n in zip(entry["raw_data"]["time"], entry["raw_data"]["iterations"])
    ]
    return statistics.mean(times) / 1e3


def std_time_us(entry: dict[str, Any]) -> float:
    times = [
        t / n for t, n in zip(entry["raw_data"]["time"], entry["raw_data"]["iterations"])
    ]
    return statistics.stdev(times) / 1e3 if len(times) > 1 else 0.0


def welch_t(baseline: dict[str, Any], candidate: dict[str, Any]) -> tuple[float, float]:
    """Return (t-statistic, degrees_of_freedom) for Welch's t-test."""
    b_times = [
        t / n
        for t, n in zip(baseline["raw_data"]["time"], baseline["raw_data"]["iterations"])
    ]
    c_times = [
        t / n
        for t, n in zip(candidate["raw_data"]["time"], candidate["raw_data"]["iterations"])
    ]

    b_mean = statistics.mean(b_times)
    c_mean = statistics.mean(c_times)

    if len(b_times) < 2 or len(c_times) < 2:
        return 0.0, 1.0

    b_var = statistics.variance(b_times)
    c_var = statistics.variance(c_times)

    se2 = b_var / len(b_times) + c_var / len(c_times)
    if se2 == 0:
        return 0.0, 1.0

    t = abs(b_mean - c_mean) / math.sqrt(se2)

    # Welch-Satterthwaite degrees of freedom
    num = (b_var / len(b_times) + c_var / len(c_times)) ** 2
    den = (b_var**2) / ((len(b_times) - 1) * (len(b_times) ** 2)) + (
        c_var**2
    ) / ((len(c_times) - 1) * (len(c_times) ** 2))
    df = num / den if den > 0 else 1.0

    return t, df


def significance_stars(t: float, df: float) -> str:
    """Return *, **, *** markers based on two-tailed p-value."""
    if df < 1:
        return ""
    # rough critical values for two-tailed t-test
    if t > 3.29:
        return " ***"
    elif t > 2.58:
        return " **"
    elif t > 1.96:
        return " *"
    return ""


def compare_files(base_path: str, cand_path: str, threshold: float, sort_by: str) -> int:
    base = {b["label"]: b for b in load_results(base_path)}
    cand = {b["label"]: b for b in load_results(cand_path)}

    labels = sorted(set(base.keys()) & set(cand.keys()))
    if not labels:
        print("error: no matching benchmarks between the two files", file=sys.stderr)
        return 1

    rows = []
    for label in labels:
        b = base[label]
        c = cand[label]
        b_us = mean_time_us(b)
        c_us = mean_time_us(c)
        delta = (c_us - b_us) / b_us * 100 if b_us > 0 else 0.0
        t, df = welch_t(b, c)
        stars = significance_stars(t, df)
        rows.append((label, b_us, c_us, delta, t, stars))

    if sort_by == "delta":
        rows.sort(key=lambda r: abs(r[3]), reverse=True)

    print(f"{'Benchmark':<48} {'Baseline':>10} {'Candidate':>10} {'Delta':>9} {'t-stat':>8}")
    print("-" * 90)
    shown = 0
    for label, b_us, c_us, delta, t, stars in rows:
        if abs(delta) < threshold:
            continue
        shown += 1
        print(
            f"{label:<48} {b_us:>9.2f}us {c_us:>9.2f}us {delta:>+8.2f}% {t:>8.2f}{stars}"
        )

    print("-" * 90)
    print(f"Shown {shown}/{len(rows)} benchmarks  |  * p<0.05  ** p<0.01  *** p<0.001")
    return 0


def compare_dirs(base_dir: str, cand_dir: str, threshold: float, sort_by: str) -> int:
    from pathlib import Path

    base_path = Path(base_dir)
    cand_path = Path(cand_dir)

    if not base_path.is_dir() or not cand_path.is_dir():
        print("error: both arguments must be directories", file=sys.stderr)
        return 1

    any_missing = False
    first = True
    for cand_file in sorted(cand_path.glob("*.json")):
        base_file = base_path / cand_file.name
        if not base_file.exists():
            print(f"missing baseline: {cand_file.name}")
            any_missing = True
            continue

        if not first:
            print()
        first = False

        print(f"==> {cand_file.stem}")
        compare_files(str(base_file), str(cand_file), threshold, sort_by)

    return 1 if any_missing else 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compare two Narrow criterion benchmark JSON outputs."
    )
    parser.add_argument("baseline", help="Baseline benchmark JSON file or directory")
    parser.add_argument("candidate", help="Candidate benchmark JSON file or directory")
    parser.add_argument(
        "--threshold", type=float, default=0.0, help="Only show deltas above this %% (default: 0)"
    )
    parser.add_argument(
        "--sort", choices=["label", "delta"], default="label", help="Sort order"
    )
    args = parser.parse_args()

    from pathlib import Path

    base_is_dir = Path(args.baseline).is_dir()
    cand_is_dir = Path(args.candidate).is_dir()

    if base_is_dir and cand_is_dir:
        return compare_dirs(args.baseline, args.candidate, args.threshold, args.sort)
    elif not base_is_dir and not cand_is_dir:
        return compare_files(args.baseline, args.candidate, args.threshold, args.sort)
    else:
        print("error: both arguments must be files or both must be directories", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
