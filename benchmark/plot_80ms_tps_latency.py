#!/usr/bin/env python3
"""
Plot a TPS-vs-latency curve from 80ms balanced benchmark summary files.

The script:
- scans `results/80ms/balanced/**/*summary.txt`
- groups runs by offered rate
- keeps only the latest batch timestamp per rate
- skips invalid summaries with zero TPS/latency
- plots end-to-end latency against end-to-end TPS
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from statistics import mean, stdev

import matplotlib.pyplot as plt

from plot_certificate_progress import configure_plot_style


RESULTS_ROOT = Path("results/80ms/balanced")
MAX_RATE = 50_000
OUTPUT_NAME = "tps_latency_80ms_balanced_upto_50k.png"
USE_END_TO_END = True


RUN_DIR_PATTERN = re.compile(
    r"(?P<timestamp>\d{8}_\d{6})_n(?P<nodes>\d+)_r(?P<rate>\d+)_run(?P<run>\d+)$"
)
TPS_PATTERN = re.compile(r"End-to-end TPS: ([\d,]+) tx/s")
LATENCY_PATTERN = re.compile(r"End-to-end latency: ([\d,]+) ms")
CONS_TPS_PATTERN = re.compile(r"Consensus TPS: ([\d,]+) tx/s")
CONS_LATENCY_PATTERN = re.compile(r"Consensus latency: ([\d,]+) ms")


@dataclass
class SummaryPoint:
    rate: int
    timestamp: str
    run: int
    tps: int
    latency_ms: int
    path: Path


def _parse_int(match: re.Match[str] | None) -> int | None:
    if match is None:
        return None
    return int(match.group(1).replace(",", ""))


def _iter_summary_files(root: Path):
    for path in sorted(root.glob("**/80ms_balanced_summary.txt")):
        yield path


def _parse_summary(path: Path) -> SummaryPoint | None:
    run_dir = path.parent.name
    match = RUN_DIR_PATTERN.match(run_dir)
    if match is None:
        return None

    rate = int(match.group("rate"))
    if rate > MAX_RATE:
        return None

    raw = path.read_text(errors="replace")
    if USE_END_TO_END:
        tps = _parse_int(TPS_PATTERN.search(raw))
        latency_ms = _parse_int(LATENCY_PATTERN.search(raw))
    else:
        tps = _parse_int(CONS_TPS_PATTERN.search(raw))
        latency_ms = _parse_int(CONS_LATENCY_PATTERN.search(raw))

    if tps is None or latency_ms is None:
        return None

    return SummaryPoint(
        rate=rate,
        timestamp=match.group("timestamp"),
        run=int(match.group("run")),
        tps=tps,
        latency_ms=latency_ms,
        path=path,
    )


def _collect_latest_valid_points(root: Path) -> tuple[list[SummaryPoint], list[str]]:
    all_points: list[SummaryPoint] = []
    skipped: list[str] = []

    for path in _iter_summary_files(root):
        point = _parse_summary(path)
        if point is None:
            skipped.append(f"skip unparsable or filtered summary: {path}")
            continue
        all_points.append(point)

    filtered: list[SummaryPoint] = []
    for rate in sorted({point.rate for point in all_points}):
        rate_points = [point for point in all_points if point.rate == rate]
        latest_timestamp = max((point.timestamp for point in rate_points), default=None)
        if latest_timestamp is None:
            continue

        for point in rate_points:
            if point.timestamp != latest_timestamp:
                continue
            if point.tps <= 0 or point.latency_ms <= 0:
                skipped.append(f"skip invalid zero-value run: {point.path}")
                continue
            filtered.append(point)

    filtered.sort(key=lambda item: (item.rate, item.run))
    return filtered, skipped


def _aggregate(points: list[SummaryPoint]):
    grouped: dict[int, list[SummaryPoint]] = {}
    for point in points:
        grouped.setdefault(point.rate, []).append(point)

    aggregated = []
    for rate in sorted(grouped):
        runs = grouped[rate]
        tps_values = [point.tps for point in runs]
        latency_values = [point.latency_ms for point in runs]
        aggregated.append(
            {
                "rate": rate,
                "runs_used": len(runs),
                "mean_tps": mean(tps_values),
                "mean_latency_ms": mean(latency_values),
                "std_tps": stdev(tps_values) if len(tps_values) > 1 else 0.0,
                "std_latency_ms": stdev(latency_values) if len(latency_values) > 1 else 0.0,
                "timestamp": runs[0].timestamp,
            }
        )
    return aggregated


def plot_tps_latency(points: list[SummaryPoint], output_path: Path):
    aggregated = _aggregate(points)
    if not aggregated:
        raise ValueError("No valid summary points found to plot.")

    fig, ax = plt.subplots(figsize=(8.5, 5.5))

    ax.scatter(
        [point.tps for point in points],
        [point.latency_ms for point in points],
        color="#9aa0a6",
        alpha=0.45,
        s=36,
        label="Individual valid runs",
    )

    tps_values = [row["mean_tps"] for row in aggregated]
    latencies = [row["mean_latency_ms"] for row in aggregated]
    tps_err = [row["std_tps"] for row in aggregated]
    latency_err = [row["std_latency_ms"] for row in aggregated]

    ax.errorbar(
        tps_values,
        latencies,
        xerr=tps_err,
        yerr=latency_err,
        fmt="-o",
        linewidth=2,
        markersize=6,
        capsize=4,
        color="#1f77b4",
        ecolor="#1f77b4",
        label="Mean across latest valid runs",
    )

    for row in aggregated:
        ax.annotate(
            f"{row['rate'] // 1000}k",
            (row["mean_tps"], row["mean_latency_ms"]),
            textcoords="offset points",
            xytext=(6, 6),
            fontsize=9,
        )

    metric_name = "End-to-end" if USE_END_TO_END else "Consensus"
    ax.set_title(f"80ms Balanced Latency vs TPS ({metric_name})")
    ax.set_xlabel("TPS (tx/s)")
    ax.set_ylabel("Latency (ms)")
    ax.grid(True, linestyle="--", alpha=0.35)
    ax.legend()
    fig.tight_layout()
    fig.savefig(output_path, dpi=220, bbox_inches="tight")
    plt.close(fig)

    return aggregated


def main():
    configure_plot_style()
    points, skipped = _collect_latest_valid_points(RESULTS_ROOT)
    output_path = RESULTS_ROOT / OUTPUT_NAME
    aggregated = plot_tps_latency(points, output_path)

    print(f"Saved plot to: {output_path}")
    print("Used points:")
    for row in aggregated:
        print(
            f"  rate={row['rate']}, runs={row['runs_used']}, "
            f"mean_tps={row['mean_tps']:.1f}, mean_latency_ms={row['mean_latency_ms']:.1f}, "
            f"timestamp={row['timestamp']}"
        )
    if skipped:
        print("Skipped summaries:")
        for item in skipped:
            print(f"  {item}")


if __name__ == "__main__":
    main()
