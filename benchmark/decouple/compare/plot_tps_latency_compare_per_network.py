#!/usr/bin/env python3
"""
Plot per-network TPS-latency comparisons across workloads for decoupled runs.

Outputs are saved under each network directory, for example:
- result_decouple/80ms/tps_latency_compare_balanced_custom-high-3_custom-high-5.png
- result_decouple/geo/tps_latency_compare_balanced_custom-high-3_custom-high-5.png
- result_decouple/geo_uniform/tps_latency_compare_balanced_custom-high-3_custom-high-5.png
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from statistics import mean, stdev

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import StrMethodFormatter

SCRIPT_DIR = Path(__file__).resolve().parent

if not hasattr(np, "Inf"):
    np.Inf = np.inf


DATA_ROOT = SCRIPT_DIR.parent
MAX_RATE = 140_000
OUTPUT_STEM = "tps_latency_compare_balanced_custom-high-3_custom-high-5"

RUN_DIR_PATTERN = re.compile(
    r"(?:(?P<prefix>.+?)_)?(?P<timestamp>\d{8}_\d{6})_n(?P<nodes>\d+)_r(?P<rate>\d+)_run(?P<run>\d+)$"
)
TPS_PATTERN = re.compile(r"End-to-end TPS: ([\d,]+) tx/s")
LATENCY_PATTERN = re.compile(r"End-to-end latency: ([\d,]+) ms")

NETWORKS = [
    {"label": "80ms", "dir_name": "80ms"},
    {"label": "geo", "dir_name": "geo"},
    {"label": "geo_uniform", "dir_name": "geo_uniform"},
]

WORKLOADS = [
    {
        "label": "balanced",
        "dir_name": "balanced",
        "color": "#1f77b4",
        "marker": "o",
        "annotate_dx": -14,
    },
    {
        "label": "custom-high-3",
        "dir_name": "custom-high-3",
        "color": "#ff7f0e",
        "marker": "s",
        "annotate_dx": 6,
    },
    {
        "label": "custom-high-5",
        "dir_name": "custom-high-5",
        "color": "#2ca02c",
        "marker": "^",
        "annotate_dx": 10,
    },
]


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


def _set_plot_style():
    plt.rcParams.update(
        {
            "font.family": "sans-serif",
            "font.sans-serif": ["DejaVu Sans", "Arial", "Helvetica"],
            "mathtext.fontset": "dejavusans",
            "font.size": 8,
            "axes.labelsize": 10,
            "axes.titlesize": 11,
            "xtick.labelsize": 8,
            "ytick.labelsize": 8,
            "legend.fontsize": 9,
            "axes.spines.top": True,
            "axes.spines.right": True,
            "axes.edgecolor": "#000000",
            "axes.linewidth": 0.8,
            "xtick.direction": "in",
            "ytick.direction": "in",
        }
    )


def _parse_summary(path: Path) -> SummaryPoint | None:
    match = RUN_DIR_PATTERN.match(path.parent.name)
    if match is None:
        return None

    rate = int(match.group("rate"))
    if rate > MAX_RATE:
        return None

    raw = path.read_text(errors="replace")
    tps = _parse_int(TPS_PATTERN.search(raw))
    latency_ms = _parse_int(LATENCY_PATTERN.search(raw))
    if tps is None or latency_ms is None or tps <= 0 or latency_ms <= 0:
        return None

    return SummaryPoint(
        rate=rate,
        timestamp=match.group("timestamp"),
        run=int(match.group("run")),
        tps=tps,
        latency_ms=latency_ms,
        path=path,
    )


def _collect_latest_valid_points(network_dir: str, workload_dir: str) -> list[SummaryPoint]:
    root = DATA_ROOT / network_dir / workload_dir
    all_points = []
    for path in sorted(root.glob("**/summary.txt")):
        point = _parse_summary(path)
        if point is not None:
            all_points.append(point)

    filtered = []
    for rate in sorted({point.rate for point in all_points}):
        rate_points = [point for point in all_points if point.rate == rate]
        latest_timestamp = max((point.timestamp for point in rate_points), default=None)
        if latest_timestamp is None:
            continue
        for point in rate_points:
            if point.timestamp != latest_timestamp:
                continue
            filtered.append(point)

    filtered.sort(key=lambda item: (item.rate, item.run))
    return filtered


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
            }
        )
    return aggregated


def _save_png_and_pdf(fig, output_stem: Path):
    png_path = output_stem.with_suffix(".png")
    pdf_path = output_stem.with_suffix(".pdf")
    fig.savefig(png_path, dpi=260, bbox_inches="tight")
    fig.savefig(pdf_path, bbox_inches="tight")
    return png_path, pdf_path


def plot_network(network_spec: dict):
    fig, ax = plt.subplots(figsize=(7.0, 4.8))
    series_rows = []

    for workload in WORKLOADS:
        points = _collect_latest_valid_points(
            network_spec["dir_name"], workload["dir_name"]
        )
        aggregated = _aggregate(points)
        if not aggregated:
            continue

        ax.scatter(
            [point.tps / 1000.0 for point in points],
            [point.latency_ms / 1000.0 for point in points],
            color=workload["color"],
            alpha=0.16,
            s=28,
        )

        ax.errorbar(
            [row["mean_tps"] / 1000.0 for row in aggregated],
            [row["mean_latency_ms"] / 1000.0 for row in aggregated],
            xerr=[row["std_tps"] / 1000.0 for row in aggregated],
            yerr=[row["std_latency_ms"] / 1000.0 for row in aggregated],
            fmt=f"-{workload['marker']}",
            linewidth=2.0,
            markersize=6,
            capsize=3.5,
            color=workload["color"],
            ecolor=workload["color"],
            label=workload["label"],
        )

        for row in aggregated:
            ax.annotate(
                f"{row['rate'] // 1000}k",
                (row["mean_tps"] / 1000.0, row["mean_latency_ms"] / 1000.0),
                textcoords="offset points",
                xytext=(workload["annotate_dx"], 5),
                fontsize=8,
                color=workload["color"],
            )

        series_rows.append((workload["label"], aggregated))

    ax.set_title(f"{network_spec['label']} Workload Comparison", pad=8)
    ax.set_xlabel("Throughput (KTps)")
    ax.set_ylabel("End-to-end Latency (s)")
    ax.xaxis.set_major_formatter(StrMethodFormatter("{x:,.0f}"))
    ax.yaxis.set_major_formatter(StrMethodFormatter("{x:.1f}"))
    ax.grid(True, axis="both", linestyle=(0, (2.2, 2.2)), alpha=0.28, color="#9a9a9a")
    ax.tick_params(direction="in", top=True, right=True)
    ax.legend(frameon=True, facecolor="white", edgecolor="#d0d0d0")
    for spine in ax.spines.values():
        spine.set_visible(True)
        spine.set_color("black")
        spine.set_linewidth(0.8)

    output_stem = DATA_ROOT / network_spec["dir_name"] / OUTPUT_STEM
    output_stem.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    saved_paths = _save_png_and_pdf(fig, output_stem)
    plt.close(fig)
    return saved_paths, series_rows


def main():
    _set_plot_style()
    for network_spec in NETWORKS:
        saved_paths, series_rows = plot_network(network_spec)
        for output_path in saved_paths:
            print(f"Saved workload TPS-latency plot to: {output_path}")
        for label, rows in series_rows:
            for row in rows:
                print(
                    f"  {network_spec['label']} / {label}: "
                    f"rate={row['rate']}, runs={row['runs_used']}, "
                    f"mean_tps={row['mean_tps']:.1f}, "
                    f"mean_latency_ms={row['mean_latency_ms']:.1f}"
                )


if __name__ == "__main__":
    main()
