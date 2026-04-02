#!/usr/bin/env python3
"""
Generate comparison figures for the selected 80ms hotspot workloads.

The script emits two figures:
1. Certificate collection progress comparison for node 0 at 30k.
2. TPS-vs-latency comparison across available rates.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from statistics import mean, stdev

import matplotlib.pyplot as plt

from plot_certificate_progress import (
    _completed_rows,
    _load_rows,
    _prepare_rows_for_plotting,
    configure_plot_style,
    filter_certificate_rows,
)


OUTPUT_DIR = Path("results/80ms/comparisons")
NODE_ID = 0
START_ROUND = 200
END_ROUND = 350
MAX_RATE = 30_000


PROGRESS_SERIES = [
    {
        "label": "balanced 30k",
        "color": "#1f77b4",
        "marker": "o",
        "csv_paths": [
            Path(
                "results/80ms/balanced/"
                "20260401_024346_n10_r30000_run1/"
                "80ms_balanced_round_certificate_analysis.csv"
            ),
            Path(
                "results/80ms/balanced/"
                "20260401_024346_n10_r30000_run2/"
                "80ms_balanced_round_certificate_analysis.csv"
            ),
        ],
    },
    {
        "label": "custom-high-3 30k",
        "color": "#ff7f0e",
        "marker": "s",
        "csv_paths": [
            Path(
                "results/80ms/custom-high-3/"
                "20260401_055149_n10_r30000_run1/"
                "80ms_custom-high-3_round_certificate_analysis.csv"
            ),
            Path(
                "results/80ms/custom-high-3/"
                "20260401_055149_n10_r30000_run2/"
                "80ms_custom-high-3_round_certificate_analysis.csv"
            ),
        ],
    },
    {
        "label": "custom-high-5 30k",
        "color": "#2ca02c",
        "marker": "^",
        "csv_paths": [
            Path(
                "results/80ms/custom-high-5/"
                "20260401_074058_n10_r30000_run1/"
                "80ms_custom-high-5_round_certificate_analysis.csv"
            ),
            Path(
                "results/80ms/custom-high-5/"
                "20260401_074058_n10_r30000_run2/"
                "80ms_custom-high-5_round_certificate_analysis.csv"
            ),
        ],
    },
]


TPS_SERIES = [
    {
        "label": "balanced",
        "root": Path("results/80ms/balanced"),
        "summary_name": "80ms_balanced_summary.txt",
        "color": "#1f77b4",
        "marker": "o",
        "annotate_dx": -16,
    },
    {
        "label": "custom-high-3",
        "root": Path("results/80ms/custom-high-3"),
        "summary_name": "80ms_custom-high-3_summary.txt",
        "color": "#ff7f0e",
        "marker": "s",
        "annotate_dx": 6,
    },
    {
        "label": "custom-high-5",
        "root": Path("results/80ms/custom-high-5"),
        "summary_name": "80ms_custom-high-5_summary.txt",
        "color": "#2ca02c",
        "marker": "^",
        "annotate_dx": 10,
    },
]


RUN_DIR_PATTERN = re.compile(
    r"(?P<timestamp>\d{8}_\d{6})_n(?P<nodes>\d+)_r(?P<rate>\d+)_run(?P<run>\d+)$"
)
TPS_PATTERN = re.compile(r"End-to-end TPS: ([\d,]+) tx/s")
LATENCY_PATTERN = re.compile(r"End-to-end latency: ([\d,]+) ms")


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


def _compute_progress_curve(spec):
    prepared_rows = []

    for csv_path in spec["csv_paths"]:
        rows, cert_columns = _load_rows(csv_path)
        filtered_rows = filter_certificate_rows(rows, NODE_ID, START_ROUND, END_ROUND)
        if not filtered_rows:
            raise ValueError(
                f"No rows found for {csv_path} with Node_ID={NODE_ID} "
                f"in round range {START_ROUND}-{END_ROUND}."
            )
        prepared_rows.extend(
            _prepare_rows_for_plotting(_completed_rows(filtered_rows), cert_columns)
        )

    if not prepared_rows:
        raise ValueError(f"No completed rounds available for {spec['label']}.")

    common_rank_count = min(len(values) for _, values in prepared_rows)
    if common_rank_count == 0:
        raise ValueError(f"No certificate latency values available for {spec['label']}.")

    return {
        "label": spec["label"],
        "color": spec["color"],
        "marker": spec["marker"],
        "progress": list(range(1, common_rank_count + 1)),
        "averages": [
            mean(values[index] for _, values in prepared_rows)
            for index in range(common_rank_count)
        ],
        "completed_rounds": len(prepared_rows),
    }


def plot_progress_comparison(output_path: Path):
    curves = [_compute_progress_curve(spec) for spec in PROGRESS_SERIES]

    fig, ax = plt.subplots(figsize=(9, 6))
    for curve in curves:
        ax.plot(
            curve["averages"],
            curve["progress"],
            marker=curve["marker"],
            linewidth=2,
            markersize=5,
            color=curve["color"],
            label=f"{curve['label']} ({curve['completed_rounds']} rounds)",
        )

    ax.set_title(
        "Certificate Collection Comparison\n"
        f"Node {NODE_ID}, Rounds {START_ROUND}-{END_ROUND}"
    )
    ax.set_xlabel("Average Time Delta (ms)")
    ax.set_ylabel("Certificate Arrival Rank")
    ax.set_yticks(list(range(1, max(max(curve["progress"]) for curve in curves) + 1)))
    ax.grid(True, linestyle="--", alpha=0.4)
    ax.legend()
    fig.tight_layout()
    fig.savefig(output_path, dpi=220, bbox_inches="tight")
    plt.close(fig)

    return curves


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


def _collect_workload_points(root: Path, summary_name: str) -> list[SummaryPoint]:
    all_points = []
    for path in sorted(root.glob(f"**/{summary_name}")):
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
            if point.tps <= 0 or point.latency_ms <= 0:
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


def plot_tps_latency_comparison(output_path: Path):
    fig, ax = plt.subplots(figsize=(9, 6))
    series_rows = []

    for spec in TPS_SERIES:
        points = _collect_workload_points(spec["root"], spec["summary_name"])
        aggregated = _aggregate(points)
        if not aggregated:
            continue

        ax.scatter(
            [point.tps for point in points],
            [point.latency_ms for point in points],
            color=spec["color"],
            alpha=0.18,
            s=28,
        )

        ax.errorbar(
            [row["mean_tps"] for row in aggregated],
            [row["mean_latency_ms"] for row in aggregated],
            xerr=[row["std_tps"] for row in aggregated],
            yerr=[row["std_latency_ms"] for row in aggregated],
            fmt=f"-{spec['marker']}",
            linewidth=2,
            markersize=6,
            capsize=4,
            color=spec["color"],
            ecolor=spec["color"],
            label=spec["label"],
        )

        for row in aggregated:
            ax.annotate(
                f"{row['rate'] // 1000}k",
                (row["mean_tps"], row["mean_latency_ms"]),
                textcoords="offset points",
                xytext=(spec["annotate_dx"], 6),
                fontsize=9,
                color=spec["color"],
            )

        series_rows.append((spec["label"], aggregated))

    ax.set_title("80ms TPS vs Latency Comparison\nbalanced vs custom-high-3 vs custom-high-5")
    ax.set_xlabel("TPS (tx/s)")
    ax.set_ylabel("Latency (ms)")
    ax.grid(True, linestyle="--", alpha=0.35)
    ax.legend()
    fig.tight_layout()
    fig.savefig(output_path, dpi=220, bbox_inches="tight")
    plt.close(fig)

    return series_rows


def main():
    configure_plot_style()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    progress_output = OUTPUT_DIR / (
        "progress_vs_avg_latency_compare_"
        "balanced_custom-high-3_custom-high-5_node0_rounds_200_350.png"
    )
    tps_output = OUTPUT_DIR / (
        "tps_latency_compare_balanced_custom-high-3_custom-high-5_upto_30k.png"
    )

    curves = plot_progress_comparison(progress_output)
    series_rows = plot_tps_latency_comparison(tps_output)

    print(f"Saved progress comparison to: {progress_output}")
    for curve in curves:
        print(
            f"  {curve['label']}: completed_rounds={curve['completed_rounds']}, "
            f"max_rank={max(curve['progress'])}"
        )

    print(f"Saved TPS-latency comparison to: {tps_output}")
    for label, rows in series_rows:
        for row in rows:
            print(
                f"  {label}: rate={row['rate']}, runs={row['runs_used']}, "
                f"mean_tps={row['mean_tps']:.1f}, mean_latency_ms={row['mean_latency_ms']:.1f}"
            )


if __name__ == "__main__":
    main()
