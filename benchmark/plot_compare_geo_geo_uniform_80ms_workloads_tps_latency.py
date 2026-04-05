#!/usr/bin/env python3
"""
Generate TPS-latency comparison figures across geo, geo_uniform, and 80ms
for the shared hotspot workloads.

This complements the existing balanced-only comparison script by emitting one
cross-network figure per workload.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from statistics import mean, stdev

import matplotlib.pyplot as plt

from plot_certificate_progress import configure_plot_style


OUTPUT_DIR = Path("results/comparisons")
MAX_RATE = 50_000
USE_END_TO_END = True

NETWORK_SPECS = [
    {
        "key": "geo",
        "label": "geo",
        "color": "#1f77b4",
        "marker": "o",
        "annotation_dx": -18,
    },
    {
        "key": "geo_uniform",
        "label": "geo_uniform",
        "color": "#d95f02",
        "marker": "s",
        "annotation_dx": 6,
    },
    {
        "key": "80ms",
        "label": "80ms",
        "color": "#2a9d8f",
        "marker": "^",
        "annotation_dx": 10,
    },
]

WORKLOADS = [
    "balanced",
    "custom-high-3",
    "custom-high-5",
    "custom-high-3opp",
]

PREFERRED_OLD_RATES = {
    ("geo", "balanced"): {40_000, 50_000},
}

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


def _is_old_path(path: Path) -> bool:
    return any(part == "old" or part.endswith("_old") for part in path.parts)


def _summary_name(network_key: str, workload: str) -> str:
    return f"{network_key}_{workload}_summary.txt"


def _parse_summary(path: Path) -> SummaryPoint | None:
    match = RUN_DIR_PATTERN.match(path.parent.name)
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


def _collect_latest_valid_points(root: Path, summary_name: str) -> tuple[list[SummaryPoint], list[str]]:
    all_points: list[SummaryPoint] = []
    skipped: list[str] = []

    for path in sorted(root.glob(f"**/{summary_name}")):
        point = _parse_summary(path)
        if point is None:
            skipped.append(f"skip unparsable or filtered summary: {path}")
            continue
        all_points.append(point)

    filtered: list[SummaryPoint] = []
    for rate in sorted({point.rate for point in all_points}):
        rate_points = [point for point in all_points if point.rate == rate]

        non_old_points = [point for point in rate_points if not _is_old_path(point.path)]
        if non_old_points:
            rate_points = non_old_points

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


def _collect_workload_points(network_key: str, workload: str) -> tuple[list[SummaryPoint], list[str]]:
    root = Path("results") / network_key / workload
    summary_name = _summary_name(network_key, workload)
    all_points, skipped = _collect_latest_valid_points(root, summary_name)
    preferred_old_rates = PREFERRED_OLD_RATES.get((network_key, workload), set())
    if not preferred_old_rates:
        return all_points, skipped

    selected: list[SummaryPoint] = []
    for rate in sorted({point.rate for point in all_points}):
        rate_points = [point for point in all_points if point.rate == rate]
        if rate in preferred_old_rates:
            old_points = [point for point in rate_points if _is_old_path(point.path)]
            if old_points:
                selected.extend(old_points)
                continue
        selected.extend(rate_points)

    selected.sort(key=lambda item: (item.rate, item.run))
    return selected, skipped


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


def _plot_series(ax, rows, *, color: str, marker: str, label: str, annotation_dx: int):
    ax.errorbar(
        [row["mean_tps"] for row in rows],
        [row["mean_latency_ms"] for row in rows],
        xerr=[row["std_tps"] for row in rows],
        yerr=[row["std_latency_ms"] for row in rows],
        fmt=f"-{marker}",
        linewidth=2,
        markersize=6,
        capsize=4,
        color=color,
        ecolor=color,
        label=label,
    )

    for row in rows:
        ax.annotate(
            f"{row['rate'] // 1000}k",
            (row["mean_tps"], row["mean_latency_ms"]),
            textcoords="offset points",
            xytext=(annotation_dx, 6),
            fontsize=9,
            color=color,
        )


def _scatter_runs(ax, points, *, color: str):
    ax.scatter(
        [point.tps for point in points],
        [point.latency_ms for point in points],
        color=color,
        alpha=0.16,
        s=30,
    )


def plot_workload(workload: str):
    per_network = []
    skipped = []

    for spec in NETWORK_SPECS:
        points, network_skipped = _collect_workload_points(spec["key"], workload)
        rows = _aggregate(points)
        if not rows:
            raise ValueError(f"Missing valid points for {spec['key']} / {workload}.")
        per_network.append((spec, points, rows))
        skipped.extend(network_skipped)

    output_path = OUTPUT_DIR / (
        f"tps_latency_geo_geo_uniform_80ms_{workload}_upto_50k.png"
    )
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    fig, ax = plt.subplots(figsize=(9.4, 6.0))
    for spec, points, rows in per_network:
        _scatter_runs(ax, points, color=spec["color"])
        _plot_series(
            ax,
            rows,
            color=spec["color"],
            marker=spec["marker"],
            label=f"{spec['label']} {workload}",
            annotation_dx=spec["annotation_dx"],
        )

    ax.set_title(f"Geo vs Geo-Uniform vs 80ms {workload} Latency vs TPS")
    ax.set_xlabel("TPS (tx/s)")
    ax.set_ylabel("Latency (ms)")
    ax.grid(True, linestyle="--", alpha=0.35)
    ax.legend()
    fig.tight_layout()
    fig.savefig(output_path, dpi=220, bbox_inches="tight")
    plt.close(fig)

    return output_path, per_network, skipped


def main():
    configure_plot_style()

    for workload in WORKLOADS:
        output_path, per_network, skipped = plot_workload(workload)
        print(f"Saved plot to: {output_path}")
        for spec, _, rows in per_network:
            for row in rows:
                print(
                    f"  {workload} / {spec['key']}: "
                    f"rate={row['rate']}, runs={row['runs_used']}, "
                    f"mean_tps={row['mean_tps']:.1f}, "
                    f"mean_latency_ms={row['mean_latency_ms']:.1f}"
                )
        if skipped:
            print("Skipped summaries:")
            for item in skipped:
                print(f"  {item}")


if __name__ == "__main__":
    main()
