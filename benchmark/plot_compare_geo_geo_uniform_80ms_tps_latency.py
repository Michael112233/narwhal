#!/usr/bin/env python3
"""
Overlay geo, geo_uniform, and 80ms balanced TPS-latency curves in one figure.

This script reuses the existing per-network summary aggregation logic so the
comparison stays consistent with the standalone plots.
"""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt

from plot_80ms_tps_latency import (
    RESULTS_ROOT as MS80_RESULTS_ROOT,
    _aggregate as aggregate_80ms,
    _collect_latest_valid_points as collect_80ms_points,
)
from plot_balanced_tps_latency import (
    RESULTS_ROOT as GEO_RESULTS_ROOT,
    _aggregate as aggregate_geo,
    _collect_latest_valid_points as collect_geo_points,
)
from plot_certificate_progress import configure_plot_style
from plot_geo_uniform_tps_latency import (
    RESULTS_ROOT as GEO_UNIFORM_RESULTS_ROOT,
    _aggregate as aggregate_geo_uniform,
    _collect_latest_valid_points as collect_geo_uniform_points,
)


OUTPUT_PATH = Path("results") / "tps_latency_geo_geo_uniform_80ms_balanced_upto_50k.png"


def _plot_series(ax, rows, *, color: str, marker: str, label: str, annotation_dx: int):
    tps_values = [row["mean_tps"] for row in rows]
    latencies = [row["mean_latency_ms"] for row in rows]
    tps_err = [row["std_tps"] for row in rows]
    latency_err = [row["std_latency_ms"] for row in rows]

    ax.errorbar(
        tps_values,
        latencies,
        xerr=tps_err,
        yerr=latency_err,
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


def main():
    configure_plot_style()

    geo_points, geo_skipped = collect_geo_points(GEO_RESULTS_ROOT)
    geo_uniform_points, geo_uniform_skipped = collect_geo_uniform_points(
        GEO_UNIFORM_RESULTS_ROOT
    )
    ms80_points, ms80_skipped = collect_80ms_points(MS80_RESULTS_ROOT)

    geo_rows = aggregate_geo(geo_points)
    geo_uniform_rows = aggregate_geo_uniform(geo_uniform_points)
    ms80_rows = aggregate_80ms(ms80_points)
    if not geo_rows or not geo_uniform_rows or not ms80_rows:
        raise ValueError("Missing valid points for geo, geo_uniform, or 80ms.")

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(9.4, 6.0))

    _scatter_runs(ax, geo_points, color="#1f77b4")
    _scatter_runs(ax, geo_uniform_points, color="#d95f02")
    _scatter_runs(ax, ms80_points, color="#2a9d8f")

    _plot_series(
        ax,
        geo_rows,
        color="#1f77b4",
        marker="o",
        label="geo balanced",
        annotation_dx=-18,
    )
    _plot_series(
        ax,
        geo_uniform_rows,
        color="#d95f02",
        marker="s",
        label="geo_uniform balanced",
        annotation_dx=6,
    )
    _plot_series(
        ax,
        ms80_rows,
        color="#2a9d8f",
        marker="^",
        label="80ms balanced",
        annotation_dx=10,
    )

    ax.set_title("Geo vs Geo-Uniform vs 80ms Balanced Latency vs TPS")
    ax.set_xlabel("TPS (tx/s)")
    ax.set_ylabel("Latency (ms)")
    ax.grid(True, linestyle="--", alpha=0.35)
    ax.legend()
    fig.tight_layout()
    fig.savefig(OUTPUT_PATH, dpi=220, bbox_inches="tight")
    plt.close(fig)

    print(f"Saved plot to: {OUTPUT_PATH}")
    print("Geo points:")
    for row in geo_rows:
        print(
            f"  rate={row['rate']}, runs={row['runs_used']}, "
            f"mean_tps={row['mean_tps']:.1f}, mean_latency_ms={row['mean_latency_ms']:.1f}"
        )
    print("Geo-uniform points:")
    for row in geo_uniform_rows:
        print(
            f"  rate={row['rate']}, runs={row['runs_used']}, "
            f"mean_tps={row['mean_tps']:.1f}, mean_latency_ms={row['mean_latency_ms']:.1f}"
        )
    print("80ms points:")
    for row in ms80_rows:
        print(
            f"  rate={row['rate']}, runs={row['runs_used']}, "
            f"mean_tps={row['mean_tps']:.1f}, mean_latency_ms={row['mean_latency_ms']:.1f}"
        )

    skipped = geo_skipped + geo_uniform_skipped + ms80_skipped
    if skipped:
        print("Skipped summaries:")
        for item in skipped:
            print(f"  {item}")


if __name__ == "__main__":
    main()
