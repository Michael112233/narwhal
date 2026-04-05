#!/usr/bin/env python3
"""
Generate per-network certificate collection progress comparison figures at 30k
for balanced, custom-high-3, and custom-high-5.

The figure style matches the existing 80ms comparison plot.
"""

from __future__ import annotations

from pathlib import Path
from statistics import mean

import matplotlib.pyplot as plt

from plot_certificate_progress import (
    _completed_rows,
    _load_rows,
    _prepare_rows_for_plotting,
    configure_plot_style,
    filter_certificate_rows,
)


NODE_ID = 0
START_ROUND = 200
END_ROUND = 350

NETWORKS = [
    {
        "key": "80ms",
        "output_dir": Path("results/80ms/comparisons"),
        "runs": {
            "balanced": [
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
            "custom-high-3": [
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
            "custom-high-5": [
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
    },
    {
        "key": "geo",
        "output_dir": Path("results/geo/comparisons"),
        "runs": {
            "balanced": [
                Path(
                    "results/geo/balanced/"
                    "20260331_061814_n10_r30000_run1/"
                    "geo_balanced_round_certificate_analysis.csv"
                ),
                Path(
                    "results/geo/balanced/"
                    "20260331_061814_n10_r30000_run2/"
                    "geo_balanced_round_certificate_analysis.csv"
                ),
            ],
            "custom-high-3": [
                Path(
                    "results/geo/custom-high-3/"
                    "20260402_052851_n10_r30000_run1/"
                    "geo_custom-high-3_round_certificate_analysis.csv"
                ),
                Path(
                    "results/geo/custom-high-3/"
                    "20260402_052851_n10_r30000_run2/"
                    "geo_custom-high-3_round_certificate_analysis.csv"
                ),
            ],
            "custom-high-5": [
                Path(
                    "results/geo/custom-high-5/"
                    "20260402_033338_n10_r30000_run1/"
                    "geo_custom-high-5_round_certificate_analysis.csv"
                ),
                Path(
                    "results/geo/custom-high-5/"
                    "20260402_033338_n10_r30000_run2/"
                    "geo_custom-high-5_round_certificate_analysis.csv"
                ),
            ],
        },
    },
    {
        "key": "geo_uniform",
        "output_dir": Path("results/geo_uniform/comparisons"),
        "runs": {
            "balanced": [
                Path(
                    "results/geo_uniform/balanced/"
                    "20260401_013808_n10_r30000_run1/"
                    "geo_uniform_balanced_round_certificate_analysis.csv"
                ),
                Path(
                    "results/geo_uniform/balanced/"
                    "20260401_013808_n10_r30000_run2/"
                    "geo_uniform_balanced_round_certificate_analysis.csv"
                ),
            ],
            "custom-high-3": [
                Path(
                    "results/geo_uniform/custom-high-3/"
                    "20260402_081216_n10_r30000_run1/"
                    "geo_uniform_custom-high-3_round_certificate_analysis.csv"
                ),
                Path(
                    "results/geo_uniform/custom-high-3/"
                    "20260402_081216_n10_r30000_run2/"
                    "geo_uniform_custom-high-3_round_certificate_analysis.csv"
                ),
            ],
            "custom-high-5": [
                Path(
                    "results/geo_uniform/custom-high-5/"
                    "20260402_091731_n10_r30000_run1/"
                    "geo_uniform_custom-high-5_round_certificate_analysis.csv"
                ),
                Path(
                    "results/geo_uniform/custom-high-5/"
                    "20260402_091731_n10_r30000_run2/"
                    "geo_uniform_custom-high-5_round_certificate_analysis.csv"
                ),
            ],
        },
    },
]

SERIES_STYLE = {
    "balanced": {"color": "#1f77b4", "marker": "o"},
    "custom-high-3": {"color": "#ff7f0e", "marker": "s"},
    "custom-high-5": {"color": "#2ca02c", "marker": "^"},
}


def _compute_progress_curve(label: str, csv_paths: list[Path]):
    prepared_rows = []

    for csv_path in csv_paths:
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
        raise ValueError(f"No completed rounds available for {label}.")

    common_rank_count = min(len(values) for _, values in prepared_rows)
    if common_rank_count == 0:
        raise ValueError(f"No certificate latency values available for {label}.")

    return {
        "label": label,
        "progress": list(range(1, common_rank_count + 1)),
        "averages": [
            mean(values[index] for _, values in prepared_rows)
            for index in range(common_rank_count)
        ],
        "completed_rounds": len(prepared_rows),
    }


def _plot_network(network_spec):
    output_dir = network_spec["output_dir"]
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / (
        "progress_vs_avg_latency_compare_"
        "balanced_custom-high-3_custom-high-5_node0_rounds_200_350.png"
    )

    curves = []
    for workload, csv_paths in network_spec["runs"].items():
        curve = _compute_progress_curve(f"{workload} 30k", csv_paths)
        style = SERIES_STYLE[workload]
        curve["color"] = style["color"]
        curve["marker"] = style["marker"]
        curves.append(curve)

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
        f"{network_spec['key']}, Node {NODE_ID}, Rounds {START_ROUND}-{END_ROUND}"
    )
    ax.set_xlabel("Average Time Delta (ms)")
    ax.set_ylabel("Certificate Arrival Rank")
    ax.set_yticks(list(range(1, max(max(curve["progress"]) for curve in curves) + 1)))
    ax.grid(True, linestyle="--", alpha=0.4)
    ax.legend()
    fig.tight_layout()
    fig.savefig(output_path, dpi=220, bbox_inches="tight")
    plt.close(fig)

    return output_path, curves


def main():
    configure_plot_style()
    for network_spec in NETWORKS:
        output_path, curves = _plot_network(network_spec)
        print(f"Saved plot to: {output_path}")
        for curve in curves:
            print(
                f"  {network_spec['key']} / {curve['label']}: "
                f"completed_rounds={curve['completed_rounds']}, "
                f"max_rank={max(curve['progress'])}"
            )


if __name__ == "__main__":
    main()
