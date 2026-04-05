#!/usr/bin/env python3
"""
Generate 30k certificate collection comparison figures across networks for the
same workload.

Each figure compares geo, geo_uniform, and 80ms under one workload using the
same "progress vs average latency" style as the existing certificate collection
comparison plots.
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
END_ROUND = 250
OUTPUT_DIR = Path("results/comparisons/cert")

NETWORK_STYLE = {
    "geo": {"color": "#1f77b4", "marker": "o"},
    "geo_uniform": {"color": "#d95f02", "marker": "s"},
    "80ms": {"color": "#2a9d8f", "marker": "^"},
}

WORKLOAD_SPECS = [
    {
        "workload": "balanced",
        "runs": {
            "geo": [
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
            "geo_uniform": [
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
            "80ms": [
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
    },
    {
        "workload": "custom-high-3",
        "runs": {
            "geo": [
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
            "geo_uniform": [
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
            "80ms": [
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
    },
    {
        "workload": "custom-high-5",
        "runs": {
            "geo": [
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
            "geo_uniform": [
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
            "80ms": [
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
]


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


def _plot_workload(workload_spec):
    workload = workload_spec["workload"]
    curves = []

    for network, csv_paths in workload_spec["runs"].items():
        curve = _compute_progress_curve(f"{network} 30k", csv_paths)
        style = NETWORK_STYLE[network]
        curve["color"] = style["color"]
        curve["marker"] = style["marker"]
        curves.append(curve)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / (
        f"progress_vs_avg_latency_compare_geo_geo_uniform_80ms_{workload}"
        f"_node{NODE_ID}_rounds_{START_ROUND}_{END_ROUND}.png"
    )

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
        f"{workload}, Node {NODE_ID}, Rounds {START_ROUND}-{END_ROUND}"
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
    for workload_spec in WORKLOAD_SPECS:
        output_path, curves = _plot_workload(workload_spec)
        print(f"Saved plot to: {output_path}")
        for curve in curves:
            print(
                f"  {workload_spec['workload']} / {curve['label']}: "
                f"completed_rounds={curve['completed_rounds']}, "
                f"max_rank={max(curve['progress'])}"
            )


if __name__ == "__main__":
    main()
