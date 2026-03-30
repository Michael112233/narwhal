#!/usr/bin/env python3
"""
Plot progress-vs-average-latency curves from multiple CSV files on one figure.

Each curve uses the same logic as `plot_certificate_progress.py`:
- filter by Node_ID and round range
- keep only completed rounds (`Round_End_Time_ms` is present)
- sort certificate arrival times within each round before plotting
- use the common certificate prefix shared by all completed rounds in that CSV

Edit the configuration block below instead of passing command-line arguments.
"""

from pathlib import Path

import matplotlib.pyplot as plt

from plot_certificate_progress import (
    _completed_rows,
    _load_rows,
    _prepare_rows_for_plotting,
    _range_label,
    _resolve_csv_path,
    configure_plot_style,
    filter_certificate_rows,
)


# ============================================================================
# Configuration: edit these values directly when you want a different plot.
# ============================================================================
CSV_SERIES = [
    {
        "label": "custom_custom35000_0_10 r20000 run1",
        "csv_path": (
        "results/geo/custom35000_0_10/"
            "20260329_100342_n10_r20000_run1/"
            "geo_custom35000_0_10_round_certificate_analysis.csv"
        ),
        "node_id": 0,
        "start_round": 56,
        "end_round": 66,
    },
    {
        "label": "balanced r60000 run1",
        "csv_path": (
            "results/geo/balanced/"
            "20260329_065217_n10_r60000_run1/"
            "geo_balanced_round_certificate_analysis.csv"
        ),
        "node_id": 0,
        "start_round": 56,
        "end_round": 66,
    },
]

# Global defaults. A series can override these with its own node_id/start_round/end_round.
NODE_ID = 0
START_ROUND = 64
END_ROUND = 66

# When None, the figure is written next to the first selected CSV.
OUTPUT_DIR = None
OUTPUT_NAME = "progress_vs_avg_latency_compare_node0_custom35000_0_10_vs_balanced_60k.png"


def _default_series_label(csv_path):
    stem = csv_path.stem
    suffix = "_round_certificate_analysis"
    if stem.endswith(suffix):
        return stem[: -len(suffix)]
    return stem


def _compute_progress_curve(rows, cert_columns):
    prepared_rows = _prepare_rows_for_plotting(_completed_rows(rows), cert_columns)
    if not prepared_rows:
        raise ValueError("No completed rounds available in the selected range.")

    common_rank_count = min(len(values) for _, values in prepared_rows)
    if common_rank_count == 0:
        raise ValueError("No certificate latency values available in the selected range.")

    averages = []
    progress = list(range(1, common_rank_count + 1))
    for index in range(common_rank_count):
        values = [values[index] for _, values in prepared_rows]
        averages.append(sum(values) / len(values))

    return {
        "progress": progress,
        "averages": averages,
        "completed_rounds": len(prepared_rows),
    }


def _load_curve(spec, default_node_id, default_start_round, default_end_round):
    csv_path = _resolve_csv_path(spec.get("csv_path"))
    node_id = spec.get("node_id", default_node_id)
    start_round = spec.get("start_round", default_start_round)
    end_round = spec.get("end_round", default_end_round)
    rows, cert_columns = _load_rows(csv_path)
    filtered_rows = filter_certificate_rows(rows, node_id, start_round, end_round)
    if not filtered_rows:
        raise ValueError(
            f'No rows found for "{csv_path}" with Node_ID={node_id} '
            f'in round range {start_round}-{end_round}.'
        )

    curve = _compute_progress_curve(filtered_rows, cert_columns)
    curve["label"] = spec.get("label") or _default_series_label(csv_path)
    curve["csv_path"] = csv_path
    curve["node_id"] = node_id
    curve["start_round"] = start_round
    curve["end_round"] = end_round
    return curve


def plot_progress_comparison(csv_series, node_id, start_round, end_round, output_dir=None, output_name=None):
    if not csv_series:
        raise ValueError("CSV_SERIES cannot be empty.")

    curves = [
        _load_curve(spec, node_id, start_round, end_round)
        for spec in csv_series
    ]

    same_scope = all(
        curve["node_id"] == curves[0]["node_id"]
        and curve["start_round"] == curves[0]["start_round"]
        and curve["end_round"] == curves[0]["end_round"]
        for curve in curves
    )

    first_csv_path = curves[0]["csv_path"]
    output_dir = Path(output_dir) if output_dir else first_csv_path.parent
    output_dir.mkdir(parents=True, exist_ok=True)

    output_name = output_name or (
        f"progress_vs_avg_latency_compare_node{node_id}_"
        f"rounds_{start_round}_{end_round}.png"
        if same_scope
        else "progress_vs_avg_latency_compare_mixed_scopes.png"
    )
    output_path = output_dir / output_name

    fig, ax = plt.subplots(figsize=(9, 6))
    cmap = plt.get_cmap("tab10")

    max_rank = 0
    for index, curve in enumerate(curves):
        progress = curve["progress"]
        averages = curve["averages"]
        max_rank = max(max_rank, max(progress))
        ax.plot(
            averages,
            progress,
            marker="o",
            linewidth=2,
            markersize=5,
            color=cmap(index % 10),
            label=(
                f'{curve["label"]} '
                f'(node={curve["node_id"]}, '
                f'rounds={curve["start_round"]}-{curve["end_round"]})'
            ),
        )

    subtitle = (
        f'Node {node_id}, {_range_label(start_round, end_round)}'
        if same_scope
        else 'Series-specific nodes and round ranges'
    )
    ax.set_title(
        f"Progress vs. Avg Latency Comparison\n"
        f"{subtitle}"
    )
    ax.set_xlabel("Average Time Delta (ms)")
    ax.set_ylabel("Certificate Arrival Rank")
    ax.set_yticks(list(range(1, max_rank + 1)))
    ax.grid(True, linestyle="--", alpha=0.4)
    ax.legend()
    fig.tight_layout()
    fig.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close(fig)

    return output_path, curves


def main():
    if START_ROUND > END_ROUND:
        raise SystemExit("START_ROUND must be less than or equal to END_ROUND")

    configure_plot_style()
    output_path, curves = plot_progress_comparison(
        CSV_SERIES,
        NODE_ID,
        START_ROUND,
        END_ROUND,
        output_dir=OUTPUT_DIR,
        output_name=OUTPUT_NAME,
    )

    print(f"Saved comparison plot to: {output_path}")
    for curve in curves:
        print(
            f'  {curve["label"]}: {curve["csv_path"]} '
            f'(node={curve["node_id"]}, rounds={curve["start_round"]}-{curve["end_round"]}, '
            f'completed_rounds={curve["completed_rounds"]})'
        )


if __name__ == "__main__":
    main()
