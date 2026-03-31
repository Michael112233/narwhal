#!/usr/bin/env python3
"""
Plot certificate collection progress from round_certificate_analysis CSV files.

This script generates the progress-vs-average-latency figure for a selected
CSV file. A separate helper script can batch-generate latency-over-rounds
figures for balanced results.

Edit the configuration block below instead of passing command-line arguments.
"""

import csv
import re
from pathlib import Path

import numpy as np
import matplotlib.pyplot as plt

# Compatibility shim for older matplotlib versions running with NumPy 2.x.
if not hasattr(np, "Inf"):
    np.Inf = np.inf


CERT_TIME_PATTERN = re.compile(r"Certificate_(\d+)_Time_Delta_ms$")

# ============================================================================
# Configuration: edit these values directly when you want a different plot.
# ============================================================================
# Set `CSV_PATH` to a single CSV. When left as None, the latest matching CSV
# under `results/` is used.
CSV_PATH = None
NODE_ID = 0
START_ROUND = 200
END_ROUND = 800
# When None, figures are written next to the selected CSV.
OUTPUT_DIR = None


def configure_plot_style():
    for style_name in (
        "seaborn-v0_8-whitegrid",
        "seaborn-whitegrid",
        "ggplot",
        "default",
    ):
        try:
            plt.style.use(style_name)
            return style_name
        except OSError:
            continue
    return "default"


def _discover_certificate_columns(fieldnames):
    pairs = []
    for field in fieldnames:
        match = CERT_TIME_PATTERN.match(field)
        if match:
            pairs.append((int(match.group(1)), field))
    return [field for _, field in sorted(pairs)]


def _load_rows(csv_path):
    with open(csv_path, "r", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        fieldnames = reader.fieldnames or []
    cert_columns = _discover_certificate_columns(fieldnames)
    return rows, cert_columns


def _resolve_csv_path(csv_path):
    if csv_path:
        path = Path(csv_path)
        if not path.exists():
            raise FileNotFoundError(f"CSV file not found: {path}")
        return path

    candidates = sorted(
        Path("results").glob("**/*_round_certificate_analysis*.csv"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        raise FileNotFoundError(
            "No round_certificate_analysis CSV files found under results/."
        )
    return candidates[0]


def _resolve_csv_paths(csv_path=None, csv_glob=None):
    if csv_path:
        return [_resolve_csv_path(csv_path)]

    pattern = csv_glob or CSV_GLOB
    candidates = sorted(Path(".").glob(pattern))
    if not candidates:
        raise FileNotFoundError(f"No CSV files found for glob: {pattern}")
    return candidates


def _to_int(value):
    return int(value) if value not in (None, "") else None


def _to_float(value):
    return float(value) if value not in (None, "") else None


def _sorted_certificate_values(row, cert_columns):
    values = [_to_float(row.get(column)) for column in cert_columns]
    values = [value for value in values if value is not None]
    return sorted(values)


def _prepare_rows_for_plotting(rows, cert_columns):
    prepared = []
    for row in rows:
        round_value = _to_int(row.get("Round"))
        if round_value is None:
            continue
        prepared.append((round_value, _sorted_certificate_values(row, cert_columns)))
    prepared.sort(key=lambda item: item[0])
    return prepared


def _completed_rows(rows):
    return [
        row for row in rows
        if _to_float(row.get("Round_End_Time_ms")) is not None
    ]


def filter_certificate_rows(rows, node_id, start_round, end_round):
    filtered = []
    for row in rows:
        row_node = _to_int(row.get("Node_ID"))
        row_round = _to_int(row.get("Round"))
        if row_node is None or row_round is None:
            continue
        if row_node != node_id:
            continue
        if row_round < start_round or row_round > end_round:
            continue
        filtered.append(row)
    return filtered


def filter_rows_by_node(rows, node_id):
    filtered = []
    for row in rows:
        row_node = _to_int(row.get("Node_ID"))
        if row_node is None:
            continue
        if row_node != node_id:
            continue
        filtered.append(row)
    return filtered


def filter_rows_by_node_and_optional_rounds(rows, node_id, start_round=None, end_round=None):
    filtered = []
    for row in rows:
        row_node = _to_int(row.get("Node_ID"))
        row_round = _to_int(row.get("Round"))
        if row_node is None or row_round is None:
            continue
        if row_node != node_id:
            continue
        if start_round is not None and row_round < start_round:
            continue
        if end_round is not None and row_round > end_round:
            continue
        filtered.append(row)
    return filtered


def _range_label(start_round, end_round):
    return f"Rounds {start_round}-{end_round}"


def _all_rounds_label():
    return "All Rounds"


def plot_progress_vs_avg_latency(rows, cert_columns, node_id, start_round, end_round, output_path):
    prepared_rows = _prepare_rows_for_plotting(_completed_rows(rows), cert_columns)
    averages = []
    progress = []

    if not prepared_rows:
        raise ValueError("No completed rounds available in the selected range.")

    common_rank_count = min(len(values) for _, values in prepared_rows)
    if common_rank_count == 0:
        raise ValueError("No certificate latency values available in the selected range.")

    for index in range(common_rank_count):
        values = [values[index] for _, values in prepared_rows]
        averages.append(sum(values) / len(values))
        progress.append(index + 1)

    if not averages:
        raise ValueError("No certificate latency values available in the selected range.")

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(averages, progress, marker="o", linewidth=2, color="#1f77b4")
    ax.set_title(
        f"Progress vs. Avg Latency\nNode {node_id}, {_range_label(start_round, end_round)}"
    )
    ax.set_xlabel("Average Time Delta (ms)")
    ax.set_ylabel("Certificate Arrival Rank")
    ax.set_yticks(progress)
    ax.grid(True, linestyle="--", alpha=0.4)
    fig.tight_layout()
    fig.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close(fig)


def plot_latency_over_rounds(rows, cert_columns, node_id, output_path, start_round=None, end_round=None):
    prepared_rows = _prepare_rows_for_plotting(rows, cert_columns)
    rounds = [round_value for round_value, _ in prepared_rows]
    sorted_rows = [values for _, values in prepared_rows]

    fig, ax = plt.subplots(figsize=(11, 6))
    cmap = plt.get_cmap("tab10")

    plotted = False
    for index in range(len(cert_columns)):
        values = [values[index] if len(values) > index else None for values in sorted_rows]
        if not any(value is not None for value in values):
            continue
        ax.plot(
            rounds,
            values,
            marker="o",
            markersize=3,
            linewidth=1.5,
            label=f"Arrival {index + 1}",
            color=cmap(index % 10),
        )
        plotted = True

    if not plotted:
        plt.close(fig)
        raise ValueError("No certificate latency series available for the selected node.")

    subtitle = (
        _range_label(start_round, end_round)
        if start_round is not None and end_round is not None
        else _all_rounds_label()
    )
    ax.set_title(
        f"Latency over Rounds\nNode {node_id}, {subtitle}"
    )
    ax.set_xlabel("Round")
    ax.set_ylabel("Time Delta (ms)")
    ax.set_ylim(0, 3000)
    ax.grid(True, linestyle="--", alpha=0.4)
    ax.legend(title="Arrival Order", ncol=2)
    fig.tight_layout()
    fig.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close(fig)


def plot_latency_figure(csv_path, node_id, output_dir=None, start_round=None, end_round=None):
    csv_path = _resolve_csv_path(csv_path)
    rows, cert_columns = _load_rows(csv_path)
    node_rows = filter_rows_by_node_and_optional_rounds(
        rows,
        node_id,
        start_round=start_round,
        end_round=end_round,
    )

    if not node_rows:
        if start_round is not None and end_round is not None:
            raise ValueError(
                f"No rows found for Node_ID={node_id} in round range {start_round}-{end_round}."
            )
        raise ValueError(f"No rows found for Node_ID={node_id}.")

    output_dir = Path(output_dir) if output_dir else csv_path.parent
    output_dir.mkdir(parents=True, exist_ok=True)

    suffix = (
        f"node{node_id}_rounds_{start_round}_{end_round}"
        if start_round is not None and end_round is not None
        else f"node{node_id}_all_rounds"
    )
    trend_path = output_dir / f"latency_over_rounds_{suffix}.png"
    plot_latency_over_rounds(
        node_rows,
        cert_columns,
        node_id,
        trend_path,
        start_round=start_round,
        end_round=end_round,
    )
    return trend_path


def plot_progress_figure(csv_path, node_id, start_round, end_round, output_dir=None):
    csv_path = _resolve_csv_path(csv_path)
    rows, cert_columns = _load_rows(csv_path)
    filtered_rows = filter_certificate_rows(rows, node_id, start_round, end_round)

    if not filtered_rows:
        raise ValueError(
            f"No rows found for Node_ID={node_id} in round range {start_round}-{end_round}."
        )

    output_dir = Path(output_dir) if output_dir else csv_path.parent
    output_dir.mkdir(parents=True, exist_ok=True)

    suffix = f"node{node_id}_rounds_{start_round}_{end_round}"
    progress_path = output_dir / f"progress_vs_avg_latency_{suffix}.png"
    plot_progress_vs_avg_latency(
        filtered_rows,
        cert_columns,
        node_id,
        start_round,
        end_round,
        progress_path,
    )
    return progress_path


def plot_certificate_figures(csv_path, node_id, start_round, end_round, output_dir=None):
    csv_path = _resolve_csv_path(csv_path)
    rows, cert_columns = _load_rows(csv_path)
    filtered_rows = filter_certificate_rows(rows, node_id, start_round, end_round)
    node_rows = filter_rows_by_node(rows, node_id)

    if not filtered_rows:
        raise ValueError(
            f"No rows found for Node_ID={node_id} in round range {start_round}-{end_round}."
        )
    if not node_rows:
        raise ValueError(f"No rows found for Node_ID={node_id}.")

    output_dir = Path(output_dir) if output_dir else csv_path.parent
    output_dir.mkdir(parents=True, exist_ok=True)

    suffix = f"node{node_id}_rounds_{start_round}_{end_round}"
    progress_path = output_dir / f"progress_vs_avg_latency_{suffix}.png"
    trend_path = output_dir / f"latency_over_rounds_node{node_id}_all_rounds.png"

    plot_progress_vs_avg_latency(
        filtered_rows,
        cert_columns,
        node_id,
        start_round,
        end_round,
        progress_path,
    )
    plot_latency_over_rounds(
        node_rows,
        cert_columns,
        node_id,
        trend_path,
        start_round,
        end_round,
    )

    return progress_path, trend_path


def main():
    if START_ROUND > END_ROUND:
        raise SystemExit("START_ROUND must be less than or equal to END_ROUND")

    configure_plot_style()
    progress_path = plot_progress_figure(
        CSV_PATH,
        NODE_ID,
        START_ROUND,
        END_ROUND,
        output_dir=OUTPUT_DIR,
    )

    selected_csv = _resolve_csv_path(CSV_PATH)
    print(f"Selected CSV: {selected_csv}")
    print(f"Saved progress plot to: {progress_path}")


if __name__ == "__main__":
    main()
