#!/usr/bin/env python3
"""
Plot round end time curves directly from a round_end_time_pivot CSV.
"""

import argparse
import csv
import json
from pathlib import Path
from typing import List, Optional

import matplotlib.pyplot as plt
import numpy as np

if not hasattr(np, "Inf"):
    np.Inf = np.inf


def discover_latest_pivot(results_root: Path) -> Path:
    candidates = list(results_root.glob("**/*_round_end_time_pivot*.csv"))
    if not candidates:
        raise FileNotFoundError(f"No pivot CSV found under {results_root}")
    return max(candidates, key=lambda path: path.stat().st_mtime)


def resolve_input_path(input_path: Optional[str], results_root: Path) -> Path:
    if input_path is None:
        return discover_latest_pivot(results_root)

    path = Path(input_path)
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve()

    if path.is_dir():
        candidates = list(path.glob("*_round_end_time_pivot*.csv"))
        if not candidates:
            raise FileNotFoundError(f"No pivot CSV found in directory {path}")
        return max(candidates, key=lambda candidate: candidate.stat().st_mtime)

    if not path.exists():
        raise FileNotFoundError(f"Input path does not exist: {path}")

    return path


def load_metadata(csv_path: Path) -> dict:
    candidates = list(csv_path.parent.glob("*_metadata.json"))
    if not candidates:
        return {}
    with candidates[0].open("r") as handle:
        return json.load(handle)


def parse_node_selection(raw_nodes: Optional[str]) -> Optional[List[str]]:
    if not raw_nodes:
        return None
    node_ids = []
    for part in raw_nodes.split(","):
        part = part.strip()
        if not part:
            continue
        if not part.startswith("Node_"):
            part = f"Node_{part}"
        node_ids.append(part)
    return node_ids or None


def load_pivot_data(csv_path: Path, selected_nodes: Optional[List[str]]):
    with csv_path.open("r", newline="") as handle:
        reader = csv.DictReader(handle)
        headers = [field for field in reader.fieldnames or [] if field and field != "Round"]
        node_columns = selected_nodes or headers

        rounds = []
        series = {node: [] for node in node_columns}
        average = []

        for row in reader:
            rounds.append(int(row["Round"]))

            numeric_values = []
            for node in node_columns:
                raw = row.get(node, "")
                value = float(raw) if raw not in ("", None) else None
                series[node].append(value)
                if value is not None:
                    numeric_values.append(value)

            average.append(sum(numeric_values) / len(numeric_values) if numeric_values else None)

    return rounds, series, average


def build_title(metadata: dict, csv_path: Path) -> str:
    if not metadata:
        return csv_path.stem

    network_tag = metadata.get("network_tag", "unknown_network")
    workload_tag = metadata.get("workload_tag", "unknown_workload")
    run_id = metadata.get("run_id", csv_path.parent.name)
    rate = metadata.get("rate")
    duration = metadata.get("duration")

    details = []
    if rate is not None:
        details.append(f"rate={rate}")
    if duration is not None:
        details.append(f"duration={duration}s")

    suffix = f" ({', '.join(details)})" if details else ""
    return f"{network_tag} / {workload_tag} / {run_id}{suffix}"


def default_output_path(csv_path: Path) -> Path:
    stem = csv_path.stem.replace("_round_end_time_pivot", "_round_end_time_plot")
    return csv_path.with_name(f"{stem}.png")


def plot_round_end_times(
    csv_path: Path,
    output_path: Path,
    selected_nodes: Optional[List[str]],
    show_average: bool,
):
    metadata = load_metadata(csv_path)
    rounds, series, average = load_pivot_data(csv_path, selected_nodes)

    plt.figure(figsize=(12, 6))
    for node, values in series.items():
        plt.plot(rounds, values, linewidth=1.2, alpha=0.85, label=node)

    if show_average:
        plt.plot(
            rounds,
            average,
            linewidth=2.6,
            color="black",
            linestyle="--",
            label="Average",
        )

    plt.title(build_title(metadata, csv_path))
    plt.xlabel("Round")
    plt.ylabel("Round End Time (ms)")
    plt.grid(True, alpha=0.3)
    plt.xlim(left=min(rounds))
    plt.ylim(bottom=0)
    plt.legend(loc="upper left", bbox_to_anchor=(1.01, 1.0), ncol=1, fontsize=8)
    plt.subplots_adjust(left=0.08, right=0.8, top=0.9, bottom=0.12)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close()


def main():
    parser = argparse.ArgumentParser(
        description="Plot round end time curves from a pivot CSV.",
    )
    parser.add_argument(
        "--input",
        default=None,
        help="Pivot CSV path or run directory. Defaults to latest pivot CSV under results/.",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output image path. Defaults to the same run directory.",
    )
    parser.add_argument(
        "--nodes",
        default=None,
        help='Comma-separated node ids to plot, e.g. "0,1,2" or "Node_0,Node_3".',
    )
    parser.add_argument(
        "--no-average",
        action="store_true",
        help="Disable the average line.",
    )
    args = parser.parse_args()

    results_root = Path(__file__).resolve().parent / "results"
    csv_path = resolve_input_path(args.input, results_root)
    output_path = (
        Path(args.output).resolve()
        if args.output is not None
        else default_output_path(csv_path)
    )
    selected_nodes = parse_node_selection(args.nodes)

    plot_round_end_times(
        csv_path=csv_path,
        output_path=output_path,
        selected_nodes=selected_nodes,
        show_average=not args.no_average,
    )
    print(f"Saved plot to: {output_path}")


if __name__ == "__main__":
    main()
