#!/usr/bin/env python3
"""
Plot per-node round timelines with certificate arrival markers.

Each observer node gets one horizontal lane. Selected rounds are concatenated
from left to right inside that lane:
  - the bar length is the round duration (`Round_End_Time_ms`)
  - colored dots inside each segment mark when certificates arrived
  - dot color encodes the certificate origin node

Edit the configuration block below or call `plot_round_timeline_by_node(...)`
from another script.
"""

import csv
import re
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.lines import Line2D
from matplotlib.patches import Patch

# Compatibility shim for older matplotlib versions running with NumPy 2.x.
if not hasattr(np, "Inf"):
    np.Inf = np.inf


CERT_TIME_PATTERN = re.compile(r"Certificate_(\d+)_Time_Delta_ms$")

# ============================================================================
# Configuration: edit these values directly when you want a different plot.
# ============================================================================
CSV_PATH = (
    "results/no_delay/custom5_5/"
    "20260328_063819_n10_r40000_run2/"
    "no_delay_custom5_5_round_certificate_analysis.csv"
)

# If ROUND_LIST is not None, it takes priority over START_ROUND / END_ROUND.
ROUND_LIST = None
START_ROUND = 700
END_ROUND = 705

# When None, include every node present in the CSV.
NODE_IDS = [9]

# When None, figures are written next to the selected CSV.
OUTPUT_DIR = None
OUTPUT_NAME = None

ANNOTATE_MARKERS = False
ROUND_GAP_MS = 12.0
BAR_HEIGHT = 0.62
MARKER_SIZE = 48
TIME_AXIS_MODE = "round_end"
MAX_CERTIFICATES_TO_PLOT = 7


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


def _to_int(value):
    if value in (None, "", "UNKNOWN"):
        return None
    return int(value)


def _to_float(value):
    if value in (None, "", "UNKNOWN"):
        return None
    return float(value)


def _discover_certificate_slots(fieldnames):
    slots = []
    for field in fieldnames:
        match = CERT_TIME_PATTERN.match(field)
        if match:
            slots.append(int(match.group(1)))
    return sorted(slots)


def _load_rows(csv_path):
    with open(csv_path, "r", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        fieldnames = reader.fieldnames or []

    slots = _discover_certificate_slots(fieldnames)
    if not slots:
        raise ValueError("No certificate time columns were found in the CSV.")
    return rows, slots, fieldnames


def _available_nodes(rows):
    return sorted(
        {
            _to_int(row.get("Node_ID"))
            for row in rows
            if _to_int(row.get("Node_ID")) is not None
        }
    )


def _available_rounds(rows):
    return sorted(
        {
            _to_int(row.get("Round"))
            for row in rows
            if _to_int(row.get("Round")) is not None
        }
    )


def _resolve_node_ids(rows, node_ids):
    available = _available_nodes(rows)
    if node_ids is None:
        return available

    requested = list(node_ids)
    missing = [node_id for node_id in requested if node_id not in available]
    if missing:
        raise ValueError(f"Requested node IDs not found in CSV: {missing}")
    return requested


def _resolve_rounds(rows, start_round=None, end_round=None, round_list=None):
    available = _available_rounds(rows)
    if not available:
        raise ValueError("No round values were found in the CSV.")

    if round_list is not None:
        requested = list(round_list)
        missing = [round_id for round_id in requested if round_id not in available]
        if missing:
            raise ValueError(f"Requested rounds not found in CSV: {missing}")
        return requested

    if start_round is None:
        start_round = available[0]
    if end_round is None:
        end_round = available[-1]
    if start_round > end_round:
        raise ValueError("start_round must be less than or equal to end_round")

    selected = [round_id for round_id in available if start_round <= round_id <= end_round]
    if not selected:
        raise ValueError(f"No rounds found in range {start_round}-{end_round}")
    return selected


def _row_index(rows):
    indexed = {}
    for row in rows:
        node_id = _to_int(row.get("Node_ID"))
        round_id = _to_int(row.get("Round"))
        if node_id is None or round_id is None:
            continue
        indexed[(node_id, round_id)] = row
    return indexed


def _extract_certificate_events(row, slots, max_certificates=None):
    events = []
    for slot in slots:
        delta = _to_float(row.get(f"Certificate_{slot}_Time_Delta_ms"))
        if delta is None:
            continue
        origin_node_id = _to_int(row.get(f"Certificate_{slot}_Origin_Node_ID"))
        events.append(
            {
                "delta_ms": delta,
                "origin_node_id": origin_node_id,
                "arrival_rank": slot,
            }
        )

    return sorted(
        events,
        key=lambda item: (
            item["delta_ms"],
            item["origin_node_id"] if item["origin_node_id"] is not None else 10**9,
        ),
    )[:max_certificates]


def _collect_lane_data(
    rows,
    slots,
    node_ids,
    rounds,
    round_gap_ms,
    time_axis_mode,
    max_certificates=None,
):
    indexed_rows = _row_index(rows)
    lanes = {}
    max_total_span = 0.0
    origin_nodes_seen = set()

    if time_axis_mode not in {"round_end", "max_event"}:
        raise ValueError(
            f"Unsupported time_axis_mode={time_axis_mode!r}; expected 'round_end' or 'max_event'"
        )

    for node_id in node_ids:
        current_offset = 0.0
        segments = []

        for round_id in rounds:
            row = indexed_rows.get((node_id, round_id))
            if row is None:
                current_offset += round_gap_ms
                continue

            duration_ms = _to_float(row.get("Round_End_Time_ms")) or 0.0
            events = _extract_certificate_events(
                row,
                slots,
                max_certificates=max_certificates,
            )
            for event in events:
                if event["origin_node_id"] is not None:
                    origin_nodes_seen.add(event["origin_node_id"])

            max_event_ms = max((event["delta_ms"] for event in events), default=0.0)
            segment_span_ms = max(duration_ms, max_event_ms, 1.0)
            axis_span_ms = (
                max(duration_ms, 1.0)
                if time_axis_mode == "round_end"
                else segment_span_ms
            )

            segments.append(
                {
                    "round_id": round_id,
                    "start_ms": current_offset,
                    "duration_ms": duration_ms,
                    "segment_span_ms": segment_span_ms,
                    "axis_span_ms": axis_span_ms,
                    "events": events,
                }
            )
            current_offset += axis_span_ms + round_gap_ms

        lanes[node_id] = segments
        max_total_span = max(max_total_span, current_offset)

    return lanes, max_total_span, sorted(origin_nodes_seen)


def _build_origin_colors(origin_node_ids):
    if not origin_node_ids:
        return {}

    cmap_name = "tab10" if len(origin_node_ids) <= 10 else "tab20"
    cmap = plt.get_cmap(cmap_name)

    return {
        node_id: cmap(index % cmap.N)
        for index, node_id in enumerate(origin_node_ids)
    }


def _rounds_label(rounds):
    if not rounds:
        return "No Rounds"
    if len(rounds) <= 6:
        return ", ".join(f"R{round_id}" for round_id in rounds)
    return f"R{rounds[0]}-{rounds[-1]} ({len(rounds)} rounds)"


def _rounds_suffix(rounds):
    if not rounds:
        return "no_rounds"
    if rounds == list(range(rounds[0], rounds[-1] + 1)):
        return f"rounds_{rounds[0]}_{rounds[-1]}"
    return f"rounds_{rounds[0]}_{rounds[-1]}_{len(rounds)}"


def _cert_limit_label(max_certificates):
    if max_certificates is None:
        return "all certs"
    return f"first {max_certificates} certs"


def plot_round_timeline_by_node(
    csv_path,
    start_round=None,
    end_round=None,
    round_list=None,
    node_ids=None,
    output_dir=None,
    output_name=None,
    annotate_markers=False,
    round_gap_ms=40.0,
    time_axis_mode="round_end",
    max_certificates=7,
):
    csv_path = _resolve_csv_path(csv_path)
    rows, slots, _ = _load_rows(csv_path)
    selected_rounds = _resolve_rounds(
        rows,
        start_round=start_round,
        end_round=end_round,
        round_list=round_list,
    )
    selected_nodes = _resolve_node_ids(rows, node_ids)
    lane_data, max_total_span, origin_nodes = _collect_lane_data(
        rows,
        slots,
        selected_nodes,
        selected_rounds,
        round_gap_ms,
        time_axis_mode,
        max_certificates,
    )
    origin_colors = _build_origin_colors(origin_nodes)

    figure_width = max(12, min(28, 8 + max_total_span / 180.0))
    figure_height = max(4.8, 1.0 + 0.65 * len(selected_nodes))
    fig, ax = plt.subplots(figsize=(figure_width, figure_height))

    y_positions = np.arange(len(selected_nodes))
    bar_fill_colors = ("#d7dde8", "#c9d3e3")
    late_marker_color = "#4a4a4a"
    clipped_markers_present = False

    for lane_index, node_id in enumerate(selected_nodes):
        y = y_positions[lane_index]
        for segment_index, segment in enumerate(lane_data[node_id]):
            bar_color = bar_fill_colors[segment_index % len(bar_fill_colors)]
            segment_start = segment["start_ms"]
            segment_width = segment["axis_span_ms"]
            if segment["duration_ms"] > 0:
                ax.broken_barh(
                    [(segment_start, segment["duration_ms"])],
                    (y - BAR_HEIGHT / 2, BAR_HEIGHT),
                    facecolors=bar_color,
                    edgecolors="#6f7785",
                    linewidth=0.9,
                    alpha=0.85,
                    zorder=1,
                )

            if segment["segment_span_ms"] > segment["axis_span_ms"]:
                ax.broken_barh(
                    [(segment_start, segment["axis_span_ms"])],
                    (y - BAR_HEIGHT / 2, BAR_HEIGHT),
                    facecolors="none",
                    edgecolors="#c7ccd6",
                    linewidth=0.6,
                    alpha=0.45,
                    zorder=0,
                )

            for event in segment["events"]:
                clipped = event["delta_ms"] > segment["axis_span_ms"]
                x = segment_start + min(event["delta_ms"], segment["axis_span_ms"])
                color = origin_colors.get(event["origin_node_id"], late_marker_color)
                ax.scatter(
                    x,
                    y,
                    s=MARKER_SIZE,
                    c=[color],
                    marker=">" if clipped else "o",
                    edgecolors="black",
                    linewidths=0.45,
                    zorder=3,
                )
                if clipped:
                    clipped_markers_present = True
                if annotate_markers and event["origin_node_id"] is not None:
                    ax.text(
                        x,
                        y + BAR_HEIGHT * 0.52,
                        str(event["origin_node_id"]),
                        ha="center",
                        va="bottom",
                        fontsize=6.5,
                        color="#222222",
                    )

            if len(selected_rounds) <= 12:
                label_x = segment_start + segment_width / 2
                ax.text(
                    label_x,
                    y + BAR_HEIGHT * 0.82,
                    f"R{segment['round_id']}",
                    ha="center",
                    va="bottom",
                    fontsize=7,
                    color="#4c5563",
                )

    ax.set_yticks(y_positions)
    ax.set_yticklabels([f"Node {node_id}" for node_id in selected_nodes])
    ax.invert_yaxis()
    axis_label = (
        "Cumulative Time Across Selected Rounds (ms, based on Round_End_Time_ms)"
        if time_axis_mode == "round_end"
        else "Cumulative Time Across Selected Rounds (ms, based on max observed arrival)"
    )
    ax.set_xlabel(axis_label)
    ax.set_ylabel("Observer Node")
    ax.set_title(
        "Per-Node Round Timeline with Certificate Arrivals\n"
        f"Rounds: {_rounds_label(selected_rounds)}, {_cert_limit_label(max_certificates)}"
    )
    ax.grid(True, axis="x", linestyle="--", alpha=0.35)
    ax.grid(False, axis="y")
    ax.set_xlim(left=0, right=max_total_span * 1.02 if max_total_span else 1)

    legend_handles = [
        Patch(
            facecolor=bar_fill_colors[0],
            edgecolor="#6f7785",
            label="Round duration",
            alpha=0.85,
        )
    ]
    for node_id in origin_nodes:
        legend_handles.append(
            Line2D(
                [0],
                [0],
                marker="o",
                color="none",
                markerfacecolor=origin_colors[node_id],
                markeredgecolor="black",
                markeredgewidth=0.45,
                markersize=7.5,
                label=f"Cert from node {node_id}",
            )
        )
    if clipped_markers_present:
        legend_handles.append(
            Line2D(
                [0],
                [0],
                marker=">",
                color="none",
                markerfacecolor=late_marker_color,
                markeredgecolor="black",
                markeredgewidth=0.45,
                markersize=7.5,
                label="Arrival exceeds displayed round span",
            )
        )

    if legend_handles:
        ax.legend(
            handles=legend_handles,
            loc="upper left",
            bbox_to_anchor=(1.01, 1.0),
            borderaxespad=0.0,
            frameon=True,
            ncol=1,
        )

    fig.tight_layout()

    output_dir = Path(output_dir) if output_dir else csv_path.parent
    output_dir.mkdir(parents=True, exist_ok=True)
    if output_name is None:
        output_name = f"round_timeline_by_node_{_rounds_suffix(selected_rounds)}.png"
    output_path = output_dir / output_name
    fig.savefig(output_path, dpi=220, bbox_inches="tight")
    plt.close(fig)
    return output_path


def main():
    configure_plot_style()
    output_path = plot_round_timeline_by_node(
        CSV_PATH,
        start_round=START_ROUND,
        end_round=END_ROUND,
        round_list=ROUND_LIST,
        node_ids=NODE_IDS,
        output_dir=OUTPUT_DIR,
        output_name=OUTPUT_NAME,
        annotate_markers=ANNOTATE_MARKERS,
        round_gap_ms=ROUND_GAP_MS,
        time_axis_mode=TIME_AXIS_MODE,
        max_certificates=MAX_CERTIFICATES_TO_PLOT,
    )

    print(f"Selected CSV: {_resolve_csv_path(CSV_PATH)}")
    print(f"Saved round timeline plot to: {output_path}")


if __name__ == "__main__":
    main()
