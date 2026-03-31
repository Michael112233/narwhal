#!/usr/bin/env python3
"""
Batch-generate latency-over-rounds figures for balanced result CSV files.

This script reuses the plotting helpers from `plot_certificate_progress.py`
but only emits `plot_latency_over_rounds` figures.
"""

from pathlib import Path

from plot_certificate_progress import (
    configure_plot_style,
    plot_latency_figure,
    _resolve_csv_paths,
)


# Set `CSV_PATH` to a single CSV to plot one file only.
# Leave it as None to batch-process all balanced analysis CSVs.
CSV_PATH = None
CSV_GLOB = "results/geo/balanced/**/*_round_certificate_analysis.csv"
NODE_ID = 0

# Optional round filter. Leave both as None to use all rounds.
START_ROUND = None
END_ROUND = None

# When None, figures are written next to each selected CSV.
OUTPUT_DIR = None


def main():
    if (
        START_ROUND is not None
        and END_ROUND is not None
        and START_ROUND > END_ROUND
    ):
        raise SystemExit("START_ROUND must be less than or equal to END_ROUND")

    configure_plot_style()
    csv_paths = _resolve_csv_paths(CSV_PATH, CSV_GLOB)
    generated = []

    for csv_path in csv_paths:
        output_dir = Path(OUTPUT_DIR) if OUTPUT_DIR else None
        trend_path = plot_latency_figure(
            csv_path,
            NODE_ID,
            output_dir=output_dir,
            start_round=START_ROUND,
            end_round=END_ROUND,
        )
        generated.append((csv_path, trend_path))

    for csv_path, trend_path in generated:
        print(f"Selected CSV: {csv_path}")
        print(f"Saved latency trend plot to: {trend_path}")


if __name__ == "__main__":
    main()
