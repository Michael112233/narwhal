#!/usr/bin/env python3
"""
Batch-generate both certificate figures for balanced result CSV files.

This script reuses `plot_certificate_figures()` from
`plot_certificate_progress.py` and emits:
1. progress_vs_avg_latency
2. latency_over_rounds
"""

from pathlib import Path

from plot_certificate_progress import (
    configure_plot_style,
    plot_certificate_figures,
    _resolve_csv_paths,
)


# Set `CSV_PATH` to a single CSV to plot one file only.
# Leave it as None to batch-process all balanced analysis CSVs.
CSV_PATH = None
CSV_GLOB = "results/80ms/custom-high-2/**/*_round_certificate_analysis.csv"
NODE_ID = 0

# Round range used by plot_certificate_figures().
START_ROUND = 200
END_ROUND = 350

# When None, figures are written next to each selected CSV.
OUTPUT_DIR = None


def main():
    if START_ROUND > END_ROUND:
        raise SystemExit("START_ROUND must be less than or equal to END_ROUND")

    configure_plot_style()
    csv_paths = _resolve_csv_paths(CSV_PATH, CSV_GLOB)
    generated = []
    skipped = []

    for csv_path in csv_paths:
        output_dir = Path(OUTPUT_DIR) if OUTPUT_DIR else None
        try:
            progress_path, trend_path = plot_certificate_figures(
                csv_path,
                NODE_ID,
                START_ROUND,
                END_ROUND,
                output_dir=output_dir,
            )
            generated.append((csv_path, progress_path, trend_path))
        except ValueError as exc:
            skipped.append((csv_path, str(exc)))

    for csv_path, progress_path, trend_path in generated:
        print(f"Selected CSV: {csv_path}")
        print(f"Saved progress plot to: {progress_path}")
        print(f"Saved latency trend plot to: {trend_path}")

    for csv_path, reason in skipped:
        print(f"Skipped CSV: {csv_path}")
        print(f"Reason: {reason}")


if __name__ == "__main__":
    main()
