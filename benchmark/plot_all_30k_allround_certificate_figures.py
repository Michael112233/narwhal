#!/usr/bin/env python3
"""
Batch-generate all-round certificate latency-over-rounds figures for all 30k
results under results/.

For each *_round_certificate_analysis.csv at 30k, this script emits the
all-round latency-over-rounds figure for a chosen node.
"""

from pathlib import Path

from plot_certificate_progress import (
    _resolve_csv_paths,
    configure_plot_style,
    plot_latency_figure,
)


CSV_PATH = None
CSV_GLOB = "results/**/*r30000_run*/**/*_round_certificate_analysis.csv"
NODE_ID = 0
OUTPUT_DIR = None


def main():
    configure_plot_style()
    csv_paths = _resolve_csv_paths(CSV_PATH, CSV_GLOB)
    generated = []
    skipped = []

    for csv_path in csv_paths:
        output_dir = Path(OUTPUT_DIR) if OUTPUT_DIR else None
        try:
            trend_path = plot_latency_figure(
                csv_path,
                NODE_ID,
                output_dir=output_dir,
                start_round=None,
                end_round=None,
            )
            generated.append((csv_path, trend_path))
        except ValueError as exc:
            skipped.append((csv_path, str(exc)))

    for csv_path, trend_path in generated:
        print(f"Selected CSV: {csv_path}")
        print(f"Saved all-round certificate plot to: {trend_path}")

    for csv_path, reason in skipped:
        print(f"Skipped CSV: {csv_path}")
        print(f"Reason: {reason}")


if __name__ == "__main__":
    main()
