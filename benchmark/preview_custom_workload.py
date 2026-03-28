#!/usr/bin/env python3
"""
Preview the per-node workload distribution produced by CustomAllocator.

This uses the same allocation logic as the benchmark runner, so the output
matches what `fab local` / `fab cloudlab-remote` will use for `rate_type=custom`.

Examples:
    python3 preview_custom_workload.py
    python3 preview_custom_workload.py --rate 40000 --percentages 1,1,1,1,8,8,8,1,1,1
"""

import argparse
from typing import List

from benchmark.imbalanced_rate import CustomAllocator


# Edit these defaults directly if you prefer not to use command-line flags.
DEFAULT_TOTAL_RATE = 40000
DEFAULT_PERCENTAGES = "8,8,8,8,8,1,1,1,1,1"


def parse_percentages(raw: str) -> List[float]:
    parts = [part.strip() for part in raw.split(",") if part.strip()]
    if not parts:
        raise argparse.ArgumentTypeError("percentages cannot be empty")

    try:
        values = [float(part) for part in parts]
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "percentages must be a comma-separated list of numbers"
        ) from exc

    if any(value < 0 for value in values):
        raise argparse.ArgumentTypeError("percentages must be >= 0")
    if sum(values) <= 0:
        raise argparse.ArgumentTypeError("sum(percentages) must be > 0")
    return values


def preview_distribution(total_rate: int, percentages: List[float]):
    allocator = CustomAllocator(total_rate, len(percentages), percentages)
    node_rates = allocator.allocate()
    normalized = allocator.percentages

    print("")
    print("Custom workload preview")
    print("-" * 72)
    print(f"Total input rate: {total_rate:,} tx/s")
    print(f"Node count: {len(percentages)}")
    print(f"Raw percentages: {percentages}")
    print(f"Normalized shares: {[round(share * 100, 2) for share in normalized]} %")
    print("-" * 72)
    print(
        f"{'Node':<8}{'Weight':>12}{'Share %':>12}{'Rate (tx/s)':>16}  Distribution"
    )
    print("-" * 72)

    for node_id, (weight, share, rate) in enumerate(
        zip(percentages, normalized, node_rates)
    ):
        bar = "#" * max(1, int(round(share * 50)))
        print(
            f"{node_id:<8}{weight:>12.2f}{share * 100:>12.2f}{rate:>16,}  {bar}"
        )

    print("-" * 72)
    print(f"Allocated total: {sum(node_rates):,} tx/s")
    print("")


def build_parser():
    parser = argparse.ArgumentParser(
        description="Preview the workload distribution for rate_type=custom."
    )
    parser.add_argument(
        "--rate",
        type=int,
        default=DEFAULT_TOTAL_RATE,
        help=f"Total input rate in tx/s (default: {DEFAULT_TOTAL_RATE})",
    )
    parser.add_argument(
        "--percentages",
        type=parse_percentages,
        default=parse_percentages(DEFAULT_PERCENTAGES),
        help=(
            "Comma-separated workload weights, for example "
            '"1,1,1,1,8,8,8,1,1,1"'
        ),
    )
    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()
    preview_distribution(args.rate, args.percentages)


if __name__ == "__main__":
    main()
