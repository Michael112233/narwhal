#!/usr/bin/env python3
"""
Preview the per-node workload distribution produced by CustomAllocator.

This uses the same allocation logic as the benchmark runner, so the output
matches what `fab local` / `fab cloudlab-remote` will use for `rate_type=custom`.
The base total `rate` is first split evenly across all nodes, then `extra_rate`
is distributed according to `percentages`.

Examples:
    python3 preview_custom_workload.py
    python3 preview_custom_workload.py --rate 40000 --extra-rate 20000 --percentages 1,1,1,1,8,8,8,1,1,1
"""

import argparse
from typing import List

from benchmark.imbalanced_rate import CustomAllocator


# Edit these defaults directly if you prefer not to use command-line flags.
DEFAULT_TOTAL_RATE = 20000  
DEFAULT_EXTRA_RATE = 15000
DEFAULT_PERCENTAGES = "0,0,0,0,10,10,10,1,1,1"


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


def preview_distribution(total_rate: int, extra_rate: int, percentages: List[float]):
    allocator = CustomAllocator(
        total_rate,
        extra_rate,
        len(percentages),
        percentages,
    )
    node_rates = allocator.allocate()
    normalized = allocator.percentages

    print("")
    print("Custom workload preview")
    print("-" * 72)
    print(f"Base total rate: {total_rate:,} tx/s")
    print(f"Extra rate total: {extra_rate:,} tx/s")
    print(f"Effective total rate: {allocator.total_tps:,} tx/s")
    print(f"Node count: {len(percentages)}")
    print(f"Raw percentages: {percentages}")
    print(f"Normalized shares: {[round(share * 100, 2) for share in normalized]} %")
    print("-" * 72)
    print(
        f"{'Node':<8}{'Weight':>12}{'Share %':>12}{'Base':>10}{'Extra':>10}{'Final':>12}  Distribution"
    )
    print("-" * 72)

    for node_id, (weight, share, base_rate, extra_share, rate) in enumerate(
        zip(
            percentages,
            normalized,
            allocator.base_node_rates,
            allocator.extra_node_rates,
            node_rates,
        )
    ):
        bar = "#" * max(1, int(round(share * 50)))
        print(
            f"{node_id:<8}{weight:>12.2f}{share * 100:>12.2f}"
            f"{base_rate:>10,}{extra_share:>10,}{rate:>12,}  {bar}"
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
        help=f"Base total input rate in tx/s (default: {DEFAULT_TOTAL_RATE})",
    )
    parser.add_argument(
        "--extra-rate",
        type=int,
        default=DEFAULT_EXTRA_RATE,
        help=f"Extra total rate distributed by percentages (default: {DEFAULT_EXTRA_RATE})",
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
    preview_distribution(args.rate, args.extra_rate, args.percentages)


if __name__ == "__main__":
    main()
