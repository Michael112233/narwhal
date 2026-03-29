#!/usr/bin/env python3
"""
Append origin node metadata to round_certificate_analysis CSV files.

This script keeps the original CSV columns and adds, for every
`Certificate_k_Origin` column:
  - `Certificate_k_Origin_Node_ID`
  - `Certificate_k_Origin_IP`
  - `Certificate_k_Origin_Full_Public_Key`

The mapping is reconstructed from:
  - `.committee.json`: public key -> primary IP
  - `cloudlab_settings.json`: host order -> node_id
"""

import argparse
import csv
from pathlib import Path

from benchmark.origin_mapping import build_origin_mapping_from_files, resolve_origin


DEFAULT_COMMITTEE = Path(".committee.json")
DEFAULT_SETTINGS = Path("cloudlab_settings.json")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Map certificate origin strings to node IDs and IPs."
    )
    parser.add_argument(
        "csv_path",
        help="Path to an existing round_certificate_analysis CSV file.",
    )
    parser.add_argument(
        "--committee",
        default=None,
        help=f"Committee JSON path (default: {DEFAULT_COMMITTEE})",
    )
    parser.add_argument(
        "--settings",
        default=None,
        help=f"CloudLab settings JSON path (default: {DEFAULT_SETTINGS})",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Optional output CSV path. Defaults to <input>_origin_mapped.csv",
    )
    return parser.parse_args()


def infer_snapshot_path(csv_path, suffix):
    matches = sorted(csv_path.parent.glob(f"*_{suffix}"))
    return matches[0] if matches else None


def resolve_mapping_paths(csv_path, committee_arg, settings_arg):
    committee_path = (
        Path(committee_arg)
        if committee_arg
        else infer_snapshot_path(csv_path, "committee.json") or DEFAULT_COMMITTEE
    )
    settings_path = (
        Path(settings_arg)
        if settings_arg
        else infer_snapshot_path(csv_path, "cloudlab_settings.json") or DEFAULT_SETTINGS
    )
    return committee_path, settings_path


def build_output_fieldnames(input_fieldnames):
    output = []
    for field in input_fieldnames:
        output.append(field)
        if field.endswith("_Origin"):
            output.append(f"{field}_Node_ID")
            output.append(f"{field}_IP")
            output.append(f"{field}_Region")
            output.append(f"{field}_Full_Public_Key")
    return output


def map_csv(csv_path, output_path, mapping_entries):
    with Path(csv_path).open("r", newline="") as handle:
        reader = csv.DictReader(handle)
        input_fieldnames = reader.fieldnames or []
        output_fieldnames = build_output_fieldnames(input_fieldnames)
        rows = list(reader)

    with Path(output_path).open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=output_fieldnames)
        writer.writeheader()

        for row in rows:
            output_row = {}
            for field in input_fieldnames:
                output_row[field] = row.get(field, "")
                if field.endswith("_Origin"):
                    resolved = resolve_origin(row.get(field), mapping_entries)
                    output_row[f"{field}_Node_ID"] = resolved["node_id"]
                    output_row[f"{field}_IP"] = resolved["ip"]
                    output_row[f"{field}_Region"] = resolved["region"]
                    output_row[f"{field}_Full_Public_Key"] = resolved["full_public_key"]
            writer.writerow(output_row)


def main():
    args = parse_args()
    csv_path = Path(args.csv_path)
    if not csv_path.exists():
        raise SystemExit(f"CSV file not found: {csv_path}")

    output_path = (
        Path(args.output)
        if args.output
        else csv_path.with_name(f"{csv_path.stem}_origin_mapped.csv")
    )

    committee_path, settings_path = resolve_mapping_paths(
        csv_path,
        args.committee,
        args.settings,
    )
    mapping_entries = build_origin_mapping_from_files(committee_path, settings_path)

    map_csv(csv_path, output_path, mapping_entries)

    print(f"Input CSV: {csv_path}")
    print(f"Output CSV: {output_path}")
    print(f"Committee: {committee_path}")
    print(f"Settings: {settings_path}")
    print("Resolved origin mapping:")
    for info in mapping_entries:
        print(
            f'  node{info["node_id"]}: {info["ip"]} <- {info["short_public_key"]}...'
        )


if __name__ == "__main__":
    main()
