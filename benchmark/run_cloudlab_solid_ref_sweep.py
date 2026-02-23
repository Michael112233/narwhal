#!/usr/bin/env python3
"""
Run CloudLab benchmark across (solid_step_length, reference) combinations.

For each combination:
1) run `fab cloudlab-remote solid_step_length=... reference=...`
2) move all files currently in `results/` into `temp_result/len=..., ref=.../`
3) clear `results/` for the next run
"""

import shutil
import subprocess
import sys
from pathlib import Path


COMBINATIONS = [(2, 2), (2, 3), (3, 2), (3, 3)]


def clear_results(results_dir: Path) -> None:
    results_dir.mkdir(parents=True, exist_ok=True)
    for item in results_dir.iterdir():
        if item.is_dir():
            shutil.rmtree(item)
        else:
            item.unlink()


def archive_results(results_dir: Path, archive_root: Path, length: int, reference: int) -> None:
    target_dir = archive_root / f"len={length}, ref={reference}"
    target_dir.mkdir(parents=True, exist_ok=True)

    moved = 0
    for item in list(results_dir.iterdir()):
        destination = target_dir / item.name
        if destination.exists():
            if destination.is_dir():
                shutil.rmtree(destination)
            else:
                destination.unlink()
        shutil.move(str(item), str(destination))
        moved += 1

    if moved == 0:
        print(f"[WARN] No files found in results/ for len={length}, ref={reference}")
    else:
        print(f"[INFO] Archived {moved} item(s) to {target_dir}")


def run_one(length: int, reference: int, benchmark_dir: Path) -> None:
    print(f"\n=== Running combination: len={length}, ref={reference} ===")
    cmd = [
        "fab",
        "cloudlab-remote",
        f"--solid-step-length={length}",
        f"--reference={reference}",
    ]
    subprocess.run(cmd, cwd=benchmark_dir, check=True)


def main() -> int:
    benchmark_dir = Path(__file__).resolve().parent
    results_dir = benchmark_dir / "results"
    archive_root = benchmark_dir / "temp_result"
    archive_root.mkdir(parents=True, exist_ok=True)

    # Start clean so each combination only archives its own outputs.
    clear_results(results_dir)

    for length, reference in COMBINATIONS:
        run_one(length, reference, benchmark_dir)
        archive_results(results_dir, archive_root, length, reference)
        clear_results(results_dir)

    print("\nAll combinations completed.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except subprocess.CalledProcessError as e:
        print(f"\n[ERROR] Command failed with exit code {e.returncode}: {e.cmd}", file=sys.stderr)
        raise
