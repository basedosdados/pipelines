"""Clean the Cricsheet global cricket bundle into partitioned Parquet.

Thin bootstrap CLI: the cleaning transform is the SHARED, pure code in
``pipelines/datasets/world_cricsheet/utils.py`` (DRY — imported here rather than
duplicated). This wrapper runs it on the already-downloaded ``input/`` and reports
row counts.

Produces four tables conforming to the architecture CSVs under ``architecture/``:

- ``deliveries``     (partitioned by ``year``) — ball-by-ball, from all_matches.csv
- ``matches``        (partitioned by ``year``) — one row per match, from ``<id>_info.csv``
- ``match_players``  (partitioned by ``year``) — one row per (match, team, player)
- ``people``         (single file, dimension)  — from people.csv

Staging parquet is written all-STRING (the recurring-pipeline / upload_to_gcs
convention); the dbt models ``safe_cast`` every column back to its real type. Row
counts are identical to the original typed onboarding run.

Usage:
    python clean_data.py --prototype   # pivot a handful of info files, print, exit
    python clean_data.py               # full run + validation report
"""

from __future__ import annotations

import argparse
import glob
import os
from pathlib import Path

from pipelines.datasets.world_cricsheet.utils import clean_all, parse_info_file

CODE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR = os.path.join(CODE_DIR, "input")
ALL_CSV2_DIR = os.path.join(INPUT_DIR, "all_csv2")
OUTPUT_DIR = os.path.join(CODE_DIR, "output")

# Validated onboarding counts (the parity target).
EXPECTED = {
    "deliveries": 11_401_958,
    "matches": 22_479,
    "match_players": 495_892,
    "people": 18_362,
}


def run_prototype() -> None:
    samples = {
        "1000851 (multi-day Test)": "1000851_info.csv",
        "1003869 (T20 no-result / D-L target)": "1003869_info.csv",
        "1023647 (tie)": "1023647_info.csv",
        "1002151 (D/L with winner + target)": "1002151_info.csv",
        "1000853 (won by an innings)": "1000853_info.csv",
        "1239545 (uses 'players' plural)": "1239545_info.csv",
    }
    for label, fname in samples.items():
        path = os.path.join(ALL_CSV2_DIR, fname)
        if not os.path.exists(path):
            print(f"[skip] {label}: {fname} not found")
            continue
        match, players = parse_info_file(path)
        print("=" * 78)
        print(label)
        print("-" * 78)
        for k, v in match.items():
            print(f"  {k:16} = {v!r}")
        print(f"  n_players        = {len(players)}")
        resolved = sum(1 for p in players if p["player_identifier"])
        print(f"  players_resolved = {resolved}/{len(players)}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--prototype", action="store_true")
    args = ap.parse_args()

    if args.prototype:
        run_prototype()
        return

    n_info = len(glob.glob(os.path.join(ALL_CSV2_DIR, "*_info.csv")))
    print(f"info files: {n_info:,}")
    result = clean_all(Path(INPUT_DIR), Path(OUTPUT_DIR))
    counts = result["counts"]

    print("\n" + "=" * 78)
    print("VALIDATION REPORT")
    print("=" * 78)
    ok = True
    for table, expected in EXPECTED.items():
        got = counts[table]
        flag = "OK" if got == expected else "MISMATCH"
        if got != expected:
            ok = False
        print(
            f"{table:15} rows: {got:>12,}  (expected {expected:>12,})  {flag}"
        )
    print(f"\nmax_start_date (source coverage): {result['max_start_date']}")
    if not ok:
        raise SystemExit("row-count parity FAILED")
    print("\nrow-count parity OK")


if __name__ == "__main__":
    main()
