"""One-shot onboarding bootstrap for au_rba_statistical_tables.

Imports the shared transform from the pipeline package — the cleaning logic
lives in exactly one place (see .claude/rules/prefect-pipeline-conventions.md).

Usage:
    uv run python models/au_rba_statistical_tables/code/clean.py [--download]

Scratch data lives under ~/Downloads/au_rba_statistical_tables_data/ (override
with RBA_DATA); nothing is written into the repo or Dropbox.
"""

import argparse
import csv
import os
from collections import Counter
from pathlib import Path

from pipelines.datasets.au_rba_statistical_tables.utils import (
    clean_all,
    download_all,
    write_partitioned,
)

DATA = Path(
    os.environ.get(
        "RBA_DATA",
        Path.home() / "Downloads" / "au_rba_statistical_tables_data",
    )
)


def validate(cleaned, counts):
    """Assert the invariants the dbt tests will later enforce in BigQuery."""
    obs, series = cleaned["observation"], cleaned["series"]
    problems = []

    keys = Counter((o[0], o[1], o[2]) for o in obs)
    dups = sum(1 for v in keys.values() if v > 1)
    if dups:
        problems.append(f"{dups:,} duplicate (table_code, series_id, date)")

    skeys = Counter((s["table_code"], s["series_id"]) for s in series)
    sdups = sum(1 for v in skeys.values() if v > 1)
    if sdups:
        problems.append(
            f"{sdups:,} duplicate (table_code, series_id) in series"
        )

    catalogue = {(s["table_code"], s["series_id"]) for s in series}
    orphans = {(o[0], o[1]) for o in obs} - catalogue
    if orphans:
        problems.append(
            f"{len(orphans):,} observation series missing from catalogue"
        )

    if any(o[3] is None for o in obs):
        problems.append("null values present in observation.value")

    dates = [o[2] for o in obs]
    if max(dates) > "2027-12-31":
        problems.append(f"implausible future date {max(dates)}")

    print("\n=== VALIDATION ===")
    print(f"observation rows          : {counts['observation']:,}")
    print(f"series rows               : {counts['series']:,}")
    print(f"series_break rows         : {counts['series_break']:,}")
    print(f"dicionario rows           : {counts['dicionario']:,}")
    print(
        f"distinct tables           : {len({s['table_code'] for s in series})}"
    )
    print(f"date range                : {min(dates)} -> {max(dates)}")
    print(f"duplicate observation keys: {dups}")
    print(f"orphan series             : {len(orphans)}")
    print(
        f"series with no observation: {sum(1 for s in series if s['observation_start'] is None)}"
    )

    print("\nfrequency:")
    for f, c in Counter(s["frequency"] for s in series).most_common():
        print(f"  {c:5,}  {f or '(blank)'}")
    print("\nsource (attribution carried per series):")
    for f, c in Counter(s["source"] for s in series).most_common(10):
        print(f"  {c:5,}  {f}")
    print(f"\nexcluded by licence gate  : {len(cleaned['excluded']):,} series")
    for f, c in Counter(s["source"] for s in cleaned["excluded"]).most_common(
        20
    ):
        print(f"  {c:5,}  {f[:80]}")
    print(f"\nskipped files             : {len(cleaned['skipped'])}")

    if problems:
        print("\n!!! PROBLEMS:")
        for p in problems:
            print("   -", p)
        raise SystemExit(1)
    print("\nAll invariants hold.")


def write_exclusion_list(cleaned):
    """Record every series dropped by the licence gate, so the filter is reviewable."""
    out = Path(__file__).parent / "excluded_series.csv"
    with open(out, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(
            ["table_code", "series_id", "source", "source_file", "title"]
        )
        for s in sorted(
            cleaned["excluded"],
            key=lambda r: (r["table_code"], r["series_id"]),
        ):
            w.writerow(
                [
                    s["table_code"],
                    s["series_id"],
                    s["source"],
                    s["source_file"],
                    s["title"],
                ]
            )
    print(f"exclusion list written to {out}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--download",
        action="store_true",
        help="re-download the source CSVs first",
    )
    args = ap.parse_args()

    if args.download:
        print(f"downloading into {DATA / 'input'} ...")
        got = download_all(DATA / "input")
        print(f"  {len(got)} files")

    cleaned = clean_all(DATA / "input")
    counts = write_partitioned(cleaned, DATA / "output")
    validate(cleaned, counts)
    write_exclusion_list(cleaned)
    print(f"\nparquet written to {DATA / 'output'}")


if __name__ == "__main__":
    main()
