"""Measure the ballot_preference source: rows, ballots, and value domains."""

from __future__ import annotations

import collections
import io
import pathlib
import sys
import zipfile

ROOT = (
    pathlib.Path.home() / "Downloads" / "au_nsw_nswec_elections_data" / "input"
)


def main() -> None:
    grand = 0
    for year in (2015, 2019, 2023):
        rows = 0
        ballots = 0
        formality = collections.Counter()
        pref_marking = collections.Counter()
        pref_counted = collections.Counter()
        header_seen = set()
        bad_bp = 0
        for zp in sorted((ROOT / str(year) / "pref").glob("*.zip")):
            with zipfile.ZipFile(zp) as zf:
                name = zf.namelist()[0]
                with zf.open(name) as fh:
                    text = io.TextIOWrapper(
                        fh, encoding="utf-8", errors="replace"
                    )
                    header = next(text).rstrip("\n").split("\t")
                    header_seen.add(tuple(header))
                    seen = set()
                    for line in text:
                        parts = line.rstrip("\n").split("\t")
                        rows += 1
                        if len(parts) < 7:
                            bad_bp += 1
                            parts = parts + [""] * (7 - len(parts))
                        formality[parts[3]] += 1
                        pref_marking[parts[5]] += 1
                        pref_counted[parts[6]] += 1
                        seen.add((parts[0], parts[1], parts[2]))
                    ballots += len(seen)
        grand += rows
        print(f"=== {year}")
        print(f"  rows              {rows:,}")
        print(f"  distinct ballots  {ballots:,}  (district+venue+BPNumber)")
        print(f"  rows per ballot   {rows / ballots:.2f}")
        print(f"  headers           {header_seen}")
        print(f"  short lines       {bad_bp:,}")
        print(f"  formality         {dict(formality)}")
        print(f"  pref_marking top  {pref_marking.most_common(12)}")
        print(f"  pref_counted top  {pref_counted.most_common(12)}")
    print(f"\nGRAND TOTAL rows: {grand:,}")


if __name__ == "__main__":
    sys.exit(main())
