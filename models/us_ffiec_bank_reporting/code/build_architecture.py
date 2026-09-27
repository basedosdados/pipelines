"""Render schema_def.py to the architecture CSVs, the dataset's source of truth.

One CSV per table under code/architecture/. csv.writer defaults to CRLF and the
repo's mixed-line-ending pre-commit hook rewrites it, so lineterminator is
pinned to "\\n" -- otherwise regeneration and the hook flip-flop forever.
"""

from __future__ import annotations

import csv

from common import ARCH_DIR
from schema_def import ARCH_HEADER, TABLES


def main() -> None:
    ARCH_DIR.mkdir(parents=True, exist_ok=True)
    for table, cols in TABLES.items():
        path = ARCH_DIR / f"{table}.csv"
        with open(path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh, lineterminator="\n")
            writer.writerow(ARCH_HEADER)
            for name, typ, desc, dic, directory, unit, obs, original in cols:
                writer.writerow(
                    [
                        name,
                        typ,
                        desc,
                        "",
                        dic,
                        directory,
                        unit,
                        "no",
                        obs,
                        original,
                    ]
                )
        print(f"{table}: {len(cols)} columns -> {path}")


if __name__ == "__main__":
    main()
