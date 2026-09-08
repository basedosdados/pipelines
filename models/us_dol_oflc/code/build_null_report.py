"""Measure how sparse each column is, for the dbt not-null proportion test.

``not_null_proportion_multiple_columns`` fails a table when any column is under
5% non-null, so the columns that are legitimately that sparse have to be named
in ``ignore_values``. Naming them by hand invites a stale list; this measures
them from the cleaned parquet and writes ``code/null_report.json``, which
``build_dbt.py`` reads when it generates ``schema.yml``.

Two scopes, because the two are not the same question:

* ``full`` — the share over the whole history. Used for the three tables small
  enough to test unscoped.
* ``latest`` — the share within the most recent fiscal year. Used for ``lca``,
  whose test is scoped to that year because the table is too wide and too long
  to scan in full on every dbt run. The list is longer there: the FY2020 form
  revision dropped fields that are well populated across the full history.

Usage:
    uv run python models/us_dol_oflc/code/build_null_report.py
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pyarrow.dataset as pads

HERE = Path(__file__).resolve().parent
DATA = Path(
    os.environ.get("OFLC_DATA_DIR", Path.home() / "Downloads/us_dol_oflc_data")
)
OUTPUT = DATA / "output"
THRESHOLD = 0.05


def shares(files: list[Path]) -> tuple[int, dict[str, float]]:
    ds = pads.dataset(sorted(files), format="parquet")
    total = 0
    nulls: dict[str, int] = {}
    for batch in ds.to_batches():
        total += batch.num_rows
        for i, name in enumerate(batch.schema.names):
            nulls[name] = nulls.get(name, 0) + batch.column(i).null_count
    if not total:
        return 0, {}
    return total, {n: (total - c) / total for n, c in nulls.items()}


def main() -> int:
    report: dict[str, dict] = {}
    for table in ["lca", "perm", "h2a", "h2b"]:
        tdir = OUTPUT / table
        if not tdir.exists():
            print(f"{table}: no cleaned output, skipping")
            continue
        latest = max(int(p.name.split("=")[1]) for p in tdir.iterdir())
        n_full, full = shares(list(tdir.rglob("*.parquet")))
        n_late, late = shares(
            list((tdir / f"year={latest}").rglob("*.parquet"))
        )
        report[table] = {
            "rows": n_full,
            "latest_fiscal_year": latest,
            "latest_rows": n_late,
            "sparse_full": sorted(n for n, s in full.items() if s < THRESHOLD),
            "sparse_latest": sorted(
                n for n, s in late.items() if s < THRESHOLD
            ),
        }
        print(
            f"{table}: {n_full:,} rows, FY{latest}; "
            f"{len(report[table]['sparse_full'])} sparse over the full history, "
            f"{len(report[table]['sparse_latest'])} in FY{latest}"
        )
    (HERE / "null_report.json").write_text(json.dumps(report, indent=1) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
