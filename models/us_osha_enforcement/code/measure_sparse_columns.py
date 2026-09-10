#!/usr/bin/env python
"""Measure per-column non-null share and write ``sparse_columns.json``.

    PYTHONPATH=$PWD python models/us_osha_enforcement/code/measure_sparse_columns.py

The generated ``schema.yml`` passes the result to
``not_null_proportion_multiple_columns`` as ``ignore_values``, so a column that
is legitimately almost always empty does not fail a test written for columns
that are not.

The cut is 10%, not the test's own 5% floor: a column measured at 5.2% today
crosses the floor as soon as the source publishes a year that uses it less, and
the failure then looks like a regression in the pipeline. Reads Parquet
row-group statistics, so nothing is loaded into memory.
"""

import glob
import json
import os
import sys

import pyarrow.parquet as pq

sys.path.insert(0, ".")
from pipelines.datasets.us_osha_enforcement.utils import _arch

OUT = os.path.expanduser("~/Downloads/us_osha_enforcement_data/output")

#: Margin over the test's own 0.05 floor.
THRESHOLD = 0.10


def main() -> int:
    arch = _arch()
    sparse: dict[str, list[str]] = {}
    complete: dict[str, list[str]] = {}
    near: list[tuple[str, str, float]] = []
    for table in arch.TABLES:
        files = sorted(
            glob.glob(f"{OUT}/{table.slug}/year=*/data.parquet")
        ) or [f"{OUT}/{table.slug}/data.parquet"]
        cols = [c.name for c in table.columns]
        total = 0
        nonnull = dict.fromkeys(cols, 0)
        for path in files:
            pf = pq.ParquetFile(path)
            total += pf.metadata.num_rows
            for rg in range(pf.metadata.num_row_groups):
                meta = pf.metadata.row_group(rg)
                for i in range(meta.num_columns):
                    cm = meta.column(i)
                    if cm.path_in_schema in nonnull:
                        nulls = (
                            cm.statistics.null_count if cm.statistics else 0
                        )
                        nonnull[cm.path_in_schema] += cm.num_values - nulls
        low, full = [], []
        for col in cols:
            prop = nonnull[col] / total if total else 0.0
            if prop < THRESHOLD:
                low.append(col)
                near.append((table.slug, col, prop))
            if nonnull[col] == total:
                full.append(col)
        if low:
            sparse[table.slug] = low
        complete[table.slug] = full
    dest = "models/us_osha_enforcement/code/sparse_columns.json"
    with open(dest, "w") as fh:
        json.dump(sparse, fh, indent=1)
    # Columns that are 100% populated. A `not_null` test is only written for a
    # key column that appears here: several key parts are nullable in the
    # source (violation_event.event_date is empty on 27,830 rows,
    # optional_code_info.information_value on 25), and asserting otherwise
    # fails on data that is correct.
    with open(
        "models/us_osha_enforcement/code/complete_columns.json", "w"
    ) as fh:
        json.dump(complete, fh, indent=1)
    for slug, col, prop in sorted(near, key=lambda x: x[2]):
        print(f"  {slug:<20} {col:<32} {100 * prop:6.2f}% non-null")
    print(f"\n{len(near)} columns below {100 * THRESHOLD:.0f}% -> {dest}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
