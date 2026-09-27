#!/usr/bin/env python
"""Check that no ``safe_cast`` in the dbt models silently dropped values.

    BD_SERVICE_ACCOUNT_DEV=~/.basedosdados/credentials/staging.json \
    PYTHONPATH=$PWD python models/us_osha_enforcement/code/check_safe_cast.py

``safe_cast`` returns NULL rather than raising, so a column whose values do not
parse comes out empty and every row count still matches. The only way to see it
is to compare non-null counts on both sides of the cast.

Only DATE, INT64 and FLOAT64 columns are checked — a STRING-to-STRING cast
cannot fail — and the staging side is read from Parquet row-group statistics,
which costs nothing. A mismatch is expected in exactly one place: ``norm_date``
already drops years outside 1900-2100 before the Parquet is written, so the
staging and modelled counts agree there too.
"""

from __future__ import annotations

import glob
import os
import sys

import pyarrow.parquet as pq
from google.cloud import bigquery

sys.path.insert(0, ".")
from pipelines.datasets.us_osha_enforcement.utils import _arch

OUT = os.path.expanduser("~/Downloads/us_osha_enforcement_data/output")
PROJECT = "basedosdados-dev"
DATASET = "us_osha_enforcement"
TYPED = {"DATE", "INT64", "FLOAT64"}


def staging_nonnull(slug: str, cols: list[str]) -> dict[str, int]:
    files = sorted(glob.glob(f"{OUT}/{slug}/year=*/data.parquet")) or [
        f"{OUT}/{slug}/data.parquet"
    ]
    counts = dict.fromkeys(cols, 0)
    for path in files:
        pf = pq.ParquetFile(path)
        for rg in range(pf.metadata.num_row_groups):
            meta = pf.metadata.row_group(rg)
            for i in range(meta.num_columns):
                cm = meta.column(i)
                if cm.path_in_schema in counts:
                    nulls = cm.statistics.null_count if cm.statistics else 0
                    counts[cm.path_in_schema] += cm.num_values - nulls
    return counts


def main() -> int:
    arch = _arch()
    client = bigquery.Client(project=PROJECT)
    bad = 0
    for table in arch.TABLES:
        cols = [c.name for c in table.columns if c.bigquery_type in TYPED]
        if not cols:
            continue
        before = staging_nonnull(table.slug, cols)
        select = ", ".join(f"count({c}) as {c}" for c in cols)
        row = next(
            iter(
                client.query(
                    f"select {select} from `{PROJECT}.{DATASET}.{table.slug}`"
                ).result()
            )
        )
        for col in cols:
            after = int(getattr(row, col))
            if after != before[col]:
                bad += 1
                lost = before[col] - after
                print(
                    f"  LOST {table.slug}.{col}: {before[col]:,} -> {after:,} "
                    f"({lost:,} values, {100 * lost / max(before[col], 1):.2f}%)"
                )
        print(f"{table.slug}: {len(cols)} typed columns checked")
    print(
        "\nno values lost to safe_cast"
        if not bad
        else f"\n{bad} columns lost values"
    )
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
