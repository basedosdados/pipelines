"""Report FEC codes present in the cleaned data but absent from the dicionario.

    python audit_codes.py

custom_dictionary_coverage fails on *any* unmapped value, and 45 years of FEC filings
contain legacy and undocumented codes that the current code-description pages omit
(the disbursement category '000', for one). Run this after clean.py and fold whatever
it reports into build_dicionario.py before enabling the test — the alternative,
loosening the test, would hide genuine cleaning bugs.

Reads the cleaned parquet directly, so it needs no BigQuery access.
"""

import csv
import sys
from collections import defaultdict
from pathlib import Path

import build_dicionario as bd
import pyarrow.dataset as ds

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from pipelines.datasets.us_fec_campaign_finance import (
    utils as fec,
)

ARCH = Path(__file__).resolve().parent / "architecture"


def coded_columns(table: str) -> list[str]:
    with (ARCH / f"{table}.csv").open(encoding="utf-8") as fh:
        return [
            row["name"]
            for row in csv.DictReader(fh)
            if row["covered_by_dictionary"] == "yes"
        ]


def main() -> None:
    known = defaultdict(set)
    for table, columns in bd.ASSIGNMENTS.items():
        for column, codebook in columns.items():
            known[(table, column)] = set(codebook)

    missing_total = 0
    for table in fec.SPECS:
        columns = coded_columns(table)
        part = fec.OUTPUT / table
        if not columns or not part.exists():
            continue
        # Pass the files explicitly rather than the directory: the parquet already
        # carries `cycle` as a column, so hive inference would try to add a second,
        # differently-typed one from the directory name and fail to merge.
        dataset = ds.dataset(sorted(part.rglob("*.parquet")), format="parquet")
        for column in columns:
            seen = set()
            for batch in dataset.to_batches(columns=[column]):
                seen.update(
                    v for v in batch.column(0).to_pylist() if v is not None
                )
            unmapped = sorted(seen - known[(table, column)])
            if unmapped:
                missing_total += len(unmapped)
                print(f"{table}.{column}: {len(unmapped)} unmapped")
                print(f"    {unmapped}")

    print(
        "\nall coded values are mapped"
        if not missing_total
        else f"\n{missing_total} unmapped code(s) — add them to build_dicionario.py"
    )


if __name__ == "__main__":
    main()
