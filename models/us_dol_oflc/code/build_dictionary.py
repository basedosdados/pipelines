"""Build the us_dol_oflc dictionary table from the cleaned parquet.

Every column flagged ``covered_by_dictionary`` in the architecture must have an
entry for each distinct value it takes, or the dbt
``custom_dictionary_coverage_eng`` test fails. The dictionary is therefore
generated from the data, not hand-written: the distinct values are read back
from ``output/<table>/``, given a curated label where the source vocabulary is
known, and otherwise labelled with a tidied form of the value itself.

``temporal_coverage`` records the fiscal years in which the value occurs, which
is where the vocabulary drift is visible — for example the legacy H-1B eFile
status codes that stop after FY2009.

Usage:
    uv run python models/us_dol_oflc/code/build_dictionary.py
"""

from __future__ import annotations

import csv
import os
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as pads
import pyarrow.parquet as pq

HERE = Path(__file__).resolve().parent
from pipelines.datasets.us_dol_oflc import canonical_map as cm  # noqa: E402

DATA = Path(os.environ.get("OFLC_DATA_DIR", Path.home() / "Downloads/us_dol_oflc_data"))
OUTPUT = DATA / "output"
ARCH = HERE / "architecture"

# Curated labels for vocabularies whose stored form is a code or an
# abbreviation. Keys are matched case-insensitively on the stored value.
LABELS: dict[str, dict[str, str]] = {
    "case_status": {
        "C": "Certified",
        "D": "Denied",
        "W": "Withdrawn",
        "I": "In progress",
        "R": "Rejected",
    },
    "visa_class": {
        "R": "H-1B",
        "A": "H-1B1 Chile",
        "C": "H-1B1 Singapore",
        "E": "E-3 Australia",
    },
    "wage_unit_of_pay": {
        "hour": "Per hour",
        "day": "Per day",
        "week": "Per week",
        "bi-weekly": "Every two weeks",
        "semi-monthly": "Twice a month",
        "month": "Per month",
        "year": "Per year",
        "piece rate": "Per unit produced",
    },
}
LABELS["prevailing_wage_unit_of_pay"] = LABELS["wage_unit_of_pay"]

YES_NO = {"Y": "Yes", "N": "No", "YES": "Yes", "NO": "No",
          "T": "Yes", "F": "No", "TRUE": "Yes", "FALSE": "No",
          "1": "Yes", "0": "No"}
YES_NO_COLUMNS = {
    "full_time_position", "h1b_dependent", "willful_violator", "support_h1b",
    "withdrawn", "secondary_entity", "refile", "schedule_a_sheepherder",
    "required_experience", "is_multiple_worksites", "h2a_labor_contractor",
    "emergency_filing", "cap_exempt", "meals_provided",
    "agent_representing_employer",
}


def covered_columns(table: str) -> list[str]:
    with open(ARCH / f"{table}.csv") as fh:
        return [r["name"] for r in csv.DictReader(fh)
                if r["covered_by_dictionary"] == "yes"]


def label(column: str, value: str) -> str:
    curated = LABELS.get(column, {})
    for key, lab in curated.items():
        if value.upper() == key.upper():
            return lab
    if column in YES_NO_COLUMNS and value.upper() in YES_NO:
        return YES_NO[value.upper()]
    # Already-readable label: normalise spacing and capitalise the first letter.
    tidy = " ".join(value.split())
    return tidy[:1].upper() + tidy[1:] if tidy else tidy


def coverage(years: set[int]) -> str:
    """Compact fiscal-year coverage, e.g. "2008(1)2012, 2015(1)2026"."""
    ys = sorted(years)
    runs, start, prev = [], ys[0], ys[0]
    for y in ys[1:]:
        if y == prev + 1:
            prev = y
            continue
        runs.append((start, prev))
        start = prev = y
    runs.append((start, prev))
    return ", ".join(f"{a}(1){b}" if a != b else f"{a}(1){a}" for a, b in runs)


def main() -> int:
    rows = []
    for table in ["lca", "perm", "h2a", "h2b"]:
        cols = covered_columns(table)
        tdir = OUTPUT / table
        if not tdir.exists():
            raise SystemExit(f"Missing cleaned output for {table}; run clean_data.py")
        seen: dict[tuple[str, str], set[int]] = defaultdict(set)
        ds = pads.dataset(tdir, format="parquet", partitioning="hive")
        for batch in ds.to_batches(columns=cols + ["year"]):
            df = batch.to_pandas()
            years = df["year"].astype(str)
            for col in cols:
                sub = df[[col]].assign(year=years).dropna(subset=[col])
                for value, year in zip(sub[col], sub["year"]):
                    v = " ".join(str(value).split())
                    if v:
                        seen[(col, v)].add(int(year))
        for (col, value), years in sorted(seen.items()):
            rows.append({"table_id": table, "column_name": col, "key": value,
                         "temporal_coverage": coverage(years),
                         "value": label(col, value)})
        print(f"{table}: {len({c for c, _ in seen})} covered columns, "
              f"{sum(1 for k in seen if k[0] in {c for c, _ in seen})} entries",
              flush=True)

    df = pd.DataFrame(rows, columns=["table_id", "column_name", "key",
                                     "temporal_coverage", "value"])
    schema = pa.schema([pa.field(c, pa.string()) for c in df.columns])
    pdir = OUTPUT / "dictionary"
    pdir.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pandas(df, schema=schema, preserve_index=False),
                   pdir / "data.parquet", compression="snappy")
    print(f"dictionary: {len(df):,} rows -> {pdir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
