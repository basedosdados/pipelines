"""Run the NSWEC cleaning transform and write all-STRING partitioned Parquet.

The transform itself lives in ``pipelines/datasets/au_nsw_nswec_elections/utils.py`` so
that a later recurring pipeline shares one implementation rather than a copy.

Staging is all-STRING by house convention and the dbt models ``safe_cast`` every
column, so the Parquet is written all-STRING with a fixed column order. The cast goes
through arrow rather than ``astype(str)``, which would render NULL as the literal
``"nan"`` and defeat ``safe_cast``.

Usage::

    PYTHONPATH=. python models/au_nsw_nswec_elections/code/clean.py [table ...]
"""

from __future__ import annotations

import json
import os
import pathlib
import sys
import time

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pipelines.datasets.au_nsw_nswec_elections import utils as u
from pipelines.datasets.au_nsw_nswec_elections.schema import (
    PARTITION_COLUMNS,
    TABLES,
    column_names,
)

DATA_DIR = pathlib.Path(
    os.environ.get(
        "NSWEC_DATA_DIR",
        str(pathlib.Path.home() / "Downloads" / "au_nsw_nswec_elections_data"),
    )
)
INPUT = DATA_DIR / "input"
OUTPUT = DATA_DIR / "output"
SED_CROSSWALK = DATA_DIR / "state_electoral_division_2021.csv"


def log(message: str) -> None:
    print(message, flush=True)


def to_string_table(frame: pd.DataFrame, table: str) -> pa.Table:
    """Cast every column to STRING, preserving the architecture's column order."""
    columns = column_names(table)
    frame = frame.reindex(columns=columns)
    arrays = []
    for name in columns:
        # NaN must become a real null before the cast: pyarrow rejects a float in a
        # string array, and astype(str) would write the literal "nan", which
        # safe_cast will not turn back into NULL.
        values = frame[name].astype(object).where(frame[name].notna(), None)
        arrays.append(
            pa.array(values.to_numpy(dtype=object), type=pa.string())
        )
    return pa.Table.from_arrays(
        arrays, schema=pa.schema([(c, pa.string()) for c in columns])
    )


def write(frame: pd.DataFrame, table: str, basename: str = "data") -> int:
    """Write one Parquet file per partition value of the table's partition column."""
    partitions = PARTITION_COLUMNS[table]
    written = 0
    if not partitions:
        target = OUTPUT / table
        target.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            to_string_table(frame, table),
            target / f"{basename}.parquet",
            compression="snappy",
        )
        return len(frame)
    for value, group in frame.groupby(partitions[0], dropna=False):
        if pd.isna(value):
            raise ValueError(f"{table}: rows with a null partition key")
        target = OUTPUT / table / f"{partitions[0]}={value}"
        target.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            to_string_table(group, table),
            target / f"{basename}.parquet",
            compression="snappy",
        )
        written += len(group)
    return written


def main(argv: list[str]) -> int:
    wanted = set(argv[1:]) or set(TABLES)
    if not SED_CROSSWALK.exists():
        raise SystemExit(
            f"missing {SED_CROSSWALK}; run fetch_directory.py first to export the "
            "state_electoral_division_2021 crosswalk from BigQuery"
        )
    sed = u.load_sed_crosswalk(SED_CROSSWALK)
    counts: dict[str, int] = {}

    simple = {
        "election": lambda: u.build_election(INPUT),
        "candidate": lambda: u.build_candidate(INPUT, sed),
        "enrolment_turnout": lambda: u.build_enrolment_turnout(INPUT, sed),
        "result_voting_centre": lambda: u.build_result_voting_centre(
            INPUT, sed
        ),
        "result_district": lambda: u.build_result_district(INPUT, sed),
        "distribution_of_preferences": lambda: (
            u.build_distribution_of_preferences(INPUT, sed)
        ),
        "voting_centre": lambda: u.build_voting_centre(INPUT, sed),
        "dicionario": u.build_dicionario,
    }
    for table, builder in simple.items():
        if table not in wanted:
            continue
        start = time.time()
        frame = builder()
        counts[table] = write(frame, table)
        log(
            f"  {table:30s} {counts[table]:>12,} rows  {time.time() - start:6.1f}s"
        )

    if "ballot_preference" in wanted:
        total = 0
        start = time.time()
        for year in u.BULK_YEARS:
            for slug, frame in u.iter_ballot_preference(INPUT, year, sed):
                total += write(
                    frame, "ballot_preference", basename=f"data_{slug}"
                )
            log(
                f"  ballot_preference {year}          {total:>12,} rows cumulative"
            )
        counts["ballot_preference"] = total
        log(
            f"  {'ballot_preference':30s} {total:>12,} rows  {time.time() - start:6.1f}s"
        )

    (DATA_DIR / "row_counts.json").write_text(json.dumps(counts, indent=2))
    log(f"DONE {json.dumps(counts)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
