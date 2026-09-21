"""Run the ECSA cleaning transform and write all-STRING partitioned Parquet.

The transform itself lives in ``pipelines/datasets/au_sa_ecsa_elections/utils.py``
so that a later recurring pipeline shares one implementation rather than a copy.

Staging is all-STRING by house convention and the dbt models ``safe_cast`` every
column, so the Parquet is written all-STRING with a fixed column order. The cast
goes through arrow rather than ``astype(str)``, which would render NULL as the
literal ``"nan"`` and defeat ``safe_cast``.

Usage::

    PYTHONPATH=. python models/au_sa_ecsa_elections/code/clean.py [table ...]
"""

from __future__ import annotations

import json
import sys
import time

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pipelines.datasets.au_sa_ecsa_elections import utils as u
from pipelines.datasets.au_sa_ecsa_elections.constants import data_dir
from pipelines.datasets.au_sa_ecsa_elections.schema import (
    PARTITION_COLUMNS,
    TABLES,
    column_names,
    column_types,
)

DATA_DIR = data_dir()
INPUT = DATA_DIR / "input"
OUTPUT = DATA_DIR / "output"
SED_CROSSWALK = DATA_DIR / "state_electoral_division_2021.csv"


def log(message: str) -> None:
    print(message, flush=True)


def to_string_table(frame: pd.DataFrame, table: str) -> pa.Table:
    """Cast every column to STRING, preserving the architecture's column order."""
    columns = column_names(table)
    extra = [c for c in frame.columns if c not in columns]
    if extra:
        raise ValueError(f"{table}: columns not in the architecture: {extra}")
    frame = frame.reindex(columns=columns)
    types = column_types(table)
    arrays = []
    for name in columns:
        series = frame[name]
        # Pass the architecture's real type through first, then stringify. Skipping
        # that step writes an INT64 year as "2026.0", which safe_cast turns into
        # NULL. NaN has to become a real null rather than the literal "nan", which
        # safe_cast will not turn back into NULL either.
        if types[name] == "INT64":
            series = pd.to_numeric(series, errors="coerce").astype("Int64")
        elif types[name] == "FLOAT64":
            series = pd.to_numeric(series, errors="coerce")
        values = [None if pd.isna(v) else str(v) for v in series]
        arrays.append(pa.array(values, type=pa.string()))
    return pa.Table.from_arrays(
        arrays, schema=pa.schema([(c, pa.string()) for c in columns])
    )


def write(frame: pd.DataFrame, table: str) -> int:
    """Write one Parquet file per partition value of the table's partition column."""
    partitions = PARTITION_COLUMNS[table]
    if not partitions:
        target = OUTPUT / table
        target.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            to_string_table(frame, table),
            target / "data.parquet",
            compression="snappy",
        )
        return len(frame)
    written = 0
    for value, group in frame.groupby(partitions[0], dropna=False):
        if pd.isna(value):
            raise ValueError(f"{table}: rows with a null partition key")
        target = OUTPUT / table / f"{partitions[0]}={int(value)}"
        target.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            to_string_table(group, table),
            target / "data.parquet",
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
    builders = {
        "election": lambda: u.build_election(INPUT),
        "candidate": lambda: u.build_candidate(INPUT, sed),
        "result_district": lambda: u.build_result_district(INPUT, sed),
        "result_voting_centre": lambda: u.build_result_voting_centre(
            INPUT, sed
        ),
        "distribution_of_preferences": lambda: (
            u.build_distribution_of_preferences(INPUT, sed)
        ),
        "voting_centre": lambda: u.build_voting_centre(INPUT, sed),
        "enrolment_turnout": lambda: u.build_enrolment_turnout(INPUT, sed),
        "disclosure_return": lambda: u.build_disclosure_return(INPUT),
        "dicionario": u.build_dicionario,
    }
    counts: dict[str, int] = {}
    for table, builder in builders.items():
        if table not in wanted:
            continue
        start = time.time()
        frame = builder()
        counts[table] = write(frame, table)
        log(
            f"  {table:32s} {counts[table]:>10,} rows  {time.time() - start:6.1f}s"
        )

    (DATA_DIR / "row_counts.json").write_text(json.dumps(counts, indent=1))
    log(f"DONE {json.dumps(counts)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
