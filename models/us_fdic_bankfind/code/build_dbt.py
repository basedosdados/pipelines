"""Generate the dbt models for us_fdic_bankfind from the architecture CSVs.

The architecture is the source of truth for column order and type, so the SQL is
generated rather than hand-written: financials alone has 290 columns.
"""

from __future__ import annotations

import csv
from pathlib import Path

HERE = Path(__file__).resolve().parent
ARCH = HERE / "architecture"
MODELS = HERE.parent
DATASET = "us_fdic_bankfind"

# year is the partition column; clustering follows the way each table is queried
PARTITIONED = {
    "financials": ["cert"],
    "financials_indicator": ["indicator_id", "cert"],
}
YEAR_RANGE = (1984, 2031)


def cast(name: str, bigquery_type: str) -> str:
    return f"    safe_cast({name} as {bigquery_type.lower()}) {name},"


def build(table: str) -> str:
    with (ARCH / f"{table}.csv").open() as handle:
        rows = list(csv.DictReader(handle))

    config = [
        f'        schema="{DATASET}",',
        f'        alias="{table}",',
        '        materialized="table",',
    ]
    if table in PARTITIONED:
        start, end = YEAR_RANGE
        config += [
            "        partition_by={",
            '            "field": "year",',
            '            "data_type": "int64",',
            f'            "range": {{"start": {start}, "end": {end}, "interval": 1}},',
            "        },",
            f"        cluster_by={PARTITIONED[table]!r},",
        ]

    casts = [cast(row["name"], row["bigquery_type"]) for row in rows]
    casts[-1] = casts[-1].rstrip(",")

    return "\n".join(
        [
            "{{",
            "    config(",
            *config,
            "    )",
            "}}",
            "",
            "",
            "select",
            *casts,
            "from",
            f'    {{{{ set_datalake_project("{DATASET}_staging.{table}") }}}}',
            "    as t",
            "",
        ]
    )


if __name__ == "__main__":
    for path in sorted(ARCH.glob("*.csv")):
        table = path.stem
        target = MODELS / f"{DATASET}__{table}.sql"
        target.write_text(build(table))
        print(
            f"{target.name:<48} {sum(1 for _ in csv.DictReader(path.open())):>4} columns"
        )
