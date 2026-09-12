"""Generate the us_nsf_ncses dbt SQL models from the architecture CSVs.

The architecture table is the source of truth, so the models are generated from
it rather than kept in sync by hand: column order and cast types can never drift
from the registered metadata.

``schema.yml`` is written by hand — its tests and descriptions are editorial.

Run
---
    python models/us_nsf_ncses/code/build_dbt_models.py
    uv run sqlfmt models/us_nsf_ncses   # the repo's SQL formatter owns the layout
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

CODE_DIR = Path(__file__).resolve().parent
ARCH_DIR = CODE_DIR / "architecture"
MODEL_DIR = CODE_DIR.parent
DATASET = "us_nsf_ncses"

# table -> (partition column, partition type, first year). The range end is the
# latest year plus five, per the Data Basis partitioning convention.
PARTITIONS = {
    "herd_institution": ("year", 1972, 2024),
    "herd_expenditure": ("year", 1972, 2024),
    "herd_personnel": ("year", 2010, 2024),
    "herd_survey_item": ("year", 2010, 2024),
    "sed_data_table": ("reference_year", 2024, 2024),
    "sed_estimate": ("reference_year", 2024, 2024),
}

CAST = {
    "STRING": "safe_cast({name} as string) {name},",
    "INT64": "safe_cast({name} as int64) {name},",
    "FLOAT64": "safe_cast({name} as float64) {name},",
}


def read_architecture(table: str) -> list[dict]:
    with open(ARCH_DIR / f"{table}.csv", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def render(table: str) -> str:
    columns = read_architecture(table)
    config = [
        "{{",
        "    config(",
        f'        schema="{DATASET}",',
        f'        alias="{table}",',
        '        materialized="table",',
    ]
    if table in PARTITIONS:
        field, first, last = PARTITIONS[table]
        config += [
            "        partition_by={",
            f'            "field": "{field}",',
            '            "data_type": "int64",',
            f'            "range": {{"start": {first}, "end": {last + 5}, "interval": 1}},',
            "        },",
        ]
    config += ["    )", "}}", "", ""]

    body = ["select"]
    for i, column in enumerate(columns):
        line = CAST[column["bigquery_type"]].format(name=column["name"])
        if i == len(columns) - 1:
            line = line.rstrip(",")
        body.append(f"    {line}")
    body.append(
        f'from {{{{ set_datalake_project("{DATASET}_staging.{table}") }}}} as t'
    )
    return "\n".join(config + body) + "\n"


def main() -> int:
    tables = sorted(p.stem for p in ARCH_DIR.glob("*.csv"))
    for table in tables:
        path = MODEL_DIR / f"{DATASET}__{table}.sql"
        path.write_text(render(table), encoding="utf-8")
        print(f"wrote {path.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
