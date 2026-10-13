"""Generate the dbt .sql models from the architecture CSVs.

Generating rather than hand-writing keeps column order and types identical to
the architecture, which is the source of truth.
"""

from __future__ import annotations

import csv
from pathlib import Path

from models.br_prf_acidentes.code.constants import ARCHITECTURE_DIR, DATASET_ID

MODEL_DIR = Path(__file__).resolve().parents[1]
PARTITION = {"start": 2007, "end": 2031, "interval": 1}

CAST = {
    "STRING": "safe_cast({c} as string) {c}",
    "INT64": "safe_cast({c} as int64) {c}",
    "FLOAT64": "safe_cast({c} as float64) {c}",
    "DATE": "safe_cast({c} as date) {c}",
    "TIME": "safe_cast({c} as time) {c}",
}

HEADER = """{{{{
    config(
        schema="{dataset}",
        alias="{table}",
        materialized="table",
        partition_by={{
            "field": "ano",
            "data_type": "int64",
            "range": {{"start": {start}, "end": {end}, "interval": {interval}}},
        }},
    )
}}}}


select
{selects}
from {{{{ set_datalake_project("{dataset}_staging.{table}") }}}} as t
"""


def build() -> None:
    for path in sorted(ARCHITECTURE_DIR.glob("*.csv")):
        table = path.stem
        with open(path, encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        selects = ",\n".join(
            "    " + CAST[r["bigquery_type"]].format(c=r["name"]) for r in rows
        )
        sql = HEADER.format(
            dataset=DATASET_ID, table=table, selects=selects, **PARTITION
        )
        out = MODEL_DIR / f"{DATASET_ID}__{table}.sql"
        out.write_text(sql, encoding="utf-8")
        print(f"{out.name:48} {len(rows):>3} columns")


if __name__ == "__main__":
    build()
