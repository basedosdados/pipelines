"""Check the architecture types against the parquet OpenFEMA actually ships.

The cached field dictionary and the published parquet do not always agree —
``NfipClaims.dateOfLoss`` is documented as ``datetime`` but shipped as
``date32[day]``. The parquet is what gets ingested, so it wins; disagreements
are recorded in ``spec.TYPE_OVERRIDE`` rather than silently absorbed by
``safe_cast``, which would return NULL instead of raising.

Run against already-downloaded input files:

    uv run python validate_types.py ~/Downloads/us_fema_openfema_data/input
"""

from __future__ import annotations

import csv
import re
import sys
from pathlib import Path

import pyarrow.parquet as pq

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parents[2]))

import glossary  # noqa: E402
import tables as spec  # noqa: E402

ARROW_TO_BQ = {
    "bool": "BOOLEAN",
    "double": "FLOAT64",
    "string": "STRING",
    "int16": "INT64",
    "int32": "INT64",
    "int64": "INT64",
}


def arrow_to_bq(arrow_type: str) -> str:
    if arrow_type.startswith("date32"):
        return "DATE"
    if arrow_type.startswith("timestamp"):
        return "DATETIME"
    if arrow_type.startswith("decimal"):
        return "FLOAT64"
    return ARROW_TO_BQ.get(arrow_type, f"UNKNOWN({arrow_type})")


def snake(name: str) -> str:
    name = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    return name.lower()


def main(input_dir: Path) -> int:
    problems = 0
    for table, cfg in spec.TABLES.items():
        path = input_dir / f"{table}.parquet"
        if not path.exists():
            print(f"{table:<28} SKIP (no {path.name})")
            continue
        rename = {**spec.HARMONISE, **cfg["rename"]}
        arch = {
            row["name"]: row["bigquery_type"]
            for row in csv.DictReader(
                (HERE / "architecture" / f"{table}.csv").open()
            )
        }
        schema = pq.ParquetFile(path).schema_arrow
        for field in schema:
            raw = snake(field.name)
            if raw in spec.DROP:
                continue
            name = rename.get(raw, raw)
            if name not in arch:
                if raw == cfg["partition_source"]:
                    continue
                print(
                    f"  {table}.{name}: in the parquet, not in the architecture"
                )
                problems += 1
                continue
            if name in glossary.CODED or name in glossary.IDENTIFIER:
                continue  # deliberately STRING whatever the source stores
            want, got = arch[name], arrow_to_bq(str(field.type))
            if want != got:
                print(f"  {table}.{name}: architecture {want}, parquet {got}")
                problems += 1
        print(f"{table:<28} checked {len(schema)} source fields")

    print("MISMATCHES:", problems)
    return 1 if problems else 0


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise SystemExit(__doc__)
    raise SystemExit(main(Path(sys.argv[1]).expanduser()))
