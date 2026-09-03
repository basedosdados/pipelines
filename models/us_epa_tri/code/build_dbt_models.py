"""Generate the dbt .sql models from the architecture CSVs.

Column order and types come from the architecture, which is the source of
truth, so the models cannot drift from it. Run after editing
``build_architecture.py``; the generated .sql files are committed.

Run ``pre-commit run --files models/us_epa_tri/*.sql`` afterwards: sqlfmt
re-wraps any over-long ``safe_cast`` line, and the committed files carry that
formatting.
"""

import csv
from pathlib import Path

HERE = Path(__file__).parent
ARCH = HERE / "architecture"
OUT = HERE.parent
DATASET = "us_epa_tri"
UPDATED = "2026-09-03"

# Tables partitioned by reporting year; the clustering column is the most
# selective filter after year.
PARTITIONED = {
    "facility": ["tri_facility_id"],
    "form": ["tri_facility_id", "tri_chemical_id"],
    "release": ["tri_chemical_id", "release_category"],
}


def cast(name: str, typ: str) -> str:
    return f"    safe_cast({name} as {typ.lower()}) {name},"


def main():
    for path in sorted(ARCH.glob("*.csv")):
        table = path.stem
        with open(path, newline="", encoding="utf-8") as fh:
            cols = list(csv.DictReader(fh))
        lines = [cast(c["name"], c["bigquery_type"]) for c in cols]
        lines[-1] = lines[-1].rstrip(",")

        if table in PARTITIONED:
            cluster = ", ".join(f'"{c}"' for c in PARTITIONED[table])
            config = (
                f"{{{{\n"
                f"    config(\n"
                f'        schema="{DATASET}",\n'
                f'        alias="{table}",\n'
                f'        materialized="table",\n'
                f"        partition_by={{\n"
                f'            "field": "year",\n'
                f'            "data_type": "int64",\n'
                f'            "range": {{"start": 1987, "end": 2035, "interval": 1}},\n'
                f"        }},\n"
                f"        cluster_by=[{cluster}],\n"
                f"    )\n"
                f"}}}}\n"
            )
        else:
            config = (
                f'{{{{ config(alias="{table}", schema="{DATASET}", '
                f'materialized="table") }}}}\n'
            )

        body = (
            f"{config}\n"
            f"-- Atualizado em {UPDATED}\n"
            "select\n" + "\n".join(lines) + "\n"
            f'from {{{{ set_datalake_project("{DATASET}_staging.{table}") }}}} as t\n'
        )
        dest = OUT / f"{DATASET}__{table}.sql"
        dest.write_text(body, encoding="utf-8")
        print(f"{dest.name}: {len(cols)} columns")


if __name__ == "__main__":
    main()
