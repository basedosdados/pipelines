"""Generate the dbt .sql models from the architecture CSVs.

Column order and types come from the architecture, which is the source of
truth, so the models cannot drift from it. Run after editing
``build_architecture.py``; the generated .sql files are committed.

Run ``pre-commit run --files models/us_hhs_nppes/*.sql`` afterwards: sqlfmt
re-wraps the few ``safe_cast`` lines that exceed the line limit, and the
committed files carry that formatting.
"""

import csv
from pathlib import Path

HERE = Path(__file__).parent
ARCH = HERE / "architecture"
OUT = HERE.parent
DATASET = "us_hhs_nppes"
UPDATED = "2026-09-02"

# Tables that stack monthly snapshots on an extraction_date partition.
PARTITIONED = {
    "provider",
    "taxonomy",
    "other_identifier",
    "other_name",
    "practice_location",
    "endpoint",
}
# The source repeats byte-identical rows in these reference files; the repeats
# carry no information, so they are collapsed.
DISTINCT = {"practice_location", "endpoint"}

GEO = set()  # no geometry columns in this dataset


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
            config = (
                f"{{{{\n"
                f"    config(\n"
                f'        schema="{DATASET}",\n'
                f'        alias="{table}",\n'
                f'        materialized="incremental",\n'
                f"        partition_by={{\n"
                f'            "field": "extraction_date",\n'
                f'            "data_type": "date",\n'
                f'            "granularity": "day",\n'
                f"        }},\n"
                f"    )\n"
                f"}}}}\n"
            )
            tail = (
                "{% if is_incremental() %}\n"
                "    where\n"
                "        safe_cast(extraction_date as date)\n"
                "        > (select max(extraction_date) from {{ this }})\n"
                "{% endif %}\n"
            )
        else:
            config = (
                f'{{{{ config(alias="{table}", schema="{DATASET}", '
                f'materialized="table") }}}}\n'
            )
            tail = ""

        note = ""
        if table in DISTINCT:
            note = (
                "-- distinct: the source repeats byte-identical rows for an NPI\n"
                "-- within one snapshot; the repeats carry no extra information.\n"
            )
        select = "select distinct\n" if table in DISTINCT else "select\n"

        body = (
            f"{config}\n"
            f"-- Atualizado em {UPDATED}\n"
            f"{note}"
            f"{select}" + "\n".join(lines) + "\n"
            f'from {{{{ set_datalake_project("{DATASET}_staging.{table}") }}}} as t\n'
            + tail
        )
        dest = OUT / f"{DATASET}__{table}.sql"
        dest.write_text(body, encoding="utf-8")
        print(f"{dest.name}: {len(cols)} columns")


if __name__ == "__main__":
    main()
