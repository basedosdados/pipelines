"""Write the dbt models and schema.yml for us_oi_opportunity_atlas.

Both are generated from the architecture CSVs (column order, types, English
descriptions) and table_meta.py (table descriptions, keys, accepted values).

Usage:
    python models/us_oi_opportunity_atlas/code/build_dbt.py
"""

import csv
import textwrap
from pathlib import Path

from table_meta import ACCEPTED, RETIRED_COUNTIES, TABLE_ORDER, TABLES

CODE_DIR = Path(__file__).resolve().parent
MODEL_DIR = CODE_DIR.parent
DATASET = "us_oi_opportunity_atlas"


def read_arch(table: str) -> list[dict[str, str]]:
    with open(
        CODE_DIR / "architecture" / f"{table}.csv", encoding="utf-8"
    ) as f:
        return list(csv.DictReader(f))


def sql(table: str, cols: list[dict[str, str]]) -> str:
    keys = TABLES[table]["keys"]
    selects = ",\n".join(
        f"    safe_cast({c['name']} as {c['bigquery_type'].lower()}) {c['name']}"
        for c in cols
    )
    return (
        "{{\n"
        "    config(\n"
        f'        schema="{DATASET}",\n'
        f'        alias="{table}",\n'
        '        materialized="table",\n'
        f"        cluster_by={keys[:1]!r},\n"
        "    )\n"
        "}}\n\n\n"
        "select\n"
        f"{selects}\n"
        f'from {{{{ set_datalake_project("{DATASET}_staging.{table}") }}}} as t\n'
    ).replace("'", '"')


def block(text: str, indent: int) -> list[str]:
    pad = " " * indent
    return [f"{pad[:-2]}description: >-"] + [
        pad + line for line in textwrap.wrap(text, 80 - indent)
    ]


def column_tests(table: str, name: str) -> list[str]:
    keys = TABLES[table]["keys"]
    tests = []
    if name in keys:
        tests.append("- not_null")
    if name == "state_id":
        tests += [
            "- custom_relationships:",
            "    to: ref('br_bd_diretorios_us__state')",
            "    field: id_state",
        ]
    if name == "county_id":
        tests += [
            "- custom_relationships:",
            "    to: ref('br_bd_diretorios_us__county')",
            "    field: id_county",
            f"    ignore_values: [{', '.join(repr(c) for c in RETIRED_COUNTIES)}]",
        ]
    if name in ACCEPTED:
        tests += [
            "- accepted_values:",
            f"    values: [{', '.join(repr(v) for v in ACCEPTED[name])}]",
        ]
    return tests


def schema() -> str:
    lines = ["---", "version: 2", "models:"]
    for table in TABLE_ORDER:
        meta = TABLES[table]
        cols = read_arch(table)
        lines.append(f"  - name: {DATASET}__{table}")
        lines += block(meta["description_en"], 6)
        lines += [
            "    tests:",
            "      - dbt_utils.unique_combination_of_columns:",
            f"          combination_of_columns: [{', '.join(meta['keys'])}]",
            "      - not_null_proportion_multiple_columns:",
            "          at_least: 0.05",
            "    columns:",
        ]
        for c in cols:
            lines.append(f"      - name: {c['name']}")
            lines += block(c["description_en"], 10)
            tests = column_tests(table, c["name"])
            if tests:
                lines.append("        tests:")
                lines += [f"          {t}" for t in tests]
    return "\n".join(lines) + "\n"


def main() -> None:
    for table in TABLE_ORDER:
        (MODEL_DIR / f"{DATASET}__{table}.sql").write_text(
            sql(table, read_arch(table))
        )
    (MODEL_DIR / "schema.yml").write_text(schema())
    print(f"wrote {len(TABLE_ORDER)} models and schema.yml")


if __name__ == "__main__":
    main()
