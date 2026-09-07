"""Generate the dbt models and schema.yml for us_dol_oflc from the architecture.

Writes ``models/us_dol_oflc/us_dol_oflc__<table>.sql`` and ``schema.yml``. The
architecture CSVs are the single source of truth for column order, types and
descriptions, so the models cannot drift from them.

Usage:
    uv run python models/us_dol_oflc/code/build_dbt.py
"""

from __future__ import annotations

import csv
import json
import sys
import textwrap
from pathlib import Path

HERE = Path(__file__).resolve().parent
MODELS = HERE.parent
ARCH = HERE / "architecture"
DATASET = "us_dol_oflc"

sys.path.insert(0, str(HERE))

FY_START, FY_END = 2008, 2031

TABLE_DESC = {
    "lca": (
        "Labor Condition Applications filed with the Office of Foreign Labor "
        "Certification for the H-1B, H-1B1 and E-3 programs, one row per case "
        "number per federal fiscal year, with the employer, the primary "
        "worksite, the occupation, the offered wage and the prevailing wage. "
        "FY2008 and part of FY2009 come from the legacy H-1B eFile system; the "
        "rest from iCERT and FLAG."
    ),
    "perm": (
        "Applications for Permanent Employment Certification (ETA-9089) filed "
        "with the Office of Foreign Labor Certification, one row per case "
        "number per federal fiscal year, with the employer, the worksite, the "
        "occupation, the offered wage, the prevailing wage and the foreign "
        "worker's citizenship and class of admission."
    ),
    "h2a": (
        "Applications for H-2A temporary agricultural labor certification "
        "filed with the Office of Foreign Labor Certification, one row per "
        "case number per federal fiscal year, with the employer, the worksite, "
        "the crop, the workers requested and certified, the offered wage and "
        "the housing the employer provides."
    ),
    "h2b": (
        "Applications for H-2B temporary non-agricultural labor certification "
        "filed with the Office of Foreign Labor Certification, one row per "
        "case number per federal fiscal year, with the employer, the worksite, "
        "the occupation, the workers requested and certified and the offered "
        "wage."
    ),
    "dictionary": (
        "Dictionary of the coded values used by the us_dol_oflc tables, with "
        "the fiscal years each value occurs in. The OFLC vocabularies change "
        "across form revisions, so a value present in one span of years may be "
        "absent in another."
    ),
}

CAST = {
    "STRING": "safe_cast({c} as string) {c}",
    "INT64": "safe_cast({c} as int64) {c}",
    "FLOAT64": "safe_cast({c} as float64) {c}",
    "DATE": "safe_cast({c} as date) {c}",
}

# The lca table is scoped to the most recent fiscal year because the proportion
# test scans every column and the table is both wide and long; the other three
# are small enough to test over their full history, which is the more meaningful
# question.
SCOPED_TO_LATEST_YEAR = {"lca"}

# Columns legitimately below the 5% non-null threshold, measured rather than
# guessed — see build_null_report.py.
_NULL_REPORT = HERE / "null_report.json"
_report = json.loads(_NULL_REPORT.read_text()) if _NULL_REPORT.exists() else {}
SPARSE: dict[str, list[str]] = {
    table: info[
        "sparse_latest" if table in SCOPED_TO_LATEST_YEAR else "sparse_full"
    ]
    for table, info in _report.items()
}

# Logical key per table, used by the uniqueness test.
KEY = {
    "lca": ["year", "case_number"],
    "perm": ["year", "case_number"],
    "h2a": ["year", "case_number"],
    "h2b": ["year", "case_number"],
    "dictionary": ["table_id", "column_name", "key"],
}


def read_arch(table: str) -> list[dict]:
    with open(ARCH / f"{table}.csv") as fh:
        return list(csv.DictReader(fh))


def sql(table: str) -> str:
    arch = read_arch(table)
    casts = ",\n    ".join(
        CAST[c["bigquery_type"]].format(c=c["name"]) for c in arch
    )
    if table == "dictionary":
        config = textwrap.dedent(f'''\
            {{{{
                config(
                    schema="{DATASET}",
                    alias="{table}",
                    materialized="table",
                )
            }}}}''')
    else:
        config = textwrap.dedent(f'''\
            {{{{
                config(
                    schema="{DATASET}",
                    alias="{table}",
                    materialized="table",
                    partition_by={{
                        "field": "year",
                        "data_type": "int64",
                        "range": {{"start": {FY_START}, "end": {FY_END}, "interval": 1}},
                    }},
                )
            }}}}''')
    return (
        f"{config}\n\n\nselect\n    {casts}\n"
        f'from {{{{ set_datalake_project("{DATASET}_staging.{table}") }}}} as t\n'
    )


def wrap(text: str, indent: str) -> str:
    body = "\n".join(
        textwrap.wrap(
            " ".join(text.split()),
            74,
            initial_indent=indent,
            subsequent_indent=indent,
        )
    )
    return f">-\n{body}"


def schema() -> str:
    out = ["---", "version: 2", "models:"]
    for table in ["lca", "perm", "h2a", "h2b", "dictionary"]:
        arch = read_arch(table)
        names = {c["name"] for c in arch}
        out.append(f"  - name: {DATASET}__{table}")
        out.append(f"    description: {wrap(TABLE_DESC[table], '      ')}")
        out.append("    tests:")
        out.append("      - dbt_utils.unique_combination_of_columns:")
        out.append("          combination_of_columns:")
        for k in KEY[table]:
            out.append(f"            - {k}")
        out.append("      - not_null_proportion_multiple_columns:")
        out.append("          at_least: 0.05")
        if SPARSE.get(table):
            out.append("          ignore_values:")
            for c in SPARSE[table]:
                out.append(f"            - {c}")
        if table in SCOPED_TO_LATEST_YEAR:
            # The proportion test scans every column; scope the widest, longest
            # table to the most recent fiscal year so a dbt test run does not
            # cost a full-table scan.
            out.append("          config:")
            out.append("            where: __most_recent_year_en__")
        covered = [
            c["name"] for c in arch if c["covered_by_dictionary"] == "yes"
        ]
        if covered:
            out.append("      - custom_dictionary_coverage_eng:")
            out.append(
                f"          dictionary_model: ref('{DATASET}__dictionary')"
            )
            out.append("          columns_covered_by_dictionary:")
            for c in covered:
                out.append(f"            - {c}")
        out.append("    columns:")
        for c in arch:
            out.append(f"      - name: {c['name']}")
            out.append(
                f"        description: {wrap(c['description'], '          ')}"
            )
            tests = []
            if c["name"] in KEY[table]:
                tests.append("not_null")
            if c["name"] == "year":
                out.append("        tests:")
                out.append("          - not_null")
                out.append("          - relationships:")
                out.append(
                    "              to: ref('br_bd_diretorios_data_tempo__ano')"
                )
                out.append("              field: ano.ano")
                continue
            if (
                c["name"] in ("employer_state", "worksite_state")
                and c["name"] in names
            ):
                out.append("        tests:")
                if tests:
                    out.append("          - not_null")
                out.append("          - custom_relationships:")
                out.append(
                    "              to: ref('br_bd_diretorios_us__state')"
                )
                out.append("              field: abbreviation")
                out.append("              proportion_allowed_failures: 0.05")
                continue
            if tests:
                out.append(f"        tests: [{', '.join(tests)}]")
    return "\n".join(out) + "\n"


def main() -> int:
    for table in ["lca", "perm", "h2a", "h2b", "dictionary"]:
        path = MODELS / f"{DATASET}__{table}.sql"
        path.write_text(sql(table))
        print(f"wrote {path.name}")
    (MODELS / "schema.yml").write_text(schema())
    print("wrote schema.yml")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
