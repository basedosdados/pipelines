#!/usr/bin/env python3
"""Generate the 12 Detailed-release dbt models and their schema.yml entries.

Column order, types and grain come from `architecture_detailed.py`, so a change
there propagates to the SQL, the schema tests and the registered metadata
together. The four Tier-1 models already in `models/au_abs_labour_force/` are
not touched: this appends to `schema.yml` rather than rewriting it, and refuses
to run if a Detailed entry is already present.

Usage:
    uv run python models/au_abs_labour_force/code/gen_dbt_detailed.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from architecture_detailed import GRAIN, TABLES, field

ROOT = Path(__file__).resolve().parents[1]
DATASET = "au_abs_labour_force"
PARTITION_START, PARTITION_END = 1984, 2031

# Low-cardinality dimension to cluster on, per table.
CLUSTER = {
    "employment_industry": ["geography"],
    "employment_industry_region": ["gccsa"],
    "employment_industry_age": ["industry_division"],
    "employment_industry_hours": ["hours_measure"],
    "employment_industry_status": ["industry_division"],
    "employment_industry_occupation": ["industry_division"],
    "employment_occupation": ["occupation_sub_major"],
    "employment_occupation_age": ["occupation_major"],
    "employment_occupation_status": ["occupation_major"],
    "employment_status_hours": ["status_in_employment"],
    "employment_job_tenure": ["geography"],
    "unemployment_duration": ["geography"],
}

DESCRIPTION = {
    "employment_industry": (
        "Quarterly employed persons and hours actually worked in all jobs by "
        "ANZSIC 2006 industry group of the main job, state and territory and "
        "sex, from the ceased ABS Labour Force Detailed release."
    ),
    "employment_industry_region": (
        "Quarterly employed persons and hours actually worked in all jobs by "
        "ANZSIC 2006 industry division of the main job, greater capital city "
        "or rest of state area and sex."
    ),
    "employment_industry_age": (
        "Quarterly employed persons and hours actually worked in all jobs for "
        "Australia by ANZSIC 2006 industry division of the main job and "
        "five-year age group."
    ),
    "employment_industry_hours": (
        "Quarterly employed persons and hours worked for Australia by ANZSIC "
        "2006 industry division of the main job and band of hours worked, on "
        "three hours concepts: hours usually worked in all jobs, hours "
        "actually worked in main job and hours usually worked in main job."
    ),
    "employment_industry_status": (
        "Quarterly employed persons and hours actually worked in all jobs for "
        "Australia by ANZSIC 2006 industry division of the main job and status "
        "in employment."
    ),
    "employment_industry_occupation": (
        "Quarterly employed persons and hours actually worked in all jobs for "
        "Australia by ANZSIC 2006 industry division, ANZSCO 2013 occupation "
        "major group of the main job and sex."
    ),
    "employment_occupation": (
        "Quarterly employed persons and hours actually worked in all jobs for "
        "Australia by ANZSCO 2013 occupation sub-major group of the main job, "
        "sex and age group."
    ),
    "employment_occupation_age": (
        "Quarterly employed persons and hours actually worked in all jobs for "
        "Australia by ANZSCO 2013 occupation major group of the main job and "
        "five-year age group."
    ),
    "employment_occupation_status": (
        "Quarterly employed persons and hours actually worked in all jobs for "
        "Australia by ANZSCO 2013 occupation major group of the main job and "
        "status in employment."
    ),
    "employment_status_hours": (
        "Quarterly employed persons and hours actually worked in all jobs for "
        "Australia by status in employment of the main job, band of hours "
        "actually worked in all jobs and sex."
    ),
    "employment_job_tenure": (
        "Quarterly employed persons and hours actually worked in all jobs by "
        "time spent with the current employer or in the current business, "
        "state and territory and sex."
    ),
    "unemployment_duration": (
        "Monthly unemployed persons and weeks spent searching for a job by "
        "duration of job search, broken down by state and territory or, for "
        "Australia, by age group."
    ),
}

MARKER = f"  - name: {DATASET}__employment_industry\n"


def wrap(text: str, width: int, indent: str) -> list[str]:
    """Fold a description onto YAML block-scalar continuation lines."""
    out, line = [], ""
    for word in text.split():
        candidate = f"{line} {word}".strip()
        if len(indent) + len(candidate) > width and line:
            out.append(indent + line)
            line = word
        else:
            line = candidate
    if line:
        out.append(indent + line)
    return out


def sql(table: str) -> str:
    cols = TABLES[table]
    casts = ",\n".join(
        f"    safe_cast({field(table, k, 'name')} as "
        f"{field(table, k, 'bigquery_type').lower()}) {field(table, k, 'name')}"
        for k in cols
    )
    cluster = ", ".join(f'"{c}"' for c in CLUSTER[table])
    return f"""{{{{
    config(
        schema="{DATASET}",
        alias="{table}",
        materialized="table",
        partition_by={{
            "field": "year",
            "data_type": "int64",
            "range": {{
                "start": {PARTITION_START},
                "end": {PARTITION_END},
                "interval": 1,
            }},
        }},
        cluster_by=[{cluster}],
    )
}}}}

select
{casts}
from {{{{ set_datalake_project("{DATASET}_staging.{table}") }}}} as t
"""


def schema_entry(table: str) -> str:
    names = [field(table, k, "name") for k in TABLES[table]]
    not_null = set(GRAIN[table]) | {"year", "month"}
    if "quarter" in names:
        not_null.add("quarter")

    lines = [f"  - name: {DATASET}__{table}", "    description: >-"]
    lines += wrap(DESCRIPTION[table], 79, "      ")
    lines += [
        "    tests:",
        "      - dbt_utils.unique_combination_of_columns:",
        "          combination_of_columns:",
    ]
    lines += [f"            - {c}" for c in GRAIN[table]]
    lines += [
        "      - not_null_proportion_multiple_columns:",
        "          at_least: 0.05",
        "    columns:",
    ]
    for key in TABLES[table]:
        name = field(table, key, "name")
        lines.append(f"      - name: {name}")
        lines.append("        description: >-")
        lines += wrap(field(table, key, "description"), 79, "          ")
        tests = []
        if name in not_null:
            tests.append("          - not_null")
        if name == "year":
            tests += [
                "          - relationships:",
                "              to: ref('br_bd_diretorios_data_tempo__ano')",
                "              field: ano.ano",
            ]
        if name == "month":
            tests += [
                "          - relationships:",
                "              to: ref('br_bd_diretorios_data_tempo__mes')",
                "              field: mes.mes",
            ]
        if tests:
            lines.append("        tests:")
            lines += tests
    return "\n".join(lines) + "\n"


def main() -> None:
    for table in TABLES:
        path = ROOT / f"{DATASET}__{table}.sql"
        path.write_text(sql(table))
        print(f"wrote {path.name}")

    schema = ROOT / "schema.yml"
    text = schema.read_text()
    if MARKER in text:
        raise SystemExit(
            "schema.yml already carries the Detailed entries; remove them "
            "before regenerating so the four Tier-1 entries are not disturbed."
        )
    if not text.endswith("\n"):
        text += "\n"
    schema.write_text(text + "".join(schema_entry(t) for t in TABLES))
    print(f"appended {len(TABLES)} entries to {schema}")


if __name__ == "__main__":
    main()
