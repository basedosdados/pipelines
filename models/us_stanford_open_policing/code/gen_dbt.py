"""Render the dbt models and schema.yml for us_stanford_open_policing.

Generated from ``architecture.py``; edit that, not the outputs.

Usage:
    python models/us_stanford_open_policing/code/gen_dbt.py
"""

import textwrap

try:
    from models.us_stanford_open_policing.code import architecture as arch
    from models.us_stanford_open_policing.code import constants
except ModuleNotFoundError:  # run as a script
    import architecture as arch  # type: ignore[no-redef]
    import constants  # type: ignore[no-redef]

DS = constants.DATASET_ID
MODEL_DIR = constants.CODE_DIR.parent

# Earliest stop is 1971 (Tennessee statewide); latest 2020. End is latest + 5.
PARTITION_RANGE = (1971, 2025)

TABLE_DESCRIPTIONS = {
    "stop": (
        "Police stops recorded by US state patrols and city police departments, "
        "standardized by the Stanford Open Policing Project, one row per stop. "
        "Each jurisdiction fills a different subset of the columns."
    ),
    "jurisdiction": (
        "Jurisdictions covered by the Stanford Open Policing Project, one row per "
        "source file: a state patrol or a city police department."
    ),
}

# Model-level tests. The stop table is a frozen ~170M-row load, so the
# expensive tests are scoped to recent years.
MODEL_TESTS = {
    "stop": textwrap.dedent(
        """\
        - dbt_utils.unique_combination_of_columns:
            combination_of_columns: [jurisdiction_id, raw_row_number]
            config:
              where: year >= 2016
        - not_null_proportion_multiple_columns:
            at_least: 0.05
            ignore_values: {ignore}
            config:
              where: year >= 2016"""
    ),
    "jurisdiction": textwrap.dedent(
        """\
        - dbt_utils.unique_combination_of_columns:
            combination_of_columns: [jurisdiction_id]
        - not_null_proportion_multiple_columns:
            at_least: 0.05"""
    ),
}

NOT_NULL = {
    "stop": {"state_abbreviation", "state_id", "jurisdiction_id"},
    "jurisdiction": {"jurisdiction_id", "state_id", "jurisdiction_name"},
}

RELATIONSHIPS = {
    "state_id": ("ref('br_bd_diretorios_us__state')", "id_state"),
}
STOP_RELATIONSHIPS = {
    "jurisdiction_id": (f"ref('{DS}__jurisdiction')", "jurisdiction_id"),
}

# Columns filled by fewer than 5% of stops since 2016, measured on the cleaned
# Parquet with measure_sparsity.py. Sparse by construction, not by error.
SPARSE_STOP_COLUMNS: list[str] = [
    "neighborhood",
    "division",
    "subdistrict",
    "subdivision",
    "police_grid_number",
    "reporting_area",
    "sector",
    "subsector",
    "substation",
    "service_area",
    "zone",
    "officer_age",
    "officer_race",
    "officer_years_of_service",
    "officer_assignment",
    "contraband_found",
    "contraband_drugs",
    "contraband_weapons",
    "contraband_alcohol",
    "contraband_other",
    "search_basis",
    "reason_for_arrest",
    "reason_for_frisk",
    "reason_for_search",
    "speed",
    "posted_speed",
    "charged_speed",
    "use_of_force_description",
    "use_of_force_reason",
    "notes",
]


def _cast(name: str, typ: str) -> str:
    return f"    safe_cast({name} as {typ.lower()}) {name},"


def render_sql(table: str) -> str:
    cols = arch.TABLES[table]
    partition = ""
    if table == "stop":
        lo, hi = PARTITION_RANGE
        partition = textwrap.indent(
            textwrap.dedent(
                f"""\
                partition_by={{
                    "field": "year",
                    "data_type": "int64",
                    "range": {{"start": {lo}, "end": {hi}, "interval": 1}},
                }},
                cluster_by=["state_abbreviation", "jurisdiction_id"],
                """
            ),
            " " * 8,
        )
    body = "\n".join(_cast(c[0], c[1]) for c in cols).rstrip(",")
    return (
        "{{\n"
        "    config(\n"
        f'        alias="{table}",\n'
        f'        schema="{DS}",\n'
        '        materialized="table",\n'
        f"{partition}"
        "    )\n"
        "}}\n"
        "select\n"
        f"{body}\n"
        f'from {{{{ set_datalake_project("{DS}_staging.{table}") }}}} as t\n'
    )


def _block(text: str, indent: int) -> str:
    return textwrap.indent(text, " " * indent)


def render_schema() -> str:
    out = ["---", "version: 2", "models:"]
    for table, cols in arch.TABLES.items():
        out.append(f"  - name: {DS}__{table}")
        out.append("    description: >-")
        out.append(_block(textwrap.fill(TABLE_DESCRIPTIONS[table], 76), 6))
        out.append("    tests:")
        tests = MODEL_TESTS[table].format(
            ignore="[" + ", ".join(SPARSE_STOP_COLUMNS) + "]"
        )
        if table == "stop" and not SPARSE_STOP_COLUMNS:
            tests = "\n".join(
                line
                for line in tests.splitlines()
                if "ignore_values" not in line
            )
        out.append(_block(tests, 6))
        out.append("    columns:")
        for c in cols:
            name, en = c[0], c[5]
            out.append(f"      - name: {name}")
            out.append("        description: >-")
            out.append(_block(textwrap.fill(en, 72), 10))
            tests = []
            if name in NOT_NULL[table]:
                tests.append("not_null")
            rel = RELATIONSHIPS.get(name) or (
                STOP_RELATIONSHIPS.get(name) if table == "stop" else None
            )
            if rel:
                tests.append(rel)
            if not tests:
                continue
            out.append("        tests:")
            for t in tests:
                if t == "not_null":
                    out.append("          - not_null")
                else:
                    out.append("          - relationships:")
                    out.append(f"              to: {t[0]}")
                    out.append(f"              field: {t[1]}")
    return "\n".join(out) + "\n"


def main() -> None:
    for table in arch.TABLES:
        path = MODEL_DIR / f"{DS}__{table}.sql"
        path.write_text(render_sql(table))
        print(path.name)
    (MODEL_DIR / "schema.yml").write_text(render_schema())
    print("schema.yml")


if __name__ == "__main__":
    main()
