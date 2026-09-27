"""Generate the au_tas_tec_elections dbt models and schema.yml.

Both are derived from ``pipelines/datasets/au_tas_tec_elections/schema.py`` so the
architecture, the cleaning transform, the staging schema and the models cannot drift
apart. Regenerate rather than hand-editing the SQL.

Run:  PYTHONPATH=. ~/.venvs/bd-pipelines-tas/bin/python models/au_tas_tec_elections/code/build_dbt.py

The committed files are the post-hook form: ``sqlfmt`` and ``yamlfix`` rewrite this
script's output on commit (list flow style, line wrapping). Run pre-commit on the
generated files after regenerating, or the next commit will appear to change them.
"""

from __future__ import annotations

import textwrap

from pipelines.datasets.au_tas_tec_elections import schema
from pipelines.datasets.au_tas_tec_elections.constants import (
    REPO_ROOT,
    constants,
)

DATASET = constants.DATASET_ID.value
MODEL_DIR = REPO_ROOT / "models" / DATASET

DIRECTORY_REFS = {
    # Keys are the backend-slug FK strings stored in the architecture; values are the
    # dbt model that builds the directory and the field the relationships test binds to.
    #
    # ``diretorios_data_tempo.ano`` needs ``ano.ano`` rather than ``ano``: the model's
    # alias and its only key column share a name, so a bare ``ano`` resolves to the row
    # STRUCT and the test passes vacuously against every value.
    "diretorios_data_tempo.ano:ano": (
        "br_bd_diretorios_data_tempo__ano",
        "ano.ano",
    ),
    (
        "diretorios_au.state_electoral_division_2021"
        ":id_state_electoral_division"
    ): (
        "br_bd_diretorios_au__state_electoral_division_2021",
        "id_state_electoral_division",
    ),
    (
        "diretorios_au.commonwealth_electoral_division_2021"
        ":id_commonwealth_electoral_division"
    ): (
        "br_bd_diretorios_au__commonwealth_electoral_division_2021",
        "id_commonwealth_electoral_division",
    ),
}

CAST = {
    "STRING": "string",
    "INT64": "int64",
    "FLOAT64": "float64",
    "DATE": "date",
    "DATETIME": "datetime",
}


def sql_for(table: str) -> str:
    cols = schema.TABLES[table]
    partitioned = bool(schema.PARTITION_COLUMNS[table])
    rng = schema.PARTITION_RANGE

    config = [
        "{{",
        "    config(",
        f'        schema="{DATASET}",',
        f'        alias="{table}",',
        '        materialized="table",',
    ]
    if partitioned:
        config += [
            "        partition_by={",
            '            "field": "year",',
            '            "data_type": "int64",',
            '            "range": {'
            f'"start": {rng["start"]}, "end": {rng["end"]}, '
            f'"interval": {rng["interval"]}}},',
            "        },",
        ]
    config += ["    )", "}}", "", ""]

    selects = [
        f"    safe_cast({c.name} as {CAST[c.bigquery_type]}) {c.name},"
        for c in cols
    ]
    selects[-1] = selects[-1].rstrip(",")
    body = [
        "select",
        *selects,
        "from",
        f'    {{{{ set_datalake_project("{DATASET}_staging.{table}") }}}}',
        "    as t",
        "",
    ]
    return "\n".join(config + body)


def _fold(text: str, indent: str) -> str:
    """Emit a description as a ``>-`` folded scalar.

    ``>-`` and not ``>``: a plain ``>`` appends a trailing newline, which makes the
    BigQuery column description differ from the backend's by exactly that newline.
    """
    wrapped = textwrap.wrap(text, width=88 - len(indent))
    return "\n".join(indent + line for line in wrapped)


def yaml_for(table: str) -> list[str]:
    meta = schema.TABLE_META[table]
    cols = schema.TABLES[table]
    out = [
        f"  - name: {DATASET}__{table}",
        "    description: >-",
        _fold(meta.description_pt, "      "),
    ]

    tests = []
    if meta.unique_key:
        tests.append("      - dbt_utils.unique_combination_of_columns:")
        tests.append("          combination_of_columns:")
        tests += [f"            - {c}" for c in meta.unique_key]
    tests.append("      - not_null_proportion_multiple_columns:")
    tests.append("          at_least: 0.05")
    if meta.ignore_null_proportion:
        listed = ", ".join(meta.ignore_null_proportion)
        tests.append(f"          ignore_values: [{listed}]")
    out.append("    tests:")
    out += tests

    out.append("    columns:")
    for c in cols:
        out.append(f"      - name: {c.name}")
        out.append("        description: >-")
        out.append(_fold(c.description, "          "))
        col_tests = []
        is_partition = c.name in schema.PARTITION_COLUMNS[table]
        if is_partition and c.name not in meta.nullable_key:
            col_tests.append("          - not_null")
        if c.directory_column:
            ref, field = DIRECTORY_REFS[c.directory_column]
            col_tests.append("          - relationships:")
            col_tests.append(f"              to: ref('{ref}')")
            col_tests.append(f"              field: {field}")
        if col_tests:
            out.append("        tests:")
            out += col_tests
    return out


def main() -> None:
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    yaml_lines = ["---", "version: 2", "models:"]
    for table in constants.TABLES.value:
        path = MODEL_DIR / f"{DATASET}__{table}.sql"
        path.write_text(sql_for(table), encoding="utf-8")
        yaml_lines += yaml_for(table)
        print(f"{table:32s} -> {path.name}")
    schema_path = MODEL_DIR / "schema.yml"
    schema_path.write_text("\n".join(yaml_lines) + "\n", encoding="utf-8")
    print(f"\n{len(constants.TABLES.value)} models -> {schema_path}")


if __name__ == "__main__":
    main()
