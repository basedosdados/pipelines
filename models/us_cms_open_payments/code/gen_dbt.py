"""Generate the dbt models and schema.yml for every table.

Reads profile.json so the null-proportion test can name the columns that are
sparse by design rather than being weakened across the board.

    uv run python gen_dbt.py
"""

import json
from pathlib import Path

import constants as c
import descriptions
import grains
import layout
import profile_data
import schema
from table_descriptions import TABLE_DESCRIPTIONS

MODELS_DIR = Path(__file__).resolve().parents[1]
DATASET = c.GCP_DATASET_ID


def _profile() -> dict:
    if profile_data.PROFILE_PATH.exists():
        with open(profile_data.PROFILE_PATH) as fh:
            return json.load(fh)
    return {}


def sparse_columns(table: str, prof: dict) -> list[str]:
    shares = prof.get(table, {}).get("non_null_share", {})
    return sorted(
        col
        for col, share in shares.items()
        if share < profile_data.SPARSE_THRESHOLD
    )


def model_sql(table: str) -> str:
    columns = layout.LAYOUT[table]
    config = [
        f'        schema="{DATASET}",',
        f'        alias="{table}",',
        '        materialized="table",',
    ]
    years = layout.COVERAGE.get(table)
    if table not in layout.UNPARTITIONED and years:
        config.append(
            "        partition_by={\n"
            '            "field": "year",\n'
            '            "data_type": "int64",\n'
            f'            "range": {{"start": {years[0]}, "end": {years[-1] + 5}, "interval": 1}},\n'
            "        },"
        )
    if table in grains.CLUSTER:
        listed = ", ".join(f'"{col}"' for col in grains.CLUSTER[table])
        config.append(f"        cluster_by=[{listed}],")

    casts = ",\n".join(
        f"    safe_cast({col} as {schema.bigquery_type(table, col).lower()}) {col}"
        for col in columns
    )
    body = "\n".join(config)
    return (
        "{{\n    config(\n"
        f"{body}\n"
        "    )\n}}\n\n\n"
        f"select\n{casts}\n"
        f'from {{{{ set_datalake_project("{DATASET}_staging.{table}") }}}} as t\n'
    )


def _yaml_block(text: str, indent: int) -> str:
    pad = " " * indent
    words, lines, current = text.split(), [], ""
    for word in words:
        if len(current) + len(word) + 1 > 86:
            lines.append(current)
            current = word
        else:
            current = f"{current} {word}".strip()
    lines.append(current)
    return "\n".join(pad + line for line in lines)


def schema_yaml(prof: dict) -> str:
    out = ["---", "version: 2", "models:"]
    for table in layout.LAYOUT:
        grain = grains.GRAIN[table]
        sparse = sparse_columns(table, prof)
        dictionary_columns = [
            col
            for col in layout.LAYOUT[table]
            if schema.covered_by_dictionary(table, col) == "yes"
            and col in prof.get(table, {}).get("values", {})
        ]

        out.append(f"  - name: {DATASET}__{table}")
        out.append("    description: >-")
        out.append(_yaml_block(TABLE_DESCRIPTIONS[table][0], 6))
        out.append("    tests:")
        out.append("      - dbt_utils.unique_combination_of_columns:")
        out.append(f"          combination_of_columns: [{', '.join(grain)}]")
        out.append("      - not_null_proportion_multiple_columns:")
        out.append("          at_least: 0.05")
        if sparse:
            out.append("          ignore_values:")
            out.extend(f"            - {col}" for col in sparse)
        if dictionary_columns:
            out.append("      - custom_dictionary_coverage:")
            out.append("          columns_covered_by_dictionary:")
            out.extend(f"            - {col}" for col in dictionary_columns)
            out.append(
                f"          dictionary_model: ref('{DATASET}__dicionario')"
            )

        out.append("    columns:")
        for col in layout.LAYOUT[table]:
            pt, _, _ = descriptions.describe(table, col)
            out.append(f"      - name: {col}")
            out.append("        description: >-")
            out.append(_yaml_block(pt, 10))
            tests = []
            if col in grain:
                tests.append("not_null")
            directory = schema.directory_column(table, col)
            if tests or directory == schema.TIME_YEAR:
                out.append("        tests:")
                for test in tests:
                    out.append(f"          - {test}")
                if directory == schema.TIME_YEAR:
                    out.append("          - relationships:")
                    out.append(
                        "              to: ref('br_bd_diretorios_data_tempo__ano')"
                    )
                    out.append("              field: ano.ano")
                elif directory == schema.US_STATE:
                    out.append("          - relationships:")
                    out.append(
                        "              to: ref('br_bd_diretorios_us__state')"
                    )
                    out.append("              field: abbreviation")
    return "\n".join(out) + "\n"


def main() -> None:
    prof = _profile()
    if not prof:
        print(
            "profile.json missing -- run profile_data.py first for sparse-column exemptions"
        )
    for table in layout.LAYOUT:
        path = MODELS_DIR / f"{DATASET}__{table}.sql"
        path.write_text(model_sql(table))
    (MODELS_DIR / "schema.yml").write_text(schema_yaml(prof))
    print(f"wrote {len(layout.LAYOUT)} models and schema.yml to {MODELS_DIR}")


if __name__ == "__main__":
    main()
