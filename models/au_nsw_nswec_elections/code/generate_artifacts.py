"""Generate the architecture CSVs, the dbt models and schema.yml from the schema.

``pipelines/datasets/au_nsw_nswec_elections/schema.py`` is the single source of truth.
Everything downstream is generated so the three can never drift: the architecture
tables, the ``.sql`` model per table, and the one ``schema.yml`` for the dataset.

Usage::

    PYTHONPATH=. python models/au_nsw_nswec_elections/code/generate_artifacts.py
"""

from __future__ import annotations

import csv
import pathlib

from pipelines.datasets.au_nsw_nswec_elections.schema import (
    PARTITION_COLUMNS,
    PARTITION_RANGE,
    TABLE_META,
    TABLES,
)

DATASET = "au_nsw_nswec_elections"
ROOT = pathlib.Path(__file__).resolve().parents[3]
MODEL_DIR = ROOT / "models" / DATASET
ARCH_DIR = MODEL_DIR / "code" / "architecture"

ARCHITECTURE_HEADER = [
    "name",
    "bigquery_type",
    "description",
    "temporal_coverage",
    "covered_by_dictionary",
    "directory_column",
    "measurement_unit",
    "has_sensitive_data",
    "observations",
    "original_name",
]

# Directory foreign keys, written as the dbt ref the relationships test needs.
DIRECTORY_REFS = {
    "br_bd_diretorios_data_tempo.ano:ano": (
        "br_bd_diretorios_data_tempo__ano",
        "ano.ano",
    ),
    "br_bd_diretorios_au.state_electoral_division_2021:"
    "id_state_electoral_division": (
        "br_bd_diretorios_au__state_electoral_division_2021",
        "id_state_electoral_division",
    ),
}


def write_architecture() -> None:
    ARCH_DIR.mkdir(parents=True, exist_ok=True)
    for table, columns in TABLES.items():
        with open(
            ARCH_DIR / f"{table}.csv", "w", newline="", encoding="utf-8"
        ) as fh:
            writer = csv.writer(fh, lineterminator="\n")
            writer.writerow(ARCHITECTURE_HEADER)
            for column in columns:
                writer.writerow(
                    [
                        column.name,
                        column.bigquery_type,
                        column.description,
                        column.temporal_coverage,
                        column.covered_by_dictionary,
                        column.directory_column,
                        column.measurement_unit,
                        column.has_sensitive_data,
                        column.observations,
                        column.original_name,
                    ]
                )


def cast(name: str, bigquery_type: str) -> str:
    return f"safe_cast({name} as {bigquery_type.lower()}) {name}"


def write_models() -> None:
    for table, columns in TABLES.items():
        partitions = PARTITION_COLUMNS[table]
        config = [
            f'        schema="{DATASET}",',
            f'        alias="{table}",',
            '        materialized="table",',
        ]
        if partitions:
            config.append(
                "        partition_by={\n"
                f'            "field": "{partitions[0]}",\n'
                '            "data_type": "int64",\n'
                '            "range": {'
                f'"start": {PARTITION_RANGE["start"]}, '
                f'"end": {PARTITION_RANGE["end"]}, '
                f'"interval": {PARTITION_RANGE["interval"]}'
                "},\n        },"
            )
        cluster = CLUSTER_BY.get(table)
        if cluster:
            config.append(f"        cluster_by={cluster!r},".replace("'", '"'))
        body = ",\n".join(
            f"    {cast(c.name, c.bigquery_type)}" for c in columns
        )
        sql = (
            "{{\n    config(\n"
            + "\n".join(config)
            + "\n    )\n}}\n\n\nselect\n"
            + body
            + "\nfrom\n    {{ set_datalake_project("
            + f'"{DATASET}_staging.{table}"'
            + ") }}\n    as t\n"
        )
        (MODEL_DIR / f"{DATASET}__{table}.sql").write_text(
            sql, encoding="utf-8"
        )


# Clustering earns its keep only on the tables large enough for a scan to cost
# something. ballot_preference is 33.7 million rows and is almost always read one
# district at a time.
CLUSTER_BY = {
    "ballot_preference": ["contest_id", "voting_centre_name"],
    "result_voting_centre": ["contest_id", "voting_centre_name"],
}


def block(text: str, indent: int) -> str:
    pad = " " * indent
    words, lines, current = text.split(), [], ""
    for word in words:
        if len(current) + len(word) + 1 > 78 - indent:
            lines.append(current)
            current = word
        else:
            current = f"{current} {word}".strip()
    lines.append(current)
    return "\n".join(pad + line for line in lines)


def write_schema_yml() -> None:
    out = ["---", "version: 2", "models:"]
    for table, columns in TABLES.items():
        meta = TABLE_META[table]
        out.append(f"  - name: {DATASET}__{table}")
        out.append("    description: >-")
        out.append(block(meta.description_en, 6))
        tests = []
        if meta.unique_key:
            tests.append("      - dbt_utils.unique_combination_of_columns:")
            tests.append("          combination_of_columns:")
            tests.extend(f"            - {c}" for c in meta.unique_key)
        tests.append("      - not_null_proportion_multiple_columns:")
        tests.append("          at_least: 0.05")
        if meta.ignore_null_proportion:
            tests.append("          ignore_values:")
            tests.extend(
                f"            - {c}"
                for c in sorted(set(meta.ignore_null_proportion))
            )
        out.append("    tests:")
        out.extend(tests)
        out.append("    columns:")
        for column in columns:
            out.append(f"      - name: {column.name}")
            out.append("        description: >-")
            out.append(block(column.description_en, 10))
            column_tests = []
            not_null = (
                column.name in meta.unique_key
                and column.name not in meta.nullable_key
            )
            if not_null:
                column_tests.append("          - not_null")
            ref = DIRECTORY_REFS.get(column.directory_column)
            if ref:
                column_tests.append("          - relationships:")
                column_tests.append(f"              to: ref('{ref[0]}')")
                column_tests.append(f"              field: {ref[1]}")
            if column_tests:
                out.append("        tests:")
                out.extend(column_tests)
    (MODEL_DIR / "schema.yml").write_text(
        "\n".join(out) + "\n", encoding="utf-8"
    )


def main() -> int:
    write_architecture()
    write_models()
    write_schema_yml()
    print(
        f"wrote {len(TABLES)} architecture CSVs, {len(TABLES)} models and schema.yml"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
