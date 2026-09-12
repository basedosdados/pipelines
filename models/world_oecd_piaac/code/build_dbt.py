"""Generate the world_oecd_piaac dbt models and schema.yml from the architecture.

Usage:
    uv run python models/world_oecd_piaac/code/build_dbt.py

Generating rather than hand-writing matters here: the respondent models carry 612
and 788 safe_cast lines each, and they have to stay in exact step with the
architecture CSVs and the cleaning transform.
"""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).parent))

import architecture as arch

CODE_DIR = Path(__file__).parent
MODEL_DIR = CODE_DIR.parent
ARCHITECTURE_DIR = CODE_DIR / "architecture"
DATASET = arch.DATASET_ID

PARTITIONED = {
    "respondent_cycle_1",
    "respondent_cycle_2",
    "respondent_cycle_1_usa_national",
    "item_response_cycle_1",
    "item_response_cycle_2",
}
ITEM_TABLES = {"item_response_cycle_1", "item_response_cycle_2"}

GRAIN_UNIQUE = {
    "respondent_cycle_1": ["year", "country_id_iso_3", "respondent_id"],
    "respondent_cycle_2": ["year", "country_id_iso_3", "respondent_id"],
    "respondent_cycle_1_usa_national": [
        "year",
        "country_id_iso_3",
        "respondent_id",
    ],
    "item_response_cycle_1": [
        "year",
        "country_id_iso_3",
        "respondent_id",
        "item_code",
    ],
    "item_response_cycle_2": [
        "year",
        "country_id_iso_3",
        "respondent_id",
        "item_code",
    ],
    "variable": ["cycle", "variable_name"],
    "dictionary": ["table_id", "column_name", "key"],
}

# Some source values arrive space-padded -- isco1_fath carries 4,370 values like
# "  7" -- and padding is never meaningful in a code, so string columns are
# trimmed. nullif keeps a value that was nothing but spaces as NULL rather than
# turning it into an empty string.
CAST = {
    "STRING": "nullif(nullif(trim(safe_cast({c} as string)), ''), '.') {c}",
    "INT64": "safe_cast({c} as int64) {c}",
    "FLOAT64": "safe_cast({c} as float64) {c}",
}


def read_architecture(slug: str) -> list[dict]:
    with (ARCHITECTURE_DIR / f"{slug}.csv").open(encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_model(slug: str, columns: list[dict]) -> None:
    if slug in PARTITIONED:
        cluster = ["country_id_iso_3"] + (
            ["item_code"] if slug in ITEM_TABLES else []
        )
        config = (
            f'        alias="{slug}",\n'
            f'        schema="{DATASET}",\n'
            '        materialized="table",\n'
            "        partition_by={\n"
            '            "field": "year",\n'
            '            "data_type": "int64",\n'
            '            "range": {"start": 2012, "end": 2028, "interval": 1},\n'
            "        },\n"
            f"        cluster_by={json.dumps(cluster)},\n"
        )
    else:
        config = (
            f'        alias="{slug}",\n'
            f'        schema="{DATASET}",\n'
            '        materialized="table",\n'
        )

    casts = ",\n".join(
        "    " + CAST[c["bigquery_type"]].format(c=c["name"]) for c in columns
    )
    sql = (
        "{{\n    config(\n"
        + config
        + "    )\n}}\n\n\nselect\n"
        + casts
        + "\nfrom "
        + '{{ set_datalake_project("'
        + f"{DATASET}_staging.{slug}"
        + '") }} as t\n'
    )
    (MODEL_DIR / f"{DATASET}__{slug}.sql").write_text(sql, encoding="utf-8")


def build_schema(sparse: dict) -> dict:
    models = []
    for slug in GRAIN_UNIQUE:
        columns = read_architecture(slug)
        tests: list = [
            {
                "dbt_utils.unique_combination_of_columns": {
                    "combination_of_columns": GRAIN_UNIQUE[slug]
                }
            }
        ]

        ignore = sparse.get(slug, [])
        proportion: dict = {"at_least": 0.05}
        if ignore:
            proportion["ignore_values"] = ignore
        tests.append({"not_null_proportion_multiple_columns": proportion})

        covered = [
            c["name"] for c in columns if c["covered_by_dictionary"] == "yes"
        ]
        if covered:
            tests.append(
                {
                    "custom_dictionary_coverage_eng": {
                        "dictionary_model": f"ref('{DATASET}__dictionary')",
                        "columns_covered_by_dictionary": covered,
                    }
                }
            )

        column_entries = []
        for column in columns:
            entry: dict = {
                "name": column["name"],
                "description": column["description"],
            }
            column_tests: list = []
            if column["name"] in GRAIN_UNIQUE[slug]:
                column_tests.append("not_null")
            # Occupation and industry columns carry PIAAC's reserved codes
            # (9995-9999) in band alongside genuine ISCO and ISIC codes, so a
            # strict relationships test would fail on values that are answers,
            # not classification codes. The directory link is still declared in
            # the architecture; only the hard test is omitted.
            if column["directory_column"].endswith("pais:sigla_iso3"):
                column_tests.append(
                    {
                        "relationships": {
                            "to": "ref('br_bd_diretorios_mundo__pais')",
                            "field": "sigla_iso3",
                        }
                    }
                )
            elif column["directory_column"].endswith("pais:id_m49"):
                column_tests.append(
                    {
                        "relationships": {
                            "to": "ref('br_bd_diretorios_mundo__pais')",
                            "field": "id_m49",
                        }
                    }
                )
            if column_tests:
                entry["tests"] = column_tests
            column_entries.append(entry)

        models.append(
            {
                "name": f"{DATASET}__{slug}",
                "description": arch.TABLE_DESCRIPTIONS[slug],
                "tests": tests,
                "columns": column_entries,
            }
        )
    return {"version": 2, "models": models}


def main() -> None:
    sparse_path = CODE_DIR / "architecture" / "sparse_columns.json"
    sparse = (
        json.loads(sparse_path.read_text()) if sparse_path.exists() else {}
    )

    for slug in GRAIN_UNIQUE:
        columns = read_architecture(slug)
        write_model(slug, columns)
        print(f"  {DATASET}__{slug}.sql  ({len(columns)} columns)")

    schema = build_schema(sparse)
    with (MODEL_DIR / "schema.yml").open("w", encoding="utf-8") as handle:
        handle.write("---\n")
        yaml.safe_dump(
            schema, handle, sort_keys=False, allow_unicode=True, width=88
        )
    print(f"  schema.yml ({len(schema['models'])} models)")


if __name__ == "__main__":
    main()
