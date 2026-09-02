#!/usr/bin/env python3
"""Generate the dbt models and schema.yml for us_ed_nces_ccd from schema.py.

Usage:
    uv run --no-project python models/us_ed_nces_ccd/code/build_dbt_files.py
"""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

# pyrefly: ignore [missing-import]
import schema

ROOT = Path(__file__).resolve().parent
MODELS = ROOT.parent
DATASET = schema.DATASET

#: Uniqueness key per table.
UNIQUE_KEY = {
    "school": ["year", "school_id"],
    "school_district": ["year", "agency_id"],
    "school_enrollment": ["year", "school_id", "grade", "race", "sex"],
    "staff": ["year", "agency_id", "staff_category"],
    "district_finance": ["year", "agency_id"],
    "dicionario": ["id_tabela", "nome_coluna", "chave"],
}

#: Columns that must never be null.
NOT_NULL = {
    "school": ["year", "school_id"],
    "school_district": ["year", "agency_id"],
    "school_enrollment": ["year", "school_id", "grade", "race", "sex"],
    "staff": ["year", "agency_id", "staff_category", "staff_fte"],
    "district_finance": ["year"],
    "dicionario": ["id_tabela", "nome_coluna", "chave", "valor"],
}

#: Tables wide or large enough that an unscoped column-wide test would scan the
#: whole table on every dbt run. `not_null_proportion_multiple_columns` compiles
#: a reference to every column, so on a 163-column or 400-million-row table it
#: is a full scan; scoping it to the most recent year keeps the bytes bounded.
SCOPE_TO_RECENT_YEAR = {
    "school",
    "school_district",
    "school_enrollment",
    "staff",
    "district_finance",
}

#: Columns that are legitimately empty over most of the panel and would drag the
#: 5% non-null floor below threshold.
IGNORE_IN_PROPORTION = {
    "school": [
        "direct_certification",
        "state_leg_district_lower",
        "state_leg_district_upper",
        "elem_cedp",
        "middle_cedp",
        "high_cedp",
        "ungrade_cedp",
        "title_i_schoolwide",
        "shared_time",
        "virtual",
        "csa_id",
    ],
    "school_district": [
        "cmsa_id",
        "necta_id",
        "supervisory_union_number",
        "csa_id",
    ],
    "district_finance": ["census_id"],
}

MODEL_TEMPLATE = """{{{{
    config(
        schema="{dataset}",
        alias="{alias}",
        materialized="table",{partition}{cluster}
    )
}}}}


select
{selects}
from {{{{ set_datalake_project("{dataset}_staging.{alias}") }}}} as t
"""


def _cast(col: schema.Col) -> str:
    t = col.type.lower()
    return f"    safe_cast({col.name} as {t}) {col.name},"


def write_model(table: schema.Table) -> None:
    partition = ""
    cluster = ""
    if table.partition:
        partition = (
            "\n        partition_by={\n"
            f'            "field": "{table.partition}",\n'
            '            "data_type": "int64",\n'
            f'            "range": {{"start": {table.year_min}, "end": {table.year_max + 6}, "interval": 1}},\n'
            "        },"
        )
    if table.cluster:
        cluster = "\n        cluster_by=" + json.dumps(table.cluster) + ","

    selects = "\n".join(_cast(c) for c in table.columns).rstrip(",")
    body = MODEL_TEMPLATE.format(
        dataset=DATASET,
        alias=table.slug,
        partition=partition,
        cluster=cluster,
        selects=selects,
    )
    path = MODELS / f"{DATASET}__{table.slug}.sql"
    path.write_text(body)
    print(f"  {path.name} ({len(table.columns)} columns)")


def _yaml_block(text: str, indent: int) -> str:
    """Fold a description into a `>-` block scalar at the given indent."""
    pad = " " * indent
    words, lines, cur = text.split(), [], ""
    for w in words:
        if len(cur) + len(w) + 1 > 88:
            lines.append(cur)
            cur = w
        else:
            cur = f"{cur} {w}".strip()
    if cur:
        lines.append(cur)
    return ">-\n" + "\n".join(pad + ln for ln in lines)


def model_schema(table: schema.Table) -> str:
    name = f"{DATASET}__{table.slug}"
    out = [f"  - name: {name}"]
    out.append("    description: " + _yaml_block(table.desc_en, 6))

    tests = []
    key = UNIQUE_KEY[table.slug]
    tests.append(
        "      - dbt_utils.unique_combination_of_columns:\n"
        f"          combination_of_columns: {json.dumps(key)}"
    )
    prop = [
        "      - not_null_proportion_multiple_columns:",
        "          at_least: 0.05",
    ]
    ignore = IGNORE_IN_PROPORTION.get(table.slug)
    if ignore:
        prop.append("          ignore_values:")
        prop += [f"            - {c}" for c in ignore]
    if table.slug in SCOPE_TO_RECENT_YEAR:
        prop.append("          config:")
        prop.append("            where: __most_recent_year__")
    tests.append("\n".join(prop))

    # `custom_dictionary_coverage` is a model-level test: it derives id_tabela
    # from the model alias and takes the coded columns as a list.
    coded = [c.name for c in table.columns if c.dictionary]
    if coded:
        block = [
            "      - custom_dictionary_coverage:",
            "          columns_covered_by_dictionary:",
        ]
        block += [f"            - {c}" for c in coded]
        block.append(
            f"          dictionary_model: ref('{DATASET}__dicionario')"
        )
        tests.append("\n".join(block))

    out.append("    tests:")
    out += tests

    out.append("    columns:")
    for c in table.columns:
        out.append(f"      - name: {c.name}")
        out.append("        description: " + _yaml_block(c.desc_en, 10))
        if c.name in NOT_NULL.get(table.slug, []):
            out.append("        tests: [not_null]")
    return "\n".join(out)


def main() -> None:
    header = next(
        csv.reader(
            (
                Path(
                    __import__("os").environ.get(
                        "CCD_DATA_DIR",
                        str(Path.home() / "Downloads" / "us_ed_nces_ccd_data"),
                    )
                )
                / "input"
                / "districts_ccd_finance.csv"
            ).open(encoding="utf-8")
        )
    )
    labels = {
        x["variable"]: x["label"]
        for x in json.loads((ROOT / "varlist_29.json").read_text())
    }
    tables = [
        *schema.STATIC_TABLES,
        schema.finance_table(header, labels),
        schema.TABLE_DICIONARIO,
    ]

    print("models:")
    for t in tables:
        write_model(t)

    print("schema.yml:")
    blocks = "\n".join(model_schema(t) for t in tables)
    (MODELS / "schema.yml").write_text(
        "---\nversion: 2\nmodels:\n" + blocks + "\n"
    )
    print(f"  {len(tables)} models described")


if __name__ == "__main__":
    main()
