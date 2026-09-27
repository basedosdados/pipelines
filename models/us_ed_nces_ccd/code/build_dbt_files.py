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
    # A district appears once per fiscal year, but 1,988 F-33 records carry no
    # NCES LEAID -- Census-reported education agencies NCES never matched --
    # and are distinguished only by the Census government id.
    "district_finance": ["year", "agency_id", "census_id"],
    "dicionario": ["id_tabela", "nome_coluna", "chave"],
}

#: Rows the uniqueness test cannot speak for, and why.
UNIQUE_WHERE = {
    # Four rows (2014 and 2016) carry neither identifier and so cannot be told
    # apart at all. They are excluded rather than left to fail forever.
    "district_finance": "agency_id is not null or census_id is not null",
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

#: Columns the non-null proportion test cannot speak for.
#:
#: Two kinds, and the difference matters. Some are sparse across the whole
#: panel. Others are **discontinued series**: richly populated historically,
#: then reported no longer, so they are empty in the single year the test
#: scopes itself to. The last year carrying data is noted against each,
#: measured from the loaded table. A column listed here should never be empty
#: everywhere -- if one ever is, that is a real defect this exclusion hides.
#: Checked at load: no column of any table is empty across the whole panel.
#: Regenerate the list with code/check_discontinued.py after a refresh.
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
        # discontinued; last year with data in the comment
        "bureau_indian_education",  # 2015
        "title_i_status",  # 2021
        "title_i_eligible",  # 2021
        "magnet",  # 2021
    ],
    "school_district": [
        "cmsa_id",
        "necta_id",
        "supervisory_union_number",
        "csa_id",
        "agency_charter_indicator",  # 2015
        "bureau_indian_education",  # 2015
        "spec_ed_students",  # 2021
        "english_language_learners",  # 2021
        "migrant_students",  # 2007
    ],
    "district_finance": [
        "census_id",
        # the ARRA stimulus ended; these Title programmes were discontinued
        "rev_fed_arra",  # 2013
        "exp_current_arra",  # 2013
        "outlay_capital_arra",  # 2013
        "rev_fed_state_math_sci_teach",  # 2018
        "rev_fed_state_drug_free",  # 2018
        "rev_cares_act_relief_serv",  # 2019
        "rev_cares_act_relief_esf_rwp",  # 2019
    ],
    # No CCD code set is time-limited, so the column is empty by construction.
    "dicionario": ["cobertura_temporal"],
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
    unique = [
        "      - dbt_utils.unique_combination_of_columns:",
        f"          combination_of_columns: {json.dumps(key)}",
    ]
    if table.slug in UNIQUE_WHERE:
        unique.append("          config:")
        unique.append(f'            where: "{UNIQUE_WHERE[table.slug]}"')
    tests.append("\n".join(unique))
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
        prop.append("            where: __most_recent_year_en__")
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
