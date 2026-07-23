#!/usr/bin/env python3
"""Generate dbt SQL models + schema.yml for us_census_acs from architecture CSVs.

Writes models/us_census_acs/us_census_acs__<table>.sql and schema.yml.
Partition: microdata + profiles by ano (int64); variables/dicionario unpartitioned.
"""

import csv
import os

HERE = os.path.dirname(__file__)
ARCH = os.path.join(HERE, "architecture")
MODELS = os.path.abspath(os.path.join(HERE, ".."))  # models/us_census_acs/
DATASET = "us_census_acs"

CAST = {
    "INT64": "int64",
    "FLOAT64": "float64",
    "STRING": "string",
    "DATE": "date",
}

# table -> (partition_start, partition_end) or None
PART = {
    "microdata_person": (2005, 2030),
    "microdata_household": (2005, 2030),
    **{
        f"data_profile_{lvl}": (2008, 2030)
        for lvl in [
            "nation",
            "state",
            "county",
            "place",
            "puma",
            "cbsa",
            "congressional_district",
            "zcta",
            "school_district",
        ]
    },
    "variables": None,
    "dicionario": None,
}
# logical key per table for unique_combination test
KEY = {
    "microdata_person": ["year", "period", "serialno", "sporder"],
    "microdata_household": ["year", "period", "serialno"],
    "variables": ["code"],
    "dicionario": ["id_tabela", "nome_coluna", "chave", "valor"],
}
# Output column -> staging source column, for the English renames done as
# dbt aliases (staging keeps original names -> no re-clean / re-upload).
STAGING_SRC = {"year": "ano"}  # all partitioned tables
STAGING_SRC_TABLE = {  # table-specific renames
    "data_profile_nation": {"id_country": "id_pais"},
    "variables": {"code": "variable_code"},
}
GEO_IDS = {  # profile geo id columns per level
    "nation": ["id_country"],
    "state": ["id_state"],
    "county": ["id_state", "id_county"],
    "place": ["id_state", "id_place"],
    "puma": ["id_state", "puma"],
    "cbsa": ["id_cbsa"],
    "congressional_district": ["id_state", "id_congressional_district"],
    "zcta": ["id_zcta"],
    "school_district": ["id_state", "id_school_district"],
}
# BQ clustering per profile level (partition is ano; clustering leads with geography
# so state-scoped queries prune, then variable_code for single-variable queries). Max 4.
CLUSTER = {lvl: (GEO_IDS[lvl] + ["variable_code"])[:4] for lvl in GEO_IDS}
CLUSTER["nation"] = [
    "variable_code"
]  # id_pais is constant 'US' — useless to cluster on


def cols_of(table):
    return [
        (r["name"], r["bigquery_type"])
        for r in csv.DictReader(open(f"{ARCH}/{table}.csv"))
    ]


def staging_src(table, name):
    """Staging (raw parquet) column name for an output column name."""
    return STAGING_SRC_TABLE.get(table, {}).get(name) or STAGING_SRC.get(
        name, name
    )


def key_of(table):
    if table in KEY:
        return KEY[table]
    lvl = table.replace("data_profile_", "")
    return ["year", "period"] + GEO_IDS[lvl] + ["variable_code"]


def gen_sql(table):
    part = PART[table]
    cfg = [
        f'        schema="{DATASET}"',
        f'        alias="{table}"',
        '        materialized="table"',
    ]
    if part:
        cfg.append(
            f'        partition_by={{"field": "year", "data_type": "int64", '
            f'"range": {{"start": {part[0]}, "end": {part[1]}, "interval": 1}}}}'
        )
    if table.startswith("data_profile_"):
        lvl = table.replace("data_profile_", "")
        cfg.append(
            "        cluster_by=["
            + ", ".join(f'"{c}"' for c in CLUSTER[lvl])
            + "]"
        )
    casts = ",\n".join(
        f"    safe_cast({staging_src(table, n)} as {CAST[t]}) {n}"
        for n, t in cols_of(table)
    )
    return (
        "{{\n    config(\n" + ",\n".join(cfg) + "\n    )\n}}\n\n"
        f'select\n{casts}\nfrom\n    {{{{ set_datalake_project("{DATASET}_staging.{table}") }}}}\n    as t\n'
    )


def gen_schema_entry(table):
    key = key_of(table)
    lines = [
        f"  - name: {DATASET}__{table}",
        "    description: >",
        f"      Tabela {table} do American Community Survey (ACS), Census Bureau dos EUA.",
        "    tests:",
        "      - dbt_utils.unique_combination_of_columns:",
        f"          combination_of_columns: [{', '.join(key)}]",
    ]
    # not_null_proportion is meaningless for the full-raw PUMS microdata: many
    # vintage/historical columns are intentionally populated only in some years
    # (< 5% non-null overall), and a 357-column query over 300M+ rows errors out.
    if not table.startswith("microdata_"):
        lines += [
            "      - not_null_proportion_multiple_columns:",
            "          at_least: 0.05",
        ]
    # not_null on partition + key columns
    nn = [
        c
        for c in (["year", "period"] if PART[table] else []) + key
        if c not in ("period",)
    ]
    seen = set()
    nn = [c for c in nn if not (c in seen or seen.add(c))]
    lines.append("    columns:")
    for c in nn:
        lines.append(f"      - name: {c}")
        lines.append("        tests: [not_null]")
    # relationships: variable_code -> variables (profiles only)
    if table.startswith("data_profile_"):
        lines.append("      - name: variable_code")
        lines.append("        tests:")
        lines.append("          - relationships:")
        lines.append(f"              to: ref('{DATASET}__variables')")
        lines.append("              field: code")
    return "\n".join(lines)


def main():
    tables = list(PART.keys())
    for t in tables:
        with open(f"{MODELS}/{DATASET}__{t}.sql", "w") as f:
            f.write(gen_sql(t))
    schema = (
        "---\nversion: 2\nmodels:\n"
        + "\n".join(gen_schema_entry(t) for t in tables)
        + "\n"
    )
    with open(f"{MODELS}/schema.yml", "w") as f:
        f.write(schema)
    print(f"wrote {len(tables)} SQL models + schema.yml to {MODELS}")
    print("dbt_project.yml entry needed:")
    print(f"  {DATASET}:\n    +materialized: table\n    +schema: {DATASET}")


if __name__ == "__main__":
    main()
