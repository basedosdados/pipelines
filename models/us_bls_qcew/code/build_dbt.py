"""Generate the dbt models (SQL) and schema.yml for us_bls_qcew.

One model per table, reading its column list/types from the architecture CSVs
(the single source of truth). Every column is ``safe_cast`` to its architecture
type; tables partition by ``year`` and cluster by industry/ownership.

Directory ``relationships`` tests are emitted only for ``year`` (the
``br_bd_diretorios_data_tempo`` directory exists). Geography FKs point at
``br_bd_diretorios_us``, which is not yet in this branch, so no relationships
test is generated for ``area_fips``/``id_state`` (the ``directory_column`` in the
architecture records the intended FK for when that directory lands).

Run: uv run python models/us_bls_qcew/code/build_dbt.py
"""

import csv
from pathlib import Path

HERE = Path(__file__).resolve().parent
ARCH = HERE / "architecture"
MODELS = HERE.parent  # models/us_bls_qcew/

DATASET = "us_bls_qcew"
PART_START, PART_END = 1975, 2030
CLUSTER = ["industry_code", "own_code"]

SQL_TYPE = {"INT64": "int64", "FLOAT64": "float64", "STRING": "string"}


def arch(table):
    with open(ARCH / f"{table}.csv", newline="") as fh:
        return list(csv.DictReader(fh))


def data_tables():
    return [
        f"{c}_{f}_{g}"
        for c in ("naics", "sic")
        for f in ("quarterly", "annual")
        for g in ("national", "state", "county", "metro")
    ]


def write_sql(table):
    cols = arch(table)
    casts = ",\n    ".join(
        f"safe_cast({c['name']} as {SQL_TYPE[c['bigquery_type']]}) {c['name']}"
        for c in cols
    )
    cluster = ", ".join(f'"{c}"' for c in CLUSTER)
    sql = f'''{{{{
    config(
        schema="{DATASET}",
        alias="{table}",
        materialized="table",
        partition_by={{
            "field": "year",
            "data_type": "int64",
            "range": {{"start": {PART_START}, "end": {PART_END}, "interval": 1}},
        }},
        cluster_by=[{cluster}],
    )
}}}}


select
    {casts}
from {{{{ set_datalake_project("{DATASET}_staging.{table}") }}}} as t
'''
    (MODELS / f"{DATASET}__{table}.sql").write_text(sql)


def key_cols(table):
    freq_q = "quarterly" in table
    keys = ["year"] + (["qtr"] if freq_q else [])
    keys += [
        "area_fips",
        "own_code",
        "industry_code",
        "agglvl_code",
        "size_code",
    ]
    return keys


def yaml_desc(text):
    return f">-\n          {text}"


def write_schema():
    lines = ["---", "version: 2", "models:"]
    for table in data_tables():
        cols = arch(table)
        names = [c["name"] for c in cols]
        keys = [k for k in key_cols(table) if k in names]
        ignore = [
            c["name"]
            for c in cols
            if c["name"].startswith(("lq_", "oty_"))
            or c["name"].endswith("disclosure_code")
        ]
        cls = "NAICS" if table.startswith("naics") else "SIC"
        freq = "quarterly" if "quarterly" in table else "annual-average"
        geo = table.rsplit("_", 1)[1]
        lines.append(f"  - name: {DATASET}__{table}")
        lines.append(
            f"    description: >-\n      QCEW {cls} {freq} employment and wage totals at the {geo} "
            f"level, one row per area, ownership, industry, aggregation level, and size class."
        )
        lines.append("    tests:")
        lines.append("      - dbt_utils.unique_combination_of_columns:")
        lines.append(f"          combination_of_columns: [{', '.join(keys)}]")
        lines.append("      - not_null_proportion_multiple_columns:")
        lines.append("          at_least: 0.05")
        if ignore:
            lines.append("          ignore_values:")
            for c in ignore:
                lines.append(f"            - {c}")
        lines.append("    columns:")
        for c in cols:
            lines.append(f"      - name: {c['name']}")
            lines.append(f"        description: {yaml_desc(c['description'])}")
            tests = []
            if c["name"] == "year":
                lines.append("        tests:")
                lines.append("          - not_null")
                lines.append("          - relationships:")
                lines.append(
                    "              to: ref('br_bd_diretorios_data_tempo__ano')"
                )
                lines.append("              field: ano.ano")
                continue
            if c["name"] in keys:
                tests.append("not_null")
            if tests:
                lines.append(f"        tests: [{', '.join(tests)}]")
    # dicionario
    dcols = arch("dicionario")
    lines.append(f"  - name: {DATASET}__dicionario")
    lines.append(
        "    description: >-\n      Dictionary mapping the coded columns of us_bls_qcew "
        "(ownership, aggregation level, size class, industry, and area) to their labels."
    )
    lines.append("    tests:")
    lines.append("      - dbt_utils.unique_combination_of_columns:")
    lines.append(
        "          combination_of_columns: [id_tabela, nome_coluna, chave]"
    )
    lines.append("    columns:")
    for c in dcols:
        lines.append(f"      - name: {c['name']}")
        lines.append(f"        description: {yaml_desc(c['description'])}")
        if c["name"] in ("id_tabela", "nome_coluna", "chave"):
            lines.append("        tests: [not_null]")
    (MODELS / "schema.yml").write_text("\n".join(lines) + "\n")


def main():
    for table in data_tables():
        write_sql(table)
    # dicionario model
    dcols = arch("dicionario")
    casts = ",\n    ".join(
        f"safe_cast({c['name']} as string) {c['name']}" for c in dcols
    )
    (MODELS / f"{DATASET}__dicionario.sql").write_text(
        f'''{{{{
    config(
        schema="{DATASET}",
        alias="dicionario",
        materialized="table",
    )
}}}}


select
    {casts}
from {{{{ set_datalake_project("{DATASET}_staging.dicionario") }}}} as t
'''
    )
    write_schema()
    print(
        f"wrote {len(data_tables())} data models + dicionario + schema.yml to {MODELS}"
    )


if __name__ == "__main__":
    main()
