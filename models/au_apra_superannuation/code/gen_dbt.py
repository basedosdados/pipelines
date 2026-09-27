#!/usr/bin/env python3
"""Generate dbt SQL models and schema.yml for au_apra_superannuation from the
architecture CSVs (the schema source of truth).

Usage:
    uv run python models/au_apra_superannuation/code/gen_dbt.py
"""

import csv
from pathlib import Path

DS = "au_apra_superannuation"
ROOT = Path(__file__).resolve().parents[1]  # models/au_apra_superannuation
ARCH = ROOT / "code" / "architecture"
DATA_TABLES = [
    "financial_performance",
    "financial_position",
    "performance_ratios",
]

TABLE_DESC = {
    "financial_performance": (
        "Quarterly financial performance of APRA-regulated superannuation funds "
        "by fund type, from the December 2004 quarter. One row per quarter and "
        "fund type, reporting contribution, benefit, investment and earnings "
        "flows in millions of Australian dollars, including the employer "
        "contribution split (defined benefit, Superannuation Guarantee and "
        "salary sacrifice). Source: APRA Quarterly Superannuation Performance."
    ),
    "financial_position": (
        "Quarterly financial position (balance sheet) of APRA-regulated "
        "superannuation funds by fund type, from the September 2004 quarter. One "
        "row per quarter and fund type, reporting assets, liabilities, member "
        "benefits and reserves in millions of Australian dollars. Source: APRA "
        "Quarterly Superannuation Performance."
    ),
    "performance_ratios": (
        "Quarterly performance ratios of APRA-regulated superannuation funds by "
        "fund type, from the December 2004 quarter. One row per quarter and fund "
        "type, reporting the quarterly and five-year annualised rate of return "
        "(as proportions) and their dollar inputs. Source: APRA Quarterly "
        "Superannuation Performance."
    ),
    "dicionario": (
        "Dictionary mapping the coded columns of au_apra_superannuation tables "
        "to their human-readable labels."
    ),
}


def arch(table):
    with open(ARCH / f"{table}.csv", newline="") as fh:
        return list(csv.DictReader(fh))


def sql_model(table):
    cols = arch(table)
    casts = ",\n".join(
        f"    safe_cast({c['name']} as {c['bigquery_type'].lower()}) {c['name']}"
        for c in cols
    )
    if table == "dicionario":
        cfg = (
            '{{\n    config(\n        schema="'
            + DS
            + '",\n        alias="dicionario",'
            '\n        materialized="table",\n    )\n}}'
        )
    else:
        cfg = (
            '{{\n    config(\n        schema="'
            + DS
            + '",\n        alias="'
            + table
            + '",'
            '\n        materialized="table",\n        partition_by={\n'
            '            "field": "year",\n            "data_type": "int64",\n'
            '            "range": {"start": 2004, "end": 2035, "interval": 1},\n'
            "        },\n"
            '        cluster_by=["fund_type"],\n    )\n}}'
        )
    body = (
        f"{cfg}\n\n\nselect\n{casts}\n"
        f'from {{{{ set_datalake_project("{DS}_staging.{table}") }}}} as t\n'
    )
    (ROOT / f"{DS}__{table}.sql").write_text(body)
    print(f"wrote {DS}__{table}.sql ({len(cols)} cols)")


def schema_yml():
    lines = ["---", "version: 2", "models:"]
    for table in DATA_TABLES:
        cols = arch(table)
        lines += [
            f"  - name: {DS}__{table}",
            "    description: >-",
        ]
        lines += [f"      {TABLE_DESC[table]}"]
        lines += [
            "    tests:",
            "      - dbt_utils.unique_combination_of_columns:",
            "          combination_of_columns: [year, quarter, fund_type]",
            "      - not_null_proportion_multiple_columns:",
            "          at_least: 0.05",
            "    columns:",
        ]
        for c in cols:
            lines.append(f"      - name: {c['name']}")
            desc = c["description"].replace('"', "'")
            lines.append("        description: >-")
            lines.append(f"          {desc}")
            if c["name"] == "year":
                lines += [
                    "        tests:",
                    "          - not_null",
                    "          - relationships:",
                    "              to: ref('br_bd_diretorios_data_tempo__ano')",
                    "              field: ano.ano",
                ]
            elif c["name"] in ("quarter", "fund_type"):
                lines.append("        tests: [not_null]")
    # dicionario
    lines += [
        f"  - name: {DS}__dicionario",
        "    description: >-",
        f"      {TABLE_DESC['dicionario']}",
        "    tests:",
        "      - dbt_utils.unique_combination_of_columns:",
        "          combination_of_columns: [id_tabela, nome_coluna, chave]",
        "    columns:",
    ]
    for c in arch("dicionario"):
        lines.append(f"      - name: {c['name']}")
        lines.append("        description: >-")
        lines.append(f"          {c['description']}")
        if c["name"] in ("id_tabela", "nome_coluna", "chave"):
            lines.append("        tests: [not_null]")
    (ROOT / "schema.yml").write_text("\n".join(lines) + "\n")
    print("wrote schema.yml")


if __name__ == "__main__":
    for t in [*DATA_TABLES, "dicionario"]:
        sql_model(t)
    schema_yml()
