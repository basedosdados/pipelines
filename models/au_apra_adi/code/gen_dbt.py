#!/usr/bin/env python3
"""Generate dbt SQL models + schema.yml for au_apra_adi from the architecture CSVs.
Wide tables key on (year, quarter, institution_type); long tables add measure.
Columns whose non-null share falls below 8% (sparse across institution types) are
added to the not_null_proportion ignore list, computed from the cleaned output.

Usage:
    AU_APRA_ADI_DATA=~/Downloads/au_apra_adi_data uv run python models/au_apra_adi/code/gen_dbt.py
"""

import csv
import glob
import os
from pathlib import Path

import pyarrow.parquet as pq

from pipelines.datasets.au_apra_adi.constants import constants

DS = "au_apra_adi"
ROOT = Path(__file__).resolve().parents[1]
ARCH = ROOT / "code" / "architecture"
DATA = Path(
    os.environ.get(
        "AU_APRA_ADI_DATA", Path.home() / "Downloads" / "au_apra_adi_data"
    )
)
WIDE = constants.WIDE_TABLES.value
LONG = constants.LONG_TABLES.value

TDESC = {
    "financial_performance": "Quarterly financial performance of APRA-regulated authorised deposit-taking institutions (ADIs) by institution type, from the September 2004 quarter. One row per quarter and institution type, with interest, fee, expense and profit flows in millions of Australian dollars. Source: APRA Quarterly ADI Performance.",
    "financial_position": "Quarterly financial position (balance sheet) of APRA-regulated ADIs by institution type, from the September 2004 quarter. One row per quarter and institution type, with assets, liabilities and equity in millions of Australian dollars. Source: APRA Quarterly ADI Performance.",
    "performance_ratios": "Quarterly performance ratios of APRA-regulated ADIs by institution type, from the September 2004 quarter. One row per quarter and institution type, with profitability, efficiency, capital and liquidity ratios and their dollar inputs. Source: APRA Quarterly ADI Performance.",
    "capital_adequacy": "Quarterly capital adequacy of APRA-regulated ADIs by institution type, long format (one row per quarter, institution type and measure). Spans the pre-Basel III and Basel III regimes. Source: APRA Quarterly ADI Performance.",
    "asset_quality": "Quarterly asset quality of APRA-regulated ADIs by institution type, long format (one row per quarter, institution type and measure): impaired and past-due facilities, provisions and non-performing exposures. Source: APRA Quarterly ADI Performance.",
    "liquidity_lcr": "Quarterly Liquidity Coverage Ratio (LCR) reporting of APRA-regulated ADIs by institution type, long format (one row per quarter, institution type and measure). Source: APRA Quarterly ADI Performance.",
    "liquidity_mlh": "Quarterly Minimum Liquidity Holdings (MLH) reporting of APRA-regulated ADIs by institution type, long format (one row per quarter, institution type and measure). Source: APRA Quarterly ADI Performance.",
    "dicionario": "Dictionary mapping the coded columns (institution_type, and measure in the long tables) of the au_apra_adi tables to their labels.",
}


def arch(t):
    with open(ARCH / f"{t}.csv", newline="") as fh:
        return list(csv.DictReader(fh))


def sparse_columns(table, floor=0.08):
    fs = sorted(
        glob.glob(
            str(DATA / "output" / table / "**/*.parquet"), recursive=True
        )
    )
    if not fs:
        return []
    import pandas as pd

    df = pd.concat(
        [pq.ParquetFile(f).read().to_pandas() for f in fs], ignore_index=True
    )
    keys = {"year", "quarter", "institution_type"}
    out = []
    for c in df.columns:
        if c in keys:
            continue
        share = df[c].replace("", None).notna().mean()
        if share < floor:
            out.append(c)
    return out


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
            + '",\n        alias="dicionario",\n        materialized="table",\n    )\n}}'
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
            '            "range": {"start": 2004, "end": 2035, "interval": 1},\n        },\n'
            '        cluster_by=["institution_type"],\n    )\n}}'
        )
    body = f'{cfg}\n\n\nselect\n{casts}\nfrom {{{{ set_datalake_project("{DS}_staging.{table}") }}}} as t\n'
    (ROOT / f"{DS}__{table}.sql").write_text(body)
    print(f"wrote {DS}__{table}.sql ({len(cols)} cols)")


def schema_yml():
    lines = ["---", "version: 2", "models:"]
    for table in [*WIDE, *LONG]:
        cols = arch(table)
        is_long = table in LONG
        key = (
            "[year, quarter, institution_type, measure]"
            if is_long
            else "[year, quarter, institution_type]"
        )
        lines += [
            f"  - name: {DS}__{table}",
            "    description: >-",
            f"      {TDESC[table]}",
            "    tests:",
            "      - dbt_utils.unique_combination_of_columns:",
            f"          combination_of_columns: {key}",
            "      - not_null_proportion_multiple_columns:",
            "          at_least: 0.05",
        ]
        sparse = [] if is_long else sparse_columns(table)
        if sparse:
            lines.append("          ignore_values:")
            lines += [f"            - {c}" for c in sparse]
        lines.append("    columns:")
        notnull = {"year", "quarter", "institution_type"} | (
            {"measure"} if is_long else set()
        )
        for c in cols:
            lines.append(f"      - name: {c['name']}")
            lines.append("        description: >-")
            lines.append(
                f"          {c['description'].replace(chr(34), chr(39))}"
            )
            if c["name"] == "year":
                lines += [
                    "        tests:",
                    "          - not_null",
                    "          - relationships:",
                    "              to: ref('br_bd_diretorios_data_tempo__ano')",
                    "              field: ano.ano",
                ]
            elif c["name"] in notnull:
                lines.append("        tests: [not_null]")
    lines += [
        f"  - name: {DS}__dicionario",
        "    description: >-",
        f"      {TDESC['dicionario']}",
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
    for t in [*WIDE, *LONG, "dicionario"]:
        sql_model(t)
    schema_yml()
