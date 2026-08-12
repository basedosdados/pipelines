"""Generate dbt models (.sql) and schema.yml for us_cfpb_hmda from the architecture TSVs.

  uv run --with duckdb python gen_dbt.py

Emits, under models/us_cfpb_hmda/:
  us_cfpb_hmda__loan_application_register.sql
  us_cfpb_hmda__loan_application_register_legacy.sql
  us_cfpb_hmda__dicionario.sql
  schema.yml

Every column is safe_cast to its architecture type; the LAR tables partition by `year`.
HMDA has no natural primary key and is legitimately sparse (pricing/underwriting/denial
fields are populated only for a minority of records), so schema.yml uses not_null on
`year` plus not_null_proportion_multiple_columns(at_least=0.05) with a DATA-DRIVEN
ignore_values list: columns whose observed non-null share (from the cleaned parquet) is
below 5% are excluded so the test reflects real coverage rather than failing on design.
"""

import glob
from pathlib import Path

import duckdb
from common import LEGACY, MODERN, OUTPUT, SHEET, load_cols

MODEL_DIR = Path(__file__).resolve().parents[1]
SAFECAST = {"INT64": "int64", "FLOAT64": "float64", "STRING": "string"}
PART = {MODERN: (2018, 2029), LEGACY: (2007, 2022)}

DESС = {
    MODERN: (
        "Registro de empréstimo/solicitação (LAR) do HMDA, esquema moderno pós-2017, "
        "um registro por solicitação de crédito hipotecário reportada por instituição "
        "coberta, com termos do empréstimo, geografia e demografia do solicitante"
    ),
    LEGACY: (
        "Registro de empréstimo/solicitação (LAR) do HMDA, esquema legado 2007-2017, "
        "um registro por solicitação de crédito hipotecário, com esquema anterior à "
        "expansão do Dodd-Frank (sem LEI; chave respondent_id + agency_code)"
    ),
}


def sparse_cols(table: str, cols) -> list[str]:
    """Columns with observed non-null share < 5% across the cleaned parquet."""
    files = glob.glob(str(OUTPUT / table / "**" / "*.parquet"), recursive=True)
    if not files:
        return []
    con = duckdb.connect()
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET memory_limit='4GB'")
    rel = f"read_parquet({files!r}, union_by_name=true)"
    total = con.execute(f"select count(*) from {rel}").fetchone()[0]
    if not total:
        return []
    names = [c.name for c in cols if c.name != "year"]
    sel = ", ".join(f"count({n}) as {n}" for n in names)
    row = con.execute(f"select {sel} from {rel}").fetchdf().iloc[0]
    con.close()
    return [n for n in names if row[n] / total < 0.05]


def gen_lar(table: str) -> None:
    cols = load_cols(table)
    start, end = PART[table]
    lines = [c for c in cols if c.name != "year"]
    body = ",\n    ".join(
        ["safe_cast(year as int64) year"]
        + [
            f"safe_cast({c.name} as {SAFECAST[c.bq_type]}) {c.name}"
            for c in lines
        ]
    )
    sql = f'''{{{{
    config(
        schema="us_cfpb_hmda",
        alias="{table}",
        materialized="table",
        partition_by={{
            "field": "year",
            "data_type": "int64",
            "range": {{"start": {start}, "end": {end}, "interval": 1}},
        }},
    )
}}}}


select
    {body}
from
    {{{{ set_datalake_project("us_cfpb_hmda_staging.{table}") }}}}
    as t
'''
    (MODEL_DIR / f"us_cfpb_hmda__{table}.sql").write_text(
        sql, encoding="utf-8"
    )


def gen_dic() -> None:
    sql = """{{
    config(
        schema="us_cfpb_hmda",
        alias="dicionario",
        materialized="table",
    )
}}


select
    safe_cast(id_tabela as string) id_tabela,
    safe_cast(nome_coluna as string) nome_coluna,
    safe_cast(chave as string) chave,
    safe_cast(cobertura_temporal as string) cobertura_temporal,
    safe_cast(valor as string) valor
from
    {{ set_datalake_project("us_cfpb_hmda_staging.dicionario") }}
    as t
"""
    (MODEL_DIR / "us_cfpb_hmda__dicionario.sql").write_text(
        sql, encoding="utf-8"
    )


def gen_schema() -> None:
    import csv

    parts = ["---", "version: 2", "models:"]
    for table in (MODERN, LEGACY):
        cols = load_cols(table)
        ignore = sparse_cols(table, cols)
        print(
            f"  {table}: {len(ignore)} sparse col(s) ignored in proportion test"
        )
        parts.append(f"  - name: us_cfpb_hmda__{table}")
        parts.append(f"    description: >\n      {DESС[table]}")
        parts.append("    tests:")
        parts.append("      - not_null_proportion_multiple_columns:")
        parts.append("          at_least: 0.05")
        if ignore:
            parts.append("          ignore_values:")
            parts += [f"            - {c}" for c in ignore]
        parts.append("    columns:")
        descmap = {}
        with open(SHEET[table], encoding="utf-8") as fh:
            for r in csv.DictReader(fh, delimiter="\t"):
                descmap[r["name"].strip()] = r["description"].strip()
        for c in cols:
            parts.append(f"      - name: {c.name}")
            parts.append(
                f"        description: >\n          {descmap.get(c.name, c.name)}"
            )
            if c.name == "year":
                parts.append("        tests:")
                parts.append("          - not_null")
    # dicionario
    parts.append("  - name: us_cfpb_hmda__dicionario")
    parts.append(
        "    description: >\n      Dicionário de valores codificados das tabelas "
        "de LAR do HMDA (chave -> significado), por tabela e coluna"
    )
    parts.append("    columns:")
    for c in [
        "id_tabela",
        "nome_coluna",
        "chave",
        "cobertura_temporal",
        "valor",
    ]:
        parts.append(f"      - name: {c}")
    (MODEL_DIR / "schema.yml").write_text(
        "\n".join(parts) + "\n", encoding="utf-8"
    )


if __name__ == "__main__":
    gen_lar(MODERN)
    gen_lar(LEGACY)
    gen_dic()
    gen_schema()
    print("wrote 3 .sql models + schema.yml")
