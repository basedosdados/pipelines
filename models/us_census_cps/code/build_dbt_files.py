#!/usr/bin/env python3
"""Generate the us_census_cps dbt models and schema.yml from the architecture CSVs.

The architecture is the source of truth: column order, names, types and
descriptions all come from it, so the models cannot drift from the sheets.

Two columns per table are *derived* rather than selected from staging:
`id_state` (joined from br_bd_diretorios_us.state on the CPS census state code)
and `id_county` (id_state concatenated with the 3-digit county code). Everything
else is a plain `safe_cast` off the staging table.

Usage: python3 build_dbt_files.py [--nonnull nonnull_years.json]
"""

import argparse
import csv
import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))
ARCH = os.path.join(HERE, "architecture")
MODELS = os.path.dirname(HERE)
DATASET = "us_census_cps"

TABLES = {
    "org": dict(
        arch="org.csv",
        start=1979,
        end=2019,
        rows=13_169_878,
        key=[
            "year",
            "month",
            "hhid",
            "hhid2",
            "hrsample",
            "hrsersuf",
            "hhnum",
            "lineno",
        ],
        strict_key=False,
        description=(
            "Extrato de grupos de rotação de saída (ORG) da Current Population Survey, "
            "reproduzido com os programas de harmonização do CEPR. Uma linha por pessoa "
            "de 16 anos ou mais entrevistada em um mês de rotação de saída (grupos 4 e 8), "
            "com rendimentos, horas e o peso de rendimentos (orgwgt). "
            "Os identificadores de domicílio não são únicos em toda a série: a chave "
            "(year, month, hhid, hhid2, hrsample, hrsersuf, hhnum, lineno) repete-se em "
            "0,21%-0,26% das linhas de 1979-1988 e em 8,6%/8,4% das de 1994/1995, quando "
            "os campos amostrais do CPS mudaram; a duplicidade vem da fonte e está "
            "presente também nos extratos publicados pelo CEPR."
        ),
    ),
    "basic_monthly": dict(
        arch="basic_monthly.csv",
        start=1994,
        end=2019,
        rows=31_922_804,
        key=[
            "year",
            "month",
            "hhid",
            "hhid2",
            "hrsample",
            "hrsersuf",
            "hhnum",
            "lineno",
        ],
        strict_key=False,
        description=(
            "Extrato mensal básico da Current Population Survey, reproduzido com os "
            "programas de harmonização do CEPR. Uma linha por pessoa de 16 anos ou mais "
            "entrevistada no mês, em todos os oito grupos de rotação, com situação na "
            "força de trabalho, características demográficas e o peso amostral básico. "
            "Contém as mesmas colunas do extrato org, exceto as 21 específicas de "
            "rendimentos da rotação de saída. "
            "Os identificadores de domicílio não são únicos em 1994 e 1995 (11,3% e 9,6% "
            "das linhas), quando os campos amostrais do CPS mudaram; a duplicidade vem "
            "da fonte."
        ),
    ),
    "march": dict(
        arch="march.csv",
        start=2014,
        end=2018,
        rows=950_065,
        key=["year", "hhseq", "pppos", "perno"],
        strict_key=True,
        description=(
            "Extrato do suplemento anual socioeconômico (ASEC/March) da Current "
            "Population Survey, reproduzido com os programas de harmonização do CEPR. "
            "Uma linha por pessoa, com renda por fonte, cobertura de saúde, pobreza, "
            "experiência de trabalho no ano anterior e migração. "
            "O ano de 2014 traz as duas amostras do redesenho: a tradicional de 5/8 "
            "(research = 0) e a de 3/8 (research = 1), que não se sobrepõem."
        ),
    ),
    "dictionary": dict(
        arch="dictionary.csv",
        start=None,
        end=None,
        rows=None,
        key=["id_tabela", "nome_coluna", "chave", "cobertura_temporal"],
        strict_key=True,
        description=(
            "Dicionário das colunas categóricas das tabelas org, basic_monthly e march. "
            "Uma linha por (tabela, coluna, chave, cobertura temporal), com o rótulo "
            "correspondente. Os rótulos vêm dos conjuntos de valores do CEPR quando "
            "existem; indicadores 0/1 e o mês na amostra são rotulados de forma "
            "explícita. Chaves cuja correspondência muda ao longo do tempo (esquemas de "
            "indústria e ocupação, por exemplo) aparecem em linhas distintas, cada uma "
            "com sua cobertura temporal."
        ),
    ),
}

# BigQuery reserved words that collide with CEPR variable names (`union`), which
# have to be backticked on both sides of the cast
RESERVED = set(
    [
        "all",
        "and",
        "any",
        "array",
        "as",
        "asc",
        "assert_rows_modified",
        "at",
        "between",
        "by",
        "case",
        "cast",
        "collate",
        "contains",
        "create",
        "cross",
        "cube",
        "current",
        "default",
        "define",
        "desc",
        "distinct",
        "else",
        "end",
        "enum",
        "escape",
        "except",
        "exclude",
        "exists",
        "extract",
        "false",
        "fetch",
        "following",
        "for",
        "from",
        "full",
        "group",
        "grouping",
        "groups",
        "hash",
        "having",
        "if",
        "ignore",
        "in",
        "inner",
        "intersect",
        "interval",
        "into",
        "is",
        "join",
        "lateral",
        "left",
        "like",
        "limit",
        "lookup",
        "merge",
        "natural",
        "new",
        "no",
        "not",
        "null",
        "nulls",
        "of",
        "on",
        "or",
        "order",
        "outer",
        "over",
        "partition",
        "preceding",
        "proto",
        "qualify",
        "range",
        "recursive",
        "respect",
        "right",
        "rollup",
        "rows",
        "select",
        "set",
        "some",
        "struct",
        "tablesample",
        "then",
        "to",
        "treat",
        "true",
        "unbounded",
        "union",
        "unnest",
        "using",
        "when",
        "where",
        "window",
        "with",
        "within",
    ]
)


def quote(name):
    return f"`{name}`" if name.lower() in RESERVED else name


# columns materialised by the model rather than read from staging
DERIVED = {
    "id_state": "  d.id_state id_state,",
    "id_county": (
        "  case\n"
        "    when safe_cast(t.fipscounty as int64) > 0\n"
        "    then concat(d.id_state, lpad(cast(safe_cast(t.fipscounty as int64) as string), 3, '0'))\n"
        "  end id_county,"
    ),
}

HEADER = """{{{{
    config(
        schema="{dataset}",
        alias="{table}",
        materialized="table",{partition}
    )
}}}}
"""

PARTITION = """
        partition_by={{
            "field": "year",
            "data_type": "int64",
            "range": {{"start": {start}, "end": {end}, "interval": 1}},
        }},"""


def load(arch):
    return list(csv.DictReader(open(os.path.join(ARCH, arch))))


def write_sql(table, spec):
    rows = load(spec["arch"])
    partition = (
        PARTITION.format(start=spec["start"], end=spec["end"] + 5)
        if spec["start"]
        else ""
    )
    out = [
        HEADER.format(dataset=DATASET, table=table, partition=partition),
        "",
    ]

    needs_dir = any(r["name"] == "id_state" for r in rows)
    select = []
    for r in rows:
        name, bqt = r["name"], r["bigquery_type"].lower()
        if name in DERIVED:
            select.append(DERIVED[name])
        else:
            select.append(
                f"  safe_cast(t.{quote(name)} as {bqt}) {quote(name)},"
            )
    select[-1] = select[-1].rstrip(",")

    out.append("select")
    out.extend(select)
    out.append("from")
    out.append(
        f'    {{{{ set_datalake_project("{DATASET}_staging.{table}") }}}}'
    )
    out.append("    as t")
    if needs_dir:
        # the state directory carries the CPS census state code as `id_census`,
        # which is what CEPR's `state` variable holds -- all 51 codes match exactly.
        # joins reference the production relation directly (never ref/set_datalake_project).
        out.append("left join `basedosdados.br_bd_diretorios_us.state` as d")
        out.append("    on safe_cast(t.state as string) = d.id_census")
    out.append("")

    path = os.path.join(MODELS, f"{DATASET}__{table}.sql")
    with open(path, "w") as fh:
        fh.write("\n".join(out))
    return len(rows)


def yaml_block(text, indent):
    """Emit a folded block scalar; safe for colons and long lines."""
    pad = " " * indent
    words, lines, cur = text.split(), [], ""
    for w in words:
        if len(cur) + len(w) + 1 > 88:
            lines.append(cur)
            cur = w
        else:
            cur = f"{cur} {w}".strip()
    lines.append(cur)
    return ">-\n" + "\n".join(pad + ln for ln in lines)


def write_schema(nonnull):
    out = ["---", "version: 2", "models:"]
    for table, spec in TABLES.items():
        rows = load(spec["arch"])
        names = [r["name"] for r in rows]

        # columns too sparse for the 5% non-null floor are declared, not hidden
        sparse = []
        if spec["rows"] and table in nonnull:
            for r in rows:
                obs = nonnull[table].get(r["name"])
                if r["name"] in DERIVED or obs is None:
                    continue
                if sum(obs.values()) / spec["rows"] < 0.05:
                    sparse.append(r["name"])

        out.append(f"  - name: {DATASET}__{table}")
        out.append("    description: " + yaml_block(spec["description"], 6))
        out.append("    tests:")
        if spec["strict_key"]:
            out.append("      - dbt_utils.unique_combination_of_columns:")
            out.append(
                f"          combination_of_columns: [{', '.join(spec['key'])}]"
            )
        else:
            out.append("      - custom_unique_combinations_of_columns:")
            out.append(
                f"          combination_of_columns: [{', '.join(spec['key'])}]"
            )
            out.append("          proportion_allowed_failures: 0.05")
        out.append("      - not_null_proportion_multiple_columns:")
        out.append("          at_least: 0.05")
        if sparse:
            out.append("          ignore_values:")
            out.extend(f"            - {c}" for c in sparse)

        out.append("    columns:")
        for r in rows:
            name = r["name"]
            out.append(f"      - name: {name}")
            out.append(
                "        description: " + yaml_block(r["description"], 10)
            )
            tests = []
            if name in ("year", "month") and spec["start"]:
                tests.append(("not_null", None))
            if name == "year" and spec["start"]:
                tests.append(
                    # pyrefly: ignore [bad-argument-type]
                    ("rel", ("br_bd_diretorios_data_tempo__ano", "ano.ano"))
                )
            if name == "month" and spec["start"]:
                tests.append(
                    # pyrefly: ignore [bad-argument-type]
                    ("rel", ("br_bd_diretorios_data_tempo__mes", "mes.mes"))
                )
            if name == "id_state":
                tests.append(
                    # pyrefly: ignore [bad-argument-type]
                    ("rel", ("br_bd_diretorios_us__state", "id_state"))
                )
            # id_county carries a directory FK but is deliberately *not* tested:
            # 0.86% of non-null rows hold county FIPS codes that were valid at the
            # time and have since been retired (Dade/FL 12025, the four Connecticut
            # counties replaced by planning regions in 2022), which no current-vintage
            # directory can match. Documented in the column's observations.
            if not tests:
                continue
            out.append("        tests:")
            for kind, arg in tests:
                if kind == "not_null":
                    out.append("          - not_null")
                else:
                    out.append("          - relationships:")
                    # pyrefly: ignore [unsupported-operation]
                    out.append(f"              to: ref('{arg[0]}')")
                    # pyrefly: ignore [unsupported-operation]
                    out.append(f"              field: {arg[1]}")
        print(
            f"  {table}: {len(names)} columns, {len(sparse)} below the 5% non-null floor"
        )

    path = os.path.join(MODELS, "schema.yml")
    with open(path, "w") as fh:
        fh.write("\n".join(out) + "\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nonnull")
    args = ap.parse_args()
    nonnull = {}
    if args.nonnull:
        with open(args.nonnull) as fh:
            nonnull = json.load(fh)

    for table, spec in TABLES.items():
        n = write_sql(table, spec)
        print(f"wrote {DATASET}__{table}.sql ({n} columns)")
    write_schema(nonnull)
    print("wrote schema.yml")


if __name__ == "__main__":
    main()
