"""Generate the dbt models and schema.yml for us_bls_cex from the architecture.

The microdata tables run to 1,269 columns, so the SQL and the schema are
generated rather than hand-written. Re-run after any architecture change:

    uv run models/us_bls_cex/code/build_dbt.py

Sparse columns are exempted from ``not_null_proportion_multiple_columns`` by
measuring the cleaned output (``--output``, default from ``pumd_files``) for the
most recent year, since that is the year the test is scoped to.
"""

import argparse
import csv
from pathlib import Path

import pyarrow.dataset as ds

from pipelines.datasets.us_bls_cex.pumd_files import OUTPUT_DIR

DATASET = "us_bls_cex"
CODE = Path(__file__).parent
MODELS = CODE.parent
ARCH = CODE / "architecture"
RECENT = "__most_recent_year_en__"
PARTITION_END = 2031

BQ_RESERVED = {
    "all", "and", "any", "array", "as", "asc", "at", "between", "by", "case",
    "cast", "collate", "contains", "create", "cross", "cube", "current",
    "default", "define", "desc", "distinct", "else", "end", "enum", "escape",
    "except", "exclude", "exists", "extract", "false", "fetch", "following",
    "for", "from", "full", "group", "grouping", "groups", "hash", "having",
    "if", "ignore", "in", "inner", "intersect", "interval", "into", "is",
    "join", "lateral", "left", "like", "limit", "lookup", "merge", "natural",
    "new", "no", "not", "null", "nulls", "of", "on", "or", "order", "outer",
    "over", "partition", "preceding", "proto", "qualify", "range",
    "recursive", "respects", "right", "rollup", "rows", "select", "set",
    "some", "struct", "tablesample", "then", "to", "treat", "true",
    "unbounded", "union", "unnest", "using", "when", "where", "window",
    "with", "within",
}  # fmt: skip

# table -> (description, partition start, unique key, cluster_by)
TABLES = {
    "series": (
        "Catalogue of the BLS Consumer Expenditure Surveys published-table time "
        "series (LABSTAT database cx), one row per series, with the item, "
        "demographic classification and group each series is cut by",
        None,
        ["series_id"],
        None,
    ),
    "annual": (
        "Published annual estimates of the BLS Consumer Expenditure Surveys, one "
        "row per series and year, 1984 onward: mean expenditure, income or "
        "characteristic per consumer unit, with standard errors, shares and "
        "aggregates from 2010 onward",
        1984,
        ["year", "series_id"],
        ["series_id"],
    ),
    "ucc": (
        "BLS hierarchical grouping files (integrated, interview and diary), one "
        "row per line per year, mapping Universal Classification Codes (UCC) to "
        "the published expenditure and income category tree",
        1996,
        ["year", "hierarchy", "line_number"],
        ["hierarchy"],
    ),
    "dicionario": (
        "Dictionary of coded values for the us_bls_cex tables",
        None,
        ["id_tabela", "nome_coluna", "chave", "cobertura_temporal"],
        None,
    ),
    "interview_household": (
        "Interview Survey public-use microdata, consumer-unit file (FMLI): one "
        "row per consumer unit interview, with characteristics, income, assets, "
        "quarterly summary expenditures, final and replicate weights",
        1996,
        ["newid"],
        None,
    ),
    "interview_member": (
        "Interview Survey public-use microdata, member file (MEMI): one row per "
        "consumer unit member per interview, with demographics, work and income",
        1996,
        ["newid", "member_number"],
        None,
    ),
    "interview_expenditure": (
        "Interview Survey public-use microdata, monthly expenditure file (MTBI): "
        "one row per consumer unit interview, reference month and expenditure "
        "record, coded by Universal Classification Code (UCC)",
        1996,
        None,
        ["ucc"],
    ),
    "interview_income": (
        "Interview Survey public-use microdata, monthly income file (ITBI): one "
        "row per consumer unit interview, reference month and income UCC",
        1996,
        None,
        ["ucc"],
    ),
    "diary_household": (
        "Diary Survey public-use microdata, consumer-unit file (FMLD): one row "
        "per consumer unit diary week, with characteristics, income, weekly "
        "summary expenditures, final and replicate weights",
        1996,
        ["newid"],
        None,
    ),
    "diary_member": (
        "Diary Survey public-use microdata, member file (MEMD): one row per "
        "consumer unit member per diary week",
        1996,
        ["newid", "member_number"],
        None,
    ),
    "diary_expenditure": (
        "Diary Survey public-use microdata, expenditure file (EXPD): one row per "
        "item purchased during the diary week, coded by UCC",
        1996,
        None,
        ["ucc"],
    ),
    "diary_income": (
        "Diary Survey public-use microdata, income file (DTBD): one row per "
        "consumer unit diary week and income UCC",
        1996,
        None,
        ["ucc"],
    ),
}


def read_arch(table):
    with open(ARCH / f"{table}.csv", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def ident(name):
    return f"`{name}`" if name in BQ_RESERVED else name


def model_sql(table, arch):
    _, start, _, cluster = TABLES[table]
    cfg = [
        f'        schema="{DATASET}"',
        f'        alias="{table}"',
        '        materialized="table"',
    ]
    if start:
        cfg.append(
            "        partition_by={\n"
            '            "field": "year",\n'
            '            "data_type": "int64",\n'
            f'            "range": {{"start": {start}, "end": {PARTITION_END}, "interval": 1}},\n'
            "        }"
        )
    if cluster:
        cfg.append(f"        cluster_by={cluster!r}".replace("'", '"'))
    cols = ",\n".join(
        f"    safe_cast({ident(a['name'])} as {a['bigquery_type'].lower()}) {ident(a['name'])}"
        for a in arch
    )
    return (
        "{{\n    config(\n"
        + ",\n".join(cfg)
        + ",\n    )\n}}\n\n\nselect\n"
        + cols
        + f'\nfrom {{{{ set_datalake_project("{DATASET}_staging.{table}") }}}} as t\n'
    )


def sparse_columns(table, arch):
    """Columns under 5% non-null in the most recent year of the cleaned output."""
    path = OUTPUT_DIR / table
    if not path.exists():
        raise SystemExit(f"{path} missing: run the cleaners first")
    years = sorted(int(p.name.split("=")[1]) for p in path.glob("year=*"))
    if not years:
        return []
    t = ds.dataset(path / f"year={years[-1]}").to_table()
    n = t.num_rows
    return [
        a["name"]
        for a in arch
        if a["name"] in t.column_names
        and n
        and (n - t.column(a["name"]).null_count) / n < 0.05
    ]


def yaml_str(text, indent):
    pad = " " * indent
    return ">-\n" + pad + text.replace("\n", " ")


def schema_yml():
    out = ["---", "version: 2", "models:"]
    for table, (desc, start, key, _) in TABLES.items():
        arch = read_arch(table)
        out += [
            f"  - name: {DATASET}__{table}",
            f"    description: {yaml_str(desc, 6)}",
            "    tests:",
        ]
        where = (
            f"\n          config:\n            where: {RECENT}"
            if start
            else ""
        )
        if key:
            cols = "\n".join(f"            - {k}" for k in key)
            out.append(
                "      - dbt_utils.unique_combination_of_columns:\n"
                f"          combination_of_columns:\n{cols}{where}"
            )
        if table != "dicionario":
            ignore = sparse_columns(table, arch) if start else []
            block = "      - not_null_proportion_multiple_columns:\n          at_least: 0.05"
            if ignore:
                block += "\n          ignore_values:\n" + "\n".join(
                    f"            - {c}" for c in ignore
                )
            out.append(block + where)
        coded = [
            a["name"] for a in arch if a["covered_by_dictionary"] == "yes"
        ]
        if coded:
            cols = "\n".join(f"            - {c}" for c in coded)
            out.append(
                "      - custom_dictionary_coverage:\n"
                f"          dictionary_model: ref('{DATASET}__dicionario')\n"
                f"          columns_covered_by_dictionary:\n{cols}{where}"
            )
        out.append("    columns:")
        for a in arch:
            out += [
                f"      - name: {a['name']}",
                f"        description: {yaml_str(a['description'], 10)}",
            ]
            tests = []
            if a["name"] == "year" and start:
                tests.append(
                    "          - not_null\n"
                    "          - relationships:\n"
                    "              to: ref('br_bd_diretorios_data_tempo__ano')\n"
                    "              field: ano.ano\n"
                    "              config:\n"
                    f"                where: {RECENT}"
                )
            elif (
                a["name"] in (key or []) or a["name"] in ("newid", "series_id")
            ) and a["name"] != "cobertura_temporal":  # empty = table coverage
                tests.append("          - not_null")
            if table == "annual" and a["name"] == "series_id":
                tests.append(
                    "          - relationships:\n"
                    f"              to: ref('{DATASET}__series')\n"
                    "              field: series_id\n"
                    "              config:\n"
                    f"                where: {RECENT}"
                )
            if tests:
                out += ["        tests:", *tests]
    return "\n".join(out) + "\n"


def main():
    argparse.ArgumentParser(description=__doc__).parse_args()
    for table in TABLES:
        (MODELS / f"{DATASET}__{table}.sql").write_text(
            model_sql(table, read_arch(table)), encoding="utf-8"
        )
    (MODELS / "schema.yml").write_text(schema_yml(), encoding="utf-8")
    print(f"wrote {len(TABLES)} models and schema.yml")


if __name__ == "__main__":
    main()
