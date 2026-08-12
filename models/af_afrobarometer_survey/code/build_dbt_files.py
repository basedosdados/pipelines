#!/usr/bin/env python3
"""Generate DBT models (.sql) and schema.yml for af_afrobarometer_survey.

`profile` reads the local Parquet to pick each round's uniqueness key and the
set of >95%-null (sparse) columns; results cache to profile_cache.json.
`generate` reads the architecture CSVs (source of truth for column order/types)
plus the cache and writes the .sql models and schema.yml.

Round tables are one-row-per-respondent survey microdata: unpartitioned, with
original variable codes as columns and value labels held in `dicionario`.

Usage:
    .venv/bin/python models/af_afrobarometer_survey/code/build_dbt_files.py profile
    .venv/bin/python models/af_afrobarometer_survey/code/build_dbt_files.py generate
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

import pyarrow.compute as pc
import pyarrow.parquet as pq

# pyrefly: ignore [untyped-import]
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent))
# pyrefly: ignore [missing-import]
from common import DATASET_ID, META_CACHE, ROUNDS

CODE_DIR = Path(__file__).resolve().parent
DATASET_DIR = CODE_DIR.parent
ARCH_DIR = CODE_DIR / "architecture"
OUTPUT_DIR = DATASET_DIR / "output"
PROFILE_CACHE = CODE_DIR / "profile_cache.json"

ROUND_SLUGS = [r["slug"] for r in ROUNDS]
# respondent-id key per round (round 1 predates RESPNO; refnumb is its unique
# reference number — casenumb restarts per country)
KEY_COL = {"round1": "refnumb"}
# above this column count the not_null_proportion macro exceeds BigQuery's
# query-complexity limit; verified locally instead.
PROP_TEST_MAX_COLS = 400

SQL_TYPES = {
    "INT64": "int64",
    "FLOAT64": "float64",
    "STRING": "string",
    "DATE": "date",
    "BOOL": "bool",
}


def key_col(slug: str) -> str:
    return KEY_COL.get(slug, "respno")


def read_arch(table: str) -> list[dict]:
    with open(ARCH_DIR / f"{table}.csv", newline="", encoding="utf-8") as f:
        return [r for r in csv.DictReader(f) if r["name"].strip()]


# --------------------------------------------------------------- profiling


def profile():
    cache = json.loads(META_CACHE.read_text())
    results = {}
    for slug in ROUND_SLUGS:
        t = pq.read_table(OUTPUT_DIR / slug / "data.parquet")
        n = t.num_rows
        key = key_col(slug)
        uniq = t.select([key]).group_by([key]).aggregate([]).num_rows
        dups = n - uniq
        sparse = []
        for name in t.schema.names:
            col = t.column(name)
            # pyrefly: ignore [missing-attribute]
            nulls = pc.sum(pc.is_null(col)).as_py() or 0
            if n and nulls / n > 0.95:
                sparse.append(name)
        results[slug] = {
            "rows": n,
            "key": key,
            "dup_rows": dups,
            "sparse_columns": sparse,
            "n_cols": t.num_columns,
            "coverage": cache[slug]["coverage"],
            "n_countries": len(cache[slug]["value_labels"].get("country", {})),
        }
        print(
            f"[profile] {slug}: rows={n} key={key} dups={dups} "
            f"sparse={len(sparse)}/{t.num_columns}"
        )
    PROFILE_CACHE.write_text(json.dumps(results, indent=1))
    return results


# -------------------------------------------------------------- generation


class Folded(str):
    pass


def _folded(dumper, data):
    return dumper.represent_scalar(
        "tag:yaml.org,2002:str", str(data), style=">"
    )


yaml.add_representer(Folded, _folded)


def cov_str(cov):
    a, b = cov
    return str(a) if a == b else f"{a}-{b}"


def round_description(slug, prof):
    n = int(slug.replace("round", ""))
    note = (
        ""
        if prof["dup_rows"] == 0
        else " A small share of duplicate respondent numbers exists in the raw"
        " source, so the uniqueness test allows up to 5 percent failures."
    )
    return (
        f"Afrobarometer merged Round {n} survey microdata "
        f"({cov_str(prof['coverage'])}), one row per respondent across "
        f"{prof['n_countries']} African countries. Original Afrobarometer "
        f"variable codes are preserved as column names; coded values are "
        f"documented in the dicionario table." + note
    )


def write_sql(table):
    arch = read_arch(table)
    lines = []
    for row in arch:
        name = row["name"].strip()
        btype = SQL_TYPES[row["bigquery_type"].strip().upper()]
        lines.append(f"    safe_cast({name} as {btype}) {name},")
    lines[-1] = lines[-1].rstrip(",")
    body = "\n".join(lines)
    sql = (
        "{{\n"
        "    config(\n"
        f'        schema="{DATASET_ID}",\n'
        f'        alias="{table}",\n'
        '        materialized="table",\n'
        "    )\n"
        "}}\n\n\n"
        "select\n"
        f"{body}\n"
        "from\n"
        f'    {{{{ set_datalake_project("{DATASET_ID}_staging.{table}") }}}}\n'
        "    as t\n"
    )
    path = DATASET_DIR / f"{DATASET_ID}__{table}.sql"
    path.write_text(sql)
    return path


def round_model_entry(slug, prof):
    key = prof["key"]
    if prof["dup_rows"] == 0:
        unique_test = {
            "dbt_utils.unique_combination_of_columns": {
                "combination_of_columns": [key]
            }
        }
    else:
        unique_test = {
            "custom_unique_combinations_of_columns": {
                "combination_of_columns": [key],
                "proportion_allowed_failures": 0.05,
            }
        }
    tests = [unique_test]
    if prof["n_cols"] <= PROP_TEST_MAX_COLS:
        prop = {"at_least": 0.05}
        if prof["sparse_columns"]:
            prop["ignore_values"] = prof["sparse_columns"]
        tests.append({"not_null_proportion_multiple_columns": prop})
    entry = {
        "name": f"{DATASET_ID}__{slug}",
        "description": Folded(round_description(slug, prof)),
        "tests": tests,
        "columns": [],
    }
    for row in read_arch(slug):
        name = row["name"].strip()
        col = {"name": name, "description": Folded(row["description"].strip())}
        if name == "country_iso3_code":
            col["tests"] = [
                {
                    "relationships": {
                        "to": "ref('br_bd_diretorios_mundo__pais')",
                        "field": "sigla_iso3",
                    }
                }
            ]
        elif name == key:
            col["tests"] = ["not_null"]
        entry["columns"].append(col)
    return entry


def dicionario_entry():
    entry = {
        "name": f"{DATASET_ID}__dicionario",
        "description": Folded(
            "Dicionário de rótulos de valores das tabelas round1..round9: para "
            "cada tabela e coluna, o significado de cada valor codificado."
        ),
        "tests": [
            {
                "dbt_utils.unique_combination_of_columns": {
                    "combination_of_columns": [
                        "id_tabela",
                        "nome_coluna",
                        "chave",
                    ]
                }
            }
        ],
        "columns": [
            {
                "name": r["name"].strip(),
                "description": Folded(r["description"].strip()),
            }
            for r in read_arch("dicionario")
        ],
    }
    return entry


def generate():
    prof = json.loads(PROFILE_CACHE.read_text())
    paths, models = [], []
    for slug in ROUND_SLUGS:
        paths.append(write_sql(slug))
        models.append(round_model_entry(slug, prof[slug]))
    paths.append(write_sql("dicionario"))
    models.append(dicionario_entry())
    doc = {"version": 2, "models": models}
    schema_path = DATASET_DIR / "schema.yml"
    with open(schema_path, "w", encoding="utf-8") as f:
        f.write("---\n")
        yaml.dump(doc, f, sort_keys=False, allow_unicode=True, width=90)
    paths.append(schema_path)
    for p in paths:
        print(f"[generate] wrote {p.relative_to(DATASET_DIR.parent.parent)}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("command", choices=["profile", "generate"])
    args = ap.parse_args()
    if args.command == "profile":
        profile()
    else:
        generate()


if __name__ == "__main__":
    main()
