"""Generate DBT model files (.sql) and schema.yml for gb_eric_ess.

Usage:
    uv run python models/gb_eric_ess/code/build_dbt_files.py profile [--cache PATH]
    uv run python models/gb_eric_ess/code/build_dbt_files.py generate [--cache PATH]

`profile` scans the local partitioned parquet in models/gb_eric_ess/output/ one round
at a time to (1) verify the [country_id, respondent_id] key empirically and
(2) compute per-column null rates. Results are cached to a JSON file.

`generate` reads the architecture CSVs (source of truth for column order, types,
and descriptions) plus the profile cache and writes the .sql models and schema.yml.

Design B: one wide table per round (round_01 … round_11), partitioned by `year`.
"""

import argparse
import csv
import json
from pathlib import Path

import pyarrow.compute as pc
import pyarrow.parquet as pq

# pyrefly: ignore [untyped-import]
import yaml

CODE_DIR = Path(__file__).resolve().parent
DATASET_DIR = CODE_DIR.parent
ARCH_DIR = CODE_DIR / "architecture"
OUTPUT_DIR = DATASET_DIR / "output"
DATASET_ID = "gb_eric_ess"

YEAR = {
    1: 2002,
    2: 2004,
    3: 2006,
    4: 2008,
    5: 2010,
    6: 2012,
    7: 2014,
    8: 2016,
    9: 2018,
    10: 2020,
    11: 2023,
}

# The logical key of a per-round table: respondent id is unique within country.
KEY = ["country_id", "respondent_id"]

# ESS round tables are all wider than ~450 columns, which makes the
# not_null_proportion_multiple_columns macro fail BigQuery query planning
# ("query is too complex" — one UNION ALL branch per column). The 5% non-null
# rule is verified locally against the partitioned parquet instead (see
# `profile`; sparse columns are listed in code/COVERAGE.md).
SKIP_PROP_TEST = True

# Rotating modules per round (in addition to the stable core), for descriptions.
ROTATING = {
    1: "immigration and citizen involvement",
    2: "family/work/well-being, health and care seeking, and economic morality",
    3: "personal and social well-being and the timing of life",
    4: "welfare attitudes and experiences and expressions of ageism",
    5: "trust in justice and family/work/well-being",
    6: "personal and social well-being and understandings and evaluations of democracy",
    7: "immigration and social inequalities in health",
    8: "welfare attitudes and climate change and energy",
    9: "the timing of life and justice and fairness",
    10: "digital social contacts and understandings and evaluations of democracy",
    11: "health and inequalities and gender in the household and labour market",
}

SQL_TYPES = {"INT64": "int64", "FLOAT64": "float64", "STRING": "string"}


def rounds_present():
    return sorted(
        int(p.name.split("_")[1])
        for p in OUTPUT_DIR.glob("round_*")
        if (p / f"year={YEAR[int(p.name.split('_')[1])]}").exists()
    )


def read_architecture(round_n):
    with open(
        ARCH_DIR / f"round_{round_n:02d}.csv", newline="", encoding="utf-8"
    ) as f:
        return [r for r in csv.DictReader(f) if r["name"].strip()]


def year_file(round_n):
    return (
        OUTPUT_DIR
        / f"round_{round_n:02d}"
        / f"year={YEAR[round_n]}"
        / "data.parquet"
    )


# ---------------------------------------------------------------- profiling


def check_key(round_n):
    f = year_file(round_n)
    t = pq.read_table(f, columns=KEY)
    n_unique = t.group_by(KEY).aggregate([]).num_rows
    return {"duplicate_rows": t.num_rows - n_unique, "total_rows": t.num_rows}


def null_rates(round_n):
    f = year_file(round_n)
    pf = pq.ParquetFile(f)
    md = pf.metadata
    total = md.num_rows
    names = pf.schema_arrow.names
    nulls = {n: 0 for n in names}
    missing = set()
    for rg in range(md.num_row_groups):
        for ci in range(md.num_columns):
            col = md.row_group(rg).column(ci)
            st = col.statistics
            if st is None or not st.has_null_count:
                missing.add(col.path_in_schema)
            else:
                nulls[col.path_in_schema] += st.null_count
    if missing:
        tt = pq.read_table(f, columns=sorted(missing))
        for n in missing:
            # pyrefly: ignore [missing-attribute]
            nulls[n] = pc.sum(pc.is_null(tt[n])).as_py() or 0
    return {n: v / total for n, v in nulls.items()}, total


def profile(cache_path):
    results = json.loads(cache_path.read_text()) if cache_path.exists() else {}
    for r in rounds_present():
        key = f"round_{r:02d}"
        if key in results:
            print(f"[profile] {key}: cached")
            continue
        print(f"[profile] {key} ...", flush=True)
        kev = check_key(r)
        rates, total = null_rates(r)
        sparse = sorted(n for n, v in rates.items() if v > 0.95)
        results[key] = {
            "key_evidence": kev,
            "total_rows": total,
            "sparse_columns": sparse,
            "null_rates": rates,
        }
        cache_path.write_text(json.dumps(results, indent=1))
        print(
            f"[profile] {key}: dups={kev['duplicate_rows']} rows={total} "
            f"sparse={len(sparse)}/{len(rates)}"
        )
    return results


# --------------------------------------------------------------- generation


class Folded(str):
    """String rendered as a YAML folded block scalar (>)."""


def _folded(dumper, data):
    return dumper.represent_scalar(
        "tag:yaml.org,2002:str", str(data), style=">"
    )


yaml.add_representer(Folded, _folded)


def write_sql(round_n):
    arch = read_architecture(round_n)
    lines = []
    for row in arch:
        name = row["name"].strip()
        btype = SQL_TYPES[row["bigquery_type"].strip().upper()]
        lines.append(f"    safe_cast({name} as {btype}) {name},")
    lines[-1] = lines[-1].rstrip(",")
    body = "\n".join(lines)
    table = f"round_{round_n:02d}"
    sql = f"""{{{{
    config(
        schema="{DATASET_ID}",
        alias="{table}",
        materialized="table",
        partition_by={{
            "field": "year",
            "data_type": "int64",
            "range": {{"start": {YEAR[round_n]}, "end": {YEAR[round_n] + 5}, "interval": 1}},
        }},
        cluster_by=["country_id"],
    )
}}}}


select
{body}
from
    {{{{ set_datalake_project("{DATASET_ID}_staging.{table}") }}}}
    as t
"""
    path = DATASET_DIR / f"{DATASET_ID}__{table}.sql"
    path.write_text(sql)
    return path


def model_entry(round_n, prof):
    table = f"round_{round_n:02d}"
    dups = prof["key_evidence"]["duplicate_rows"]
    note = ""
    if dups == 0:
        unique_test = {
            "dbt_utils.unique_combination_of_columns": {
                "combination_of_columns": ["year", *KEY]
            }
        }
    else:
        unique_test = {
            "custom_unique_combinations_of_columns": {
                "combination_of_columns": ["year", *KEY],
                "proportion_allowed_failures": 0.05,
            }
        }
        note = (
            " A small proportion of duplicate respondent identifiers exists"
            " in the raw source, so the uniqueness test allows up to 5 percent"
            " failures."
        )
    desc = (
        f"European Social Survey Round {round_n} integrated file (fielded "
        f"{YEAR[round_n]}/{str(YEAR[round_n] + 1)[2:]}): one row per respondent "
        f"across all participating countries. Covers the stable core module plus "
        f"the round-specific rotating modules on {ROTATING[round_n]}. Categorical "
        f"variables retain their original numeric codes, decoded via the companion "
        f"dictionary table." + note
    )
    tests = [unique_test]
    entry = {
        "name": f"{DATASET_ID}__{table}",
        "description": Folded(desc),
        "tests": tests,
        "columns": [],
    }
    for row in read_architecture(round_n):
        name = row["name"].strip()
        col = {"name": name, "description": Folded(row["description"].strip())}
        if name == "year":
            # the ano directory column is a STRUCT<ano, bissexto>; reference the subfield
            col["tests"] = [
                "not_null",
                {
                    "relationships": {
                        "to": "ref('br_bd_diretorios_data_tempo__ano')",
                        "field": "ano.ano",
                    }
                },
            ]
        elif name == "country_id":
            # severity warn: ESS codes Kosovo as the non-standard "XK", which is
            # absent from the ISO-based pais directory (round 6 onward).
            col["tests"] = [
                "not_null",
                {
                    "relationships": {
                        "to": "ref('br_bd_diretorios_mundo__pais')",
                        "field": "sigla_iso2",
                        "config": {"severity": "warn"},
                    }
                },
            ]
        elif name in ("round", "respondent_id"):
            col["tests"] = ["not_null"]
        entry["columns"].append(col)
    return entry


DICIONARIO_COLS = [
    (
        "id_tabela",
        "Slug of the round table the coded column belongs to (e.g. round_01).",
    ),
    ("nome_coluna", "Name of the coded column in that round table."),
    ("chave", "Stored code value (as text)."),
    (
        "cobertura_temporal",
        "Reference year of the round the mapping applies to.",
    ),
    ("valor", "Human-readable label the code decodes to."),
]


def write_dicionario_sql():
    body = ",\n".join(
        f"    safe_cast({c} as string) {c}" for c, _ in DICIONARIO_COLS
    )
    sql = f"""{{{{
    config(
        schema="{DATASET_ID}",
        alias="dicionario",
        materialized="table",
    )
}}}}


select
{body}
from
    {{{{ set_datalake_project("{DATASET_ID}_staging.dicionario") }}}}
    as t
"""
    path = DATASET_DIR / f"{DATASET_ID}__dicionario.sql"
    path.write_text(sql)
    return path


def dicionario_model_entry():
    desc = (
        "Dictionary table for the European Social Survey. Maps the stored numeric "
        "and string codes of every dictionary-covered column, in each round table, "
        "to their human-readable labels, as published in the ESS value labels."
    )
    entry = {
        "name": f"{DATASET_ID}__dicionario",
        "description": Folded(desc),
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
        "columns": [],
    }
    for name, cdesc in DICIONARIO_COLS:
        col = {"name": name, "description": Folded(cdesc)}
        if name in ("id_tabela", "nome_coluna", "chave"):
            # pyrefly: ignore [bad-assignment]
            col["tests"] = ["not_null"]
        entry["columns"].append(col)
    return entry


def generate(cache_path):
    profiles = json.loads(cache_path.read_text())
    paths, models = [], []
    for r in rounds_present():
        paths.append(write_sql(r))
        models.append(model_entry(r, profiles[f"round_{r:02d}"]))
    if (OUTPUT_DIR / "dicionario" / "data.parquet").exists():
        paths.append(write_dicionario_sql())
        models.append(dicionario_model_entry())
    doc = {"version": 2, "models": models}
    schema_path = DATASET_DIR / "schema.yml"
    with open(schema_path, "w", encoding="utf-8") as f:
        f.write("---\n")
        yaml.dump(doc, f, sort_keys=False, allow_unicode=True, width=88)
    paths.append(schema_path)
    for p in paths:
        print(f"[generate] wrote {p.relative_to(DATASET_DIR.parent.parent)}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("command", choices=["profile", "generate"])
    ap.add_argument("--cache", default=str(CODE_DIR / "profile_cache.json"))
    args = ap.parse_args()
    cache_path = Path(args.cache)
    if args.command == "profile":
        profile(cache_path)
    else:
        generate(cache_path)


if __name__ == "__main__":
    main()
