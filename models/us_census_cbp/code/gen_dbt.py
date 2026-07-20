"""Generate CBP dbt models (.sql) and schema.yml from the architecture sheet TSVs.

Single source of truth = code/sheet_<table>.tsv (which gen_arch_sheets.py produced).
Keeps the ~85-column wide models consistent with the Drive sheets and the cleaner.
"""

import csv
from pathlib import Path

import yaml

HERE = Path(__file__).resolve().parent
MODELS = HERE.parent  # models/us_census_cbp
DATASET = "us_census_cbp"

# per-NAICS-vintage FK test: (where clause, directory model, allowance, ignore_values).
# All five dirs are CBP-published code sets (1997 via concordance), so each matches
# the data of its vintage exactly. CBP publishes NAICS 2017 through 2023, so there is
# no NAICS-2022 CBP vintage and the official naics_2022 dir is not referenced.
#
# Allowance is 0 for every vintage: measured against the built directories, national
# resolves at 0.000% unmatched for 2002/2007/2012/2017 and 0.094% for 1997 (only the
# '95'/'99' admin codes, which ignore_values drops). A non-zero allowance here would
# only serve to mask a future unmapped code, so none is set. Sector split-codes need
# no exception - these CBP-derived dirs store 31/44/48 natively, not ranges like
# '31-33', unlike the official naics_2022 dir.
NAICS_TESTS = [
    # 1997 dir is the full official structure; '95'/'99' admin codes aren't in it.
    (
        "naics_version = '1997'",
        "br_bd_diretorios_us__naics_1997",
        0.0,
        ["95", "99"],
    ),
    ("naics_version = '2002'", "br_bd_diretorios_us__naics_2002", 0.0, []),
    ("naics_version = '2007'", "br_bd_diretorios_us__naics_2007", 0.0, []),
    ("naics_version = '2012'", "br_bd_diretorios_us__naics_2012", 0.0, []),
    ("naics_version = '2017'", "br_bd_diretorios_us__naics_2017", 0.0, []),
]

# (directory model, field, proportion_allowed_failures).
# id_county needs an allowance (~1.2% of rows): CBP uses a county part of 999 for
# establishments not allocated to a county (statewide), and early years carry county
# vintages the current directory no longer has (pre-2022 Connecticut counties,
# dissolved Alaska boroughs, Dade County FL).
GEO_DIR = {
    "id_state": ("br_bd_diretorios_us__state", "id_state", 0.0),
    "id_county": ("br_bd_diretorios_us__county", "id_county", 0.02),
}

# Columns the cleaner always populates - verified 100.00% non-null across the full
# history of all three tables, so the proportion test runs at at_least=1.0 (zero nulls
# tolerated). Every other column is legitimately sparse (era-specific flags, size bands
# below the disclosure threshold) and is passed to the test's ignore_values.
CORE_NOTNULL = {
    "year",
    "id_state",
    "id_county",
    "naics",
    "naics_version",
    "lfo",
    "establishments",
    "employment",
    "payroll_first_quarter",
    "payroll_annual",
}

# Cost control. county (48.7M) and state (6.8M) re-scan the whole table on every test
# run, but CBP's historical partitions are immutable - only the newest year gets new
# data. So the big tables test ONLY the most recent partition (BD's __most_recent_*
# convention; _en variant added to macros/custom_get_where_subquery.sql for the
# English `year` column). national (225k rows) stays unscoped: it is cheap AND carries
# every NAICS code of every vintage, so it keeps the full 5-vintage FK battery and
# remains the table that validates all of history.
BIG_TABLES = {"state", "county"}
RECENT = "__most_recent_year_en__"


def _scope(body, table):
    """Filter a test to the most recent partition on the big tables."""
    if table not in BIG_TABLES:
        return body
    cfg = body.setdefault("config", {})
    prev = cfg.get("where")
    cfg["where"] = f"{RECENT} and ({prev})" if prev else RECENT
    return body


def _not_null(table):
    return (
        {"not_null": {"config": {"where": RECENT}}}
        if table in BIG_TABLES
        else "not_null"
    )


def read_sheet(table):
    rows = []
    with open(HERE / f"sheet_{table}.tsv", newline="") as f:
        r = csv.DictReader(f, delimiter="\t")
        for row in r:
            rows.append(row)
    return rows


def key_cols(table):
    return {
        "national": ["naics"],
        "state": ["id_state", "naics"],
        "county": ["id_county", "naics"],
    }[table]


def gen_sql(table, cols):
    casts = []
    for c in cols:
        t = "int64" if c["bigquery_type"] == "INT64" else "string"
        casts.append(f"    safe_cast({c['name']} as {t}) {c['name']}")
    body = ",\n".join(casts)
    return f"""{{{{
    config(
        schema="{DATASET}",
        alias="{table}",
        materialized="table",
        partition_by={{
            "field": "year",
            "data_type": "int64",
            "range": {{"start": 1998, "end": 2028, "interval": 1}},
        }},
        cluster_by=["naics"],
    )
}}}}


select
{body}
from {{{{ set_datalake_project("{DATASET}_staging.{table}") }}}}
    as t
"""


def col_tests(table, name):
    tests = []
    if name == "year":
        tests.append(_not_null(table))
        tests.append(
            {
                "relationships": _scope(
                    {
                        "to": "ref('br_bd_diretorios_data_tempo__ano')",
                        "field": "ano.ano",
                    },
                    table,
                )
            }
        )
    elif name in GEO_DIR:
        ref, field, allow = GEO_DIR[name]
        tests.append(_not_null(table))
        body = {"to": f"ref('{ref}')", "field": field}
        if allow:
            body["proportion_allowed_failures"] = allow
        tests.append({"custom_relationships": _scope(body, table)})
    elif name == "naics":
        tests.append(_not_null(table))
        if table in BIG_TABLES:
            # Only the current vintage exists in the most recent partition; the full
            # per-vintage battery runs on `national` (see BIG_TABLES note). Measured
            # 0.0000% unmatched on the 2023 partition of both tables, so no allowance.
            tests.append(
                {
                    "custom_relationships": _scope(
                        {
                            "to": "ref('br_bd_diretorios_us__naics_2017')",
                            "field": "id_naics",
                        },
                        table,
                    )
                }
            )
        else:
            for where, ref, allow, ignore in NAICS_TESTS:
                body = {
                    "to": f"ref('{ref}')",
                    "field": "id_naics",
                    "config": {"where": where},
                }
                if allow:
                    body["proportion_allowed_failures"] = allow
                if ignore:
                    body["ignore_values"] = ignore
                tests.append({"custom_relationships": body})
    return tests


def model_entry(table, cols):
    ignore = [c["name"] for c in cols if c["name"] not in CORE_NOTNULL]
    uniq = {
        "national": ["year", "naics", "lfo"],
        "state": ["year", "id_state", "naics", "lfo"],
        "county": ["year", "id_county", "naics"],
    }[table]
    # Every table uses the strict uniqueness test. county carries one unavoidable
    # duplicate key in 48.7M rows - the 1999 source file repeats a block for FIPS
    # 01045 (Dale County, AL) with a conflicting NAICS 62111 record - but that sits
    # in the 1999 partition, and county is scoped to the most recent partition, which
    # has zero duplicates (verified per year across all 26 partitions). A relaxed
    # test here would only license new duplicates in the newest year.
    uniq_test = {
        "dbt_utils.unique_combination_of_columns": _scope(
            {"combination_of_columns": uniq}, table
        )
    }
    m = {
        "name": f"{DATASET}__{table}",
        "description": TABLE_DESC[table],
        "tests": [
            uniq_test,
            {
                "not_null_proportion_multiple_columns": _scope(
                    {"at_least": 1.0, "ignore_values": ignore}, table
                )
            },
        ],
        "columns": [],
    }
    for c in cols:
        entry = {"name": c["name"], "description": c["description"]}
        t = col_tests(table, c["name"])
        if t:
            entry["tests"] = t
        m["columns"].append(entry)
    return m


TABLE_DESC = {
    "national": "County Business Patterns, national totals: establishments, employment (week of March 12) and payroll by 6-digit NAICS industry and legal form of organization, with breakdowns by establishment employment-size class, annual 1998-2023.",
    "state": "County Business Patterns, by state: establishments, employment (week of March 12) and payroll by state, NAICS industry and legal form of organization, with breakdowns by establishment employment-size class, annual 1998-2023.",
    "county": "County Business Patterns, by county: establishments, employment (week of March 12) and payroll by county and NAICS industry, with establishment counts by employment-size class, annual 1998-2023. Two documented source characteristics: (a) a county part of 999 marks establishments not allocated to a specific county (statewide), and early years use the county vintage of their reference year (pre-2022 Connecticut counties, dissolved Alaska boroughs, Dade County FL), so about 1.2% of rows do not match the current county directory; (b) the 1999 source file repeats a block for FIPS 01045 (Dale County, AL), leaving one conflicting duplicate key at NAICS 62111 out of 48.7M rows.",
    "dicionario": "Dictionary of coded values used across the us_census_cbp tables (legal form of organization, NAICS vintage, and disclosure/noise flags).",
}


def dicionario_sql():
    cols = ["id_tabela", "nome_coluna", "chave", "cobertura_temporal", "valor"]
    casts = ",\n".join(f"    safe_cast({c} as string) {c}" for c in cols)
    return f"""{{{{
    config(
        schema="{DATASET}",
        alias="dicionario",
        materialized="table",
    )
}}}}


select
{casts}
from {{{{ set_datalake_project("{DATASET}_staging.dicionario") }}}}
    as t
"""


def dicionario_entry():
    desc = {
        "id_tabela": "Table name the key belongs to",
        "nome_coluna": "Column name the key belongs to",
        "chave": "Coded value as stored in the data",
        "cobertura_temporal": "Temporal coverage of the key",
        "valor": "Human-readable label of the coded value",
    }
    # (id_tabela, nome_coluna, chave) is the dictionary's logical key: one label per
    # coded value per column per table. All five columns are always populated.
    return {
        "name": f"{DATASET}__dicionario",
        "description": TABLE_DESC["dicionario"],
        "tests": [
            {
                "dbt_utils.unique_combination_of_columns": {
                    "combination_of_columns": [
                        "id_tabela",
                        "nome_coluna",
                        "chave",
                    ]
                }
            },
            {"not_null_proportion_multiple_columns": {"at_least": 1.0}},
        ],
        "columns": [
            (
                {"name": k, "description": v}
                if k == "cobertura_temporal"
                else {"name": k, "description": v, "tests": ["not_null"]}
            )
            for k, v in desc.items()
        ],
    }


def main():
    schema = {"version": 2, "models": []}
    for table in ["national", "state", "county"]:
        cols = read_sheet(table)
        (MODELS / f"{DATASET}__{table}.sql").write_text(gen_sql(table, cols))
        schema["models"].append(model_entry(table, cols))
        print(f"wrote {DATASET}__{table}.sql ({len(cols)} cols)")
    (MODELS / f"{DATASET}__dicionario.sql").write_text(dicionario_sql())
    schema["models"].append(dicionario_entry())
    print(f"wrote {DATASET}__dicionario.sql")
    with open(MODELS / "schema.yml", "w") as f:
        f.write("---\n")
        yaml.safe_dump(
            schema,
            f,
            sort_keys=False,
            default_flow_style=False,
            width=100,
            allow_unicode=True,
        )
    print("wrote schema.yml")


if __name__ == "__main__":
    main()
