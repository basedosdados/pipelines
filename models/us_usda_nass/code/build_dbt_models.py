"""Generate the dbt .sql models for us_usda_nass from the architecture CSVs.

One model per fact table (per-grain), each reading its own all-STRING staging
table, safe_cast-ing to the architecture types, and de-duplicating to one row per
natural key (the QuickStats bulk carries ~0.006% benign duplicates once the
source-only columns are dropped). Plus the static dicionario model.

Run: ``uv run python models/us_usda_nass/code/build_dbt_models.py``
"""

import csv
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ARCH = ROOT / "code" / "architecture"

FACT_TABLES = [
    "survey_national",
    "survey_state",
    "survey_agricultural_district",
    "survey_county",
    "census_of_agriculture_national",
    "census_of_agriculture_state",
    "census_of_agriculture_county",
]

# Columns that form the natural key wherever present in a grain.
KEY_SET = {
    "year",
    "state_fips",
    "agricultural_district_code",
    "county_fips",
    "commodity",
    "commodity_class",
    "production_practice",
    "utilization_practice",
    "statistic_category",
    "unit",
    "domain",
    "domain_category",
    "reference_period",
}
# Name columns used only to make the dedup deterministic.
ORDER_NAMES = ["state_name", "county_name", "agricultural_district_name"]

_CAST = {
    "INT64": "safe_cast({c} as int64) {c}",
    "FLOAT64": "safe_cast({c} as float64) {c}",
    "STRING": "safe_cast({c} as string) {c}",
}


def read_arch(table):
    with open(ARCH / f"{table}.csv", encoding="utf-8") as f:
        return [(r["name"], r["bigquery_type"]) for r in csv.DictReader(f)]


def build_fact(table):
    cols = read_arch(table)
    names = [n for n, _ in cols]
    selects = ",\n        ".join(_CAST[t].format(c=n) for n, t in cols)
    key = [n for n in names if n in KEY_SET]
    order = ["value desc", "short_description"] + [
        n for n in ORDER_NAMES if n in names
    ]
    return f"""{{{{
    config(
        schema="us_usda_nass",
        alias="{table}",
        materialized="table",
        partition_by={{
            "field": "year",
            "data_type": "int64",
            "range": {{"start": 1840, "end": 2035, "interval": 1}},
        }},
    )
}}}}

-- One row per natural key: the QuickStats bulk has a few rows that collapse to
-- the same key once source-only columns (load_time, week_ending, begin/end
-- codes) are dropped -- a handful fully identical, the rest differing only in a
-- geography name functionally determined by a code already in the key.
with src as (
    select
        {selects}
    from {{{{ set_datalake_project("us_usda_nass_staging.{table}") }}}} as t
)

select * from src
qualify row_number() over (
    partition by {", ".join(key)}
    order by {", ".join(order)}
) = 1
"""


DICIONARIO = """{{
    config(
        schema="us_usda_nass",
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
from {{ set_datalake_project("us_usda_nass_staging.dicionario") }} as t
"""


def main():
    for t in FACT_TABLES:
        (ROOT / f"us_usda_nass__{t}.sql").write_text(
            build_fact(t), encoding="utf-8"
        )
        print(f"  wrote us_usda_nass__{t}.sql")
    (ROOT / "us_usda_nass__dicionario.sql").write_text(
        DICIONARIO, encoding="utf-8"
    )
    print("  wrote us_usda_nass__dicionario.sql")


if __name__ == "__main__":
    main()
