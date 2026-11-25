# ruff: noqa: SIM115
"""Generate dbt artifacts for us_anes_time_series from columns.json + the parquet:
  models/us_anes_time_series/us_anes_time_series__cumulative.sql
  models/us_anes_time_series/us_anes_time_series__dicionario.sql
  models/us_anes_time_series/schema.yml
Also prints the dbt_project.yml entry to add.

Sparse columns (<5% non-null across the pooled file — legitimately year-specific)
are added to the not_null_proportion test's ignore_values so the test stays
meaningful for dense columns instead of failing on expected sparsity.
"""

import json
import subprocess
from pathlib import Path

import pyarrow.dataset as ds

ROOT = Path(__file__).resolve().parent.parent
MAN = json.load(open(ROOT / "code" / "build" / "columns.json"))
DS = ROOT / "us_anes_time_series"  # model dir == dataset dir
order = [m["name"] for m in MAN]
typ = {m["name"]: m["type"].lower() for m in MAN}

# ---- non-null fraction per column (for ignore_values) -----------------------
dset = ds.dataset(
    ROOT / "output" / "cumulative", format="parquet", partitioning="hive"
)
tbl = dset.to_table()
n = tbl.num_rows
sparse = []
for name in order:
    if name == "year":
        continue
    nn = n - tbl.column(name).null_count
    if nn / n < 0.05:
        sparse.append(name)
print(f"rows={n}  sparse(<5% non-null)={len(sparse)} added to ignore_values")

# ---- cumulative.sql ---------------------------------------------------------
casts = []
for name in order:
    t = typ[name]
    # Backtick identifiers so sqlfmt preserves the uppercase VCF codes (it
    # lowercases unquoted identifiers), keeping BigQuery columns in sync with
    # the registered metadata.
    casts.append(f"    safe_cast(`{name}` as {t}) `{name}`,")
casts[-1] = casts[-1].rstrip(",")
cum_sql = f"""{{{{
    config(
        schema="us_anes_time_series",
        alias="cumulative",
        materialized="table",
        partition_by={{
            "field": "year",
            "data_type": "int64",
            "range": {{"start": 1948, "end": 2029, "interval": 1}},
        }},
    )
}}}}


select
{chr(10).join(casts)}
from
    {{{{ set_datalake_project("us_anes_time_series_staging.cumulative") }}}}
    as t
"""
(ROOT / "us_anes_time_series__cumulative.sql").write_text(cum_sql)

# ---- dicionario.sql ---------------------------------------------------------
dic_sql = """{{
    config(
        schema="us_anes_time_series",
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
    {{ set_datalake_project("us_anes_time_series_staging.dicionario") }}
    as t
"""
(ROOT / "us_anes_time_series__dicionario.sql").write_text(dic_sql)

# ---- schema.yml -------------------------------------------------------------
# Emit the ignore_values key only when there is at least one sparse column;
# an empty list would render `ignore_values:` as null and break the macro.
nnp = "      - not_null_proportion_multiple_columns:\n          at_least: 0.05"
if sparse:
    ign = "\n".join(f"              - {c}" for c in sparse)
    nnp += "\n          ignore_values:\n" + ign
schema = f"""---
version: 2
models:
  - name: us_anes_time_series__cumulative
    description: >
      ANES Time Series Cumulative Data File (1948-2024): pooled cross-section
      respondents with harmonized variables (VCF codes) asked in three or more
      ANES Time Series studies. One row per respondent per study year.
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns: [year, VCF0006]
{nnp}
    columns:
      - name: year
        description: Year of study
        tests: [not_null]
      - name: VCF0006
        description: 'Study respondent number: year-level case ID'
        tests: [not_null]
      - name: VCF0006a
        description: Unique respondent number (cross-year ID for panel cases)
        tests: [not_null]
  - name: us_anes_time_series__dicionario
    description: >
      Dictionary of coded values for the cumulative table: for each categorical
      column, the code (chave) and its English label (valor) per the ANES codebook.
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns: [nome_coluna, chave, cobertura_temporal]
    columns:
      - name: nome_coluna
        description: Name of the categorical column described by the dictionary
        tests: [not_null]
      - name: chave
        description: Code or value taken by the categorical column
        tests: [not_null]
"""
(ROOT / "schema.yml").write_text(schema)

# Finalize formatting so re-running this script reproduces the committed files
# byte-for-byte (sqlfmt reflows the SQL and preserves the backticked identifiers;
# yamlfix normalizes the YAML). Falls back to pre-commit if a tool is absent.
sql_files = [
    str(ROOT / "us_anes_time_series__cumulative.sql"),
    str(ROOT / "us_anes_time_series__dicionario.sql"),
]
subprocess.run(["uv", "run", "sqlfmt", *sql_files], check=False)
subprocess.run(["uv", "run", "yamlfix", str(ROOT / "schema.yml")], check=False)

print("wrote cumulative.sql, dicionario.sql, schema.yml")
print(
    "\nAdd to dbt_project.yml under models: basedosdados:\n"
    "  us_anes_time_series:\n    +materialized: table\n    +schema: us_anes_time_series"
)
