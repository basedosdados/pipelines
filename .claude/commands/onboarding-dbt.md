---
description: Write DBT .sql and schema.yaml files for a Data Basis dataset
argument-hint: <dataset_slug> [--tables table1,table2]
---

Write DBT SQL models and schema.yaml for a Data Basis dataset following `pipelines/` conventions.

**Dataset:** $ARGUMENTS

## Step 1 — Read conventions from a neighboring dataset

Read 1–2 existing DBT files from a similar dataset as style reference. Pick a dataset with a similar structure (e.g. for a municipal finance dataset, read `br_cgu_orcamento_publico/`).

## Step 2 — Write SQL model files

One file per table: `models/<dataset_slug>/<dataset_slug>__<table_slug>.sql`

Template:
```sql
{{
    config(
        schema="<dataset_slug>",
        alias="<table_slug>",
        materialized="table",
        partition_by={
            "field": "<partition_col>",
            "data_type": "<int64|date>",
            "range": {"start": <start_year>, "end": <end_year + 5>, "interval": 1},
        },
    )
}}


select
    safe_cast(<col1> as <type1>) <col1>,
    safe_cast(<col2> as <type2>) <col2>,
    ...
from
    {{ set_datalake_project("<dataset_slug>_staging.<table_slug>") }}
    as t
```

Column order must match the architecture table exactly.

## Step 3 — Write schema.yaml

One file: `models/<dataset_slug>/schema.yml`

Template:
```yaml
---
version: 2
models:
  - name: <dataset_slug>__<table_slug>
    description: <description in Portuguese from architecture>
    tests:
      - not_null_proportion_multiple_columns:
          at_least: 0.05
    columns:
      - name: <col>
        description: <description in Portuguese>
        tests: [not_null]   # for primary key / partition columns
      - name: <directory_col>
        description: ...
        tests:
          - relationships:
              to: ref('<directory_table>')
              field: <field>
```

Rules:
- Add `not_null` test to partition columns and primary keys
- Add `relationships` test to any column with a `directory_column` in the architecture
- Add `not_null_proportion_multiple_columns` at 0.05 to every model
- Use Portuguese descriptions from architecture

## Step 4 — Check dbt_project.yml

Verify the dataset has an entry in `dbt_project.yml`. If not, add:
```yaml
<dataset_slug>:
  +materialized: table
  +schema: <dataset_slug>
```

## Step 5 — Dictionary model (if needed)

If a `dicionario` table exists, add its SQL model and schema entry. Pattern:
```sql
select
    safe_cast(id_tabela as string) id_tabela,
    safe_cast(nome_coluna as string) nome_coluna,
    safe_cast(chave as string) chave,
    safe_cast(cobertura_temporal as string) cobertura_temporal,
    safe_cast(valor as string) valor
from
    {{ set_datalake_project("<dataset_slug>_staging.dicionario") }}
    as t
```
