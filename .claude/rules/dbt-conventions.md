# DBT Conventions

Reference for all agents that write or run DBT models for Data Basis.

## Environment

- dbt-core `1.5.6` with `dbt-bigquery 1.5.9`
- Python `>=3.10,<3.11`; run via `uv run dbt ...` inside the `pipelines/` root.
- Default target: `dev` (reads/writes `basedosdados-dev`). Never specify `--target dev` explicitly — it is the default.

## File naming

SQL models: `models/<dataset_slug>/<dataset_slug>__<table_slug>.sql` (double underscore)

## The `set_datalake_project` macro

All staging references must use this macro:

```sql
from {{ set_datalake_project("<dataset_slug>_staging.<table_slug>") }} as t
```

**Never** use `set_datalake_project` for joins. Joins against production tables use the full path:

```sql
join basedosdados.<dataset_id>.<table_id> on ...
```

## SQL model template

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

- Column order must match the architecture table exactly.
- Use `safe_cast` for every column — never hard casts.
- Use Portuguese descriptions from architecture tables.

## Geometry columns

Cast WKT geometry to GEOGRAPHY (not STRING) in the DBT model:

```sql
st_geogfromtext(safe_cast(geometria as string), make_valid => true) geometria,
```

## `schema.yml` structure

One `schema.yml` per dataset directory:

```yaml
---
version: 2
models:
  - name: <dataset_slug>__<table_slug>
    description: >
      <description in Portuguese — use > block scalar for any multi-line
      description or any description that may contain a colon>
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns: [<partition_col>, <primary_key_col>]
      - not_null_proportion_multiple_columns:
          at_least: 0.05
    columns:
      - name: <col>
        description: <description in Portuguese>
        tests: [not_null]   # partition columns and primary keys only
      - name: <directory_col>
        description: ...
        tests:
          - relationships:
              to: ref('<directory_table>')
              field: <field>
```

**YAML rule:** Always use `>` block scalar for descriptions — bare scalars with `:` in continuation lines break YAML parsing.

## Standard tests

| Test | When to add |
|------|-------------|
| `not_null` | Partition columns and primary keys |
| `relationships` | Any column with a `directory_column` in the architecture |
| `dbt_utils.unique_combination_of_columns` | Every model (use partition + primary key) |
| `not_null_proportion_multiple_columns` at `0.05` | Every model |

## Handling sparse columns

Columns that are legitimately empty use `ignore_values` in the proportion test:

```yaml
- not_null_proportion_multiple_columns:
    at_least: 0.05
    ignore_values:
      - column_that_is_legitimately_empty
```

## Custom test for relaxed uniqueness

When a small proportion of duplicate keys is unavoidable:

```yaml
- custom_unique_combinations_of_columns:
    combination_of_columns: [col_a, col_b]
    proportion_allowed_failures: 0.05
```

Always document the exception in the model description.

## Incremental tests (large tables)

Scope tests to the most recent rows using `where`:

| Keyword | Columns used |
|---------|-------------|
| `__most_recent_year_month__` | `ano`, `mes` |
| `__most_recent_date__` | `data` |
| `__most_recent_year__` | `ano` |

```yaml
config:
  where: __most_recent_year_month__
```

## `dbt_project.yml` entry

Every dataset must have an entry:

```yaml
<dataset_slug>:
  +materialized: table
  +schema: <dataset_slug>
```

## Dictionary model pattern

If a `dicionario` table exists:

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

## Running dbt

```bash
# Run models
uv run dbt run --select <dataset_slug>

# Run tests
uv run dbt test --select <dataset_slug>

# Single model
uv run dbt run --select <dataset_slug>__<table_slug>
```
