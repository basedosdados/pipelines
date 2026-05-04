# BigQuery Conventions

Reference for all agents that upload data or write SQL for Data Basis.

## Project mapping

| Environment | GCP project | Backend URL |
|-------------|-------------|-------------|
| dev | `basedosdados-dev` | `development.backend.basedosdados.org` |
| prod | `basedosdados` | `backend.basedosdados.org` |

Default: always use **dev** unless the user explicitly approves prod.

## Dataset ID vs slug

| Term | Format | Example | Used in |
|------|--------|---------|---------|
| Dataset slug | short identifier | `cnuc` | BD backend API calls |
| GCP dataset ID | `<org>_<slug>` | `br_mma_cnuc` | BigQuery, cloud table fields, staging paths |

Always pass the bare slug to `create_update_dataset` and other backend MCP tools.

## Partitioning

- Prefer `ano` (INT64) for annual data.
- Use `DATE` partitioning only when the data has a `data` column with full dates.
- Partition range: `start = <earliest_year>`, `end = <latest_year + 5>`, `interval = 1`.

```sql
partition_by={
    "field": "ano",
    "data_type": "int64",
    "range": {"start": 2000, "end": 2030, "interval": 1},
}
```

## Type casting

Always use `safe_cast` — never hard casts. Common patterns:

| Target type | Cast expression |
|-------------|----------------|
| INT64 | `safe_cast(col as int64)` |
| FLOAT64 | `safe_cast(col as float64)` |
| STRING | `safe_cast(col as string)` |
| DATE | `safe_cast(col as date)` |
| GEOGRAPHY | `st_geogfromtext(safe_cast(col as string), make_valid => true)` |

## Staging reference pattern

Raw data lands in `<gcp_dataset_id>_staging.<table_slug>` in the dev project. Always reference staging tables via the `set_datalake_project` dbt macro:

```sql
from {{ set_datalake_project("<gcp_dataset_id>_staging.<table_slug>") }}
```

Production joins bypass the macro and use the full path:

```sql
join basedosdados.<gcp_dataset_id>.<table_slug> on ...
```

## Parquet output conventions

- Format: Snappy-compressed Parquet (`compression="snappy"`)
- Partitioned using `pyarrow` with `partition_cols` from the architecture
- Path pattern (relative to dataset root):
  - Municipal: `output/<table_slug>/ano=<year>/sigla_uf=<uf>/data.parquet`
  - State: `output/<table_slug>/ano=<year>/sigla_uf=<uf>/data.parquet`
  - National: `output/<table_slug>/ano=<year>/data.parquet`
- Always build an explicit `pa.Schema` to prevent INT64/FLOAT64 type mismatches across partitions.

## Upload prerequisites

- `~/.basedosdados/config.toml` must exist (run `basedosdados config init` if missing).
- `GOOGLE_APPLICATION_CREDENTIALS` must point to a valid service account key.
- GCS bucket is requester-pays — monkey-patch `gcs.Client.bucket` with `user_project=BILLING_PROJECT`.

## Upload sequence per table

1. Instantiate `bd.Table(table_id=<table_slug>, dataset_id=<gcp_dataset_id>)`
2. Delete stale GCS staging prefix before upload (avoids BQ partition key conflicts)
3. `table.create(if_exists="replace")`
4. `table.upload(path, if_exists="replace")`
5. Verify row count after upload

Never continue to subsequent tables if one fails — report the error and wait for user confirmation.
