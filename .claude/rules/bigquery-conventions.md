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

### Choose the type by arithmetic meaning

Type follows meaning, not the raw storage format. A numeric-looking value in the source does **not** justify INT64/FLOAT64.

- Use **INT64 / FLOAT64 only when arithmetic on the values is meaningful** — you would sum, average, difference, or otherwise compute with them. These are genuine quantities: counts, durations, ages, monetary amounts, rates, proportions, indices, statistical weights.
- **Every numeric (INT64/FLOAT64) column must carry a `measurement_unit`.** The single recognized exception is a dimensionless statistical/sampling weight (unit blank; note it in `observations`). If a numeric column has no sensible measurement unit, that is the tell that it is *not* a quantity — it is a category, flag, or identifier, and must be `STRING`.
- Encode as **STRING** every column where arithmetic is meaningless even though the source stores digits: categorical codes (labor-force status, activity code, education level), FIPS/geographic codes, boolean/flag fields (0/1, 1/2, yes/no), sequence/line numbers, and identifiers. Categorical/coded STRING columns take `covered_by_dictionary = yes` whenever a value→label set exists (record the labels in the `dicionario` table). Do not model 0/1 flags as INT64 — they are booleans stored as codes, so STRING (dictionary-covered) preserves the sentinel/"blank/refused" values.
- **Exception — partition columns:** `ano`/`year` stay INT64 (annual) or DATE, as required by the partitioning convention above, even though they read as labels.

Quick test for any numeric-looking column: *"Would summing or averaging these values across rows produce something meaningful, and can I name its unit?"* If no to either, it is STRING.

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
- Build an explicit `pa.Schema` for a stable column order across partitions, but make
  **every column STRING**. Staging is all-STRING by house convention, and the dbt model
  `safe_cast`s each column to its architecture type — so the staging schema carries order,
  not types.
- **Both upload paths must write all-STRING** — the one-shot onboarding upload *and* the
  Prefect-pipeline `upload_to_gcs` path. They share one staging dataset, so a typed onboarding
  external table left behind collides with the pipeline's later all-STRING overwrite: dbt then
  reads a stale typed table against string files (`has type BYTE_ARRAY which does not match the
  target cpp_type INT32`) or `safe_cast`s a still-typed column (`Invalid cast from INT64 to
  DATE`). Keeping both all-STRING removes the divergence. (`gcs.py::dump_header` also stringifies
  the pipeline's staging header, so typed parquet is rejected there regardless.)
- **Cast to string via arrow, never `astype(str)`** (which renders NULL as the literal `"nan"`,
  which `safe_cast` will not turn back into NULL); pass the architecture's real types through
  first so `year` serializes as `"1959"`, not `"1959.0"`. See "Staging parquet must be
  all-STRING" in `prefect-pipeline-conventions.md`.

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
