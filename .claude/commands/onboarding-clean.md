---
description: Write and run data cleaning code to produce partitioned parquet output
argument-hint: <dataset_slug> <raw_data_path> [output_path]
---

Write and run data cleaning code for a Data Basis dataset.

**Dataset / paths:** $ARGUMENTS

## Folder structure

Raw data and output live **external to the `pipelines/` repo** (large files are
never committed). Cleaning code lives **inside** the repo under the model folder.

```text
<dataset_root>/          ← external working directory (e.g. ~/Downloads/<slug>/)
├── input/               ← raw files (CSV, Excel, JSON, etc.) — do not modify
└── output/
    └── <table_slug>/
        └── ano=<year>/sigla_uf=<uf>/   (municipio/UF tables)
        └── ano=<year>/                  (Brasil-level tables)

pipelines/models/<dataset_gcp_id>/code/   ← write cleaning scripts here
    └── clean.py    (one script per dataset if tables share raw source)
    └── clean_<table>.py  (one per table if they don't)
```

## Step 1 — Read architecture tables

Read the architecture tables from Drive (URLs from `databasis-architecture` output or ask the user). These define:
- Exact column names and types
- Partition columns
- Which columns are categorical (need dictionary)
- Directory column mappings

## Step 2 — Inspect raw data

Read the first 20 rows of each raw file to understand structure. Check:
- File format (CSV, Excel, JSON, fixed-width, etc.)
- Encoding: try `utf-8-sig` first, fall back to `latin1`. For files where column
  names with accents come out garbled under `latin1`, re-decode with
  `.encode("latin1").decode("utf-8", errors="replace")`.
- Column names and their mapping to architecture names
- Any header rows, footer rows, or skip rows
- Date formats
- Number formatting: Brazilian CSVs often use `.` as thousands separator and `,`
  as decimal separator (e.g. `"1.234,56"` = 1234.56). Strip `.` first, then
  replace `,` with `.` before calling `pd.to_numeric`.

## Step 3 — Write cleaning code

Write Python cleaning code. Use pandas or polars — choose whichever fits the data better (polars for large files or complex transformations; pandas otherwise).

Rules:
- **Start with a small subset** (1 year or the smallest available partition) before scaling
- Output column order must match architecture exactly
- Use `safe_cast` logic: coerce types with error handling, not hard casts
- For wide data: pivot to long format
- Partition outputs using `pyarrow` with the partition columns from the architecture
- Never modify input files

Standard column types:
- INT64: `pd.to_numeric(col, errors='coerce').astype('Int64')`
- FLOAT64: `pd.to_numeric(col, errors='coerce')`
- STRING: `.astype(str).str.strip().replace('nan', pd.NA)`
- DATE: `pd.to_datetime(col, errors='coerce').dt.date`

### Explicit pyarrow schema (required)

Always build an explicit `pa.Schema` and pass it to `pa.Table.from_pandas`. This
prevents INT64/FLOAT64 mismatches when some partitions have all-integer values
in columns that should be FLOAT64.

```python
import pyarrow as pa

def _build_schema() -> pa.Schema:
    fields = []
    for col in OUTPUT_COLUMNS:
        if col in partition_cols:
            continue
        if col in INT_COLS:
            fields.append(pa.field(col, pa.int64()))
        elif col in FLOAT_COLS:
            fields.append(pa.field(col, pa.float64()))
        elif col in DATE_COLS:
            fields.append(pa.field(col, pa.date32()))
        else:
            fields.append(pa.field(col, pa.string()))
    return pa.schema(fields)

_SCHEMA = _build_schema()

def write_partition(df, ...):
    table = pa.Table.from_pandas(data, schema=_SCHEMA, preserve_index=False)
    pq.write_table(table, out / "data.parquet", compression="snappy")
```

### Geometry columns

If the dataset includes a shapefile or WKT geometry:
- Convert to WKT using `geopandas`: ensure CRS is EPSG:4674 (SIRGAS 2000).
- Store as STRING in parquet (`pa.string()`).
- **Verify the join key** between the shapefile and tabular data — shapefile IDs
  and tabular IDs are often different systems (e.g. `cd_cnuc` vs `id_uc`).
  Inspect both before joining.
- In the DBT model, cast with `ST_GEOGFROMTEXT(col, make_valid => true)` and
  type the column as GEOGRAPHY, not STRING.

## Step 4 — Validate subset output

After running on the subset:
1. Check the parquet schema with `pq.read_schema(path)` — verify all column types
   match the architecture before uploading.
2. Verify column names match architecture exactly.
3. Check for unexpected nulls in primary key columns.
4. Print row counts and a sample.

Only proceed to full data after subset is verified. Ask the user to confirm.

## Step 5 — Dictionary table

If any column has `covered_by_dictionary: yes`, create a `dicionario` table:
- Schema: `id_tabela | nome_coluna | chave | cobertura_temporal | valor`
- One row per (table, column, key) combination
- All tables and columns in a single dictionary file
- Output to: `output/dicionario/dicionario.csv` (not partitioned)

## Step 6 — Scale to full data

Run on all years/partitions and report final row counts per partition.
