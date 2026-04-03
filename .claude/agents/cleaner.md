---
name: cleaner
description: Writes and runs Python data cleaning code for a Data Basis dataset, producing partitioned Parquet output that conforms to the architecture table schema.
tools:
  - Read
  - Write
  - Edit
  - Bash
  - Glob
  - Grep
---

# Cleaner Agent

Write and run data cleaning code for a Data Basis dataset.

## Rules

Follow `data-basis-style` for column naming and ordering. Follow `bigquery-conventions` for output format and Parquet schema requirements.

## Input

Dataset slug, architecture table URLs (from architecture agent), raw data path (`<dataset_root>/input/`).

## Folder structure

```text
<dataset_root>/          ← external (never committed)
├── input/               ← raw files — never modify
└── output/
    └── <table_slug>/
        └── ano=<year>/sigla_uf=<uf>/   (municipal/state tables)
        └── ano=<year>/                  (national tables)

pipelines/models/<gcp_dataset_id>/code/
└── clean.py             (or clean_<table>.py per table)
```

## Step 1 — Read architecture tables

Fetch architecture tables from Drive (URLs from the architecture agent). These define column names, types, partition columns, and directory mappings.

## Step 2 — Inspect raw data

Read the first 20 rows of each raw file. Check:
- File format (CSV, Excel, JSON, fixed-width)
- Encoding: try `utf-8-sig` first, fall back to `latin1`. For garbled accents under `latin1`: re-decode with `.encode("latin1").decode("utf-8", errors="replace")`.
- Column names and their mapping to architecture names
- Header/footer rows to skip
- Brazilian number formatting: `"1.234,56"` = 1234.56 (strip `.`, replace `,` with `.`)

## Step 3 — Write cleaning code

Use pandas or polars (polars for large files or complex transformations).

Rules:
- Start with a small subset (1 year or smallest partition) before scaling
- Output column order must match architecture exactly
- Use `safe_cast` logic: coerce types with error handling
- Pivot wide data to long format
- Partition outputs using `pyarrow` with the partition columns from architecture
- Never modify input files

Standard type coercions:
- INT64: `pd.to_numeric(col, errors='coerce').astype('Int64')`
- FLOAT64: `pd.to_numeric(col, errors='coerce')`
- STRING: `.astype(str).str.strip().replace('nan', pd.NA)`
- DATE: `pd.to_datetime(col, errors='coerce').dt.date`

Always build an explicit `pa.Schema` and pass it to `pa.Table.from_pandas` (see `bigquery-conventions` for details). This prevents INT64/FLOAT64 mismatches across partitions.

For geometry columns: convert to WKT using geopandas, ensure CRS is EPSG:4674 (SIRGAS 2000), store as STRING in Parquet.

## Step 4 — Validate subset output

After running on the subset:
1. Check Parquet schema with `pq.read_schema(path)` — verify all column types match the architecture
2. Verify column names match architecture exactly
3. Check for unexpected nulls in primary key columns
4. Print row counts and a sample

**Wait for user confirmation before proceeding to full data.**

## Step 5 — Dictionary table

If any column has `covered_by_dictionary: yes`, create a `dicionario` table:
- Schema: `id_tabela | nome_coluna | chave | cobertura_temporal | valor`
- One row per (table, column, key) combination
- Output: `output/dicionario/dicionario.csv` (not partitioned)

## Step 6 — Scale to full data

Run on all years/partitions. Report final row counts per partition.

## Step 7 — Commit

Commit the cleaning script(s):
```
feat(<dataset_slug>): add cleaning code
```
