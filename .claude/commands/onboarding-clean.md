---
description: Write and run data cleaning code to produce partitioned parquet output
argument-hint: <dataset_slug> <raw_data_path> [output_path]
---

Write and run data cleaning code for a Data Basis dataset.

**Dataset / paths:** $ARGUMENTS

## Folder structure

Work in a folder **external to the `pipelines/` repo**:

```text
<dataset_root>/
├── input/          ← raw files (CSV, Excel, JSON, etc.) — do not modify
├── output/
│   └── <table_slug>/
│       └── ano=<year>/sigla_uf=<uf>/   (municipio/UF tables)
│       └── ano=<year>/                  (Brasil-level tables)
└── code/
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
- Encoding (UTF-8, ISO-8859-1, etc.)
- Column names and their mapping to architecture names
- Any header rows, footer rows, or skip rows
- Date formats

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

## Step 4 — Validate subset output

After running on the subset:
1. Verify column names match architecture exactly
2. Verify types are correct
3. Check for unexpected nulls in primary key columns
4. Print row counts and a sample

Only proceed to full data after subset is verified. Ask the user to confirm.

## Step 5 — Dictionary table

If any column has `covered_by_dictionary: yes`, create a `dicionario` table:
- Schema: `id_tabela | nome_coluna | chave | cobertura_temporal | valor`
- One row per (table, column, key) combination
- All tables and columns in a single dictionary file
- Output to: `output/dicionario/dicionario.csv` (not partitioned)

## Step 6 — Scale to full data

Run on all years/partitions and report final row counts per partition.
