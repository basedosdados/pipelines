---
description: Fetch or create architecture tables for a Data Basis dataset
argument-hint: <dataset_slug> [drive_folder_path]
---

Fetch or create architecture tables for a Data Basis dataset. Each table in the dataset gets one Google Sheets file.

**Dataset:** $ARGUMENTS

## Step 0 — Fetch the Data Basis style manual

Determine the dataset's core language from context (default: Portuguese for Brazilian government datasets).

Fetch the appropriate style manual with WebFetch:
- Portuguese: `https://raw.githubusercontent.com/basedosdados/website/refs/heads/main/next/content/docs/pt/style_data.md`
- English: `https://raw.githubusercontent.com/basedosdados/website/refs/heads/main/next/content/docs/en/style_data.md`
- Spanish: `https://raw.githubusercontent.com/basedosdados/website/refs/heads/main/next/content/docs/es/style_data.md`

Read and internalize the manual before proceeding. Key sections to apply:
- Column naming conventions (snake_case, standard prefixes like `id_`, `sigla_`, `nome_`, `quantidade_`, `valor_`)
- Standard partition columns (`ano`, `mes`, `sigla_uf`)
- Ordering rules: partition columns first, then identifiers, then descriptive columns
- Measurement unit conventions
- Standard directory column references

## Architecture table schema

Each file has these columns (in order):

| Column | Description |
|--------|-------------|
| `name` | BQ column name (snake_case) |
| `bigquery_type` | BQ type: INT64, STRING, FLOAT64, DATE, etc. |
| `description` | Description in Portuguese |
| `temporal_coverage` | Empty = same as table; different = explicit, e.g. `2013(1)` or `2013(1)2022` |
| `covered_by_dictionary` | `yes` / `no` |
| `directory_column` | BD directories FK: `<dataset>.<table>:<column>`, e.g. `br_bd_diretorios_brasil.municipio:id_municipio` |
| `measurement_unit` | e.g. `year`, `BRL`, `hectare` — blank if none |
| `has_sensitive_data` | `yes` / `no` |
| `observations` | Free text notes |
| `original_name` | Column name in the raw source |

**Temporal coverage notation:** `START(INTERVAL)END` — e.g. `2004(1)2022` = annual from 2004 to 2022. `2013(1)` = from 2013, ongoing. Empty = same as the table's coverage.

## Step 1 — Check if architecture files already exist

Use the `databasis-workspace` Google Drive MCP to check the Drive folder:
`BD/Dados/Conjuntos/<dataset>/architecture/`

If files exist: read and validate them. Report any missing required columns or schema mismatches.

## Step 2 — Ask design questions (if creating new tables)

Before creating architecture tables, ask the user:

1. **Format:** Should tables be in long format? (Data Basis default: yes — each row = one observation)
2. **Partition columns:** What columns partition the data? (default: `ano`, `sigla_uf`; Brasil-level: `ano` only)
3. **Unit of observation:** What does one row represent? (e.g. "one municipality-year-account")
4. **Categorical columns:** Which columns have a finite set of categories that need a dictionary?
5. **Directory columns:** Which columns link to BD standard directories (municipalities, states, time)?

## Step 3 — Infer or create schema

If creating new tables:
1. Read the first 20 rows of each raw data file
2. Infer column names, types, and candidate partition columns
3. Apply long-format transformation if the data is wide (one column per year → pivot to long)
4. Rename columns to follow the style manual conventions (snake_case, standard prefixes)
5. Order columns: partition columns first, then identifiers (`id_*`, `sigla_*`, `codigo_*`), then descriptive columns
6. Map known standard columns to BD directories:
   - `ano` → `br_bd_diretorios_data_tempo.ano:ano`
   - `mes` → `br_bd_diretorios_data_tempo.mes:mes`
   - `sigla_uf` → `br_bd_diretorios_brasil.uf:sigla_uf`
   - `id_municipio` → `br_bd_diretorios_brasil.municipio:id_municipio`

## Step 4 — Translate descriptions

Architecture files have descriptions in Portuguese. Translate all descriptions to English and Spanish. The translation should be accurate and use the correct technical terminology for Brazilian public finance / the relevant domain.

## Step 5 — Save to Drive

Save each architecture table as a Google Sheet in:
`BD/Dados/Conjuntos/<dataset>/architecture/<table_slug>.xlsx`

Use `mcp__databasis-workspace__create_spreadsheet` and `mcp__databasis-workspace__modify_sheet_values`.

## Step 6 — Output

Return a summary listing all tables found/created and the Drive URLs for each architecture file. Store these URLs — they are needed by `databasis-metadata`.
