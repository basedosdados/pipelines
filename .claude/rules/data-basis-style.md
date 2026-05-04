# Data Basis Style Manual

Reference for all agents that design or validate column names, table structure, and descriptions.

## Style manual source

Always fetch the language-appropriate style manual before designing columns:

| Language | URL |
|----------|-----|
| Portuguese | `https://raw.githubusercontent.com/basedosdados/website/refs/heads/main/next/content/docs/pt/style_data.md` |
| English | `https://raw.githubusercontent.com/basedosdados/website/refs/heads/main/next/content/docs/en/style_data.md` |
| Spanish | `https://raw.githubusercontent.com/basedosdados/website/refs/heads/main/next/content/docs/es/style_data.md` |

Default language: Portuguese for Brazilian government datasets.

## Column naming conventions

- All column names: **snake_case**, lowercase, no accents.
- Standard prefixes and what they signal:

| Prefix | Meaning | Example |
|--------|---------|---------|
| `id_` | Numeric or string identifier | `id_municipio` |
| `sigla_` | Abbreviation / acronym | `sigla_uf` |
| `codigo_` | Administrative code | `codigo_cnuc` |
| `nome_` | Human-readable name | `nome_municipio` |
| `quantidade_` | Count | `quantidade_funcionarios` |
| `valor_` | Monetary or numeric value | `valor_transferido` |
| `percentual_` | Percentage (0–100) | `percentual_cobertura` |
| `proporcao_` | Proportion (0–1) | `proporcao_area` |
| `taxa_` | Rate | `taxa_crescimento` |
| `indice_` | Index | `indice_gini` |
| `data_` | Date field | `data_referencia` |
| `descricao_` | Free-text description | `descricao_atividade` |
| `tipo_` | Category type | `tipo_unidade` |

## Column ordering (strict)

1. **Partition columns** (temporal first, then geographic): `ano`, `mes`, `sigla_uf`, `id_municipio`
2. **Identifiers** (`id_*`, `sigla_*`, `codigo_*`)
3. **Descriptive columns** (all other columns)

## Standard partition columns

| Scope | Partition columns |
|-------|------------------|
| National, annual | `ano` |
| National, monthly | `ano`, `mes` |
| State-level | `ano`, `sigla_uf` |
| Municipal-level | `ano`, `sigla_uf`, `id_municipio` |

## Standard directory column mappings

Always declare these in the `directory_column` field of architecture tables:

| Column name | Directory FK |
|-------------|-------------|
| `ano` | `br_bd_diretorios_data_tempo.ano:ano` |
| `mes` | `br_bd_diretorios_data_tempo.mes:mes` |
| `sigla_uf` | `br_bd_diretorios_brasil.uf:sigla_uf` |
| `id_municipio` | `br_bd_diretorios_brasil.municipio:id_municipio` |
| `id_pais` | `br_bd_diretorios_mundo.pais:id_pais` |

## Descriptions

- Write all descriptions in **Portuguese, English, and Spanish**.
- When only Portuguese is available, translate to the other two using domain knowledge of Brazilian public administration and statistics.
- Be direct and technical: state what the column contains, its unit, and any relevant constraints.
- Do not use evaluative adjectives; prefer factual descriptions with units and ranges.

## Temporal coverage notation

Format: `START(INTERVAL)END`

| Notation | Meaning |
|----------|---------|
| `2004(1)2022` | Annual data, 2004–2022 |
| `2013(1)` | Annual data, from 2013, ongoing |
| *(empty)* | Same coverage as the parent table |

## Architecture table schema

Each architecture file (Google Sheet) has these columns, in this order:

| Column | Description |
|--------|-------------|
| `name` | BigQuery column name (snake_case) |
| `bigquery_type` | `INT64`, `STRING`, `FLOAT64`, `DATE`, etc. |
| `description` | Description in Portuguese |
| `temporal_coverage` | Empty = same as table; or explicit notation |
| `covered_by_dictionary` | `yes` / `no` |
| `directory_column` | BD directory FK: `<dataset>.<table>:<col>` |
| `measurement_unit` | e.g. `year`, `BRL`, `hectare` — blank if none |
| `has_sensitive_data` | `yes` / `no` |
| `observations` | Free-text notes |
| `original_name` | Column name in the raw source |

## Architecture table is the source of truth

When there is a conflict between raw data column names, DBT file conventions, and the architecture table, **the architecture table wins**. Update all other artifacts to match.
