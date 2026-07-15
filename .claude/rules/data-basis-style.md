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

### Shared entities get their own directory

Any entity that recurs across datasets — geography (state, county, metro area), **industry**, **occupation**, institutions, people — belongs in a directory table, not repeated inline. In every non-directory dataset that references such an entity:

- Keep the entity's ID column as **STRING** with **`covered_by_dictionary = no`** (a directory, not a per-dataset dictionary, is the source of truth).
- Set `directory_column` to the directory's foreign key (`<dataset>.<table>:<pk>`).
- If the appropriate directory does not exist yet, note the intended FK in `observations` and create the directory before metadata registration (an unresolved `directory_column` gets the whole column dropped at `upload_columns_from_sheet`).

Reserve `covered_by_dictionary = yes` for genuinely dataset-local coded values (survey flags, instrument-specific categories) whose labels live in that dataset's `dicionario`.

## Descriptions

- Write all descriptions in **Portuguese, English, and Spanish**.
- **Capitalize the first letter of every description** (in all three languages). Not "what was the main reason…" but "What was the main reason…".
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
| `bigquery_type` | `INT64`, `STRING`, `FLOAT64`, `DATE`, etc. — pick by **arithmetic meaning**, see rule below |
| `description` | Description in Portuguese |
| `temporal_coverage` | Empty = same as table; or explicit notation |
| `covered_by_dictionary` | `yes` / `no` — `yes` for categorical/coded columns whose value→label set lives in `dicionario` |
| `directory_column` | BD directory FK: `<dataset>.<table>:<col>` |
| `measurement_unit` | e.g. `year`, `BRL`, `hectare` — blank if none; **required on every INT64/FLOAT64 quantity** (except dimensionless weights) |

### Type by arithmetic meaning (not raw storage)

A value stored as digits is not automatically numeric. Assign `INT64`/`FLOAT64` **only when arithmetic is meaningful** (sum/average/difference) — genuine quantities (counts, durations, ages, money, rates, weights), and every such column must carry a `measurement_unit` (only dimensionless statistical weights are exempt). Numeric-looking columns where math is meaningless — categorical codes, FIPS/geo codes, boolean/flag fields (0/1, 1/2, yes/no), line/sequence numbers, identifiers — must be `STRING`, with `covered_by_dictionary = yes` when a label set exists. Rule of thumb: **if a numeric column has no sensible unit, it is a categorical/boolean → STRING.** Partition columns `ano`/`year` stay INT64 by the partitioning convention. See `bigquery-conventions.md` for the full rule.
| `has_sensitive_data` | `yes` / `no` |
| `observations` | Free-text notes |
| `original_name` | Column name in the raw source |

## When to set `covered_by_dictionary`

`covered_by_dictionary = yes` only when the column's **stored values are codified** — short codes that require a separate `dicionario` table to interpret. It is NOT a property of "is this categorical"; it is a property of "are the stored values codes or already readable".

- **`yes`** — stored values are opaque codes: `"M"`/`"F"` for gender, `1`/`2`/`3` for a Likert scale, a party/occupation/ISCED code, etc. The human meaning lives in the dictionary.
- **`no`** — stored values are already human-readable labels (`"Male"`/`"Female"`, `"Very good"`), free text, dates/times, identifiers, continuous measures, or a code that is resolved through a **directory** (`directory_column`) rather than the dicionario. Directory-referenced columns are always `no`.

Consequence: `yes` implies the column is STRING (a code), but STRING does NOT imply `yes` — a STRING of readable labels is `no`. The flag therefore depends on the cleaning decision of whether to keep raw codes or decode them to labels; decide that per dataset before filling the sheet.

## Architecture table is the source of truth

When there is a conflict between raw data column names, DBT file conventions, and the architecture table, **the architecture table wins**. Update all other artifacts to match.
