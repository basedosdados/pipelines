# us_bls_cpi — Onboarding Plan

US Consumer Price Index (CPI), U.S. Bureau of Labor Statistics.

## Source

- Landing page: https://www.bls.gov/cpi/data.htm
- Flat files (authoritative, machine-readable, monthly refresh):
  - CPI-U: https://download.bls.gov/pub/time.series/cu/
  - CPI-W: https://download.bls.gov/pub/time.series/cw/
- Format: BLS `time.series` tab-delimited flat files.
- Access note: `download.bls.gov` returns **403 without a browser User-Agent**. All
  fetches must send a UA string that includes a contact email (BLS policy), e.g.
  `Mozilla/5.0 ... rdahis@basedosdados.org`.
- License: US federal government work → **public domain** (US Government Work).

### File anatomy (per survey directory)

| File | Role |
|------|------|
| `<s>.series` | series_id, area_code, item_code, seasonal, periodicity_code, base_code, base_period, series_title, begin/end year+period |
| `<s>.data.N.*` | series_id, year, period, value, footnote_codes (the observations) |
| `<s>.area` | area_code → area_name (+ display_level, sort_sequence) |
| `<s>.item` | item_code → item_name (+ display_level, sort_sequence) |
| `<s>.period` | M01–M12 (months), M13 (annual avg), S01–S03 (semiannual) |
| `<s>.seasonal` | S = Seasonally Adjusted, U = Not Seasonally Adjusted |
| `<s>.base` / `<s>.periodicity` | base + periodicity code lookups |

`series_id` = `CU`/`CW` + seasonal(`S`/`U`) + periodicity(`R`/`S`) + area(4) + item(rest),
e.g. `CUUR0000SA0` = CPI-U, not-seasonally-adjusted, monthly, US city average, All items.
Cleaning resolves dimensions by **joining data → `.series`** (authoritative), not by
string-splitting the id.

Coverage: monthly from **1947**; CPI-W similar. ~8,100 CPI-U series, ~2,000 CPI-W series.

## Scope decisions (approved by user, 2026-07-16)

1. **Surveys:** CPI-U (`cu`) + CPI-W (`cw`). Distinguished by a `survey` column.
2. **Derived rates:** store the published `index_value` **and** compute
   `monthly_change` and `twelve_month_change` per series (matches `br_ibge_ipca`).
3. **Frequencies → separate tables:** monthly, annual (M13), semiannual (S01/S02).
4. **Language:** English table slugs and column names (US dataset; consistent with
   `us_bls_atus`, `us_ed_ipeds`). Partition on `year` (INT64), not `ano`.

## Tables

All partitioned by `year` (INT64). `area_id`, `item_id`, `survey`,
`seasonal_adjustment` are STRING codes covered by the `dicionario` table.
`index_value` is FLOAT64 (index number); base differs by series, carried in
`base_period`. Change columns are FLOAT64 percentages (derived).

### 1. `monthly` — period M01–M12
Key: `(year, month, survey, seasonal_adjustment, area_id, item_id)`

| column | type | notes |
|--------|------|-------|
| year | INT64 | partition |
| month | INT64 | 1–12 |
| survey | STRING | CPI-U / CPI-W (dict) |
| seasonal_adjustment | STRING | S / U (dict) |
| area_id | STRING | cu/cw area code (dict) |
| item_id | STRING | cu/cw item code (dict) |
| base_period | STRING | e.g. `1982-84=100` |
| index_value | FLOAT64 | CPI index |
| monthly_change | FLOAT64 | % vs previous month (derived) |
| twelve_month_change | FLOAT64 | % vs same month prior year (derived) |

### 2. `annual` — period M13 (annual average)
Key: `(year, survey, seasonal_adjustment, area_id, item_id)`
Columns as monthly minus `month`/`monthly_change`/`twelve_month_change`, plus
`annual_change` FLOAT64 (% vs prior year annual average).
Note: `M13` only (annual average of monthly series). `S03` (annual average of
semiannual series) is excluded — it collides on the natural key with `M13` where
an area has both a monthly and semiannual series, and equals the mean of the two
halves already in the `semiannual` table.

### 3. `semiannual` — period S01/S02
Key: `(year, half, survey, seasonal_adjustment, area_id, item_id)`
Adds `half` INT64 (1/2); `index_value` + `semiannual_change` FLOAT64.

### 4. `dicionario` — standard BD dictionary
Value→label maps for `survey`, `seasonal_adjustment`, `area_id`, `item_id`
(union of CPI-U and CPI-W dimension files, deduplicated). Uses the platform-standard
`dicionario` schema (columns `id_tabela`, `nome_coluna`, `chave`,
`cobertura_temporal`, `valor`) so the website's `covered_by_dictionary` linkage
renders — matching `us_bls_atus`.

### Deferred (noted, not in MVP)
- Item/area **hierarchy** (`display_level`, `sort_sequence`) — enables the CPI tree
  (All items → Food → Food at home → …). Candidate future column or reference table.
- Mapping CPI `area_id` to a US geography directory once `br_bd_diretorios_us` covers
  CPI regions/metros.
- C-CPI-U (`su`) survey.

## Step sequence (per .claude/rules/onboarding-workflow.md)

1. context — done inline (this doc)
2. architecture — build 4 Google Sheets (one per table) on Drive
3. download — pull cu.* and cw.* flat files → input/ (UA required)
4. clean — Python: join data↔series↔period, split by frequency, compute changes →
   partitioned parquet
5. upload — parquet → BigQuery dev (`basedosdados-dev.us_bls_cpi_staging`)
6. dbt — 4 models + schema.yml
7. validate — dbt tests + spot-check against published headline CPI numbers
8. discover — resolve backend reference IDs (dev)
9. metadata — register in dev backend
   **[PAUSE — verification checkpoint, await "approved"]**
10. metadata --env prod
11. pr — open PR with changelog

## Cleaning validation (2026-07-16)

| Table | Rows | Dup keys | Years | Null index |
|-------|------|----------|-------|------------|
| monthly | 2,638,757 | 0 | 1913–2026 | 0.15% (BLS `-` markers) |
| annual | 229,016 | 0 | 1913–2025 | 0.00% |
| semiannual | 392,891 | 0 | 1984–2026 | 0.00% |
| dicionario | 1,386 | — | — | — |

Spot-checks against published CPI-U All items, US city average, NSA
(`CUUR0000SA0`, base 1982-84=100): 2024 annual average 313.689 / +2.95%;
2022-06 index 296.311 / +9.06% (peak); 2020-12 +1.36%. All match BLS.
Duplicate raw observations (~706k across both surveys; series cross-listed in
multiple by-group data files, identical values) are deduplicated on
`(series_id, year, period)`.

## Monthly update (Prefect) — future

Design cleaning idempotent and re-runnable so a Prefect flow can: re-fetch `cu`/`cw`
flat files monthly, rebuild parquet, re-upload with `if_exists="replace"`, rerun dbt.
Not built in this pass; noted for a follow-up.

## Meta-goal

First onboarding driven with Claude. Capture friction and lessons into
`.claude/agents/*` and `.claude/skills/*` as we go (e.g. BLS UA requirement,
English-naming rule for non-BR datasets, join-on-.series pattern).
