# ANES Time Series Cumulative Data File — Onboarding Plan

**Status:** DRAFT — awaiting approval
**Source:** https://electionstudies.org/data-center/anes-time-series-cumulative-data-file/
**Version onboarded:** February 5, 2026 (`anes_timeseries_cdf_csv_20260205`)

## Decisions locked with user
- **Scope:** Cumulative Data File (CDF) only → **one data table** + one `dictionary`. Individual-year studies and Panels/Pilots deferred.
- **Column names:** keep the `VCF####` codes verbatim (codebook is keyed to them; ANES warns the data "cannot be used with codebooks other than its own"). Meaning lives in the trilingual descriptions.
- **Data access:** files already downloaded to `input/` (no login needed after all).

## The data (verified)
- `anes_timeseries_cdf_csv_20260205.csv` — 156 MB, **73,745 respondents × 1,030 columns**.
- Column 1 is `Version` (constant string `ANES_CDF_VERSION:2026-Feb-5`) → **drop** in cleaning, record the version in table `observations`.
- Key structural columns: `VCF0004`=study year (1948–2024, biennial), `VCF0006`=case id within year, `VCF0006a`=unique cross-year id (natural key), weights `VCF0009x/y/z`, `VCF0010x/z`, `VCF0011x/z`.
- Row grain = **one respondent in one Time Series study year** (pooled cross-sections; panel/supplement-only cases already removed by ANES).

## Dataset identity
| Field | Value |
|---|---|
| Organization | `anes` (create; coverage area `us`) |
| GCP dataset id | `us_anes_time_series` |
| Backend slug | `time_series` |
| Data table | `cumulative` |
| Dictionary table | `dicionario` |

## Column model
- **Partition:** `year` INT64 = renamed `VCF0004`. This is the **single exception** to "keep VCF codes" — BD needs a standard INT64 partition column. Everything else keeps its VCF code.
- **Type by arithmetic meaning** (per repo rule): default is **STRING + `covered_by_dictionary = yes`** because almost every VCF variable is a coded categorical with sentinel missing codes (0/8/9/-8…). Promote to numeric **only** where arithmetic is meaningful and a unit exists:
  - INT64/FLOAT64: `year`, feeling thermometers (0–97/0–100), age (`VCF0101`), any dollar/income amounts, the weight variables (dimensionless — unit blank, noted in `observations`).
  - Everything else (party id, vote choice, education, region, race, hundreds more) → STRING, dictionary-covered.
- **Primary key / uniqueness:** enforced in dbt via `unique_combination_of_columns [year, VCF0006]` (equivalently `VCF0006a`). No `is_primary_key` flag (non-directory table, per repo rule).
- **Observation levels:** `individual` (person) + `year` (time). No geographic OL — the grain is the respondent, not a place.

## The dictionary (largest sub-task)
Built from the 619-page variable codebook (`…codebook_var_20260205.pdf`). For every STRING/coded column, extract the `Valid` and `Missing` value→label blocks into the standard `dicionario` schema: `id_tabela, nome_coluna, chave, cobertura_temporal, valor`. **Values stay in English only — one `chave` → one `valor`, no translation.**

## Build steps
1. **Org + context** — create organization `anes` (area `us`); finalize dataset/table descriptions (PT/EN/ES). License: ANES terms (research/statistical use, citation required, no re-identification) → map to closest BD availability/license.
2. **Architecture** — parse the codebook PDFs into:
   - `code/architecture/cumulative.csv` — 1,030 rows: `name` (VCF code / `year`), `bigquery_type`, `description` (PT), `covered_by_dictionary`, `directory_column`, `measurement_unit`, `original_name`, EN/ES columns.
   - `code/architecture/dictionary.csv` — the value-label long table.
   - Upload both to Google Sheets (architecture agent). **Checkpoint: I'll show you a ~20-variable sample of the parsed types + descriptions before generating all 1,030**, since the type/description classification is the main quality risk.
3. **Clean** (`code/to_parquet.py`) — read CSV as string, drop `Version`, rename `VCF0004`→`year`, cast per architecture, write partitioned parquet `output/cumulative/year=YYYY/data.parquet` (typed parquet — one-shot upload path).
4. **Upload** — `bd.Table` to `us_anes_time_series` staging in `basedosdados-dev`.
5. **dbt** — `us_anes_time_series__cumulative.sql`, `…__dictionary.sql`, `schema.yml`, `dbt_project.yml` entry.
6. **Validate** — `dbt run` + `dbt test` (unique combo, not-null on `year`, not-null-proportion with `ignore_values` for sparse vars).
7. **Discover IDs** → **register metadata in dev** (dataset `under_review`).
8. **[PAUSE — verification checkpoint]** — you approve → promote metadata to **prod** (`under_review`).
9. **PR** with changelog.
10. **Post-merge:** verify prod tables materialized → **publish** (flip dataset to `published`).

## Open / notable
- **Coverage granularity:** annual/biennial → coverage `1948(2)2024`, partition range start 1948 end 2029.
- **`year` directory link:** US datasets — confirm whether to link `year` to a BD time directory or leave unlinked (will match sibling `us_*` datasets).
- **Recurring pipeline (step 12):** CDF is re-released roughly annually. Out of scope now; a Prefect refresh pipeline could be added later.
- **Thermometers:** treated as INT64 (means are standard); flag if you'd prefer STRING.
