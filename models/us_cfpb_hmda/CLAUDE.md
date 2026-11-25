# us_cfpb_hmda — dataset context

Home Mortgage Disclosure Act (HMDA) loan/application-level mortgage records, published by
the **CFPB / FFIEC**. US federal government work → **public domain** (17 U.S.C. §105); no
usage restrictions. GCP id `us_cfpb_hmda` · org `cfpb` · backend slug `us_cfpb_hmda`.
The **architecture TSVs** in `code/` (mirrored to Drive sheets) are the source of truth for
column names, types, order, and the raw→clean mapping.

## What this dataset is
One row per mortgage loan/application reported by covered institutions, privacy-modified by
the Bureau, with Census-tract fields appended. Two schema-incompatible eras → two tables,
plus a shared value→label dictionary.

## Tables (one row per source record; `year` = INT64 hive partition)
- `loan_application_register` — **2018–2024**, modern post-2017 schema, **99 cols**, LEI-keyed. ~124.6M rows.
- `loan_application_register_legacy` — **2007–2017**, pre-Dodd-Frank schema, **42 cols**, `respondent_id`+`agency_code`-keyed. ~187.5M rows.
- `dicionario` — 719 rows; code→label (English `valor`) for every coded field in both tables.

## Sources (curl-verified)
- Modern 2018–2024: `https://ffiec.cfpb.gov/v2/data-browser-api/view/nationwide/csv?years={Y}`
  → HTTP 301 → CSV on `files.ffiec.cfpb.gov` (comma-delimited, 99 cols). Use the
  `/view/nationwide/csv` variant — plain `/view/csv` 400s without a geo/LEI filter. ~4.2 GB/yr.
- Legacy 2007–2017: `https://files.consumerfinance.gov/hmda-historic-loan-data/hmda_{Y}_nationwide_all-records_codes.zip`
  (raw numeric codes, zipped CSV). Field spec: `.../loan-level-datasets/lar-data-fields[-v1]`.

## Pipeline (`code/`, run via `uv run --with duckdb`)
`common.py` (arch parse; scratch = `~/Downloads/us_cfpb_hmda_data`, override `HMDA_DATA_DIR`),
`download.py`, `clean.py` (duckdb out-of-core), `upload.py` (→ `basedosdados-dev` staging),
`gen_dbt.py`, `gen_dicionario.py`, `run_backfill.py` (download→clean→delete, one year at a
time), `gen_arch_legacy.py` / `apply_geo_renames.py` / `rename_parquet.py` (one-off arch/data
patches). Backend IDs (org/tags/OLs/etc.) recorded in `code/discovered_ids.md`.
Output: `output/<table>/year=<YYYY>/data.parquet` (year excluded from file → hive partition).

Refresh is annual; a recurring Prefect pipeline is a **separate later PR** (reuses `clean.py`).

## Non-obvious things that will bite you
1. **duckdb memory.** `clean.py` MUST `SET preserve_insertion_order=false` (+ `threads=2`,
   `memory_limit='4GB'`). Without it, `COPY (SELECT … FROM read_csv(4–5 GB)) TO parquet`
   buffers the whole 26M×99 scan → tens of GB RAM. With it, peak ≈ 0.8 GB. Process one year
   at a time and delete each raw file after cleaning (`run_backfill.py` does this).
2. **Sentinels & units.** HMDA uses `NA`/`Exempt`/blank → `TRY_CAST` maps them to NULL for
   numeric columns. `income` (both eras) and legacy `loan_amount`/`income` are reported in
   **thousands of dollars** → multiplied ×1000 at clean time so `measurement_unit = USD` is
   truthful (see `common.MULTIPLY_1000`). Source has real outliers/sentinels (e.g. LTV =
   4e9, `property_value` = 2147483647); kept faithfully, not cleaned.
3. **Modern header is authoritative, not the docs.** The real 99-col header (`code/modern_header.txt`)
   corrects doc errors: it is `loan_to_value_ratio` (not `combined_…`) and `applicant_age`
   (not `ageapplicant`); hyphens → underscores on `derived_msa-md`, `aus-*`, `applicant_ethnicity-*`,
   `co-applicant_*`, `denial_reason-*`.
4. **Legacy is 45 cols raw → 42 kept.** `edit_status`, `sequence_number`,
   `application_date_indicator` are 100% empty in the public files → dropped.
5. **`derived_*` are readable labels, not codes** (`derived_race`="White", etc.) →
   `covered_by_dictionary = no`, excluded from `dicionario`.
6. **Geography naming differs by era + FK is PK-only.** Modern state is a 2-letter code →
   `state_abbreviation` (the directory `br_bd_diretorios_us.state` PK is `id_state`/FIPS, so
   the abbreviation has **no** directory FK). Legacy state is numeric FIPS → `state_id` →
   FK `state:id_state`. Both eras: `county_id` → `county:id_county`, `census_tract_id` →
   `census_tract_2020:id_census_tract`. `msa_md_id` has no clean directory match (no FK).
7. **`table-approve` OOM.** The action's header reader does `pd.read_parquet` on the first
   staging file whole; the first-alpha file here is ~660 MB (modern 2018) / ~258 MB (legacy
   2007) — borderline. If a merge run dies with "lost communication"/"canceled" (no
   traceback), prepend a 0-row `00_header.parquet` to the offending table's staging prefix
   and `gh run rerun` — see `[[project_table_approve_parquet_header_oom]]`.
8. **dbt grant is benign locally.** `dbt run` errors on `bigquery.tables.setIamPolicy` with
   the local dev SA, but the CTAS commits first so the tables exist; the CI/prod SA has the
   grant. `dbt test` runs fine against the built tables.

## Conventions (see `.claude/rules/`)
- Types by arithmetic meaning: coded/flag columns → STRING + `covered_by_dictionary=yes`;
  every INT64/FLOAT64 carries a `measurement_unit`.
- No `is_primary_key` on these (non-directory) tables; the logical key is enforced only in
  dbt (and HMDA has no unique key anyway — sparse fields drive a data-driven `ignore_values`
  in the not-null-proportion test, computed by `gen_dbt.py`).
- Annual data → **AllFree** (no BD Pro paywall).
- Backend env: metadata registered on **staging** (dev backend down) + **prod**; GCP upload
  targets `basedosdados-dev`; prod tables materialize from the merge via table-approve.
