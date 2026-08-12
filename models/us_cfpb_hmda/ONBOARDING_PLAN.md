# Onboarding Plan — us_cfpb_hmda

**Status:** APPROVED (2026-08-12)
**Dataset:** `us_cfpb_hmda` (Home Mortgage Disclosure Act, loan/application-level)
**Org:** CFPB / FFIEC (Consumer Financial Protection Bureau)
**License:** US federal government work → **public domain** (17 U.S.C. §105); no stated usage restrictions.
**Environment:** dev only (`basedosdados-dev`) through the verification checkpoint. Stop at checkpoint.
**Scratch:** `~/Downloads/us_cfpb_hmda_data/` (`input/`, `output/`). Process **one year at a time**; delete each archive after cleaning. Never in repo/Dropbox.

---

## Scope (confirmed)

- **Coverage:** Full **2007–2024**, two schema eras (incompatible → two tables).
- **Source file:** **Snapshot National Loan-Level Dataset** (2018+) and the equivalent historic combined LAR flat files (2007–2017). Combined national files, all institutions, privacy-modified, Census fields appended.

## Sources

| Era | Source | Host / mechanism |
|-----|--------|------------------|
| 2018–2024 | Snapshot National Loan-Level Dataset (combined LAR CSV per year) | `ffiec.cfpb.gov` data-publication; download via S3/`files.ffiec.cfpb.gov` or data-browser-api streaming CSV (`/v2/data-browser-api/view/csv?years=YYYY`, returns gzip). Downloader resolves the exact per-year URL from the page's network calls. |
| 2007–2017 | Historic LAR flat files + code sheets | `consumerfinance.gov/data-research/hmda/historic-data/`; dictionaries at `files.consumerfinance.gov/hmda-historic-data-dictionaries/`. |

Docs: LAR field spec (2018+) `ffiec.cfpb.gov/documentation/publications/loan-level-datasets/lar-data-fields/`; legacy spec `.../lar-data-fields-v1`.

## Tables (proposed — slugs adjustable at checkpoint)

1. **`loan_application_register`** — modern LAR, **2018–2024**, ~99 columns. Partition `year` (INT64). ~15–25M rows/yr.
2. **`loan_application_register_legacy`** — legacy LAR, **2007–2017**, ~43 columns. Partition `year` (INT64). ~15–30M rows/yr.
3. **`dicionario`** — value→label code sheets for every coded categorical field, both eras (`id_tabela`, `nome_coluna`, `chave`, `cobertura_temporal`, `valor`).

Two tables because the eras are schema-incompatible: 2018+ is LEI-keyed with expanded pricing/underwriting fields (99); 2007–2017 is `respondent_id`+`agency_code`-keyed with 43 fields (`property_type`, no LEI, no rate-spread expansions, no AUS/DTI/credit-score/loan-costs fields).

## Column design (per project rules)

- **English column names** (data is English). CFPB field names kept, normalized: hyphens→underscores (`open-end_line_of_credit`→`open_end_line_of_credit`, `aus-1`→`aus_1`, `applicant_ethnicity-1`→`applicant_ethnicity_1`, `co-applicant_*`→`co_applicant_*`); `activity_year`→**`year`** (partition); `ageapplicant`→`applicant_age`.
- **Column order** per style manual: partition (`year`) → geography (`state_code`, `county_code`, `census_tract`, `derived_msa_md`) → institution id (`lei`) → loan/derived/financial → applicant & co-applicant demographics → Census-appended tract fields.
- **Types by arithmetic meaning:**
  - **STRING + `covered_by_dictionary=yes`** — every coded categorical (`action_taken`, `loan_type`, `loan_purpose`, `preapproval`, `lien_status`, `hoepa_status`, race/ethnicity/sex codes, `denial_reason_*`, `aus_*`, `applicant_credit_score_type`, `purchaser_type`, `occupancy_type`, `construction_method`, derived_* categories, binned `debt_to_income_ratio`, `total_units` ranges, age ranges, agency_code, property_type, etc.).
  - **STRING (no dictionary)** — `lei`, `state_code`, `county_code` (FIPS), `census_tract`, `respondent_id`. Geography columns get a `directory_column` FK to `br_bd_diretorios_us` **only where a matching directory table exists** (resolved at discover); otherwise STRING with a note.
  - **FLOAT64 + measurement_unit** — dollar amounts (`loan_amount`, `income`, `property_value`, `total_loan_costs`, `origination_charges`, `discount_points`, `lender_credits`, `total_points_and_fees` → USD; `income` in USD thousands, noted), rates (`interest_rate`, `rate_spread` → percent), ratios (`combined_loan_to_value_ratio`, `tract_to_msa_income_percentage`, `tract_minority_population_percent`, `multifamily_affordable_units` → percent).
  - **INT64 + measurement_unit** — genuine counts/durations (`loan_term`, `intro_rate_period`, `prepayment_penalty_term` → months; `tract_population`, `tract_owner_occupied_units`, `tract_one_to_four_family_homes` → count; `ffiec_msa_md_median_family_income` → USD; `tract_median_age_of_housing_units` → years).
  - **`year`** stays INT64 (partition convention).
- Descriptions in **PT / EN / ES**; first letter capitalized; no trailing period on column descriptions.

## Observation levels & coverage

- **OL:** geographic — census tract (finest geography). Confirm entity at metadata step; add a second OL if an appropriate transaction/loan entity exists.
- **Coverage:** annual, area `us`. **AllFree** (annual data is not paywalled under the BD Pro rule; only monthly+ tables paywall). No Row Access Policies.

## Step sequence (per onboarding-workflow)

1. **context** — org CFPB/FFIEC, themes, tags, coverage, Drive folder.
2. **architecture** — two data sheets + dicionario sheet on Drive (via `architecture` agent); build from confirmed field specs. Architecture table is source of truth.
3. **download** — one year at a time to `~/Downloads/us_cfpb_hmda_data/input/`; resolve exact snapshot URLs; delete archive after each year cleaned.
4. **clean** — Python → partitioned parquet (`output/<table>/year=YYYY/data.parquet`), explicit `pa.Schema`, snappy. Reuse-friendly for later pipeline.
5. **upload** — parquet → `us_cfpb_hmda_staging.*` in `basedosdados-dev`.
6. **dbt** — `.sql` models (`safe_cast` every column, `set_datalake_project`) + `schema.yml`.
7. **validate** — dbt tests + row-count checks per year; fix or flag.
8. **discover** — resolve backend IDs (dev): org, themes, tags, entities, statuses; check `br_bd_diretorios_us` geography tables for FKs.
9. **metadata** — register in dev, dataset `status=under_review`; OLs, columns, cloud tables, coverage, updates.
9b. **publish dev/staging** — flip dev/staging dataset `under_review→published` for reviewer preview.
   **[PAUSE — verification checkpoint; wait for "approved"]**
10. **metadata --env prod** — promote metadata to prod (`under_review`) after approval.
11. **pr** — branch `data/us_cfpb_hmda`; PR with changelog. `.gitignore` covers scratch/output.
12. **pipeline** *(SEPARATE PR, after step 13 — only once prod table-approve has materialised the prod tables)* — annual Prefect refresh (source releases ~yearly); reuses dbt/architecture/cleaning transform.
13. **publish prod** — post-merge, after table-approve materialises `basedosdados.us_cfpb_hmda.*` and prod tables+metadata verified.
14. **cleanup** — delete `~/Downloads/us_cfpb_hmda_data/`.

## Risks / notes

- **Volume:** ~300M+ rows total across both tables. Fine for BigQuery (year-partitioned). At prod **table-approve**, guard the largest staging prefixes against the CI OOM (prepend a 0-row `00_header.parquet`) per `project_table_approve_parquet_header_oom`.
- **Legacy quirks:** 2009 rate-spread rule change (Q1–Q3 vs Q4); `respondent_id`+`agency_code` composite key; a 2017 ARID→LEI crosswalk exists (not ingested unless requested).
- **Translation:** all names/descriptions PT/EN/ES; consistent terminology across both tables.
- Commit after each logical unit; never commit data. Conventional commits `feat(us_cfpb_hmda): …`.
