# us_cfpb_hmda — Onboarding Context

Home Mortgage Disclosure Act (HMDA) loan/application-level records (LAR).
Publisher: Consumer Financial Protection Bureau (CFPB) / Federal Financial Institutions
Examination Council (FFIEC). US federal agency data.

## License
US federal government work → **public domain** (17 U.S.C. §105). No stated usage
restrictions; availability = free/open. License/availability resolved at discover step
(cf. other `us_*` public-domain datasets).

## Organization
Likely backend slug **`cfpb`** (US org slugs drop the country prefix — cf. `bls`, `census`,
`ed`). FFIEC co-publishes the combined national file. NOT yet verified against backend →
discover step resolves or creates (dev).

## Coverage
- Temporal: **2007–2024**, annual. Geographic: United States (area = `us`).
- Update cadence: annual; year YYYY snapshot released ~May of YYYY+1 (lag ~7 months).
- Two schema-incompatible eras → two tables:
  - **Legacy 2007–2017** — 43-field LAR, keyed on `respondent_id` + `agency_code`.
  - **Modern 2018–2024** — 99-field LAR, LEI-keyed.
- Scale: full panel ≈ 250M+ rows. Per-year all-records counts captured at download.

## Sources (curl-verified 2026-08-12)

| Era | Years | Request URL template | Resolves to / format |
|-----|-------|----------------------|----------------------|
| Modern | 2018–2024 | `https://ffiec.cfpb.gov/v2/data-browser-api/view/nationwide/csv?years={YEAR}` | **HTTP 301 →** `https://files.ffiec.cfpb.gov/data-browser/datasets/{YEAR}/filtered-queries/.../<hash>.csv` (comma-delimited CSV, 99 cols, string codes). Follow redirects (`curl -L`). Verified for 2018 & 2023. |
| Legacy | 2007–2017 | `https://files.consumerfinance.gov/hmda-historic-loan-data/hmda_{YEAR}_nationwide_all-records_codes.zip` | zipped comma-delimited CSV, raw **numeric codes**, 43 legacy cols. Verified (HTTP 206, PK zip magic) for 2007, 2010, 2017. |

- Modern: use the **`/view/nationwide/csv`** variant — the plain `/view/csv` returns HTTP 400 without a geo/LEI filter.
- Legacy filename variants (same dir): `_codes.zip` (numeric codes — **preferred**, feeds dicionario), `_labels.zip` (inline labels). Do NOT use the `first-lien-owner-occupied-1-4-family-records` subsets — they are filtered.

## Modern header — AUTHORITATIVE field order (verified from 2023 file, 99 fields)
This supersedes the documentation summary (which wrongly listed `combined_loan_to_value_ratio`
and `ageapplicant`). Real header saved to `code/modern_header.txt`. Field order:

```
activity_year, lei, derived_msa-md, state_code, county_code, census_tract,
conforming_loan_limit, derived_loan_product_type, derived_dwelling_category,
derived_ethnicity, derived_race, derived_sex, action_taken, purchaser_type, preapproval,
loan_type, loan_purpose, lien_status, reverse_mortgage, open-end_line_of_credit,
business_or_commercial_purpose, loan_amount, loan_to_value_ratio, interest_rate,
rate_spread, hoepa_status, total_loan_costs, total_points_and_fees, origination_charges,
discount_points, lender_credits, loan_term, prepayment_penalty_term, intro_rate_period,
negative_amortization, interest_only_payment, balloon_payment, other_nonamortizing_features,
property_value, construction_method, occupancy_type, manufactured_home_secured_property_type,
manufactured_home_land_property_interest, total_units, multifamily_affordable_units, income,
debt_to_income_ratio, applicant_credit_score_type, co-applicant_credit_score_type,
applicant_ethnicity-1..5, co-applicant_ethnicity-1..5, applicant_ethnicity_observed,
co-applicant_ethnicity_observed, applicant_race-1..5, co-applicant_race-1..5,
applicant_race_observed, co-applicant_race_observed, applicant_sex, co-applicant_sex,
applicant_sex_observed, co-applicant_sex_observed, applicant_age, co-applicant_age,
applicant_age_above_62, co-applicant_age_above_62, submission_of_application,
initially_payable_to_institution, aus-1..5, denial_reason-1..4, tract_population,
tract_minority_population_percent, ffiec_msa_md_median_family_income,
tract_to_msa_income_percentage, tract_owner_occupied_units, tract_one_to_four_family_homes,
tract_median_age_of_housing_units
```

Normalization for BigQuery column names: hyphens→underscores (`derived_msa-md`→`derived_msa_md`,
`open-end_line_of_credit`→`open_end_line_of_credit`, `applicant_ethnicity-1`→`applicant_ethnicity_1`,
`co-applicant_*`→`co_applicant_*`, `aus-1`→`aus_1`, `denial_reason-1`→`denial_reason_1`);
`activity_year`→`year` (INT64 partition). `original_name` keeps the raw hyphenated header.

## Legacy header — documented 43-field order (confirm exact names at download)
`activity_year, respondent_id, agency_code, loan_type, property_type, loan_purpose,
owner_occupancy, loan_amount_000s, preapproval, action_taken, msa_md, state_code,
county_code, census_tract, applicant_ethnicity, co_applicant_ethnicity,
applicant_race_1..5, co_applicant_race_1..5, applicant_sex, co_applicant_sex,
income_000s, purchaser_type, denial_reason_1..3, rate_spread, hoepa_status, lien_status,
[+ Census tract fields: population, minority_population, ffiec_median_family_income,
tract_to_msamd_income, number_of_owner_occupied_units, number_of_1_to_4_family_units,
edit_status, sequence_number]`. Exact raw names captured from the unzipped `_codes.zip`
header at step 3 (legacy raw names differ from modern, e.g. `as_of_year`, `owner_occupancy`,
`loan_amount_000s`).

## Code sheets / dictionaries (dicionario source)
- Modern (2018–2024): values embedded in the field spec —
  https://ffiec.cfpb.gov/documentation/publications/loan-level-datasets/lar-data-fields/
- Legacy (2007–2017): PDFs under `https://files.consumerfinance.gov/hmda-historic-data-dictionaries/`
  (`lar_record_codes.pdf`, `lar_record_format.pdf`); `_labels.zip` also carries code→label inline.
- Field spec (legacy v1): https://ffiec.cfpb.gov/documentation/publications/loan-level-datasets/lar-data-fields-v1

## Themes & tags
- Themes: `economia`/`economy` (add `finance` if present in vocabulary).
- Tags (resolve/create at discover): `mortgage`, `housing`, `credit`, `real-estate`,
  `fair-lending`, `banking`, `loan`, `demographics`.

## Drive folder
- No existing `us_cfpb_hmda` architecture folder. Create under
  `Base dos Dados - Geral/Dados/Conjuntos/` (parent ID `1OYYGPFPW6WuXNInxzitrX2iQuw2L_8xk`;
  sibling precedent `us_census_cbp` = `1okxMbHDrOIUXfoWGnmipUVb39vlc18bb`).

## Open items for downstream steps
1. Legacy exact raw column names + per-year record counts — capture at download (step 3).
2. Backend org `cfpb` + license/availability + geography directory (`br_bd_diretorios_us`)
   FK targets for `county_code`/`census_tract`/`state_code` — resolve at discover (step 8).
