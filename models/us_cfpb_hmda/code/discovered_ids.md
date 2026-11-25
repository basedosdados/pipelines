# Discovered backend IDs (env=staging) — us_cfpb_hmda

Resolved 2026-08-12 by the discover step. Metadata registers on the **staging** backend
(dev backend down); GCP cloud tables point at **basedosdados-dev**.

## Status
- under_review: `47208305-325a-4da9-9222-ac6849405b78`  ← register with this
- published:    `e16221de-ac30-4926-83d3-de219998dab3`  ← staging publish (9b) / post-merge prod

## BigQuery types
- INT64:   `ace7bf06-4a20-4cee-a8ca-369cdf7a55a7`
- FLOAT64: `a5f6c9c2-b631-4583-8b4a-c1912c21b1f2`
- STRING:  `8488363a-4887-40a0-afd8-d0d5021538b1`

## Organization (created this run)
- cfpb: `ac6963a3-2d25-4178-a423-84afad6288b7` — "Consumer Financial Protection Bureau (CFPB)"

## Theme
- economics (primary): `ad6a413a-e882-4dd6-a497-8a62eec8511b`
- government (secondary): `6dd730bb-89ab-4dba-a1bf-a25ca1c35003`

## Tags (created this run)
- mortgage `a88b7ba0-80bf-4252-9682-ad78336c2633`
- housing `f531a28b-bd7d-4954-976b-1b903fc8a289`
- credit `7a05e71b-0bc0-421c-af53-d495e58ff9ba`
- real_estate `8ecf126f-75c1-4c68-af2a-b61cf59d71c2`
- fair_lending `55a8e9e4-8c4f-4cf8-bb78-b80943731a0b`
- banking `fad5f379-4f6b-4497-b929-5817caeb2c78`
- loan `8413b45b-b81c-4e5c-956b-cfd677fee99e`
- demographics `a31f7ad7-ed58-4dcf-a143-a8128ab48457`

## License / availability (US federal public domain)
- license.cc0: `7fb71004-2abe-4fc8-a258-e2aac27c71d9`
- availability.online: `dd396d7d-0264-4c1f-bf0d-6efe2dc89cbe`

## Entities (observation levels)
- census_tract: `a60054ba-3f9a-4772-818a-e11894404e9c`
- county: `01069a8a-5c20-4969-b295-a55082a828e8`
- state: `839765a7-9c7a-44bd-bb88-357cedba03f6`
- year: `e1bf146e-b6bb-4b65-bee7-c800876e80a5`
- transaction: `26887c0c-2a57-4808-b317-9c6cc63c2f3b`
- **Chosen OLs (both LAR tables): `year` (→ column year) + `census_tract` (→ column census_tract).**
  Skip `transaction` (no identifying column → would render "Não informado"). dicionario: no OL.

## Account
- publishedBy / dataCleanedBy id: `57` (rdahis@basedosdados.org)

## Open items for metadata step
- **Measurement-unit slugs:** MCP enumeration has a GraphQL casing bug. Before writing units,
  read an existing US dataset (e.g. us_bls_cpi / us_census_cbp) column `measurementUnit` slugs
  to confirm exact casing for currency (`USD`?), `percent`, `month`, `year`, count/`unit`.
- **Column-registration recipe (types matter):** use `upload_columns_from_sheet` FIRST — it is
  the ONLY tool that sets `bigquery_type` (at create). `bulk_upsert_columns` ignores type and
  `update_column` has no type field, so a bulk-first path leaves typeless columns needing
  delete+recreate. Correct order: (1) `upload_columns_from_sheet(table_id, architecture_url=<sheet>,
  observation_levels={"year": <year OL>, "census_tract": <census_tract OL>})` — sets types +
  units + PT desc + links OLs; (2) `bulk_upsert_columns` to add EN/ES descriptions;
  (3) `update_column(year, is_partition=True)`. All `directory_column` cells are now EMPTY
  (incl. `year`, blanked to protect the partition column from any directory-resolution drop),
  so no column is dropped at upload. Geography FK targets live in `observations` for the future.
- `us_cfpb_hmda` does not exist on staging — create fresh.
