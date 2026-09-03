# us_epa_ghgrp — Onboarding Plan

**Source:** EPA Greenhouse Gas Reporting Program (GHGRP), https://www.epa.gov/ghgreporting
**GCP dataset id:** `us_epa_ghgrp` · **backend slug:** `ghgrp` (pre-existing shell, id
`35d072bc-ddf5-4975-b8ab-9a75f6d2fcfa` on both staging and prod) · **org:** `epa`
**License:** `cc0` — work of the U.S. federal government, 17 U.S.C. §105 (public domain).
EPA publishes no reuse restriction on GHGRP data.
**Naming:** English column names, partition `year` (INT64) — matches `us_fhfa_hpi` / `us_bls_cpi`.
Metadata (names/descriptions) in PT/EN/ES.
**Cadence:** one reporting year published each autumn (2023 data: October 2024), prior years
occasionally revised → recurring Prefect pipeline (step 12), full replace per run.

---

## 1. Source facts (verified 2026-09-03)

EPA publishes the same data through three portals. This dataset reads the **Envirofacts
GHG REST API** (`https://data.epa.gov/efservice/`), which exposes the FLIGHT data model as
plain tables with stable ids — programmatic, complete, and pollable. The FLIGHT "Data
Summary Spreadsheets" (`ghgp_data_<year>.xlsx`) are a wide, per-year rendering of the same
facts and were used to cross-check layout and to lift EPA's FAQ into the auxiliary files.

| API table | Grain | Rows (2010–2023) |
|---|---|---|
| `pub_dim_facility` | facility × year | 136,005 |
| `pub_facts_subp_ghg_emission` | facility × year × subpart × gas | 395,894 |
| `pub_facts_sector_ghg_emission` | facility × year × sector × subsector × gas | 346,683 |
| `pub_dim_sector` / `pub_dim_subsector` / `pub_dim_ghg` / `pub_dim_subpart` | lookups | 16 / 71 / 14 / 42 |

- Coverage **2010–2023**; the API holds no 2024 rows yet (`year/2024/count` = 0).
- Paging: `/year/<y>/rows/<a>:<b>/CSV` returns whole windows of 50,000; every year's row
  count is asserted against the API's own `count`.
- `pub_dim_facility` carries facilities that **stopped reporting** (`reporting_status`
  set, no `submission_id`): 3,174 of 11,281 rows in 2023. They are kept.
- `naics_code`: 6 digits, vintage varies. Coverage against `br_bd_diretorios_us`:
  NAICS 2007 ≈ 99% for 2010–2017, NAICS 2017 ≈ 91% for 2018+, NAICS 2022 ≈ 91% for 2022+.
- `county_fips`: 99.3% resolve in `br_bd_diretorios_us.county`; the misses are legacy
  Connecticut counties (`09xxx`) and Alaska `02261`. 622 rows (0.5%) carry a county in a
  different state from the reported state (corporate-office addresses on basin-level
  reporters) — kept as reported.
- 12 ZIP codes lost a leading zero upstream (4 characters) — left-padded.
- `pub_facts_sector_ghg_emission` publishes 81 keys twice (a zero/null placeholder or two
  components). **Summing** them reproduces the subpart-table facility totals exactly for
  all 32 affected facility-years; taking the max does not. Rows are summed.
- `pub_facts_sector_ghg_emission` carries 25,950 rows with neither a gas nor a value —
  sector-membership placeholders, 84% of them for facilities that stopped reporting.
  Dropped. Null `co2e_emission` otherwise: 1,188 in the subpart table (all subpart UU,
  confidential CO2 injection) and 2,871 in the sector table (confidential supplier
  quantities). Kept null.
- Sector and subpart facility totals (excluding biogenic CO2) agree within 1 t in 95.3%
  of facility-years; the rest differ by construction (FAQ 5: subpart C attribution).

## 2. Tables

| Table | Grain / key | Rows |
|---|---|---|
| `facility` | `year, facility_id` | 136,005 |
| `emission_subpart` | `year, facility_id, subpart, gas` | 395,894 |
| `emission_sector` | `year, facility_id, sector, subsector, gas` | 320,667 |
| `dicionario` | `id_tabela, nome_coluna, chave` | 139 |

Dictionary-covered codes: `subpart`, `gas`, `sector`, `subsector`, `reporting_status`,
`cems_used`, `co2_captured`, `co2_supplied`. `facility_type` and `industry_type` are
comma-separated lists and stay readable text.

Observation levels: `facility` → year, establishment (`facility_id`), state (`state_id`),
county (`county_id`); `emission_*` → year, establishment, sector (`subpart` / `sector`).

Directory links: `year` → `br_bd_diretorios_data_tempo.ano`, `state_id` → `..._us.state`,
`county_id` → `..._us.county` (1% tolerance), `naics_id` → `..._us.naics_2017` (tested by
vintage: 2007 up to 2017 at 2%, 2017 from 2018 at 12%).

## 3. Pipeline

`pipelines/datasets/us_epa_ghgrp/` — one flow, `us_epa_ghgrp_flow`. The poll runs first
on count requests (`source_max_year`), compared against the registered coverage of
`emission_subpart`; only a new reporting year triggers the download. Every run rebuilds
2010→latest and replaces the tables (`dump_mode="overwrite"`), so a revised prior year is
re-materialized rather than appended. Schedule `33 15 3,10,17,24,31 10,11,12,1 *`
(America/Sao_Paulo), memory 2Gi. All tables `AllFree` on `year`.

## 4. Auxiliary files

Per-table bundles (README + EPA's "FAQs about this Data" + subpart/industry-type list)
built by `code/build_auxiliary_files.py` and uploaded to
`gs://basedosdados/auxiliary_files/us_epa_ghgrp/<table>/auxiliary_files.zip`.

## 5. Status

See `CLAUDE.md` for the running status and ids.
