# us_bls_qcew — locked onboarding context

Full plan: `ONBOARDING_PLAN.md`. This file = quick-resume context for any session.

**Dataset:** GCP `us_bls_qcew` · slug `qcew` · org `us_bls` · US public domain (17 USC §105).
**Source:** BLS QCEW singlefiles, https://www.bls.gov/cew/downloadable-data-files.htm

## Locked decisions (user-approved 2026-07-30)
1. Full 47-field NAICS kept faithfully (incl `lq_*`, `oty_*`). SIC = core 21.
2. 4 class×freq groups: naics_quarterly, naics_annual (1990–2025), sic_quarterly, sic_annual (1975–2000). Skip 1975–89 NAICS.
3. **4-level geographic split** by tens digit of `agglvl_code`: national / state / county / metro → **16 data tables + dicionario = 17**.
4. Clean directory FKs, no relaxed test: county→`br_bd_diretorios_us.county`; state→`.state` (derive `id_state`); national/metro→dicionario.
5. BD Pro: only `naics_quarterly_*` (all 4 geo) = PartBdpro, latest 2 quarters closed (free_lag=6 months). Everything else AllFree.
6. Column names English (match us_bls_cpi); partition `year` INT64; metadata PT/EN/ES.

## Geo routing (tens digit of agglvl_code)
- national: 1x, 2x, 9x  (area_fips US000 + specials)
- state:    5x, 6x       (SS000 → derive id_state)
- county:   7x           (SSCCC → FK county)
- metro:    3x, 4x, 8x   (C#### CSA/MSA/MicroSA → dict)

## URL patterns
- NAICS: `data.bls.gov/cew/data/files/{yr}/csv/{yr}_qtrly_singlefile.zip` / `{yr}_annual_singlefile.zip`
- SIC:   `data.bls.gov/cew/data/files/{yr}/sic/csv/sic_{yr}_qtrly_singlefile.zip` / `sic_{yr}_annual_singlefile.zip`
- Titles: bls.gov/cew/classifications/{industry,areas,ownerships,size,aggregation}/*-titles.htm (+ .csv/.txt)
- `data.bls.gov` requires a browser User-Agent (403 otherwise).

## Dev subset (checkpoint validation only; full backfill after approval)
NAICS years 1990, 2000, 2010, 2020, 2024, 2025 · SIC years 1975, 1990, 2000 · all geo levels + both freqs.

## Backend (2026-07-31)
dev backend 503 (down). Register on **env="staging"** (maps dev cloud tables → basedosdados-dev). `discover_ids` broken on staging (schema drift) — use get_dataset/lookup_id. Resolved IDs + created record ids: see `metadata/registration_spec.json` and memory `project_us_bls_qcew`.

## Post-crash recovery (2026-07-31)
Crash = OOM from loading a full 2.1GB/14.7M-row NAICS quarterly file into pandas. FIXED: `utils.clean_year` streams in 500k-row chunks (peak 1.75GB), writes `data_<n>.parquet` per partition. Dev subset trimmed to naics [1990,2024] + sic [1975,2000]. Local ADC lacks BQ jobs.create → verify counts / run dbt with SA keyfile `~/.basedosdados/credentials/prod.json` (dbt: `--profiles-dir ~/.dbt --target dev`; upload: `GOOGLE_APPLICATION_CREDENTIALS=…/prod.json`).

## Status
- [x] Research, design, plan approved
- [x] Architecture CSVs + columns_json (build_architecture.py) — 17 tables
- [x] dbt models + schema.yml (build_dbt.py) — 17, dbt_project.yml entry
- [x] Cleaning code (constants/utils shared; clean_data/upload bootstrap). Streaming download.
- [x] Cleaning validated (geo routing, all-string parquet, id_state, PT/ES contractions fixed)
- [~] Download+clean subset (naics 1990/2024/2025 + sic 1975/2000) — RUNNING (slow big NAICS quarterly downloads)
- [x] Metadata staging: dataset `qcew`=441d8d57 (under_review) + 2 raw sources (naics 180c2c08, sic afa5ebe0); template table naics_quarterly_national fully proven; 2 agents registering all 17 tables
- [ ] Upload dev (basedosdados-dev) → dbt run/test → verify metadata → CHECKPOINT (stop before prod/PR/pipeline)
