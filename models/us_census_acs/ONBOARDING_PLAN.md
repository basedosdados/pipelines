# Onboarding plan — American Community Survey (ACS)

**Dataset:** `us_census_acs` (GCP id) · slug `acs` · org `us_census` (US Census Bureau)
**Status:** planning · settled 2026-07-12 · not yet started (steps 1–11 pending)
**Branch/worktree:** `claude/acs-data-architecture-110a6b`

This document is the durable reference for onboarding ACS to Data Basis. Tick boxes as
steps complete. The architecture Google Sheets (step 2) become the source of truth once
created; this file records the decisions behind them.

---

## 1. Settled decisions (2026-07-12)

| # | Decision | Choice |
|---|----------|--------|
| S1 | Aggregate table shape | **Long/tidy + `variables` catalog** (one row per geo×variable×year; matches `tidycensus` output; scales to all ACS tables later) |
| S2 | First-onboarding scope | **Module A (microdata) + Module B (data profiles)**; stop at dev checkpoint |
| S3 | Estimate period | **Both 1-year and 5-year**, distinguished by a `period` column |
| S4 | Source schema | **Census raw** (public domain): raw PUMS for microdata, Census API variable codes for profiles. NOT IPUMS (license blocks re-hosting) |
| S5 | Language | Table/column **names in English**; descriptions **PT/EN/ES** (matches `us_*` precedent) |
| S6 | Org / naming | org `us_census`, slug `acs`, GCP id `us_census_acs` (room for future `us_census_decennial`, `us_census_cps`) |

## 2. License

- **Public Domain** — U.S. Government work, 17 U.S.C. §105. No copyright, freely redistributable.
- Record `has_sensitive_data = no`. PUMS is already de-identified (geography floored at PUMA, top/bottom coding).
- One ToS caveat to note in metadata `observations`: users must not use the data to **re-identify** individuals/households. This is a use restriction, not a redistribution restriction, and does not affect DB hosting.
- Backend license reference: map to "Public Domain / U.S. Government Work" (resolve exact `license_id` at step 8; create the license reference if absent).

## 3. Standard-schema investigation (findings)

No single universal schema. Three de-facto standards:

1. **Census API variable codes** — canonical source of truth. `B`/`C` detailed (~20k vars), `S` subject, `DP` data profiles (DP02 Social, DP03 Economic, DP04 Housing, DP05 Demographic). Every wrapper (`tidycensus`, `censusapi`, `cenpy`, `censusdata`) is built on these. `tidycensus` returns tidy-long `geoid, name, variable, estimate, moe` — the shape most researchers work in, and the shape adopted here (S1).
2. **IPUMS USA** — gold-standard *harmonized microdata* (uniform names/codes across years, constructed family/income/occupation vars). **License blocks re-hosting** — users must register. We host Census raw PUMS instead; an IPUMS crosswalk is a possible future enhancement, not hosted data.
3. **Google BigQuery public `census_bureau_acs`** — closest ready-made BigQuery schema: 5-year aggregates, one **wide** table per geography level, ~250 curated human-readable columns, joined by `geo_id`. No microdata. Its geography levels align with our existing `br_bd_diretorios_us`.

## 4. Directory dependency (already in prod)

All geography FKs point to **`br_bd_diretorios_us`** (onboarded 2026-07-09, PR #1640):
`state`, `county`, `place`, `census_tract_2020`, `puma_2020`, `cbsa_2023`,
`congressional_district_119`, `zcta_2020`, `school_district`. FKs resolve at metadata time.
Reused entities (already created): person, state, county, city (place), census_tract,
region (cbsa/puma), district, zip_code, school_district. **New entity likely needed:**
`household`/`housing_unit` (create at step 8 if absent).

---

## 5. Table architecture

### Module A — Microdata (PUMS)  *(core)*

Two tables, one row per person / per housing unit. Naturally **wide** (one column per PUMS
variable). Source: Census PUMS CSV files (person `p`, housing `h`), 1-year and 5-year.

| Table | Grain | Partition | Geo keys (→ directory) | Weights |
|-------|-------|-----------|------------------------|---------|
| `microdata_person` | person | `ano` | `id_state` → state; `puma` STRING (no dir FK, D6) | `PWGTP` + `PWGTP1..80` replicate |
| `microdata_household` | housing unit | `ano` | `id_state` → state; `puma` STRING (no dir FK, D6) | `WGTP` + `WGTP1..80` replicate |

- Also carry `period` (`1-year` / `5-year`) and `serial_number` (SERIALNO) + `person_number` (SPORDER, person table only) so person↔household join is possible.
- Weights are dimensionless statistical weights → `measurement_unit` blank, noted in `observations` (the recognized exception to the unit rule).
- Column set = **full raw** (D1 locked): keep every PUMS variable, original mnemonic lowercased as column name, uppercase in `original_name`.
- **Typing (per `bigquery-conventions.md`):** coded categorical columns (SEX, RAC1P, SCHL, ESR, TEN, COW…) → STRING with `covered_by_dictionary=yes` (labels in the standard `dicionario`); genuine quantities (AGEP, WAGP, PINCP, HINCP, weights) → INT64/FLOAT64 with units. `serial_number`/`person_number` → STRING identifiers. OCCP/INDP codes → STRING (link to `br_bd_diretorios_us` occupation/industry directories where vintage allows; else `dicionario`).
- **Schema-drift caveat (full-raw across 2005–2024):** PUMS variable names/codes change over time — vars are added, dropped, split, or redefined. A single wide table per record type is the **union of all variables ever present**, with `NULL` where a variable didn't exist that year. A few codes are reused with changed meaning across vintages (e.g. occupation/industry code systems, `RAC*`, PUMA 2010→2020); flag these per-column in `observations` and lean on the PUMS data dictionary. Alternative if drift proves unmanageable: split by PUMS dictionary era (e.g. 2005–2011, 2012–2016, 2017–2024) — deferred, revisit at step 2.

### Module B — Data Profiles  *(first aggregate module)*

Long/tidy, **one table per geography level** so each `geo_id` FKs to exactly one directory.
Plus one shared `dictionary`. Source: Census API `acs1/profile` and `acs5/profile`.

| Table | Geo keys (→ directory) | 1-yr | 5-yr |
|-------|------------------------|:----:|:----:|
| `data_profile_nation` | (national; `id_pais` = US) | ✓ | ✓ |
| `data_profile_state` | `id_state` → state | ✓ | ✓ |
| `data_profile_county` | `id_state`,`id_county` → county | ✓ (≥65k) | ✓ |
| `data_profile_place` | `id_state`,`id_place` → place | ✓ (≥65k) | ✓ |
| `data_profile_puma` | `id_state` → state, `puma` (STRING, no dir FK — see D6) | ✓ | ✓ |
| `data_profile_cbsa` | `id_cbsa` → cbsa_2023 | ✓ (≥65k) | ✓ |
| `data_profile_congressional_district` | `id_congressional_district` → congressional_district_119 | ✓ | ✓ |
| `data_profile_zcta` | `id_zcta` → zcta_2020 | — | ✓ |
| `data_profile_school_district` | `id_school_district` → school_district | — | ✓ |

Note: Data Profiles are **not** published at block-group level (only detailed `B` tables are),
so no block-group table here. 1-year is published only for geographies ≥65k population.

**Columns of each `data_profile_<level>` table (long):**

| col | type | notes |
|-----|------|-------|
| `ano` | INT64 | partition; reference year (5-yr = **end** year, e.g. 2024 = 2020–2024) |
| `period` | STRING | `1-year` / `5-year`; part of key |
| geo id col(s) | STRING | e.g. `id_state`(+`id_county`); zero-padded; FK → directory |
| `variable_code` | STRING | e.g. `DP05_0001E`; `covered_by_dictionary=no`; `relationships` FK → `variables` |
| `estimate` | FLOAT64 | value; **unit varies by variable** → `measurement_unit` left **blank** (documented exception, see D5); per-variable unit lives in `variables.unit` |
| `margin_of_error` | FLOAT64 | 90% MOE; same blank-unit treatment |

Key per table = (`ano`, `period`, geo cols, `variable_code`) → dbt `unique_combination_of_columns`.
Count vs percent lines are distinguished by the code (`…E` vs `…PE`) and decoded in `variables`.

### Two reference tables

DB distinguishes two different reference constructs; ACS needs both:

- **`variables`** — bespoke catalog for the profile `variable_code`s (richer than the standard
  `dicionario` can hold). All-STRING columns: `variable_code` (PK), `profile` (DP02/03/04/05),
  `profile_name` (Social/Economic/Housing/Demographic), `line_number`, `label` (hierarchical),
  `concept` (table title), `universe`, **`unit`** (data field — count/dollars/percent/years/ratio;
  *not* the reserved `measurement_unit`), `is_percent` (yes/no). Built from the Census API
  `.../profile/variables.json` per year, deduped across years. Profile facts link via a
  `relationships` test on `variable_code`.
- **`dicionario`** — DB-standard dictionary (`id_tabela, nome_coluna, chave, cobertura_temporal,
  valor`) decoding the **coded categorical columns of the PUMS microdata** (SEX, RAC1P, SCHL,
  ESR, TEN, …). Those microdata columns are STRING with `covered_by_dictionary=yes`; genuine
  quantities (AGEP, incomes, weights) stay INT64/FLOAT64 and are *not* in `dicionario`. Built
  programmatically from the `PUMS_Data_Dictionary_<year>.csv` files (large — code lists drift
  across years; carry `cobertura_temporal` per key).

### Table count (v1)

2 microdata + 9 profile + `variables` + `dicionario` = **13 tables** (comparable to `br_bd_diretorios_us`'s 14).

---

## 6. Decisions (all resolved 2026-07-12)

| # | Decision | Resolution |
|---|----------|------------|
| D1 | Microdata column scope | **Full raw** — all ~285 person / ~235 housing PUMS variables. Original PUMS mnemonic lowercased as column name (`agep`, `pwgtp`, `serialno`), raw uppercase in `original_name`, meaning in the description. Reassess later (may prune if the column/metadata burden proves too high). See **schema-drift caveat** in §5. |
| D2 | Temporal span | **Full available** — 1-yr **2005–2024** (end years), 5-yr **2009–2024**. |
| D3 | Replicate weights | **Include** `PWGTP1..80` / `WGTP1..80` (required for correct SDR standard errors). Dimensionless statistical weights → `measurement_unit` blank, noted in `observations`. |
| D4 | Nation-level profile | **Separate** `data_profile_nation` table. No geo id column (single national entity); key = (`ano`,`period`,`variable_code`). Entity = country/nation. |
| D5 | `estimate` unit (long design) | `estimate`/`margin_of_error` carry a **blank column-level `measurement_unit`** (documented exception — the second after weights, specific to melted indicator tables), with an `observations` note. The per-variable unit is **data** in `variables.unit` (a plain STRING field, *not* the reserved `measurement_unit`). |
| D6 | PUMA vintage | PUMA meaning drifts by year (2000 vintage 2005–2011, 2010 vintage 2012–2021, 2020 vintage 2022+); the `puma_2020` directory covers only 2022+. A single column can't FK three vintage directories, so in v1 **carry `puma` as a STRING geo code with the `id_state` FK only** (no PUMA directory FK; note vintage in `observations`, derivable from `ano`). Applies to both microdata and `data_profile_puma`. **Future:** add `puma_2010`/`puma_2000` directories + a vintage-aware crosswalk. |
| D6b | Congressional district & ZCTA vintage | Same pattern as D6. API `congressional district` codes follow the Congress in session for that data year (2023→118th) but directory is `congressional_district_119`; ZCTA pre-2022 is 2010-vintage vs directory `zcta_2020`. In v1 carry `id_congressional_district` / `id_zcta` as STRING codes (CD keeps `id_state`), **no directory FK**, vintage noted. Future: vintage-aware CD/ZCTA directories + crosswalk. School districts = **unified** only in v1 (elementary/secondary summary levels deferred). |

---

## 7. Sources / download

- **PUMS:** `https://www2.census.gov/programs-surveys/acs/data/pums/<year>/{1-Year,5-Year}/csv_p<st>.zip` (person) and `csv_h<st>.zip` (housing), per state (or national `csv_pus.zip`/`csv_hus.zip`). Data dictionary: `PUMS_Data_Dictionary_<year>.csv`.
- **Data Profiles:** Census API `https://api.census.gov/data/<year>/acs/acs{1,5}/profile?get=group(DPxx)&for=<geo>&key=<KEY>`. **Key now mandatory** (keyless → `missing_key.html`); stored locally at `/Users/rdahis/acs_data/.census_api_key` (outside repo/Dropbox). **Coverage confirmed:** `acs5/profile` 2009–2024; `acs1/profile` 2008–2024 (no 2020). Variable metadata: `.../profile/variables.json` per year. Each level pulls nationwide in **one** `group()` call (use `in=state:*` wildcard for `puma`/`school district (unified)`; `place`/`cbsa`/`congressional district`/`zip code tabulation area` need no `in`). MOE sentinels `-555555555`/`-222222222`/`-333333333`/`-666666666`/`-888888888`/`-999999999` → **NULL** in cleaning. Melt: each `DPxx_NNNNE` (+ its `M`) and `DPxx_NNNNPE` (+ its `PM`) → separate `variable_code` rows; drop `*EA/*MA/*PEA/*PMA` annotation columns.
- **Reference:** PUMS User Guide (`.../tech_docs/pums/`), data.census.gov microdata tool.

---

## 8. Onboarding step checklist (canonical 11-step workflow)

- [ ] 1. **context** — confirm source URLs, coverage, org, themes (territory, demography, economy, housing)
- [~] 2. **architecture** — CSVs DONE for all 13 tables in `models/us_census_acs/code/architecture/` (PUMS person/household from dict C/N; 9 profile long tables; variables catalog; dicionario). Hybrid schema (357/267). **Google Sheets NOT yet created** (needed for `upload_columns_from_sheet` at metadata). PT/ES translations of PUMS descriptions still TODO.
- [x] 3. **download** — **DONE.** PUMS: 64 GB at `/Users/rdahis/acs_data/pums` (1yr 2005–2024 minus 2020, 5yr 2009–2024, `csv_pus.zip`+`csv_hus.zip` each). Profiles: 3,056 files / 2.8 GB at `/Users/rdahis/acs_data/profiles/{acs1,acs5}/<year>/<level>_<DPxx>.json.gz` (gzipped JSON). Scripts in scratchpad: `pums_backfill.sh`, `prof_driver.sh`+`prof_fetch_one.sh`, `sd_backfill.sh`.
  - **Only genuine gap:** ZCTA acs5 2009–2010 (Census doesn't publish; ZCTA5 profiles start 2011).
  - **School-district two shapes (cleaning must glob both):** nationwide `school_district_<DPxx>.json.gz` for 2010 & 2020–2024; **per-state** `school_district-st<FIPS>_<DPxx>.json.gz` for 2009 & 2011–2019 (those years reject `in=state:*` wildcard → 52 per-state files/year). Melt = glob `school_district*_<DPxx>` per year.
  - **PUMS dict gap:** have 1yr 2017–2024 (CSV) + 5yr 2009-2013→2020-2024; MISSING 1yr 2005–2016 & 5yr 2005-2009→2008-2012 → nearest-year fallback at clean (vars ~stable; watch OCCP/INDP label drift).
  - **Profile parse notes:** MOE sentinels `-555555555`/`-222222222`/`-333333333`/`-666666666`/`-888888888`/`-999999999` → NULL. Response cols per line: `E`(est)/`M`(moe)/`PE`(pct)/`PM`(pct-moe) + `*EA/*MA/*PEA/*PMA` annotations (drop annotations). geo id cols returned by API (`state`,`county`,`place`,`zip code tabulation area`,`school district (unified)`, etc.).
- [x] 4. **clean** — DONE. Partitioned parquet at `/Users/rdahis/acs_data/output` (hive path `ano=<y>`).
- [x] 5. **upload** — DONE. All 13 tables in `basedosdados-dev.us_census_acs_staging` (verified row counts).
- [x] 6. **dbt** — DONE. 13 `.sql` models + `schema.yml` (build_dbt.py). Column names English (`year`/`id_country`/`code`) via rename-on-read aliases.
- [~] 7. **validate** — 11/13 models rebuilt + 75/75 tests PASS earlier. **microdata_person/_household await BQ daily-quota reset** to rebuild with `year`, then re-test.
- [x] 8. **discover** — DONE (staging backend; dev was down). IDs recorded in memory.
- [x] 9. **metadata** — DONE on **staging**. 13 tables + cols (PT/EN/ES) + OLs + cloud + coverage + updates + 2 raw sources.
- [~] **REVIEW REVISIONS (2026-07-16):** English renames (year/id_country/code), OL column-links (point-1), coverage confirmed (2024=frontier). 3 backend residuals blocked by get_dataset 200-col cap (person/household year is_partition; person serialno/sporder→OL). See CLAUDE.md gotchas + memory.
- [ ] **[PAUSE — verification checkpoint; await "approved"]**
- [ ] 10. **metadata --env prod** — promote after approval
- [ ] 11. **pr** — open PR with changelog

## 9. Future modules (out of v1 scope)

- **Module C — Detailed / Subject tables** (`B`/`S`, ~20k vars): long + dictionary only; huge. Deferred.
- **IPUMS variable crosswalk** — documentation mapping Census raw PUMS vars → IPUMS harmonized names (no IPUMS data hosted).
- **Comparison Profiles (CP)**, Narrative Profiles — low priority.
- **Boundaries/geometry** — TIGER/Line shapefiles could enrich `br_bd_diretorios_us` geographies (separate track).
- **Other Census products** under `us_census`: Decennial Census, CPS.
