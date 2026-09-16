# us_bls_qcew — Onboarding Plan

**Status:** DRAFT — awaiting approval
**Source:** BLS Quarterly Census of Employment and Wages (QCEW), https://www.bls.gov/cew/downloadable-data-files.htm
**GCP dataset id:** `us_bls_qcew` · **backend slug:** `qcew` · **org:** `us_bls`
**License:** US public domain (work of the US government, 17 USC §105)
**Naming:** English column names, partition column `year` (INT64) — matches `us_bls_cpi` precedent. Metadata (names/descriptions) in PT/EN/ES.

---

## 1. Source facts (verified from bls.gov/cew)

QCEW ships **single files**: one CSV zip per year holding every `area_fips × own_code × industry_code × agglvl_code × size_code` combination, for all quarters of that year. Two classifications with **different schemas**:

| Classification | Coverage | Quarterly URL | Annual URL | Columns |
|---|---|---|---|---|
| NAICS | 1990–2025 | `data.bls.gov/cew/data/files/{yr}/csv/{yr}_qtrly_singlefile.zip` | `{yr}_annual_singlefile.zip` | 47 (core + `lq_*` + `oty_*`) |
| SIC | 1975–2000 | `…/{yr}/sic/csv/sic_{yr}_qtrly_singlefile.zip` | `sic_{yr}_annual_singlefile.zip` | 21 (core only) |

- Singlefiles **exclude** the five title fields (#9–13); titles come from the associated code files (see §4).
- SIC industry codes carry an `SIC_` prefix (10 chars); no location quotients or over-the-year changes.
- Skipping the 1975–1989 NAICS by-industry files (ownership-totals only, different schema; SIC covers those years). — *user decision*

## 2. Tables — *user decisions: 4 class×freq groups, full 47-field NAICS, 4-level geo split*

**16 data tables + 1 dicionario = 17.** Each of the 4 class×freq groups is split into 4 geographic-level tables by the tens digit of `agglvl_code`. Industry-detail levels (ones digit: total→6-digit NAICS) stay **stacked** within each table, disambiguated by `agglvl_code`.

| Group (schema) | Coverage | Geo tables | BD Pro |
|---|---|---|---|
| `naics_quarterly_*` (47 cols) | 1990–2025 | national, state, county, metro | **PartBdpro** (latest 2 qtrs closed) — all 4 |
| `naics_annual_*` (annual 47-equiv) | 1990–2025 | national, state, county, metro | AllFree |
| `sic_quarterly_*` (21 cols) | 1975–2000 | national, state, county, metro | AllFree (frozen) |
| `sic_annual_*` (21-equiv) | 1975–2000 | national, state, county, metro | AllFree (frozen) |
| `dicionario` | associated code files | — | AllFree |

**Geographic routing** (tens digit of `agglvl_code`):

| Geo table | agglvl tens | `area_fips` | Directory link |
|---|---|---|---|
| `*_national` | 1x, 2x, 9x | `US000` + specials | none |
| `*_state` | 5x, 6x | `SS000` | derive `id_state` (2-digit) → FK `br_bd_diretorios_us.state` |
| `*_county` | 7x | `SSCCC` | `area_fips` → FK `br_bd_diretorios_us.county` (exact 5-digit) |
| `*_metro` | 3x, 4x, 8x | `C####` | dictionary (QCEW 4-digit metro ≠ 5-digit CBSA) |

Est. total ~700M+ rows; `*_county` tables dominate. Partition `year` INT64; cluster `industry_code`,`own_code` (area already split out).

Column shape:
- **Quarterly** tables keep `qtr` (INT64, 1–4). **Annual** tables drop `qtr` (always 'A') and use annual-average measure columns (`annual_avg_estabs`, `annual_avg_emplvl`, `total_annual_wages`, `taxable_annual_wages`, `annual_contributions`, `annual_avg_wkly_wage`, `avg_annual_pay`, + `lq_*`/`oty_*` for NAICS).
- `naics_*` keep all 47/annual-equivalent BLS columns faithfully (incl. `lq_*` location quotients and `oty_*` over-the-year changes).
- `sic_*` = core 21 only (establishments, `month1-3_emplvl`, total/taxable wages, contributions, avg weekly wage, disclosure).
- `*_state` tables add derived `id_state`; `*_national`/`*_metro` keep raw `area_fips`.

## 3. Types (per bigquery-conventions "arithmetic meaning")

- **STRING (coded/id):** `area_fips`, `own_code`, `industry_code`, `agglvl_code`, `size_code`, `disclosure_code`, `lq_disclosure_code`, `oty_disclosure_code`.
- **INT64:** `year` (partition), `qtr`, establishment counts, employment levels, over-the-year *count* changes.
- **FLOAT64:** wages, contributions, avg weekly/annual wage, all `lq_*`, all `*_pct_chg`.
- Wages/contributions kept in **whole USD** as published (no unit rescale). `measurement_unit` on every numeric.

## 4. Directories & auxiliary — *user decision: geo split → clean FKs, no relaxed test*

The 4-level geo split makes each table's `area_fips` homogeneous, so each gets **one** consistent treatment (no per-row relaxed test):
- **`*_county`** — `area_fips` (5-digit) → `directory_column` FK `br_bd_diretorios_us.county`, `covered_by_dictionary = no`. Every row is a real county → clean `relationships` test.
- **`*_state`** — derive `id_state` (2-digit, from `SS000`) → FK `br_bd_diretorios_us.state`, `covered_by_dictionary = no`. Keep raw `area_fips` too (STRING, no dict).
- **`*_national`** — `area_fips` ∈ {US000, 9x specials}; small fixed set → `dicionario`, `covered_by_dictionary = yes`.
- **`*_metro`** — `area_fips` = QCEW `C####`; no clean CBSA FK → `dicionario` (area titles), `covered_by_dictionary = yes`.
- **`dicionario`** also covers `own_code`, `agglvl_code`, `size_code` (NAICS vs SIC sets differ → scoped by `id_tabela`) and NAICS + SIC **industry titles** (`industry_code` → title; real + supersector/aggregate pseudo-codes uniformly). All `covered_by_dictionary = yes`.
- No new NAICS-vintage or QCEW-area directories built this pass. Note in `observations` that `industry_code` real codes align with `br_bd_diretorios_us.naics_2022` (2022-vintage rows) and metro `C####` codes map to CBSA via a future crosswalk.
- Associated code files (auxiliary): `industry-titles`, `sic-industry-titles`, `qcew-area-titles`, `sic-area-titles`, `ownership`/`agg-level`/`size` titles → source for `dicionario`.

## 5. Onboarding steps (per onboarding-workflow.md)

1. **context** — org `us_bls`, license public-domain, coverage; raw source URLs (§1). Drive folder.
2. **architecture** — 5 Google Sheets (naics_quarterly, naics_annual, sic_quarterly, sic_annual, dicionario). Fetch exact annual-layout columns from bls.gov annual layout docs. Column order: partition (`year`[,`qtr`]) → ids (`area_fips`,`own_code`,`industry_code`,`agglvl_code`,`size_code`) → measures.
3. **download** — pull singlefile zips (browser UA; `data.bls.gov` needs it) → `input/`. NAICS 1990–2025 ×2, SIC 1975–2000 ×2, + associated code files. ~large; download per year.
4. **clean** — Python: unzip, add `year`, coerce `-`→NULL, split classification/frequency, write hive-partitioned parquet `output/<table>/year=YYYY/data.parquet`. Shared pure functions in `pipelines/datasets/us_bls_qcew/utils.py`, imported by `models/us_bls_qcew/code/` (DRY for the pipeline).
5. **upload** — BigQuery **dev** (`basedosdados-dev`) per table, year by year; verify row counts.
6. **dbt** — 5 models + `schema.yml`; `safe_cast` every column; relaxed area relationships test; `unique_combination_of_columns` on full key.
7. **validate** — dbt tests; spot-check totals vs BLS published (e.g. US000/national total employment a given quarter).
8–9. **discover / metadata (dev)** — register dataset `under_review`, tables, OLs (geography/industry/ownership/time), columns, cloud tables, coverage, updates. Two coverages on `naics_quarterly` (free + pro `is_closed=True`) so the PartBdpro pipeline won't fail `assert_coverage_topology`.
   **→ PAUSE: verification checkpoint, wait for "approved".**
10. **metadata (prod)** — promote `under_review`.
11. **PR** — `feat(us_bls_qcew): onboard QCEW NAICS + SIC` (+ `deploy-flow` label for step 12).
12. **pipeline (recurring)** — Prefect 3, quarterly cron. Re-download current + prior year NAICS singlefiles, `dump_mode="overwrite"` on those year partitions; poll source max quarter; `PartBdpro(free_lag=6 months)` on `naics_quarterly` (rolls the 2-quarter paywall automatically). SIC tables frozen (not in pipeline). Dev run with `{materialize_to_prod:False, update_metadata:False, force_run:True}` is the definition of done for step 12.
13. **publish** — flip dataset → `published` only after PR merge + table-approve materialises prod + prod verified.

## 6. Existing repos (searched — none is a cleaned public-domain panel)

QCEW singlefiles are already the clean product; QCEW is public domain. Found only tooling: `entrydatar` (R, Loualiche), `TrentLThompson/qcew` (Python API client), `jjchern/qcewAPI` (R API client), `Nonprofit-Open-Data-Collective/bls-qwec-nonprofit-data` (nonprofit subset, wide reshape). → onboard BLS raw faithfully; these are possible future derived tables, not sources.

## 7. Open risks

- **Scale.** `naics_quarterly` ~450M rows. Clean/upload strictly year-by-year; never hold all years in memory. Confirm dev BigQuery cost is acceptable before full-history upload.
- **Staging all-STRING (pipeline path).** `upload_to_gcs` infers staging schema as all-STRING; pipeline parquet must be cast to all-string via arrow (not `astype(str)`). Onboarding one-shot upload keeps typed parquet.
- **Relaxed area FK** may surprise the site's directory linkage on aggregate-area rows — documented, expected.
