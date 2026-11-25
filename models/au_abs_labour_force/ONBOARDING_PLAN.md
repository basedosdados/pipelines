# Onboarding Plan — `au_abs_labour_force`

**Dataset:** `au_abs_labour_force` (org `au_abs`) — Labour Force, Australia (ABS cat. 6202.0)
**Source:** https://www.abs.gov.au/statistics/labour/employment-and-unemployment/labour-force-australia/latest-release
**License:** CC BY 4.0 (ABS)
**Frequency:** Monthly (reference month released ~2 weeks later, Thursday 11:30 AEST)
**Scope (decided):** Tier 1 + Tier 2 = the full 6202.0 headline release. The separate
*Detailed* release (6291.0.55.001: industry/occupation/duration cubes) is a deferred phase 2.
**Source mechanism (decided):** ABS Data API (SDMX) first; Excel pivot cubes for grains the
API does not serve.

---

## 1. The core architectural insight

The release page lists ~30 downloadable files, but they are the **same measures sliced
many ways**, not 30 datasets. We slice by **grain**, not by ABS spreadsheet:

- **Tables 1–16** (national / each state / youth / working-age age bands) are *one cube*:
  `month × region × sex × age × adjustment-type → measures`. ABS exposes exactly this as the
  SDMX **`LF`** dataflow (MEASURE ×31, SEX ×3, AGE ~27, REGION = AUS + 8 states, TSEST =
  original/seasonally-adjusted/trend, FREQ = M, from Feb 1978). → **one Data Basis table**.
- The **pivot cubes** (LMS1–5, HRS1–2, SEM1, MLF1, X28–X29) are genuinely different grains →
  one table each.

`GM1`/`RM1` etc. are Excel **data-cube codes, not SDMX dataflow ids** (they 404 on the API).
ABS has *ceased* the SA4 direct-survey cubes (RM1/RQ); our `region_sa4` uses **MLF1
(modelled)**, which is in the main release and still published.

---

## 2. Table architecture (8 data tables + `dicionario`)

Shape: **wide-by-measure** for the status-type cubes (matches the source; each measure column
carries its own `measurement_unit` — persons `'000`, rates `%`, hours `'000 hours`);
**long** for gross flows. Column names in **English** (`year`, `month`, not `ano`/`mes`).

### Tier 1 — core (monthly)

| Table | Grain | Key measures | Source |
|---|---|---|---|
| `labour_force_status` | month × adjustment_type × geography × sex × age_group | employed_total/full_time/part_time, unemployed, labour_force, unemployment_rate, participation_rate, employment_to_population_ratio, monthly_hours_worked | SDMX `LF` |
| `hours_worked` | month × geography × sex × hours_band | employed persons (`'000`) | Excel HRS1/HRS2 (Table 18) |
| `status_in_employment` | month × geography × sex × status_in_employment | persons (`'000`) | Excel SEM1 (Table 19) |
| `underutilisation` | month × adjustment_type × geography × sex × age_group | underemployed, underemployment_rate, underutilisation_rate, unemployed | Excel X28/X29 (has SA/trend) |

### Tier 2 — regional & flows (monthly; the large pivots)

| Table | Grain | Key measures | Source |
|---|---|---|---|
| `gross_flows` | month × geography × sex × age_group × lfs_current × lfs_previous | matched-sample persons (`'000`) — **long** | Excel LMS1 |
| `region_sa4` | month × id_sa4 × sex × age_group | employed, unemployed, labour_force, unemployment_rate, participation_rate | Excel MLF1 (modelled) |
| `capital_city` | month × gccsa (greater capital city / rest of state) × sex × age_group | labour force status | Excel LMS2/LMS3 |
| `country_of_birth` | month × geography × sex × age_group × birth_country_group × years_since_arrival | labour force status (`'000`) | Excel LMS4/LMS5 |

`dicionario` — coded dimensions: `adjustment_type`, `hours_band`, `status_in_employment`,
`lfs_current`/`lfs_previous`, `birth_country_group`, `years_since_arrival`, age_group codes.

Notes:
- `adjustment_type ∈ {original, seasonally_adjusted, trend}` exists only for
  `labour_force_status` and `underutilisation`; the pivots are Original-only (column omitted).
- Sensitivity upper/lower (TSEST 40/50) dropped.
- LMS2+LMS3 consolidate into `capital_city`, LMS4+LMS5 into `country_of_birth`, at the finest
  consistent grain; split only if the grains genuinely conflict (resolved at architecture step).

---

## 3. Geography modelling & directory FK (open design point → resolved at architecture step)

- **states/territories:** `labour_force_status`, `hours_worked`, `status_in_employment`,
  `underutilisation`, `gross_flows`, `country_of_birth` carry a geography dimension =
  **Australia + 8 states/territories**. National ("AUS") is an aggregate, not a state, so it
  cannot FK a state directory. Decision: state code column FK-linked to `br_bd_diretorios_au`
  for state rows; handle the national aggregate per BD's national-total convention (confirm the
  AU directory has/handles a national entry, else sentinel + `covered_by_dictionary`).
- **`region_sa4`** → `id_sa4` FK to `br_bd_diretorios_au` (SA4).
- **`capital_city`** → GCCSA FK to `br_bd_diretorios_au` (GCCSA) if present.

**Dependency/risk:** `br_bd_diretorios_au` (ASGS 2016+2021) must be **live in prod** for these
FK dbt tests. It is in progress. If not live at metadata time: onboard the geo columns as
STRING `covered_by_dictionary=no` with `directory_column` noted, and defer the FK
`relationships` tests (re-enable once the directory lands) rather than block the whole dataset.

---

## 4. Recurring monthly pipeline (`pipelines/datasets/au_abs_labour_force/`)

Reference implementation: `us_bls_cpi`. Because both sources ship **full history each release**,
use `dump_mode="overwrite"`; staging parquet **all-STRING via arrow** (dump_header bug), real
types applied first. Poll guard on the release month (SDMX `LF` latest period + Excel release
month), so a scheduled run no-ops until ABS publishes. One raw source per table (multi-raw-source
client bug): core tables → the `LF` API source; pivots → the 6202.0 Excel release source.

**BD Pro rolling window:** every table refreshes monthly → **`PartBdpro(free_lag=6 months)`** per
house rule; `dicionario` takes no coverage. Requires a pro Coverage (`is_closed=True`) created
**before** the first pipeline run (else `assert_coverage_topology` hard-fails).

---

## 5. Step sequence

**A. Design**
1. `context` — confirm org `au_abs`, license CC BY 4.0, coverage (Feb 1978–present), Drive folder.
2. `architecture` — 9 Google Sheets (one per table) with English column names, PT/EN/ES
   descriptions, types by arithmetic meaning, directory FKs. **← architecture reviewed here.**

**B. Build (dev)**
3. `download` — SDMX `LF` (full history) + the 6202.0 Excel pivot cubes (LMS1–5, HRS1–2, SEM1,
   MLF1, X28–X29). Verify per-grain API coverage; fall back to Excel where absent.
4. `clean` — reshape to the architecture schema → partitioned parquet (`year=` partition).
5. `upload` — BigQuery `basedosdados-dev` staging.
6. `dbt` — 9 models + `schema.yml` (safe_cast, FK relationships, unique-combination tests).
7. `validate` — dbt tests + row-count / coverage checks.

**C. Metadata**
8. `discover` + `metadata` (dev, status `under_review`) → **verification checkpoint (pause)**.
9. `metadata --env prod` (`under_review`) after approval.
10. `pr` — open PR (add **`deploy-flow`** label for the pipeline dev run).

**D. Pipeline**
11. Build `pipelines/datasets/au_abs_labour_force/` (constants/utils/tasks/flows), reusing the
    cleaning transform from step 4. Dev run with
    `{materialize_to_prod:false, update_metadata:false, force_run:true}`; confirm `dbt run OK` +
    `dbt test OK` per table. Create the pro Coverage + source `Update`/`Poll` records.

**E. Post-merge**
12. After merge → table-approve materialises prod → verify row counts / cloud tables → **publish**
    (`status.published`). Arm the schedule (Django admin `is_schedule_active`) deliberately.

---

## 5b. Tier-1 as-built architecture (2026-08-05)

Phasing decided: **Tier 1 (4 tables) first; Tier 2 = phase 2.** Columns registered via
`bulk_upsert_columns` from `code/columns_json/*.json` (built by `build_columns_json.py` from
`code/architecture/*.csv`) — **not** a Google Sheet, which drops `description_en` for
English-source datasets. All categories decoded to readable English labels ⇒
`covered_by_dictionary=false` everywhere ⇒ **no `dicionario` table**. All ABS counts converted
from thousands → absolute persons (`person`) / hours (`hour`); rates are `percent`.

| Table | Cols | Grain | Source | Adj. type |
|---|---|---|---|---|
| `labour_force_status` | 20 | year·month·geography·sex·age_group·adjustment_type → 14 measures | SDMX `LF` | Orig/SA/Trend |
| `hours_worked` | 8 | year·month·geography·sex·hours_band → employed_persons, hours_worked, hours_per_person | Table 18 (national) | Original only |
| `status_in_employment` | 8 | year·month·geography·sex·status_in_employment → employed_total/full_time/part_time | Table 19 (national) + SEM1 (states) | Original only |
| `underutilisation` | 10 | year·month·geography·sex·age_group·adjustment_type → underemployed_total, underemployment_ratio, underemployment_rate, underutilisation_rate | X28 (states) + X29 (age) | Orig/SA/Trend |

Deliberate scoping calls (documented for review):
- **`hours_worked` is national-only in Tier-1.** ABS fragments hours across incompatible cuts:
  Table 18 (national, sex, 3 measures incl. hours-per-person), HRS1 (states, no sex, FT/PT
  split), HRS2 (age×sex, national). Table 18 is the cleanest headline distribution; state hours
  (HRS1/SEM1) are phase 2. `geography` column kept (= Australia) for schema continuity.
- **`underutilisation` carries only the four underutilisation-specific measures.** Employment /
  unemployment / labour-force levels live in `labour_force_status` on the same keys (joinable),
  avoiding duplication.
- **`geography`** = Australia + 8 states/territories as readable names; no `directory_column`
  yet (br_bd_diretorios_au unpublished + national aggregate has no state-directory row). FK noted.
- National (Persons + national totals) sourced from the time-series spreadsheets; state
  disaggregation (Males/Females only) from the pivots — ABS benchmarks national separately, so
  national is **never** derived by summing states.

## 5c. Build status (2026-08-05)

Done through dbt, all in dev:
- **Sourcing finalised (API-heavy):** `labour_force_status` <- SDMX `LF` + `LF_AGES`;
  `underutilisation` <- SDMX `LF_UNDER`; `hours_worked` <- Excel Table 18;
  `status_in_employment` <- Excel Table 19 + SEM1. (`not_in_labour_force` /
  `civilian_population_15_over` kept — national via `LF_AGES`; NULL for states, which the
  `LF` flow does not carry.)
- **Cleaning** in `pipelines/datasets/au_abs_labour_force/utils.py` (shared, no Prefect).
  Row counts: labour_force_status 83,664 (1978–2026), underutilisation 85,407 (1978–2026),
  hours_worked 19,170 (1991–2026), status_in_employment 68,754 (1991–2026). Staging parquet
  all-STRING, `year` as `'2026'`, NULLs preserved.
- **Uploaded** to `basedosdados-dev` staging (row counts exact).
- **dbt** run OK (4/4) + test OK (38/38): unique-combination, not_null, not_null_proportion,
  and year/month relationships to `br_bd_diretorios_data_tempo`. Ran with
  `--profiles-dir ~/.dbt` (repo `profiles.yml` uses the CI-only `/credentials-dev/dev.json`).

Verify in dev BigQuery, e.g.:
```sql
select geography, unemployment_rate from `basedosdados-dev.au_abs_labour_force.labour_force_status`
where year=2026 and month=6 and sex='persons' and age_group='total' and adjustment_type='seasonally_adjusted'
order by geography
```

Not yet done: metadata registration (dev → prod), commit, PR, monthly pipeline. Nothing committed.

## 6. Open items to confirm during build
- Exact SDMX dataflow ids for hours / status-in-employment / underutilisation (else Excel).
- LMS2/3 and LMS4/5 grain consolidation.
- `br_bd_diretorios_au` prod availability (FK dependency above).
- Whether `au_abs` org already exists in the backend (community-profiles onboarding) or needs creating.
