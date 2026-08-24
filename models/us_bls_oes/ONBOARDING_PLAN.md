# us_bls_oes — Onboarding Plan

**Source:** BLS Occupational Employment and Wage Statistics (OEWS), https://www.bls.gov/oes/tables.htm
**GCP dataset id:** `us_bls_oes` · **backend slug:** `oes` · **org:** `us-bls`
**License:** US public domain (work of the US government, 17 USC §105)
**Naming:** English column names, partition `year` (INT64) — matches `us_bls_cpi` / `us_bls_qcew`. Metadata (names/descriptions) in PT/EN/ES.
**Cadence:** annual, released each spring for the preceding May reference period → recurring Prefect pipeline (step 12).

---

## 1. Source facts (verified against bls.gov, 2026-08-24)

OEWS publishes one release per year with a **May reference period**. Files live under
`https://www.bls.gov/oes/special-requests/`. `download.bls.gov` and `www.bls.gov` both
require a browser User-Agent (403 otherwise) — see `reference_bls_flat_files`.

| Era | Years | Files | Shape |
|---|---|---|---|
| 1 | 1997–2002 | `oes{YY}{nat,st,ma,in3\|in4}.zip` | Title-row preambles, per-year header offsets, `h_wpct10`-style names; **1997–1999 use OES occupation codes, not SOC** |
| 2 | 2003–2010 | `oesm{YY}{nat,st,ma,in4}.zip` | Separate `.xls` per geography/industry level; geography implied by filename; no `area_type`/`naics`/`own_code` columns |
| 3 | 2011–2016 | `oesm{YY}all.zip` → one `.xlsx` | 29 cols, stacked; has `area_type`, `naics`, `own_code`, single `group` |
| 4 | 2017–2025 | `oesm{YY}all.zip` → one `.xlsx` | 30–32 cols; `group` split into `i_group`/`o_group`; `prim_state`, `pct_rpt` added |

**Scope — user decision: eras 2–4, i.e. 2003–2025.** Era 1 is excluded: 1997–1999 use the
pre-SOC OES occupational taxonomy with no crosswalk shipped, and 2000–2002 need per-year
header handling for three years of data. 2003 and 2004 also have a November survey
(`oesn03`, `oesn04`); **May only** is used so the whole panel is a single May series.

Total download: **1.3 GB, 47 zips**. Header signatures across all 32 era-2 zips reduce to
**14 distinct shapes**, all subsets of one union schema after normalising case and spaces.

### Sentinels (from the source's own Field Descriptions sheet)

| Sentinel | Meaning | Handling |
|---|---|---|
| `*` | Wage estimate not available | → NULL |
| `**` | Employment estimate not available | → NULL |
| `#` | Wage ≥ $115.00/hour or $239,200/year (**top-code**, not missing) | → NULL **+ `wage_top_coded` flag** |
| `~` | Percent of establishments reporting < 0.5% (**bound**, not missing) | → NULL **+ `establishments_reporting_below_threshold` flag** |

*User decision: numeric columns get nulled and flagged.* Both flagged fields are numeric
(FLOAT64), so a dictionary entry does not apply — the flag is a separate STRING column.

`#` is rare and structured: in May 2025, 380 of 413,527 rows carry it, and it always covers
a **contiguous upper run** of the wage ladder (362 rows: the entire ladder; 13 rows: `pct90`
only; 1 row: `median`+`pct75`+`pct90`), with the hourly and annual ladders always masked at
the same positions. One row-level flag is therefore sufficient to recover which values were
top-coded rather than suppressed.

---

## 2. Tables — *user decision: split area vs industry*

**2 data tables + `dicionario` = 3.** OEWS publishes two distinct estimate universes in one
file; splitting them makes every column dense in its own table.

| Table | Universe | Grain | Rows (2003–2025, est.) |
|---|---|---|---|
| `area` | Cross-industry estimates by geography | year × area × ownership × occupation | ~5.5M |
| `industry` | National estimates by NAICS industry | year × industry × ownership × occupation | ~3.5M |
| `dicionario` | Code → label for the coded columns | — | ~40 |

### Split rule

A row belongs to `area` iff its NAICS code is one of the six OEWS **cross-industry** codes:

| NAICS | `own_code` | Meaning |
|---|---|---|
| `000000` | `1235` | Cross-industry, all ownerships |
| `000001` | `5` | Cross-industry, private ownership only |
| `999001` | `123` | Federal, state and local government |
| `999101` | `1` | Federal government, including the Postal Service |
| `999201` | `2` | State government, including schools and hospitals |
| `999301` | `3` | Local government, including schools and hospitals |

Everything else is a real (or OEWS-aggregate) industry and goes to `industry`.

Verified on May 2014 and May 2025: this set is **exactly equivalent** to
`i_group LIKE 'cross-industry%'` where `i_group` exists (2017+), it is **1:1 with
`own_code`** on the area side, and every non-cross-industry row is `area_type = 1`
(national). Because the mapping is 1:1, `area` carries `ownership_id` and drops the
pseudo-NAICS entirely. The cleaning code **asserts** both invariants per year and fails
loudly if a future release breaks them.

Do not use the sibling codes `999000`/`999100`/`999200`/`999300` — those are *industry*
estimates for government excluding schools and hospitals, and belong in `industry`.

### Ownership is derived from the pseudo-NAICS code, not from `own_code`

All ten pseudo-NAICS codes determine their own ownership, so `ownership_id` is taken from
the industry code on those rows. This is not cosmetic: **the May 2012 release publishes
`own_code = 5` (Private) on every one of them**, where 2011, 2013 and 2014 all publish
`1235` on `000000`. Left as published, May 2012 would label the cross-industry totals as
private, return nothing for `ownership_id = '1235'`, and put `000000` and `000001` on the
same area-table key. Ordinary industry rows keep the published code, and the cleaning run
logs every row where the two disagree.

### Keys

BLS republishes some rows twice under two different level tags — the same broad occupation
tagged both `broad` and `detailed` when nothing below it is published (1,176 such pairs in
2025), and the same industry tagged at two `i_group` levels (2,349 pairs). **The measure
values are byte-identical**; only the level tag differs. The rows are kept faithfully and
the level tags join the key:

- `area`: `(year, area_id, ownership_id, occupation_id, occupation_group)`
- `industry`: `(year, industry_id, ownership_id, occupation_id, occupation_group, industry_group)`

Verified zero duplicates on both keys for May 2014 and May 2025.

---

## 3. Columns

Order per `data-basis-style.md`: partition → identifiers → descriptive. All names English.

### `area`

| Column | Type | Unit | Source | Notes |
|---|---|---|---|---|
| `year` | INT64 | year | derived | partition; FK `br_bd_diretorios_data_tempo.ano:ano` |
| `area_id` | STRING | | `AREA` | `99` national, state FIPS, CBSA code, OEWS nonmetro code |
| `area_type` | STRING | | `AREA_TYPE` | dictionary |
| `state_abbreviation` | STRING | | `PRIM_STATE`/`ST` | `US` for national |
| `ownership_id` | STRING | | `OWN_CODE` | dictionary |
| `occupation_id` | STRING | | `OCC_CODE` | SOC or OEWS-specific code |
| `occupation_group` | STRING | | `O_GROUP`/`GROUP` | dictionary |
| `area_name` | STRING | | `AREA_TITLE` | |
| `occupation_name` | STRING | | `OCC_TITLE` | |
| `employment` | INT64 | worker | `TOT_EMP` | rounded to nearest 10 by source |
| `employment_prse` | FLOAT64 | percent | `EMP_PRSE` | |
| `jobs_per_1000` | FLOAT64 | ratio | `JOBS_1000` | state/MSA only |
| `location_quotient` | FLOAT64 | ratio | `LOC_QUOTIENT` | state/MSA only |
| `hourly_wage_mean` | FLOAT64 | USD | `H_MEAN` | |
| `annual_wage_mean` | FLOAT64 | USD | `A_MEAN` | |
| `wage_mean_prse` | FLOAT64 | percent | `MEAN_PRSE` | |
| `hourly_wage_percentile_10/25`, `hourly_wage_median`, `hourly_wage_percentile_75/90` | FLOAT64 | USD | `H_PCT*`/`H_MEDIAN` | |
| `annual_wage_percentile_10/25`, `annual_wage_median`, `annual_wage_percentile_75/90` | FLOAT64 | USD | `A_PCT*`/`A_MEDIAN` | |
| `annual_wage_only` | STRING | | `ANNUAL` | `TRUE`/`FALSE` |
| `hourly_wage_only` | STRING | | `HOURLY` | `TRUE`/`FALSE` |
| `wage_top_coded` | STRING | | derived | `TRUE` when any wage field carried `#` |

### `industry`

Same occupation, wage and flag columns, with the geography block replaced by:

| Column | Type | Source | Notes |
|---|---|---|---|
| `industry_id` | STRING | `NAICS` | NAICS or OEWS-aggregate code |
| `industry_group` | STRING | `I_GROUP` | dictionary; NULL before 2017 (source field did not exist) |
| `industry_name` | STRING | `NAICS_TITLE` | |
| `percent_total_employment` | FLOAT64 | `PCT_TOTAL` | percent |
| `percent_establishments_reporting` | FLOAT64 | `PCT_RPT` | percent; absent before 2017 |
| `establishments_reporting_below_threshold` | STRING | derived | `TRUE` when the source reported `~` (< 0.5%) |

`industry` carries no geography columns — every row is national.

### Types

Per `feedback_type_by_arithmetic_meaning`: only genuine quantities are numeric and every
numeric column carries a `measurement_unit`. All codes (`area_id`, `area_type`,
`ownership_id`, `occupation_id`, `industry_id`, the `*_group` level tags) and all flags are
STRING. `covered_by_dictionary = yes` only for the four coded columns whose labels live in
`dicionario`: `area_type`, `ownership_id`, `occupation_group`, `industry_group`.

---

## 4. Directories

The three code systems all change vintage inside the panel, so none gets a
`directory_column` — an unresolved FK would drop the whole column at
`upload_columns_from_sheet`, and a `relationships` test would fail on the older years.
Each records the intended link in `observations`, following the `us_bls_qcew` precedent:

| Column | Why no FK | Noted in `observations` |
|---|---|---|
| `occupation_id` | SOC 2000 (2003–2009), SOC 2010 (2010–2018), SOC 2018 (2019–2025), plus OEWS-specific codes and the `00-0000` all-occupations total | Codes from 2019 align with `br_bd_diretorios_us.soc_2018:id_soc` |
| `industry_id` | NAICS vintage changes every five years, plus OEWS aggregates and the `999*` government pseudo-codes | Real codes align with the matching `br_bd_diretorios_us.naics_*` vintage |
| `area_id` | One column holds national, state FIPS, CBSA and OEWS nonmetro codes | `area_type = 2` rows carry state FIPS (`br_bd_diretorios_us.state:id_state`); `area_type = 4` rows carry CBSA codes (`br_bd_diretorios_us.cbsa_2023:id_cbsa`) |

`year` does link to `br_bd_diretorios_data_tempo.ano:ano`.

---

## 5. Era-2 harmonisation (2003–2010)

Era 2 ships the same estimates split across files with the dimension columns implied by the
filename rather than stored. The union schema is reconstructed as:

| File group | Table | Synthesised |
|---|---|---|
| `national_*_dl.xls` | `area` | `area_id='99'`, `area_type='1'`, `state_abbreviation='US'`, `ownership_id='1235'` |
| `state_*_dl.xls` | `area` | `area_id` = `AREA` (2-digit FIPS), `area_type` `2`/`3` by FIPS, `ownership_id='1235'` |
| `MSA_*`, `aMSA_*` | `area` | `area_type` from the pooled 2011–2016 `AREA`→`AREA_TYPE` lookup, else `4` |
| `BOS_*` | `area` | `area_type='6'` (nonmetropolitan) |
| `national_*owner*_dl.xls` | `area` | `area_id='99'`, ownership title → `ownership_id` |
| `nat{3d,4d,5d,sector}_*_dl.xls` | `industry` | `ownership_id` = all-ownership code |
| `nat{3d,4d}_*owner*_dl.xls` | `industry` | ownership title → `ownership_id` |

Verified: `MSA_*`, `aMSA_*` and `BOS_*` area codes **do not overlap** in any year, so the
union is clean. Era 2 has no `i_group`, so `industry_group` is NULL for those years, exactly
as for 2011–2016.

---

## 6. Onboarding steps (per `onboarding-workflow.md`)

1. **context** — org `us-bls`, theme `economics`, area `us`, public-domain licence. ✔
2. **architecture** — 3 CSVs → Google Sheets (`area`, `industry`, `dicionario`).
3. **download** — 47 zips → `~/Downloads/us_bls_oes_data/input/` (browser UA). ✔ 1.3 GB
4. **clean** — Python; shared pure functions in `pipelines/datasets/us_bls_oes/utils.py`,
   imported by `models/us_bls_oes/code/` (DRY with the step-12 pipeline). Hive-partitioned
   all-STRING parquet at `output/<table>/year=YYYY/data.parquet`.
5. **upload** — BigQuery **dev** (`basedosdados-dev`), per table; verify row counts.
6. **dbt** — 3 models + `schema.yml`; `safe_cast` every column.
7. **validate** — dbt tests; spot-check national all-occupations employment and median wage
   against the BLS published figures for a few years.
8–9. **discover / metadata (dev)** — register `under_review`, then publish on dev/staging.
   **→ PAUSE: verification checkpoint.**
10. **metadata (prod)** — promote after approval.
11. **PR** — `feat(us_bls_oes): onboard BLS Occupational Employment and Wage Statistics`.
12. **pipeline** — annual Prefect flow; poll the OEWS tables page for the new May year and
    append that year's partition. `AllFree` (annual cadence → no BD Pro window).
13. **publish** — prod `under_review` → `published`, post-merge only.
14. **cleanup** — delete `~/Downloads/us_bls_oes_data/`.

## 7. BD Pro

**None.** The BD Pro rolling window applies to tables refreshed monthly or more often;
OEWS is annual, so both tables are `AllFree` with a single free Coverage.

## 8. Open risks

- **Era-2 `area_type` for metropolitan divisions.** 2003–2010 files do not carry the field.
  For 2005–2010 it is reconstructed from a lookup pooled over the 2011–2013 releases
  (`code/area_type_map.csv`, 641 areas); roughly 1,000–1,300 rows a year have no match and
  fall back to `4`, and the cleaning run logs the count. The reconstruction is corroborated
  by 2010 resolving to exactly the same area counts as 2011 — 380 MSAs, 34 metropolitan
  divisions, 172 nonmetropolitan areas.
- **2003–2004 metropolitan geography.** Those releases predate the CBSA delineations and
  carry 4-digit MSA/PMSA codes, a different code system from the 5-digit CBSA codes used
  from 2005. They all carry `area_type = 4`, since metropolitan divisions did not exist as
  such, and they do not join to `cbsa_2023`. Nonmetropolitan estimates begin in 2006.
  Documented on `area_id` and `area_type`.
- **Era-2 all-ownership code.** Pre-2009 national industry files have no ownership column.
  The assigned code (`1235`) was validated against May 2009, which ships both the plain and
  the by-ownership files: the by-ownership rows sum to the plain rows (median ratio 1.000,
  383 of 758 cells exact and the rest within the source's rounding to the nearest 10),
  confirming the plain files are all-ownership totals rather than private-only.
- **Cross-year comparability.** BLS warns OEWS estimates are not designed as a time series
  (occupational/industrial/areal definitions and estimation methods change). Stated in the
  dataset and table descriptions rather than silently stacked.
