# us_bls_oes — locked onboarding context

Full plan: `ONBOARDING_PLAN.md`. This file is quick-resume context for any session.

**Dataset:** GCP `us_bls_oes` · backend slug `oes` · org `us-bls` · US public domain (17 USC §105)
**Source:** BLS Occupational Employment and Wage Statistics, https://www.bls.gov/oes/tables.htm
**Coverage:** May 2003 – May 2025, annual. `www.bls.gov` 403s without a browser User-Agent.

## Locked decisions (user-approved 2026-08-24)

1. **Years 2003–2025.** Era 1 (1997–2002) excluded: 1997–1999 use the pre-SOC OES
   occupational taxonomy. 2003/2004 November surveys excluded — May only, so the
   panel is one series.
2. **Two tables, split by universe** (not BLS's single stacked file): `area`
   (cross-industry by geography) and `industry` (national by NAICS), plus `dicionario`.
3. **Sentinels that are bounds, not missing values, are nulled *and flagged*:**
   `#` (wage ≥ $115.00/hour) → `wage_top_coded`; `~` (< 0.5% of establishments
   reporting) → `establishments_reporting_below_threshold`. `*` and `**` are
   genuinely missing and are only nulled.
4. English column names; partition `year` INT64; metadata PT/EN/ES.
5. Annual cadence → **no BD Pro window**; both tables `AllFree`.

## Split rule and keys

A row is `area` iff its NAICS is one of the six OEWS cross-industry pseudo-codes
(`000000`, `000001`, `999001`, `999101`, `999201`, `999301`). Those map 1:1 onto
ownership, so `area` carries `ownership_id` and no industry code. Everything else
is `industry` (always national).

- `area` key: `year, area_id, ownership_id, occupation_id, occupation_group`
- `industry` key: `+ industry_group`, and `industry_id` instead of `area_id`

The level tags are in the key because BLS republishes some rows twice under two
tags with identical values (a broad occupation also tagged `detailed`).

## Four source quirks the code handles (all asserted, not assumed)

1. **May 2012 publishes `own_code = 5` (Private) on every cross-industry and
   government pseudo-NAICS row.** 2011, 2013 and 2014 all publish `1235` on
   `000000`. `ownership_id` is therefore derived from the pseudo-NAICS code; the
   run logs every disagreement (2012: 256,717 rows).
2. **2003–2010 files carry no `area_type`.** Rebuilt from `code/area_type_map.csv`
   (pooled 2011–2013, 641 areas) for 2005–2010; ~1,000–1,300 rows a year fall back
   to `4` and the run logs the count. 2010 reconstructs to exactly the 2011 area
   counts (380 MSAs, 34 divisions, 172 nonmetro), which is the corroboration.
3. **2003–2004 metropolitan areas use the pre-CBSA 4-digit MSA/PMSA codes**, a
   different system from the 5-digit CBSA codes used from 2005. Nonmetropolitan
   estimates start in 2006.
4. **May 2022 publishes one row twice, verbatim** (Minneapolis-St. Paul, occ
   43-5053). Exact full-row duplicates are dropped and logged; a key collision
   whose values differ still fails the run.

## Gotchas

- Excel readers return `AREA` as an **integer**, dropping the leading zeros in
  state FIPS (`01`) and nonmetro codes (`0100001`). `_pad_area_id` restores them
  by area type. NAICS and `occ_code` are genuinely text and need no padding.
- Read `.xlsx` with **calamine** (fast) but `.xls` with **xlrd** — calamine panics
  on some of the legacy workbooks.
- Column presence varies by year: `prim_state` 2020+, `pct_rpt` 2003–2010 and
  2021+, `jobs_1000` absent in 2019, `loc_quotient` 2010+, `i_group`/`o_group`
  2017+ (`group` before). The reader reindexes to the union, so a missing field
  is null rather than an error.

## Scratch data

`~/Downloads/us_bls_oes_data/` — `input/` (47 zips, 1.3 GB) and `output/`
(partitioned parquet). Never in the repo or Dropbox. Delete at step 14.

## Directories

No `directory_column` on `occupation_id`, `industry_id` or `area_id`: all three
change code vintage inside the panel, so a `relationships` test would fail on the
older years. The intended links are recorded in each column's `observations`.
`year` links to `br_bd_diretorios_data_tempo.ano:ano`.

## Status

- [x] Source study + plan approved (year range, table split, sentinel handling)
- [x] Architecture CSVs (`code/build_architecture.py`) — area 29, industry 29, dicionario 5
- [x] Cleaning code (`pipelines/datasets/us_bls_oes/{constants,utils}.py` + bootstrap)
- [x] dbt models + `schema.yml` (`code/build_dbt.py`), `dbt_project.yml` entry
- [x] Recurring pipeline (`tasks.py`, `flows.py`) — annual, append-per-year
- [ ] Full clean 2003–2025 → verify row counts
- [ ] Upload dev → dbt run/test → metadata dev → **CHECKPOINT**
- [ ] Prod metadata → PR → pipeline dev run → merge → publish → cleanup
