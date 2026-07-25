# CPS (Current Population Survey) — Onboarding Plan

Status: **planning settled 2026-07-12** (revised to CEPR basis). Nothing downloaded/cleaned/registered yet.
Branch: `claude/cps-data-onboarding-5f6c90`. Backend: **staging** (dev is down — see memory).

Source program: US Census Bureau + Bureau of Labor Statistics, Current Population Survey.
<https://www.census.gov/programs-surveys/cps.html>

**Approach:** reproduce the **CEPR Uniform Data Extracts** (harmonization already done, GPL programs) on
public-domain source data. This gives consistent variable names/codes across years — no layout drift —
without re-hosting restricted data.

---

## Naming

| Field | Value |
|-------|-------|
| Organization | `us_census` (reuse the ACS org — "US Census Bureau") |
| Dataset slug | `cps` |
| GCP dataset id | `us_census_cps` |
| Dataset name | English; descriptions in PT/EN/ES |
| Table slugs | English (`basic_monthly`, `org`, `march`, `dictionary`) |

Column names: **CEPR harmonized variable names**, lowercased (`age`, `female`, `wbho`, `lfstat`,
`educ`, `rw`, `orgwgt`). `original_name` carries the CEPR variable name; descriptions carry semantics
(PT/EN/ES). Where a CEPR variable derives from raw Census codes, note the raw code in `observations`.

---

## License

**Public Domain** — the published dataset. Rationale:
- Underlying data are facts from **public-domain sources**: raw Census/BLS CPS microdata and the NBER
  MORG files (US Government works, 17 U.S.C. §105).
- Harmonization **method** is CEPR's, distributed under the **GNU GPL** (programs + extracts). We
  reproduce the extracts by running CEPR's GPL programs on public-domain inputs. Program *output* run
  on public-domain data is not a GPL derivative work, so the resulting data is not GPL-encumbered.
- **Attribution:** credit CEPR (harmonization method / Uniform Data Extracts) and Census/BLS/NBER (raw
  source) in dataset + table descriptions. `has_sensitive_data = no` (Title 13 de-identified).
- We do **not** re-host CEPR's output files (that route would carry GPL copyleft) — see D5.

CEPR citation to include: "Center for Economic and Policy Research. CPS ORG / March Uniform Extracts.
Washington, DC." Version + timestamp captured at build.

---

## Standard-schema finding (why CEPR)

No single universal schema, but CEPR is the harmonized schema most used in labor economics and is
**already done, openly licensed (GPL), and reproducible**.

- **IPUMS-CPS** — the largest harmonization (1962+), but Terms of Use block re-hosting. Reference only.
- **CEPR Uniform Data Extracts** (ceprdata.org) — GPL data + Stata programs; consistent names/codes
  across years; the ORG (earnings) extract is a labor-economics standard. **Chosen basis.**
- **Census microdata API / `cpsR` / NBER CPS** — public-domain raw; the inputs CEPR's programs consume.
- **EPI Microdata Extracts** (microdata.epi.org) — a parallel harmonized product; reference/cross-check.

---

## Settled decisions (user-approved 2026-07-12)

- **D1 — v1 scope:** three CEPR extracts — **Basic Monthly + ORG + March (ASEC)**. Topical supplements
  (Voting, Food Security, Fertility, …) deferred to **v2**.
- **D2 — schema:** adopt **CEPR's harmonized, curated variable set** (consistent names/codes across
  years). *Not* a full raw dump — this is the fix for layout drift.
- **D3 — record structure:** **flat person-level** for all three tables (CEPR ships person records with
  household/family attributes attached). *Not* the raw household/family/person hierarchy.
- **D4 — coverage (REVISED 2026-07-16, see D-coverage-cap):** capped at what CEPR v2.5 actually reproduces —
  **Basic Monthly 1994–2019, ORG 1979–2019, March 2014–2018**. (Raw inputs are downloaded through
  Jun 2026 / 2025 and stay on disk for a later extension, but v1 ships at the CEPR cap.)
- **D5 — build route:** **reproduce → Public Domain.** Run/port CEPR's GPL programs on NBER + raw
  Census; publish as Public Domain, crediting CEPR. (March limited to 2014+ because CEPR's 1980–2013
  March extract depends on proprietary **Unicon** inputs we do not have; extending it backward would
  require re-hosting CEPR's GPL file — deferred to v2, see open decisions.)
- **D6 — build engine (two-phase):** **(A)** run CEPR's `.do` programs in **Stata** to generate the
  extracts → convert to parquet → validate against CEPR's published files. **(B)** *after* validation
  passes, **port the harmonization logic to Python** (the pipeline's native stack), using the validated
  Stata-generated parquet as the **golden regression fixture** the Python port must reproduce exactly.
  Stata is the reference implementation; Python is the maintainable production path.

---

## Data architecture

### Tables (v1)

| # | Table | Grain | Coverage | Partition | Reproduced from |
|---|-------|-------|----------|-----------|-----------------|
| 1 | `basic_monthly` | person, all rotation groups | 1994–2019 | `year` (+ `month`) | CEPR Basic Programs on raw Census basic monthly |
| 2 | `org` | person, outgoing rotation months (earnings) | 1979–2019 | `year` (+ `month`) | CEPR ORG programs on NBER MORG (1979–93) + raw CPS (1994+) |
| 3 | `march` | person (hh/family attrs attached) | 2014–2018 | `year` | CEPR March programs on raw Census March |
| 4 | `dictionary` | code/value → label | — | — | CEPR codebooks |

Note: `org` 1994+ overlaps `basic_monthly` (ORG months ⊂ basic monthly). Both are kept because
ORG adds 1979–93 history and the ORG-specific earnings weight; document the overlap in `observations`.

### Observation level

**Person (individual)** for all three data tables. No separate household/family tables (D3). Register a
single `person`/`individual` entity level (likely already exists from ACS — confirm at discover step).

### Variable content (final column lists come from CEPR codebooks at the architecture step)

- **basic_monthly / org:** year, month, state; demographics (`age`, `female`, `wbho`/`wbhao`
  race, `hispanic`, `married`, `forborn`, `citizen`, `educ`); labor (`lfstat`, `empl`, `unem`, `nilf`,
  `selfemp`, `class` class-of-worker, hours); industry/occupation (CEPR harmonized `ind`/`occ`); union
  (`union`, `unioncov`); earnings — ORG only (`rw` real hourly wage, `wage`/`weekpay`, `paidhre`,
  `hourslw`); weights (`orgwgt`/earnings weight, `basicwgt`/final weight); metro/CBSA where present.
- **march:** demographics; income (`hhincome`, `faminc`, person income components); poverty
  (`povcut`, poverty status); health insurance (`hcov`, `hipriv`, `himcaid`, `himcare`); work
  experience (weeks/hours last year, full/part-time); weights (`marchwgt`/supplement weight).

### Type-by-arithmetic-meaning

- Quantities → INT64/FLOAT64 **with** `measurement_unit`: `age`, hours, weeks, earnings, income, all
  weights (weights are dimensionless — note in `observations`).
- Coded → **STRING** + `covered_by_dictionary = yes`: `lfstat`, `class`, `wbho`, `educ`, marital,
  citizenship, all flags. FIPS/CBSA codes → STRING. Industry/occupation codes → STRING (see FKs).

### Directory foreign keys (confirm exact FK columns at architecture step)

| Column | Directory FK | Notes |
|--------|-------------|-------|
| `year`, `month` | `br_bd_diretorios_data_tempo.ano:ano`, `.mes:mes` | local cols `year`/`month`; values match the directory's `ano`/`mes` |
| `state` (FIPS) | `br_bd_diretorios_us` state table (FIPS key) | CPS public-use geography is state-level |
| `ind` (industry) | Census **industry** directory (ATUS, PR #1650) — **or** CEPR-code `dictionary` | resolve alignment: do CEPR harmonized ind codes match the directory keys? (open decision) |
| `occ` (occupation) | Census **occupation** directory (ATUS, PR #1650) — **or** CEPR-code `dictionary` | same |

---

## Sources & build mechanism

**Reproduce (D5):** run CEPR's GPL programs on public-domain inputs, then load the harmonized output
into the parquet/dbt pipeline. Validate against CEPR's published extract files.

- CEPR programs (GPL):
  - ORG master: `cepr_org_master.do` — <https://ceprdata.org/cps-uniform-data-extracts/cps-outgoing-rotation-group/>
  - Basic Monthly programs (1994+): <https://ceprdata.org/cps-uniform-data-extracts/cps-basic-programs/cps-basic-monthly-programs/>
  - March programs (2014+): <https://ceprdata.org/cps-uniform-data-extracts/march-cps-supplement/march-cps-programs/>
- Raw public-domain inputs:
  - NBER MORG (ORG 1979–93): <https://data.nber.org/morg/>
  - Raw Census basic monthly (1994+): `https://www2.census.gov/programs-surveys/cps/datasets/<yyyy>/basic/`
  - Raw Census March (2014+): `https://www2.census.gov/programs-surveys/cps/datasets/<yyyy>/march/`
- CEPR published extract files (AWS S3) — **validation cross-check only**, not the hosted source.

---

## Architecture Google Sheets (Drive)

Built from the repo CSVs (step 2). Folder `us_census_cps`:
<https://drive.google.com/drive/folders/1DqCeel9QPb9VMgOOJgHmcqFS6GRHnurk>

| Table | Sheet ID (use as `architecture_url`, tab `arquitetura`) |
|-------|--------------------------------------------------------|
| org | `1UIVHEKm2OipXEXUrOuJdGd3GiyO_wmQA-FyfdeKvgWw` |
| basic_monthly | `1980N1ohYTJ1JiRBUJhFs5urNFbZwT24eAH9rR88_Srw` |
| march | `1BDBodhF8Ikl5b152JWPtKY8O9V9LYMPKobTFOEpHxu8` |
| dictionary | `1CIkNnZY1EUVPlWKriAMnpasWCo54nDZmMBbGpfoJlhY` |

## Known complications (document in table `observations`)

1. **Two-phase build (D6)** — CEPR's programs are `.do` files. Phase A runs them in Stata (reference /
   fidelity); Phase B ports the logic to Python, validated against the Stata-generated parquet. Stata is
   a Phase-A dependency only; production runs on the Python port.
2. **March limited to 2014+** — pre-2014 CEPR March depends on proprietary Unicon inputs; not reproducible
   by us. Backfill only by re-hosting CEPR's GPL file (v2).
3. **ORG ⊂ Basic Monthly for 1994+** — document the overlap; earnings exist only in ORG months.
4. **Industry/occupation alignment** — confirm whether CEPR's harmonized `ind`/`occ` codes map to the
   Census directory keys or need their own `dictionary` entries.
5. **CEPR versioning** — capture the exact CEPR extract version + timestamp used; note in provenance.

---

## Onboarding checklist (11-step workflow)

- [ ] **1. context** — confirm CEPR program URLs + versions, raw input URLs, coverage, org, themes/tags.
- [~] **2. architecture** — 4 sheets built as CSVs in `models/us_census_cps/architecture/`
      (org 162, basic_monthly 141, march 477, dictionary 5); generator + parsed CEPR
      inventory in `architecture/build/`. Column lists parsed from CEPR `*_keepord.do`; types by
      arithmetic meaning; `original_name` = CEPR variable; descriptions trilingual (EN from CEPR labels).
      **Pending:** user review of CSVs → convert to Google Sheets; confirm `state` →
      `br_bd_diretorios_us` FK column.
- [x] **3. download** — DONE via `download_raw.sh` (resumable) → `input/` (6.0 GB, gitignored, Dropbox-synced).
      CEPR programs: 95 `.do` in `input/cepr_programs/`. **Basic** 389 files 4.4 GB (1994–Jun 2026; only
      genuine gap = `oct25`, not yet posted by Census). **March** 13 files 1.2 GB (2014–2025; 2014 has
      5/8 traditional + 3/8 redesign). **MORG 15/15 valid** in `input/nber_morg/` (1979–93, ~320–386k rows
      each; all read via `pandas.read_stata(convert_categoricals=False)`). March naming:
      `asecpub<yy>csv.zip` (2019+), `asec<yyyy>_pubuse*.dat.gz` (2014–18).
      **Inputs complete — step 3 closed.**
      **⚠ Reproduce coverage cap:** CEPR v2.5 programs read only through **2019** (ORG/basic) and **2018**
      (March) — see D-coverage-cap. Raw data runs to 2025/2026 (ready for extension), but Phase-A Stata
      output stops at CEPR's latest year.
- [~] **4a. clean (Stata)** — staged via `stage_build.sh` → build tree at `~/cps_build` (OUTSIDE Dropbox:
      CEPR gunzips/recompresses in place and emits ~312 per-month `.dta`). Stata **16.1 MP** at
      `/Applications/Stata/StataMP.app/Contents/MacOS/stata-mp` (not on PATH).
      **Key insight:** NBER inputs are just the Census files renamed (CEPR comment: `orginal file name
      "month"19pub.dat.Z`) → `input/census_basic/<mmm><yy>pub.dat.gz` → `$locbas/<year>/cpsb_<year>_<m>.txt.gz`;
      MORG `morg<yy>.dta` → `$locin/morg<yyyy>.dta` (4-digit; `year` var already 4-digit so CEPR's y2k step
      is NOT needed). Master patched: `gnulin=1` + 6 path globals.
      Pre-flight all green: `set mem` ok, `gzip -d`/`-vN` ok, `saveold` ok, and a one-month smoke test read
      141,875 rows × 392 vars with `prtage` non-missing and `hrmis` 1–8.
      **Run 1 (`cepr_org_master.do`): partial success.** Built all 312 `cps_basic_raw_<y>_<m>.dta` and
      ORG 1979–93 (`$loctmp/cepr_org_1979..1993.dta`), then died at Part 3 with
      `command fullyr is unrecognized` r(199) — **note Stata still exited 0, so never trust the exit code
      here; grep the log for `^r([0-9]+);`.** Cause: loading `cepr_basic_read_all.do` (26 `b` programs)
      leaves the parser collecting lines, so the master's own inline `fullyr` definition (line 141) got
      swallowed — `fullyr` defines fine standalone (rc=0), so it is a parse-context bug, not a syntax bug.
      **Fix = `resume_org.do`** (generated from the master): comments out line 108 `do cepr_basic_read_all.do`,
      the 26 `b19xx/b20xx` calls, and the `orgnber 1979..1993` call — all already built, so it resumes at
      `fullyr`. Confirmed working: 312/312 `cepr_org_<y>_<m>.dta`, 0 errors.
      **Re-running is cheap** — the monthly parse is cached; never re-run Part 2b.
      Watch disk (56 GB free at start → 29 GB after ORG assembly).
      **✅ ORG 1979–2019 BUILT + VALIDATED.** 41 annual files in `$loctmp/cepr_org_<year>.dta`
      (162 cols each — exactly matching the `org` architecture sheet). Verified against CEPR's published
      extracts (`https://ceprdata.s3.amazonaws.com/data/cps/data/cepr_org_<year>.zip`, unzipped to
      `~/cps_build/validate/`): rows, column count, column order, and an order-insensitive row-hash
      multiset over all 162 columns are **IDENTICAL for 14/14 years checked** (`validate_org.py`):
      1979, 1985, 1993, 1994, 1995, 1998, 2003, 2005, 2007, 2010, 2013, 2015, 2017, 2019 — i.e. every
      CPS layout-era boundary plus all three code paths (MORG, first CPS-Basic year, latest year).
      **Validation gotcha:** element-wise comparison shows spurious diffs because CEPR's `sortit` is a
      non-stable sort — row ORDER differs run to run. Compare with sorted row hashes, not element-wise.
      **`march` (2014–2018):** staged via `stage_march.sh` → `~/cps_build/CPS_March`. Same NBER-style
      renaming (CEPR comment: `orginal file name asec2015_pubuse.dat.gz`) →
      `$locraw/<year>/cpsm_<year>.txt.gz` (kept gzipped; the `m<year>` programs gunzip themselves);
      2014 needs BOTH 5/8 traditional (`cpsm_2014.txt.gz`) and 3/8 redesign (`cpsm_2014_redes.txt.gz`,
      read by `comb2014`, consumed by `marcensus2`). Master patched: `gnulin=1`, 7 path globals, and the
      UNICON path disabled (`y2k`, `marunicon1/2/3` — 1980–2013 needs proprietary Unicon we lack).
      `$repwgt` is NOT needed (its only call is already commented out upstream).
      **Same parse bug hit again:** run 1 died at `marcensus 2014` → `file mar2014.dta not found` r(601),
      because the read block (`do cepr_march_read_all.do` + `m2014..m2018` + `comb2014`) was swallowed
      into a comment/continuation (log shows those lines prefixed `>` with a stray trailing `*/`), so the
      reads never executed. **Fix = `march_read_driver.do`** — runs the reads standalone (globals + the
      6 program calls), then the master handles `marcensus`/`marcensus2` from the resulting `mar<year>.dta`.
      **✅ MARCH BUILT.** Driver produced `$loctmp/mar{2014,2014_research,2015..2018}.dta`; re-running the
      master then wrote 6 outputs to `$locout/cepr_march_<tag>.dta` (2014 5/8, 2014_research 3/8, 2015–18),
      0 errors. **477 columns — exactly matching the `march` architecture sheet** (2nd independent
      confirmation after ORG's 162).
      **✅ MARCH VALIDATED — 5/6 byte-identical, 2016 explained** (`validate_march_final.py`). A naive
      comparison reports all 6 failing; three artifacts must be handled or you get FALSE FAILURES:
      1. **`hjrid`** — arbitrary sequential ID; values merely swap between adjacent tied rows under the
         non-stable sort. **Exclude it** (same artifact class as ORG row order).
      2. **`cert`/`certgov`** — present in my build for all years but **entirely empty** pre-2015 (the CPS
         certification questions didn't exist); CEPR's published 2014–16 files predate those columns being
         added to the programs. Compare **shared columns only**. Zero data difference; my rebuild is
         internally consistent across years where CEPR's published set is not.
      3. **2016 tax block** — after 1–2, 2016 still differs in exactly 9 CEPR tax-model columns
         (`taxinc, filestat, margtax, agi, fedtaxbc, fedtaxac, sttaxbc, sttaxac, eitc`; 0.008–1.9% of rows).
         Cause is **input revision, not code**: `cepr_march_tax.do` applies uniform `2014<=year<=2018`
         logic with no randomness/external deps, and 2014/15/17/18 match exactly. My input is Census's
         **revised `asec2016_pubuse_v3`** — now the ONLY 2016 file Census distributes — while CEPR's
         published 2016 was built pre-revision. **Our output reflects the corrected source data.**

      **`basic_monthly` (1994–2019) — AUTHORED, no CEPR master exists.** `orgcpsb`'s chain is:
      monthly raw → `fullyr` (`keep if hrmis==4|8`) → `combcps` (annual append) → `cepr_basic_*` topic
      programs → `cepr_org_wages` → `keepord`. Our build mirrors that in
      `CPS_Basic/CEPR/DoFiles/basic_monthly_build.do` (standalone driver, per the parse-bug lesson) with
      **user-approved defaults**: all 8 rotation groups (no `hrmis` filter); **age 16+** (CEPR's universe);
      drop **`pwsswgt<0` only** (`pworwgt` is ORG-specific); **141 cols** via a `keepordbas` variant
      (ORG's `keepord` minus the 21 ORG-only vars); **no `cepr_org_wages.do`** (earnings are ORG-only).
      Programs: `basyr` (monthly subset → annual append) + `basmonth` (harmonize + keepordbas).
      **Acceptance test (no CEPR reference):** for `minsamp==4|8` person-months, `basic_monthly` must
      equal the validated ORG output on shared columns. 2019 test: **141 cols = sheet exactly**, rotation
      groups 1–8 present, ORG-month subset = **291,390 rows (exact match to ORG)**, and **139/141 columns
      byte-identical**.
      **⚠ `imphrs`/`uhoursi` differ by design (D-hours-imputation, RESOLVED 2026-07-22 = keep
      all-rotation fit).** CEPR imputes "hours vary" respondents with `reg pehrusl1 ...` fit on whatever
      sample is in memory; ORG fits on rotation groups 4&8, `basic_monthly` on all 8 → predictions differ
      ±1h after rounding (mean +0.11h `imphrs` / +0.005h `uhoursi`; 0.5%/0.3% of ORG-month rows; identical
      non-missing counts). Not a bug — the all-rotation fit uses ~4× the estimation sample and is more
      precisely estimated. **Consequence: these 2 columns will not tie out against the `org` table.**
      **✅ BUILT + ACCEPTANCE TEST PASSED — 26/26 years** (`validate_basic_monthly.py`). Every year's
      `minsamp==4|8` subset equals the validated `org` table on all **139** shared columns, with row
      counts matching exactly (e.g. 2019: 1,149,244 total → 291,390 ORG-month = org's 291,390).
      `imphrs`/`uhoursi` deltas are small and stable across all 26 years (+0.09 to +0.17h `imphrs`,
      +0.004 to +0.011h `uhoursi`) — consistent with the approved all-rotation fit, not drift.

      **Parquet conversion** — `to_parquet.py` (in repo). Layout per bigquery-conventions:
      `output/<table>/year=<Y>/month=<M>/data.parquet` (org, basic_monthly) and
      `output/<table>/year=<Y>/data.parquet` (march, annual — `month` stays a DATA column).
      Types enforced from the architecture CSVs via an explicit pyarrow schema, so partitions can't drift.
      **Verified:** march file on disk = 476 cols (`year` excluded — it lives in the path), hive dataset
      view reconstructs it to 477 with types 22 int64 / 256 string / 198 double = architecture exactly.
      *Note:* `pq.read_table()` on a `year=YYYY/` path re-adds `year` as a `dictionary<int32>` — that is
      pyarrow partition reconstruction, NOT a schema bug; inspect `pq.ParquetFile(...).schema_arrow` for
      the true file schema.
      **D-march-2014-samples — RESOLVED (include both), user-approved 2026-07-22:** `march` year 2014
      carries BOTH CEPR samples, distinguished by the `research` flag — 5/8 traditional (`research=0`,
      139,415 rows) + 3/8 redesign (`research=1`, 60,141) = **199,556 rows**. Safe to combine: identical
      477-col schema AND column order, and the two samples are **disjoint** (0 overlap on
      `year`+`hhseq`+`pppos`). Implemented via the `extra` map in `to_parquet.py`, which asserts schema
      equality before concatenating.

      **✅ PARQUET DONE — GOLDEN FIXTURE COMPLETE** at `~/cps_build/parquet` (2.3 GB, 46,042,747 rows):
      | table | files | years | rows | cols |
      |-------|------:|------:|-----:|-----:|
      | `org` | 492 | 41 (1979–2019) | 13,169,878 | 162 |
      | `basic_monthly` | 312 | 26 (1994–2019) | 31,922,804 | 141 |
      | `march` | 5 | 5 (2014–2018) | 950,065 | 477 |
      All three schemas conform exactly to their architecture CSVs (names + INT64/FLOAT64/STRING types),
      year coverage is contiguous with no gaps, and parquet row counts equal their source `.dta` files
      on spot-checks across every code path. **This is the fixture the Phase-B Python port must reproduce.**
- [ ] **4b. clean (Python port)** — after 4a validates, reimplement the harmonization in Python; assert
      it reproduces the 4a parquet exactly. Production uses the Python port.
- [x] **4c. dictionary data** — DONE 2026-07-23 via `code/build_dictionary.py` → **23,752 rows**
      (org 6,257 / basic_monthly 5,551 / march 11,944) covering 468 dictionary columns. Labels are read
      off the built `.dta` files (`StataReader.value_labels` + the variable→label-set list) rather than
      re-parsed from `label define` blocks, so they are exactly the sets attached to the shipped columns.
      Where CEPR attaches none: 0/1 indicators become No/Yes and `minsamp`/`mis` become "Month in sample
      N" — each verified against the fixture's actual value set before being written (a dictionary-flagged
      column that none of the three sources can label is a hard error, not a silent skip). Codes whose
      meaning changes over time (industry/occupation vintages) get one row per era with its own
      `cobertura_temporal`.
- [x] **5. upload** — DONE 2026-07-23 → `basedosdados-dev.us_census_cps_staging`, row counts exact
      against the Phase-A fixture (`march` 950,065 / `org` 13,169,878 / `basic_monthly` 31,922,804).
      Script: `code/upload.py` (reads the fixture at `~/cps_build/parquet`, outside the repo).
- [x] **6. dbt** — DONE 2026-07-23. `code/build_dbt_files.py` generates all four `.sql` models plus
      `schema.yml` **from the architecture CSVs**, so the models cannot drift from the sheets. Notes:
      `union` is a BigQuery reserved word and is backticked; sparse columns are listed explicitly in
      each model's `not_null_proportion_multiple_columns.ignore_values` rather than lowering the floor.
- [x] **7. validate** — DONE 2026-07-23. **23/23 dbt tests pass.** External check against published BLS
      annual averages, weighting `basic_monthly` by `fnlwgt`:

      | year | CPS u-rate | BLS | CPS employment (mi) | BLS |
      |------|-----------:|----:|--------------------:|----:|
      | 1995 | 5.7 | 5.6 | | |
      | 2000 | 4.1 | 4.0 | 135.5 | 136.9 |
      | 2010 | 9.7 | 9.6 | 139.4 | 139.1 |
      | 2015 | 5.3 | 5.3 | | |
      | 2019 | 3.7 | 3.7 | 158.2 | 157.5 |

      The unemployment rate reproduces BLS to within 0.1 pp and levels to within 1%. The residual is
      expected: BLS revised its published levels for later population controls, while the microdata
      weights are as originally published.
      Two shared dbt macros needed fixing for this dataset and were changed in place (both are
      general-purpose fixes, not CPS workarounds): `not_null_proportion_multiple_columns` backticks
      column names (CEPR ships a column literally named `union`) and builds its pivot by unnesting an
      array instead of a per-column `UNION ALL`, which overran BigQuery's planner at 478 columns.
      Re-verified against the 10 existing `us_bls_atus` tests — all still pass.
- [x] **8. discover** — DONE. org `us_census` = 8b6e7743-2777-4fb4-85da-3482d409d82b; entity `person` =
      b4e76213-888b-40ea-b877-d82ce76d71a2; area `us` = 61a2c232-c649-4b41-a5a3-1467b7393e11;
      account 57. Directory FKs resolved under D-state-fk below.
- [x] **9. metadata (staging)** — DONE 2026-07-23. Dataset `cps` = ed832f98-377d-491d-b26e-58d31eba3765,
      4 tables, 4 raw data sources, observation levels, cloud tables, coverage (area `us`), datetime
      ranges, update records, and **all 790 columns** — verified on the backend: 0 columns missing any
      of the PT/EN/ES descriptions, `year` flagged as the partition on all three data tables, and the
      directory FKs resolved (`year`→ano, `month`→mes, `id_state`→state, `id_county`→county).
      Both column tools fetch the architecture sheet unauthenticated, so the four sheets had to be
      readable without login. Rather than flipping per-file sharing, they were moved into
      `Base dos Dados/Dados/Conjuntos/us_census_cps/architecture` (Drive folder
      1cyHWR2gQJRkReUzVzK_v3SWd9Z3AHU3u), which is where every other dataset's sheets live and where
      link-viewable permission is inherited. Sheet IDs are unchanged. Before uploading, all four sheets
      were diffed against the local architecture CSVs: **790 rows × 12 fields, zero mismatches**.
      Note for re-runs: the derived columns were *appended* to the sheets (the Sheets API cannot insert
      rows), so `reorder_columns` has to follow the column upload to put `id_state`/`id_county` back
      into position 3-4.
- [x] **— PAUSE: verification checkpoint —** approved 2026-07-23; prod set to `under_review` per request.
- [x] **10. metadata --env prod** — DONE 2026-07-23. Dataset `cps` = fd0ba961-d93e-4d94-aac8-96c95671a22a,
      registered on `backend.basedosdados.org` with **status = under_review** (the user's gate: the
      dataset stays hidden on the site until the flip to `published` after the PR merges and CI
      materialises the prod tables). All four tables `published`, 790 columns (0 missing PT/EN/ES),
      `year` partition on the three data tables, cloud tables → `basedosdados.us_census_cps.*`,
      coverage (area `us`), datetime ranges, updates and raw-source links — all verified on the prod
      GraphQL. Prod org slug is **`census_bureau`** (not `us_census`), cc0 license UUID differs from
      staging, account id is **4**. The two directory datasets are absent from the prod backend, so the
      `directoryPrimaryKey` FK links were silently skipped there (expected; dbt's `ref()` relationship
      tests are independent of backend metadata). **Prod DATA is not loaded by this step** — merging the
      PR triggers CI's `dbt --target prod` (reads `basedosdados-staging.us_census_cps_staging`, writes
      `basedosdados.us_census_cps.*`); the staging-project parquet load is the DB team's deploy path.
- [~] **11. pr** — opening now: changelog + CEPR/GPL + Census/BLS/NBER attribution. After merge: load
      prod data, then flip dataset status `under_review → published`.

---

## Open sub-decisions

- **D-indocc — RESOLVED (dictionary):** CEPR ships multiple era-specific `ind`/`occ`/`docc` codings, so
  they are dictionary-covered STRING columns (no directory FK); labels go in the `dicionario` value data.
- **D-partition — RESOLVED (year/month):** keep CEPR `year`/`month` as the column names (INT64); `year`
  is the partition. FK to `br_bd_diretorios_data_tempo` (values match the directory's `ano`/`mes`).
- **D-coverage-cap — RESOLVED (a) 2026-07-16:** v1 ships at CEPR v2.5's cap — **ORG/basic → 2019, March
  → 2018** — a faithful reproduction validated against CEPR's published extracts. Extending to present
  (raw already on disk: basic → Jun 2026, March → 2025) means writing `cps_*_read_<year>` programs in
  CEPR's pattern; deferred to v2 / the Phase-B Python port.
- **D-march-backfill:** whether to add March 1980–2013 later by re-hosting CEPR's GPL file (v2, GPL span).
- **D-state-fk — RESOLVED (derived `id_state`) 2026-07-23:** CEPR's `state` is **not** FIPS — it is the
  Census/CPS state code (11 Maine … 95 Hawaii), which the sheets previously mislabelled. The directory
  `br_bd_diretorios_us.state` carries exactly that code as `id_census`, and all **51 codes match the
  directory both ways with zero mismatches**, so the FIPS key is derivable by join. Resolution:
  - `state` keeps its CEPR values, is now `covered_by_dictionary = yes` (51 labels), and carries no FK;
  - a derived **`id_state`** (2-digit FIPS) carries `br_bd_diretorios_us.state:id_state`;
  - a derived **`id_county`** (5-digit FIPS = `id_state` ‖ 3-digit `fipscounty`, null when the county is
    unidentified) carries `br_bd_diretorios_us.county:id_county` in `org`/`basic_monthly` (`march` has
    no county code).
  Both are materialised **in the dbt models**, not in the parquet, so the Phase-A fixture stays a
  faithful CEPR reproduction. Verified on `march`: 0 null `id_state`, 51 distinct states.
- **D-identifier-flags — RESOLVED 2026-07-23:** `fipscounty`, `principalcty` and `relahh` were flagged
  `covered_by_dictionary = yes` but are identifiers or unlabelled source codes, not value-labelled
  categories (CEPR's own note: `principalcty` "must combine with cbsa code … to uniquely identify
  principal cities"). All three are now `no`, with the reason recorded in `observations`.

---

## References

- CPS program: <https://www.census.gov/programs-surveys/cps.html>
- CEPR CPS extracts: <https://ceprdata.org/cps-uniform-data-extracts/>
- CEPR ORG: <https://ceprdata.org/cps-uniform-data-extracts/cps-outgoing-rotation-group/>
- CEPR March/ASEC: <https://ceprdata.org/cps-uniform-data-extracts/march-cps-supplement/>
- CEPR Basic Programs: <https://ceprdata.org/cps-uniform-data-extracts/cps-basic-programs/cps-basic-monthly-programs/>
- CEPR license (GPL): <https://ceprdata.org/contact/license/>
- NBER MORG: <https://data.nber.org/morg/> · NBER CPS: <https://data.nber.org/cps/>
- Census CPS datasets: <https://www.census.gov/programs-surveys/cps/data/datasets.html>
- IPUMS-CPS (reference only): <https://cps.ipums.org/cps/>

Related memory: `project-us-census-acs`, `feedback-type-by-arithmetic-meaning`,
`feedback-shared-entity-directories`, `feedback-use-staging-backend`, `project-us-directories`.
