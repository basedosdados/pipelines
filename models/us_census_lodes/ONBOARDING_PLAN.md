# us_census_lodes — Onboarding Plan (architecture spec)

LEHD Origin-Destination Employment Statistics (LODES), U.S. Census Bureau,
Longitudinal Employer-Household Dynamics program. Job counts by 2020 census
block, by worker and job characteristics, annual 2002–2023.

- GCP dataset id: `us_census_lodes` · org: `us_census` · backend slug: `lodes`
- License: **public domain** (U.S. Government work, 17 U.S.C. §105). Registered
  as `cc0`, the closest slug the backend carries.
- Landing page: https://lehd.ces.census.gov/data/lodes/
- Downloads: https://lehd.ces.census.gov/data/lodes/LODES8/
- Data dictionary: `LODESTechDoc8.4.pdf` at the same location — authoritative for
  every column meaning. Do not paraphrase it from memory.
- Release read at onboarding: **format version 8.4, data vintage 20251202**.

## Decisions locked (user, 2026-09-06)

The brief proposed a LONG reshape and asked whether OD should ship at tract
aggregation. Both were re-decided against measurements taken from the source.

### 1. RAC and WAC ship WIDE, as published

Measured non-zero density of the value columns: **19.3 of 40 per block (RAC)**
and **14.2 of 50 (WAC)** — on Vermont, a rural state, and 29/40 on DC. A long
reshape therefore multiplies rows by ~15–19, putting `residence_jobs` at
**~5.7 bn rows** and `workplace_jobs` at **~2.0 bn**. Wide is 298 M and 146 M.

The published wide layout is not "suffix-column soup": it is 41 (RAC) / 51 (WAC)
count columns with an exact dictionary. Renamed to readable English
(`jobs_naics_23`, `jobs_education_bachelors_or_higher`) it is self-describing.
`us_census_cbp` in this repo reversed the same LONG→WIDE decision for the same
arithmetic; this follows that precedent.

### 2. Segment `S000` only

RAC/WAC filenames carry a `[SEG]` component (`S000`, `SA01`–`SA03`, `SE01`–`SE03`,
`SI01`–`SI03`). A segment file is **not a different column set** — it repeats the
full grid restricted to a worker subset, i.e. it carries two-way interactions
(age × industry, earnings × race, …). Verified: in `dc_rac_SA01_JT00_2022`,
`CA01 == C000` and `CA02 = CA03 = 0`.

Shipping all ten segments would multiply both tables by ten. `S000` alone
preserves every marginal distribution, which is what nearly all use needs, and
matches the `lehdr` R package's default. A `segment` column can be added later
without breaking the schema.

### 3. All six job types, as a column

`job_type` ∈ {JT00 All Jobs, JT01 Primary Jobs, JT02 All Private Jobs, JT03
Private Primary Jobs, JT04 All Federal Jobs, JT05 Federal Primary Jobs}. JT04
and JT05 exist only from 2010 (OPM-supplied federal employment).

### 4. `origin_destination` is deferred to a second PR

Measured OD size, as published (10 count columns, block × block):

| Scope | gz | est. rows |
|---|---|---|
| JT00 only | 14.6 GB | ~2.6 bn |
| JT00 + JT01 | 28.2 GB | ~5.3 bn |
| all six job types | 53.3 GB | ~10.6 bn |

Aggregating to a coarser geography does **not** rescue it. Measured on DC 2022
JT00, 579,640 block-pairs collapse to 296,857 block-group pairs (1.95×) and
146,505 tract pairs (**3.96×**) — county is 409× but discards the point of the
dataset. Tract-level OD for JT00 alone is still ~650 M rows.

So OD is out of scope here and gets its own onboarding, where its scale can be
signed off on its own terms. Nothing in this schema blocks it.

## Source structure (measured)

53 directories under `LODES8/`: 50 states + DC + PR + `us`. `us` holds only a
national crosswalk and is skipped — its blocks are the union of the state files.
PR publishes RAC only.

```
LODES8/<st>/rac/<st>_rac_S000_<JT>_<YEAR>.csv.gz    41 cols + createdate
LODES8/<st>/wac/<st>_wac_S000_<JT>_<YEAR>.csv.gz    51 cols + createdate
LODES8/<st>/<st>_xwalk.csv.gz                       40 cols + createdate
```

### Structural zeroes become NULL

LODES reserves space for every variable in every year and **fills the
unavailable ones with `0`**:

| Columns | Published only for |
|---|---|
| race, ethnicity, education, sex (`CR*`, `CT*`, `CD*`, `CS*`) | data year ≥ 2009 |
| education (`CD*`), additionally | workers aged 30+ |
| firm age, firm size (`CFA*`, `CFS*`) | data year ≥ 2011 **and** job type JT02 |

Publishing that `0` would make `sum(jobs_sex_female)` over 2002–2008 return 0
rather than "not collected". The cleaner converts these cells to NULL **and
asserts they were in fact zero** — if the Census Bureau ever backfills them, the
run fails loudly instead of discarding real data
(`utils._null_unavailable`).

### Crosswalk sentinels become NULL

An inapplicable geography is padded with an all-nines code of the column's own
width (`99999`, `9999999`, `9999999999999999999999`) and an empty name. Both
sides become NULL so a join never matches a placeholder.

### Geography vintage — do not join across it

LODES 8 is enumerated on **2020 census blocks** and restates all history back to
2002 onto them (LODES 6 and 7 used 2010 blocks). Every table here is therefore a
single-vintage panel, and a block code from a LODES 7 extract is **not**
comparable without the Census Bureau's 2010↔2020 relationship files. This is
stated in each table description, not just here.

## Tables

| Table | Grain | Rows (est.) | Cols |
|---|---|---|---|
| `residence_jobs` | year × job_type × block (residence) | ~298 M | 48 |
| `workplace_jobs` | year × job_type × block (workplace) | ~146 M | 58 |
| `geography_crosswalk` | 2020 tabulation block | ~8.2 M | 41 |
| `dicionario` | code → label | 12 | 5 |

Partition: `year` INT64 on the two fact tables, range 2002–2028.
Cluster: `state_id`, `county_id`.
`geography_crosswalk` is **not partitioned** — it is a single-vintage snapshot
restated wholesale at each release, so a full replace is the correct refresh
semantics for it.

### Key columns

`state_id`, `county_id` and `census_tract_id` are prefixes of `block_id`,
materialised so the table can be clustered and joined to `br_bd_diretorios_us`
without a substring on hundreds of millions of rows.

**`br_bd_diretorios_us` has no census-block and no block-group table** — it stops
at `census_tract_2020`. `block_id` and `block_group_id` therefore carry no
directory foreign key, and this is recorded in their `observations` rather than
papered over by inventing a level. Directory links that do apply:

| Column | Directory FK |
|---|---|
| `year` | `diretorios_data_tempo.ano:ano` |
| `state_id` | `diretorios_us.state:id_state` |
| `county_id` | `diretorios_us.county:id_county` |
| `census_tract_id` | `diretorios_us.census_tract_2020:id_census_tract` |

`state_abbreviation` in the crosswalk holds USPS codes while the state directory
is keyed on FIPS, and the backend only accepts a link to a directory's primary
key — so it carries no FK and the referential check is a dbt test instead.
`cbsa_id` is left unlinked because `cbsa_2023` is a fixed 2023 delineation while
LODES carries whatever delineation was current at release.

## Coverage gaps (reported, not smoothed over)

Measured from the directory listings by `code/availability.py` → `code/availability.md`.
LODES publishes nothing for a state-year-jobtype with no data, so a 404 is a real
gap; the downloader treats only 404 as a gap and raises on any other HTTP error.

Known from LODESTechDoc8.4 and confirmed against the listings:

| Years | States with RAC but no WAC |
|---|---|
| 2002 | AR, AZ, DC, MA, MS, NH |
| 2003 | AZ, DC, MA, MS |
| 2004–2009 | DC, MA |
| 2010 | MA |
| 2011–2016 | none |
| 2017–2021 | AK |
| 2022–2023 | AK, MI |
| all years | PR (RAC only — LED partner, but no OD/WAC infrastructure) |

## Scratch data

`~/Downloads/us_census_lodes_data/{input,output}`, overridable via
`LODES_DATA_ROOT`. Never in the repo or under Dropbox. The cleaner works one
(state, year) at a time and deletes each gzipped CSV after use, so peak disk is
about a dozen files rather than the ~13 GB the full S000 download would occupy.
Deleted entirely as the final onboarding step.

## Recurring pipeline

Annual release with roughly a two-year lag (2023 data in the December 2025
release), static between releases. `pipelines/datasets/us_census_lodes/` polls
the LODES8 listing for a new data year and appends it; the cleaning transform is
imported from `pipelines/datasets/us_census_lodes/utils.py` by both the pipeline
and the one-shot bootstrap, so it exists once.

All tables are **all-free** — annual cadence, so the BD Pro rolling window does
not apply (it is for tables refreshed monthly or more often).
