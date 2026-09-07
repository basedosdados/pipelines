# us_census_lodes — session context

LEHD Origin-Destination Employment Statistics (LODES), U.S. Census Bureau.
**Read `ONBOARDING_PLAN.md` in this folder first** — it is the architecture
source of truth and records the measurements behind every scope decision.

## What this dataset is

Job counts by **2020 census block**, by worker and job characteristics, annual
2002–2023, for 50 states + DC + Puerto Rico. Public domain (17 U.S.C. §105),
registered as `cc0`. GCP id `us_census_lodes` · org `us_census` · backend slug
`lodes`.

## Tables

| Table | Grain | Rows | Source files |
|---|---|---|---|
| `residence_jobs` | year × job_type × residence block | 490,994,834 | `<st>_rac_S000_<JT>_<yr>.csv.gz` |
| `workplace_jobs` | year × job_type × workplace block | 194,030,022 | `<st>_wac_S000_<JT>_<yr>.csv.gz` |
| `geography_crosswalk` | 2020 tabulation block | 8,174,955 | `<st>_xwalk.csv.gz` |
| `dicionario` | code → label | 12 | hand-built |

Wide, as published. Partitioned on `year`, clustered on `state_id, county_id`.

## Non-obvious things that will bite you

1. **The `[SEG]` component of a RAC/WAC filename is a worker filter, not a
   different column set.** `<st>_rac_SA01_<JT>_<yr>` repeats the full 41-column
   grid restricted to workers aged ≤29 — so `CA01 == C000` and `CA02 = CA03 = 0`
   in that file. Only `S000` is shipped; the other nine segments are the
   two-way interactions and would multiply both tables by ten. A `segment`
   column can be added later without a schema break.

2. **`0` in the source often means "not collected", and is converted to NULL.**
   LODES reserves space for every variable in every year. Race, ethnicity,
   education and sex are published only for data year ≥ 2009; firm age and firm
   size only for ≥ 2011 **and** only for job type JT02. Outside those windows
   the files carry literal zeroes. `utils._null_unavailable` NULLs them **and
   asserts they were zero** — if the Census Bureau backfills, the run fails
   rather than silently discarding real data. Education is additionally
   restricted to workers aged 30+ even inside its window.

3. **No census-block directory exists.** `br_bd_diretorios_us` stops at
   `census_tract_2020`; there is no block and no block-group table. `block_id`
   and `block_group_id` therefore carry **no** directory FK. `state_id`,
   `county_id` and `census_tract_id` are materialised on the fact tables and do
   link, which is also what makes clustering possible without a substring on
   491 M rows.

4. **Do not slice `block_id` to get county or tract — use the crosswalk.**
   Connecticut replaced counties with planning regions in 2022, so its 2020
   block GEOIDs carry the legacy county (`09001`–`09015`) while the crosswalk
   carries the planning region (`09110`–`09190`). Measured: the two disagree on
   **100%** of CT blocks, for county and tract alike, and on 60 Vermont blocks
   (some of which change county). The fact tables take both from the state's
   crosswalk, so they agree with `geography_crosswalk` and join to the
   directory. `clean_state_year` raises if a job block is absent from its
   state's crosswalk rather than emitting NULL geography.

5. **`state_abbreviation` cannot be linked either** — it holds USPS codes, the
   state directory is keyed on FIPS, and the backend only accepts a link to a
   directory's primary-key column. Referential integrity is a dbt test instead.

6. **The geography vintage is 2020 and history is restated onto it.** LODES 6
   and 7 used 2010 blocks. A block code from an older extract is not comparable
   without the Census Bureau's 2010↔2020 relationship files. Stated in every
   table description, because a silent cross-vintage join produces plausible
   nonsense.

7. **A 404 is data, not a failure.** LODES publishes nothing for a
   state-year-jobtype with no data. `utils.download` returns None on 404 and
   raises on every other HTTP status, so a transient outage is never recorded as
   a coverage gap. Measured gaps: `code/availability.md`.

8. **`us/` is not a state.** The LODES8 root has 53 directories: 50 states, DC,
   PR, and `us`, which holds only a national crosswalk. It is skipped.

9. **PR has RAC but no WAC or OD** in any year — a LED partner without the data
   infrastructure. AK is missing WAC from 2017, MI from 2022.

10. **OD is deliberately not here.** Measured at ~2.6 bn rows for JT00 alone and
   ~10.6 bn for all six job types, and tract aggregation only buys 3.96× (measured
   on DC 2022). It gets its own onboarding.

11. **Staging parquet is all-STRING**, written through `utils.write_parquet` by
    both this bootstrap and the pipeline, so the two upload paths cannot produce
    divergent staging schemas. Values pass through the architecture's real types
    first, then cast via arrow — never `astype(str)`, which renders NULL as
    `"nan"`.

12. **`bd.Table.create`, not a BigQuery load job.** The load job would make the
    staging table NATIVE, and the recurring pipeline writes only to GCS — dbt
    would then serve a stale native snapshot forever.

## Code layout

The cleaning transform lives in `pipelines/datasets/us_census_lodes/` (pure
functions, no Prefect) and is imported by `code/` here, so the pipeline and the
bootstrap share one implementation.

```
pipelines/datasets/us_census_lodes/constants.py  column maps, URLs, scope
pipelines/datasets/us_census_lodes/utils.py      download + clean + write
models/us_census_lodes/code/gen_architecture.py  -> architecture/*.csv
models/us_census_lodes/code/clean.py             driver
models/us_census_lodes/code/upload.py            -> basedosdados-dev staging
models/us_census_lodes/code/gen_dbt.py           -> *.sql + schema.yml
models/us_census_lodes/code/gen_dicionario.py
models/us_census_lodes/code/availability.py      -> availability.md
```

`architecture/*.csv` is the schema source of truth; the dbt models, the parquet
schema and the backend columns are all generated from it. Regenerate with
`gen_architecture.py` then `gen_dbt.py` — never hand-edit the `.sql`.

## Scratch data

`~/Downloads/us_census_lodes_data/{input,output}`, override with
`LODES_DATA_ROOT`. Never the repo, never Dropbox. The cleaner deletes each
gzipped CSV after reading it, so peak disk is about a dozen files rather than
the ~13 GB the full S000 download occupies.

## Conventions (see .claude/rules/)

- English dataset → English column names, `_id` **suffix** for identifiers.
- Types by arithmetic meaning: `job_type` is a code → STRING +
  `covered_by_dictionary=yes`; every INT64/FLOAT64 carries a `measurement_unit`.
- No `is_primary_key` on these tables (non-directory); the logical key is
  enforced in dbt only.
- Backend env: **staging** per standing directive.
