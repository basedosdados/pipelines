# us_cms_hcris — dataset context

The Healthcare Cost Report Information System: the annual Medicare cost report every
Medicare-certified US hospital files, from fiscal year 1996. Published by the **Centers
for Medicare & Medicaid Services**.

US federal government work, in the public domain → licensed `cc0` in the backend,
following `us_cms_open_payments` and `us_fed_fred`. GCP id `us_cms_hcris` · org
`centers_for_medicare_medicaid_services_cms` · backend slug `hcris`.

The commercial angle is the point: hospital financials are otherwise sold by vendors,
and this is the public source they resell.

## HCRIS is not a rectangular file

It is a long triple of (report, worksheet, line, column, value), plus a small report
index. Nothing in the data says what any cell means; the meaning lives in the CMS form
instructions. Two layers are published, and both matter:

| Table | Rows | Grain |
|---|---|---|
| `report` | 182,903 | one cost report — the RPT file |
| `report_value` | 537,970,903 | one form cell — NMRC and ALPHA, joined on the cell key |
| `hospital_financial` | 182,903 | one cost report, 66 named measures |
| `dicionario` | 40 | the coded columns |

`report_value` is the complete form: 884 worksheets, both form versions, nothing
dropped. `hospital_financial` is a **dbt model over it**, not a third staging table —
which is why every measure's provenance is literally the SQL.

## The mapping is the whole job, and it is not ours

`code/measures.py` holds 66 measures. Every cell address comes from a published,
maintained mapping — Adam Sacarny's `hospital-cost-reports` (`lookup.xlsx`), Ian
McCarthy's `HCRIS` R code, and CMS's own `HOSP2010_README.txt`. Where the two research
mappings overlap they agree cell for cell, which is the cross-check that let both be
merged rather than one being picked. `provenance()` renders each measure's addresses
into the column's `observations`, so a user traces any number back without leaving the
dataset.

**Validate the mapping against the data before believing it.** `code/verify_measures.py`
counts, per measure and form version, how many reports carry a value at the mapped
address. That pass found two real defects on the first run:

1. **`medicare_inpatient_nursery_cost` maps to a line that does not exist.** Worksheet
   `D10A181` runs 04100 → 04300; there is no 04200, and the cell is empty in all 538
   million values. Dropped. asacarny ships it `enabled=0`, which is why.
2. **CMS split Worksheet S-10 in two and neither published mapping covers it.**
   Effective for cost reporting periods beginning on or after 1 October 2022 (Pub. 15-2
   §4014), `S100000` became `S100001` (Part I), and a new `S100002` (Part II) collects
   the same items for services billable under the hospital CCN alone — "a subset of the
   data reported on Part I". Read against `S100000` alone, **every uncompensated care
   measure silently goes null from fiscal year 2023** while the rest of the table
   carries on. The fix reads both codes: no report of the 182,903 carries `S100000` and
   `S100001` together, so summing them is the same as coalescing. Recovered coverage
   through 2026 and raised `cost_of_uncompensated_care` from 60,832 to 72,339 reports.
   `S100002` is deliberately **excluded** — it is a narrower measure, not more of the
   same one.

## Non-obvious things that will bite you

1. **Column numbers are four characters wide on 2552-96 and five on 2552-10.** Column 1
   is `0100` on the old form and `00100` on the new one; CMS documents this in
   `HCRIS_DataDictionary.csv`. A query spanning both forms must give both literals.
   Line numbers are five characters on both.
2. **`year` is the fiscal-year-end year, not a calendar year and not the extract's
   federal fiscal year.** Hospital fiscal years do not align to the calendar and vary by
   hospital, so a report in `year=2023` may cover any twelve months ending in 2023. One
   extract feeds two or three partitions and one partition is fed by two or three
   extracts — which is why file names encode the extract, so re-cleaning one replaces
   exactly its own files.
3. **`fiscal_year_days` is load-bearing.** Short and long periods are legitimate and
   common — a fiscal year change or a change of ownership produces a period of weeks or
   of more than a year. Any cross-hospital aggregation of a flow measure must weight by
   it rather than assume a year.
4. **The key is `(form_version, report_id)`, never `report_id` alone.** CMS restarted
   the `RPT_REC_NUM` sequence when it introduced Form 2552-10, so 107 ids are reused
   between the two forms — for unrelated hospitals in unrelated years. Grouping
   `hospital_financial` on `report_id` alone merged two hospitals into one row for each
   of those pairs. Every uniqueness test and the curated pivot use the pair. Found by
   `dbt test`, not by any aggregate check: within one extract, and across the four
   extracts spot-checked by hand, `report_id` looked perfectly unique.
5. **The grain is the report, not the hospital-year.** CMS states plainly that one
   hospital can file two or more reports for the same year. The collapse rules the two
   published mappings use for that disagree with each other, so neither is baked in;
   `fiscal_year_begin_date`, `fiscal_year_end_date` and `fiscal_year_days` are what a
   user collapses with.
6. **The ROLLUP file is not ingested.** CMS's own production notes: "The methods of
   combining reported data as published in the 2552-96 forms have proven to be
   unreliable. Users are encouraged to develop their own methods of summarizing HCRIS
   data."
7. **`state_id` comes from the CCN, not from the address.** CMS documents `PRVDR_NUM` as
   `xxyyyy` — an SSA state code and a facility range — so the first two characters
   resolve the state for every report on both forms. Exactly one report of 182,903 fails
   to resolve. The Worksheet S-2 address is hospital-typed free text and is published
   as-is in `reported_state_abbreviation` and `county_name`; `county_name` is
   deliberately not linked to the county directory, because the spellings do not
   resolve.
8. **No `00_header.parquet`.** The staging external tables are **hive-partitioned** on
   `year`, so a 0-row file at the prefix root makes every read fail with "Incompatible
   partition schemas … Observed schema ([]) has 0 columns". It is also unnecessary here:
   the first blob is `year=1996/…`, 7.4M rows and ~40 MB, far inside what the
   table-approve runner handles. See the note in `code/upload.py`.
9. **Seeding staging from a 0-row file types every column INTEGER.** Hit on the first
   upload run: handing `_upload_to_gcs` the header directory before the data made
   `dump_header` infer the schema from an empty file. The data must go first.
10. **The two most recent fiscal years are substantially incomplete**, and stay that way
   for years — CMS keeps receiving, settling and amending. `year=2026` held 12 reports
   at onboarding against ~6,200 for a settled year.
11. **`beds_burn_intensive_care_unit` is 1.6% populated and that is correct** — few
    hospitals have a burn ICU. Sparse is not the same as broken; `verify_measures.py`
    distinguishes them.

## Code (`code/`, run with `~/.venvs/bd-pipelines/bin/python`)

The download and cleaning transform lives in `pipelines/datasets/us_cms_hcris/utils.py`
and is **imported** here, never duplicated, so the bootstrap and the recurring pipeline
cannot drift.

`clean.py` (download + clean, 33 extracts in ~4 minutes) → `verify_measures.py` (writes
`measured.json`) → `null_proportions.py` (writes `null_proportions.json`, measured from
parquet footers, no BigQuery scan) → `gen_architecture.py` → `gen_dbt.py` →
`upload.py` → `build_auxiliary_files.py` → `register.py`.

`schema.py`, `measures.py` and `codes.py` are the source of truth for columns, the cell
mapping and the code labels; the architecture CSVs, dbt models, `schema.yml` and backend
payloads are all generated from them, so a rename changes one place.

Scratch lives in `~/Downloads/us_cms_hcris_data`, never in the repo or Dropbox —
including duckdb's spill. `utils.connect()` pins `temp_directory` there because
duckdb otherwise writes `.tmp/` **relative to the working directory**, and these
scripts run from inside the checkout: one interrupted 538M-row group-by left 22 GB
of `.tmp` in `code/`, which the next `git add` then spent nine minutes hashing.

## Refresh

CMS reissues every extract quarterly, and reports for a fiscal year keep arriving and
being amended for years afterwards. A recurring Prefect pipeline is a separate step
after the static onboarding is verified.
