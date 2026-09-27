# us_bls_employment — onboarding plan

US employment from the three headline BLS programs, published as `time.series`
flat files at <https://download.bls.gov/pub/time.series/>.

| Table | Program | Source dir | Rows | Coverage |
|---|---|---|---:|---|
| `ces_national` | Current Employment Statistics, national | `ce` | 8,359,938 | 1939–2026 |
| `ces_state_metro` | State and Metro Area Employment, Hours, and Earnings | `sm` | 9,894,476 | 1939–2026 |
| `laus` | Local Area Unemployment Statistics | `la` | 15,601,885 | 1976–2026 |
| `jolts` | Job Openings and Labor Turnover Survey | `jt` | 626,683 | 2000–2026 |
| `dicionario` | — | — | 10,231 | — |

**34,482,982 rows** across the four fact tables.

## Shape

One long fact table per program, keyed on `(year, period_id, series_id)`, with
`value` FLOAT64 and the series dimensions decomposed into their own columns.

The series id is **not** string-split. Every program ships a `.series`
catalogue that already carries each component as its own field — which is what
the program documentation says the catalogue is for — so the observations are
joined to it. The full id is kept as `series_id` regardless, so a series can be
looked up directly against BLS.

`seasonal_adjustment` is a dimension, not an annotation. Adjusted and
unadjusted values for the same cell are different numbers, and every table
carries both; they are separated by this column and by distinct `series_id`s.

`period_id` keeps the raw BLS period code. `M01`–`M12` are months and set
`month` 1–12; `M13` is the annual average BLS publishes in the same files and
sets `month` NULL, so a monthly filter can never pick up an annual average by
accident.

### Units travel with the row

Each program packs several measures into one value column — employment counts,
hours, earnings, index numbers and response rates in CES; rates, levels and
population in LAUS; levels and rates in JOLTS. There is therefore no single
column-level unit for `value`. `measurement_unit` carries the unit per row,
derived from the dimension that fixes it (`data_type_id` for CES, `measure_id`
for LAUS, `ratelevel_id` for JOLTS).

### Geography and industry

| Column | Directory | Set on |
|---|---|---|
| `state_id` | `br_bd_diretorios_us.state:id_state` | state rows (NULL on national aggregates) |
| `county_id` | `br_bd_diretorios_us.county:id_county` | LAUS area type F |
| `cbsa_id` | `br_bd_diretorios_us.cbsa_2023:id_cbsa` | metropolitan and micropolitan areas |
| `naics_id` | `br_bd_diretorios_us.naics_2022:id_naics` | CES industries with one NAICS equivalent |

Four code systems are deliberately *not* linked, because they would not
resolve and an unresolved directory link drops the whole column at
`upload_columns_from_sheet`:

- **CES `industry_id`** is a BLS 8-digit hierarchy, not NAICS. `naics_id`
  carries the resolvable NAICS code, taken from `ce.industry`'s own
  `naics_code` field and NULL wherever CES aggregates span several codes
  (`21221,3,9`), name a partial industry (`part 238`), or have no NAICS
  equivalent at all.
- **JOLTS `industry_id`** is a coarser 6-digit BLS hierarchy with aggregates
  that span NAICS sectors.
- **LAUS `area_id`** covers fourteen area types under one 15-character code.
- **SAE `area_id`** mixes CBSAs, metropolitan divisions and two BLS-specific
  codes; only the genuine CBSAs reach `cbsa_id`.

## Traps handled

1. **403 without a browser User-Agent.** `download.bls.gov` rejects every
   request that does not carry one, contact email included.
2. **`-` means NULL.** Not zero, not a negative number. 39,110 observations
   across the four tables.
3. **Overlapping observation files.** LAUS publishes the same series in its
   per-state, statewide, region and seasonally-adjusted files; 15,006,703 of
   30,608,588 raw LAUS rows are duplicates. Cleaning shards every file by year
   first, so deduplication on `(series_id, year, period)` only ever holds one
   year in memory and catches an overlap wherever it falls. CES, SAE and JOLTS
   come out with zero duplicates, which is itself the check that the right
   files were selected.
4. **Short downloads look like success.** Every file is size-checked against
   the directory listing and re-fetched on mismatch.
5. **Silent join loss.** Observations whose `series_id` is missing from the
   catalogue would vanish in the join, so they are counted and reported. All
   four programs report zero.
6. **`footnote_codes` is a list.** It can hold several codes at once (`E,F`),
   which a one-key-one-label dictionary cannot represent from the lookup file
   alone, so composite values present in the built tables get a composite
   label.
7. **Metropolitan divisions are not CBSAs.** They are numbered in the same
   range and sit in the same field. They are identified from their own names in
   `sm.area`, so a division added in a later release is excluded automatically.
8. **JOLTS hides census regions in `state_code`** as `MW`/`NE`/`SO`/`WE`.
   Split into `region_id`; `area_code` is the constant `00000` and is dropped.
9. **Four retired Alaska census areas** (02201, 02232, 02261, 02280) appear in
   LAUS for 1990–2019 and predate the current county delineation. They use
   `custom_relationships` with an explicit `ignore_values`, so a *new*
   unmatched county still fails the build.

## Recurring pipeline

`pipelines/datasets/us_bls_employment/` — monthly, **full replace**.

BLS revises the trailing months with every release and restates the whole back
series at the annual benchmark (CES against the QCEW employment count, LAUS
against new population controls). A pipeline that appended the new month and
left history alone would diverge from the published series within a year and
never converge back. The flat files carry the full history on every release, so
rebuilding from them with `dump_mode="overwrite"` absorbs the trailing
revisions and the annual benchmark alike, with no separate benchmark branch.

The source poll makes a scheduled run between releases a cheap no-op.

## Data location

Raw downloads and cleaned parquet live at `~/Downloads/us_bls_employment_data`
(override with `US_BLS_EMPLOYMENT_DATA`) — never in the repo, never in Dropbox.

```bash
uv run python models/us_bls_employment/code/clean_data.py --download
uv run python models/us_bls_employment/code/upload.py --env dev
uv run dbt run  --select us_bls_employment
uv run dbt test --select us_bls_employment
```

## Verification (dev/staging, 2026-09-12)

| Check | Result |
|---|---|
| Upload row counts vs cleaning | 5/5 exact |
| `dbt run --select us_bls_employment` | PASS=5, ERROR=0 |
| `dbt test --select us_bls_employment` | PASS=46, FAIL=0, ERROR=0 |
| Directory foreign keys | 0 unmatched across all 8 links |
| Dictionary coverage | 0 uncovered values across 31 coded columns |
| `month` NULL exactly where `period_id = 'M13'` | 4/4 tables |
| Seasonal adjustment present as both S and U | 4/4 tables, distinct series ids |

Spot checks against published BLS figures: CES total nonfarm payroll employment
158,913k (Jul 2026) and 159,075k (Aug, preliminary); average hourly earnings of
all employees, total private, $37.65; Texas unemployment rate 4.5% seasonally
adjusted; JOLTS quits rate 1.9%. Autauga County, AL for March 2026 satisfies the
labor-force identity exactly — 28,151 employed plus 714 unemployed equals a
labor force of 28,865, and 714/28,865 rounds to the published 2.5%.

## BD Pro

All four fact tables refresh monthly, so each carries the standard rolling
window: the most recent 6 months are BD Pro, everything older is free. Both
coverages exist with `is_closed` set on the Coverage and on its DateTimeRange,
and the ranges do not overlap. Nothing is paywalled until the pipeline is armed
— the Row Access Policies are issued by the first armed run.

## Not done here

- Auxiliary-file bundles are in `gs://basedosdados-dev`, not the prod bucket:
  this machine's service account has no `storage.objects.create` on
  `gs://basedosdados`. Both buckets are requester-pays, so every published
  `auxiliaryFilesUrl` returns HTTP 400 to an anonymous fetch — verified, and the
  same for all 84 production tables using the field.
- Prod table data is materialised by the table-approve action when the PR
  merges, not from here.
