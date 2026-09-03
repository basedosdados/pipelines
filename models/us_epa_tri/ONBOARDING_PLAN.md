# us_epa_tri — onboarding plan and record

Status (2026-09-03): cleaned, uploaded to `basedosdados-dev`, dbt 5/5 models built
and 38/38 tests green on dev, metadata registered and published on staging;
at the pre-prod verification checkpoint. See `CLAUDE.md` for the design summary and where the code lives.

## Source

EPA Toxics Release Inventory (TRI), reporting years 1987-2024 (38 years),
"TRI Basic Data Files" — one national CSV per year with the 100 most-used fields
of Form R / Form A, 122 columns in the August 2024 layout. Files are generated
on the fly by Envirofacts; the page says "Includes reporting forms processed as
of: November 5, 2025". RY 2025 was not yet published (its endpoint returns 404).
U.S. Government work, public domain; registered as `cc0`.

Second source, dataset-level only: the Envirofacts REST API, whose `tri_facility`
table (64,990 facilities) supplies the county FIPS code the Basic file lacks.

## Row counts (full history, 1987-2024)

| table | rows | notes |
|---|---|---|
| facility | 880,957 | year × TRIFID; 21,482 (2024) to 22,399 (2015) per year |
| chemical | 17,612 | year × TRI chemical id (710 distinct ids over 38 years) |
| form | 3,218,093 | year × DOC_CTRL_NUM; 305,626 Form A, 2,912,467 Form R |
| release | 7,270,354 | nonzero (form × category) rows; 40,912 in grams (dioxins) |
| dicionario | 60 | 47 release categories, 6 management groups, form type, classification |

Key uniqueness holds exactly on every table (0 duplicates). Every quantity is
in pounds (26,345 dioxin forms converted from grams at 453.59237 g/lb).

## Measured against the directories

| column | directory | unmatched | decision |
|---|---|---|---|
| facility.state | `state.abbreviation` | 0 | strict `relationships` |
| facility.county_id | `county.id_county` | 1.5% (13,146 Connecticut rows on legacy FIPS 09001-09015; the directory carries the 2022 planning regions) | `custom_relationships`, 2%; `00000` (652 rows) → null |
| form.primary_sic | `sic.id_sic` | 1.1% after nulling `INVA` (8,990) and `NA` (593); rest are pre-1987 SIC codes (3079, 2641, …) | `custom_relationships`, 2% |
| form.primary_naics | `naics_<vintage>.id_naics` | 0.4% (2002), 1.2% (2007), 1.25% (2012), 1.3% (2017), 0.1% (2022) | one `custom_relationships` per vintage, 2% |

NAICS vintage by year was measured, not assumed: RY 2012 codes match NAICS 2007
at 98.7% and NAICS 2012 at 88%, so TRI adopts each Census revision one year
late. Mapping: 1987-2007 → 2002 (EPA's own back-assignment), 2008-2012 → 2007,
2013-2016 → 2012, 2017-2021 → 2017, 2022+ → 2022.

## Reconciliation with EPA's totals

Sum of `release` rows per form equals `on_site_release_total` on every one of
the 3.2M forms. Sum of the off-site rows equals `total_transfer` on all but
19,502 pre-2003 forms (1.4%) and 57 later ones — EPA's early OFF-SITE TREATED
TOTAL omits the POTW treatment share. Totals are copied as published.

## Source facts that shaped the design

- `DOC_CTRL_NUM` is the key; (facility, chemical) repeats (694 pairs in 1987).
- SIC/NAICS vary across a facility's forms within a year (EPA assigned NAICS per
  submission through 2005): 3,269 facilities in 1987 alone. They live on `form`.
  Every other facility attribute is constant within (year, TRIFID) — asserted at
  clean time, 0 conflicts in 38 years.
- Chemical attributes are constant within a year; 16 ids change name across
  years. `chemical` keeps the latest year's variant; `form.chemical_name` keeps
  the per-form value.
- `METAL CATEGORY` is a text label (`Non_Metal`, `Elemental metals`, …), not the
  1-4 codes in the documentation; `CLASSIFICATION` uses `Dioxin`, not `DIOXIN`.
- `HORIZONTAL DATUM` is empty in all 880,957 facility rows and was dropped.
- Zero quantities are dropped from `release`: the source fills zeros for NA,
  blank and Form A, so a zero carries no information.
- The download endpoint streams at ~150 KB/s and returns HTTP 200 on a dropped
  stream; four truncated files passed a header-only check before the last-row
  check was added. A concurrent second downloader on the same `.part` file also
  corrupted one year. `validate_files.py` (scratch) confirmed all 38 files.

## What the first dev run caught

The pipeline refreshes only the two newest reporting years. `chemical` was
originally a single unpartitioned dimension built from the years cleaned in that
run, so the run replaced 710 chemicals with the 582 appearing in 2023-2024 and
broke the chemical ids that `form` and `release` carry for earlier years. It is
now partitioned by year like every other data table, `form.tri_chemical_id` and
`release.tri_chemical_id` are FK-tested against it, and `assert_output_layout`
refuses to clean when a file from an earlier layout is still present (a stale
unpartitioned `chemical/data.parquet` silently added 710 phantom rows to
staging before that guard existed). Making the year explicit also surfaces real
history: the PFAS flag appears in 2020 (0 chemicals in 2019, 46 in 2020, 64 in
2024).

## Pipeline (step 12)

`pipelines/datasets/us_epa_tri/flows.py`: weekly poll of the page's "processed
as of" date against `Table.Update.latest`; on change, re-download the two
newest reporting years, rewrite their `year=` partitions (`dump_mode="append"`)
and rebuild the tables. `AllFree` on every table (annual data).

**Dev run passed** (2026-09-03, run `wakeful-pogona`, 24 min, PR #1966): the two
newest years downloaded, cleaned and uploaded, `dbt run OK` and `dbt test OK`
for all five tables, and the older `year=` partitions survived the append. It is
also what exposed the `chemical` defect above.

## Open items

- Auxiliary bundles are on `gs://basedosdados-dev/auxiliary_files/us_epa_tri/<table>/`
  (prod bucket write is 403 locally); the registered URLs point at the prod
  bucket per convention and need a bucket-to-bucket copy. Anonymous fetch of the
  dev URLs returns HTTP 400 (requester-pays).
- New tag `chemical` (pt "produto químico", es "producto químico") created on
  staging; recreate on prod. New entity `chemical` (category science) likewise.
