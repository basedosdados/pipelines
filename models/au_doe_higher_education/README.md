# au_doe_higher_education

Australian higher education statistics published by the Department of Education:
students, staff, and undergraduate applications and offers.

Source: https://www.education.gov.au/higher-education-statistics
Licence: CC BY 4.0 (https://www.education.gov.au/using-site/copyright)

## Where the data actually lives

The three landing pages carry no downloads. Every file sits at
`/higher-education-statistics/resources/<slug>`, linking
`/download/<nid>/<slug>/<fid>/document/{xlsx,xls,ods,pdf}`.

The source ships two very different shapes.

### 1. Pivot cubes (tables 1-4)

The "pivot table" releases look like ~190-row spreadsheets. They are not: the
data lives in the Excel **pivot cache** (`xl/pivotCache/pivotCacheRecords1.xml`,
65 MB uncompressed for enrolments), a complete long-format fact table. Parse it
directly rather than reading the visible sheet.

Each publication vintage carries a rolling 5-to-7 year window. Stacking the
vintages gives:

| Table | Vintages | Coverage | Rows/vintage |
|---|---|---|---|
| student_enrolment | 2020-2024 | 2016-2024 | 201k-268k |
| student_load | 2020-2024 | 2016-2024 | 83k-119k |
| award_course_completion | 2020-2024 | 2016-2024 | 76k-117k |
| staff | 2022-2025 | 2018-2025 | 15k-19k |

Vintages agree on overlapping years: 2020 totals are identical between the 2020
and 2024 enrolment releases (1,622,867 nationally, every institution matching).
The one difference is Avondale, reclassified out of the "Non-University Higher
Education Providers" bucket. Dedupe on the dimension key, preferring the newest
vintage.

Field names drift cosmetically across vintages (spaces vs underscores,
`Programs` -> `Programmes`, `Agriculture Environmental` -> `Agriculture,
Environmental`, `Non-Award course/Course/Courses Count`). The semantic field set
is stable.

**The wide `<field> Count` measure block is dropped.** The 2021+ vintages ship 13
(enrolments) / 12 (completions) per-field count columns alongside the headline
measure. They carry no information: the column matching
`Broad Field of Education Primary` equals `Enrolment Count` in 4,000/4,000
sampled rows, and for combined courses the secondary column does too (839/839).
Summing them double-counts every combined course.

**Values are perturbed.** The source applies input perturbation to every cell
except grand totals; `-1` is a sentinel and becomes NULL.

### 2. Published cross-tabs (tables 5-9)

Sections 11, 15, 16 and 17 of the student publication, plus the undergraduate
applications appendices. Each carries its own back-series inside one file, so a
single recent vintage yields many years:

| Table | Source | Coverage |
|---|---|---|
| student_equity_group | Section 11 | 2011-2024 national; institution detail per vintage |
| student_equity_performance | Section 16 | 2011-2024 |
| student_attrition_retention_success | Section 15 | 2014-2023 |
| student_completion_rate | Section 17 | cohorts from 2007 |
| application_offer | UAO appendices | 2010-2025 |

These are kept **semi-wide**: real dimensions as columns, and the source's fixed
measure set wide (applicants/offers/offer_rate; the four completion outcomes;
the 13 equity indicators). A fully generic `indicator`/`value` melt was rejected
as unusable.

Two traps in Section 11/16: a zero often means "not measured under that
SEIFA/ASGS vintage" rather than zero students, and must become NULL; and `< 5`
appears as a suppression marker inside numeric columns.

`application_offer` carries a `series` column because the source publishes a
revised series as `2019a`/`2020a`/`2021a` alongside the original. The two must
not be chained without adjustment. Acceptances were discontinued after 2021.

## Not covered

- Students before 2016 and staff before 2018 in long format. The publication
  cross-tabs run back to 2004 (students) and 2000 (staff) but would require
  parsing roughly 4,000 bespoke sheets across two naming regimes
  ("Appendix N" -> "Section N" around 2016) and three file formats.
- The 1949-2000 and 2003-2008 time series are **PDF only**.
- Undergraduate applications before 2018 are only on the National Library
  archive (TROVE).

## Directory prerequisite

`br_bd_diretorios_au.higher_education_institution` must be built first;
`institution_id` in every table references it. Built from the department's
published institution list (162 institutions: 38 Table A, 9 Table B, 115
Table C), keyed on a derived slug because the source publishes no provider code.

`state_abbreviation` carries **no** `directory_column`: `br_bd_diretorios_au.state`
is keyed on `id_state` (a 1-9 code), and a directory link must target the
directory's primary key. Declare the referential check as a dbt
`custom_relationships` test against `abbreviation` instead. State is not
redundant with institution — the "Non-University Higher Education Providers"
bucket legitimately spans states, while all 47 real institutions map to exactly
one.

## Overlap with br_bd_diretorios_au.higher_education_provider

`main` already carries `br_bd_diretorios_au.higher_education_provider`, added by
the `au_doe_higher_education_finances` onboarding and built from the Higher
Education Research Data Collection. It is 43 rows keyed on `hep_code`.

The two directories describe the same entity. **42 of its 43 codes match this
directory's `provider_code` exactly**, so `hep_code` and `provider_code` are the
same identifier; the one it has that this did not, Batchelor Institute (2246),
is now backfilled here from it.

This directory is a superset — 151 rows against 43 — because the statistics
tables reference things HERDC does not carry:

- Table C providers and non-university providers, which are outside HERDC
- the aggregate buckets the published tables use in place of individual
  providers ("Non-University Higher Education Providers", "Table A Providers")

Those aggregates have no `hep_code`, which is why this directory cannot be keyed
on the provider code and uses a slug instead. `higher_education_provider` in turn
carries `cohort` (Go8, ATN, IRU, RUN, non-aligned), which this one does not.

**These should be consolidated into one directory.** Doing so means changing a
dataset that is already merged and live in production, so it is left as a
decision for review rather than made here.

## Recurring pipeline

`pipelines/datasets/au_doe_higher_education/` refreshes the dataset annually.
The transform is the same code this bootstrap uses — `utils.build_all` — so the
two cannot drift.

**Discovery.** Every download URL carries an opaque node id that changes with
each release, so nothing can be hardcoded. Only the resource slug is stable and
it carries the year, so the flow walks landing page → newest slug → resource
page → download href. Staff data sits one level deeper, under a per-year
sub-page.

**The refresh is partition-scoped, not a rebuild.** This is the part that is
easy to get wrong. The pivot releases carry a rolling five-to-seven year window
and the department delists older ones: 2016-2019 exists only because the
onboarding stacked vintages that can no longer be downloaded. A run that
rebuilt the tables from the current release would silently drop those years. So
the flow replaces exactly the partitions it rebuilt and leaves the rest alone,
deleting those staging prefixes first because `upload_to_gcs` appends.

For the same reason the institution directory is **merged, not replaced**: it
is unpartitioned, and rewriting it from one release's institutions would orphan
the foreign keys in the older partitions.

All tables are annual, well below the monthly threshold for a BD Pro window, so
every one is `AllFree`.
