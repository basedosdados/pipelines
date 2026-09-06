# us_nchs_vital_statistics — harmonization decisions

The NCHS public-use files are one fixed-width file per year with a record layout
that changes almost every year: 56 distinct birth layouts and 57 death layouts
over 1968–2024. This note records what was harmonized, what was deliberately left
un-harmonized, and what the source simply does not contain.

Positions and widths for every year are in `code/layouts/nchs_layouts.csv`
(19,189 rows), derived from the CDC record layouts. The concept-to-variable map is
`COLUMN_SPEC` in `pipelines/datasets/us_nchs_vital_statistics/utils.py`.

## Guiding rule

**Where the source coding changed, the eras are separate columns.** Merging them
would produce a column that looks continuous and is not. Four such breaks exist,
and each is carried as two or more columns rather than one.

## The four breaks

### 1. Education — years of schooling vs categories

| Column | Era | Meaning |
|---|---|---|
| `mother_education_years` / `education_years` | 1989 certificate | Years of schooling completed (00–17) |
| `mother_education_code` / `education_code` | 2003 certificate | Eight ordered categories |

These are different measurements, not different encodings of one measurement.
Registration areas adopted the 2003 certificate on a staggered schedule between
2003 and 2017; both variables are populated during the transition and
`education_reporting_flag` (deaths) says which applies to a given record. The
1989-revision variable was removed from the death file after 2020.

On the death side the source reuses the name `educ` for both concepts — 2 characters
for the 1989 revision, 1 character for the 2003 revision. `COLUMN_SPEC` therefore
disambiguates on **field width**, not name.

### 2. Race — bridged single race vs OMB 1997 multi-race

| Column | Era | Basis |
|---|---|---|
| `mother_race_code`, `father_race_code` | 1968–2013 | Single/bridged race |
| `mother_race_bridged_code` | 2003–2019 | Bridged race |
| `mother_race_recode_6/31`, `father_race_recode_6` | 2014– | OMB 1997 multi-race |

The 1997 OMB standards allowed multiple races to be reported; states adopted the
2003 birth certificate on a staggered schedule. A person recorded as one race
before 2014 and as a combination afterwards is not the same measurement, so no
single harmonized race column is offered. `mother_race_hispanic_code` (2003–2024)
is the one combined race/Hispanic recode that is internally consistent over its
own span, and is the closest thing to a usable long series.

### 3. Hispanic origin by race — 1977 vs 1997 OMB standards (deaths)

NCHS widened this field from one character to two in 2022 and changed its basis
from bridged race (1977 OMB) to single race (1997 OMB), stating in the user guide
that the two are **not comparable**. 2021 is reserved and not populated.

| Column | Era |
|---|---|
| `hispanic_origin_race_recode` | 1989–2020, bridged race |
| `hispanic_origin_race_recode_1997` | 2022–, single race |

### 4. Cause of death — ICD revision

`underlying_cause_code` spans 1968–2024 but the code system changes underneath it:

| Revision | Deaths occurring |
|---|---|
| ICD-8 | 1968–1978 |
| ICD-9 | 1979–1998 |
| ICD-10 | 1999– |

The derived column `icd_revision` states which applies. Codes are **not**
comparable across revisions and must not be pooled without a bridge. The grouped
cause recodes are revision-specific by construction: `cause_recode_72` is ICD-9
(1979–1998); `cause_recode_113`, `_358` and `_39` are ICD-10 (1999–).

## Geography: what exists, by year

This is the single most consequential limitation and it is sharper than the usual
summary ("sub-state detail removed in 2005").

| Table | Column | Years present |
|---|---|---|
| `birth` | `state_residence_id` | 1968–2004 |
| `birth` | `county_residence_id` (FIPS) | 1982–2002 |
| `death` | `state_residence_id` | 1968–2004 |
| `death` | `county_residence_id` (FIPS) | 1982–2002 |

**From 2005 onward the public-use files carry no state of residence at all**, for
either births or deaths. For deaths, 2005–2024 contains no sub-national geography
whatsoever beyond `residence_status`. For births, 2005–2013 and 2018–2024 retain a
county code for large counties only, and 2014–2017 carries no geography at all.
`place_of_death` is a care setting (hospital, home, hospice), not a place.

Two encodings feed `state_residence_id`, both normalised to state FIPS:

- 1982 onward the file publishes FIPS directly (`stresfip` / `fipsstr`).
- 1968–1981 it publishes the NCHS state code — the 50 states plus DC numbered
  01–51 **alphabetically**. Verified empirically against the state-sorted 1975
  natality file (01 Alabama, 02 Alaska, 03 Arizona, 04 Arkansas, 05 California)
  and confirmed by the output containing exactly the 51 valid state FIPS codes,
  correctly skipping 03, 07 and 14, which are not state FIPS.
- 2003–2004 publish a **postal abbreviation** — `mrstate` for births and, under
  the same name it used for the numeric code, `staters` for deaths. Numeric and
  alphabetic codes cannot be told apart by variable name, so the transform
  disambiguates on the value itself; mapping a postal code through the numeric
  table silently yields an all-NULL column.

`county_residence_id` is **not** back-filled before 1982: the earlier `cntyres` /
`countyrs` are NCHS county codes on a different numbering system, and mapping them
into a FIPS column would silently mix two code systems.

Values that are not one of the 51 state FIPS codes — foreign residence (`00`) and
territory codes — are emitted as NULL rather than as a bogus code.

## Reproducing NCHS published totals

Two adjustments are required, and together they reproduce the published figures
**exactly**:

1. **Exclude `residence_status = 4`** (foreign residents). Published US natality
   and mortality totals count residents only.
2. **Weight births by `record_weight`.** The natality file is a 50 percent sample
   for states outside the 100 percent reporting programme in the early years.

Verified against NCHS published totals:

| Table | Year | Rows | After both adjustments | Published |
|---|---|---|---|---|
| `death` | 1968 | 1,930,082 | 1,930,082 | 1,930,082 |
| `death` | 1990 | 2,151,890 | 2,148,463 | 2,148,463 |
| `death` | 2019 | 2,861,523 | 2,854,838 | 2,854,838 |
| `birth` | 2003 | 4,096,092 | 4,089,950 | 4,089,950 |
| `birth` | 2019 | 3,757,582 | 3,747,540 | 3,747,540 |
| `birth` | 1968 | 1,750,782 | 3,501,564 (weighted) | 3,501,564 |

`record_weight` is 1 for all states from 1985. It is not published before 1972;
those years are a 50 percent sample throughout, established empirically — the 1968
and 1970 files hold exactly 0.500 of published births — so an implicit weight of 2
is applied for 1968–1971. Deaths carry no weight: mortality is a complete count.

## What is not included

- **1969 births.** No record layout is published by CDC or NBER, so the year cannot
  be parsed and is absent. Every other year 1968–2024 is present for both tables.
- **Territory files.** The `ps` files (Puerto Rico, Guam, Virgin Islands and other
  areas) exist only from 1994 and cover a different population. Including them
  would make the universe inconsistent across the series, so only the US files
  (50 states and DC) are loaded.
- **Provisional releases.** NCHS publishes provisional counts ahead of the final
  annual file. They are revised in place, so ingesting them into the same table
  would silently change published figures. Only final annual files are loaded.
- **Most of the source's columns.** The wide modern files carry 237–348 variables;
  these tables carry a documented analytic core (36 birth, 33 death). Medical and
  behavioural detail — obstetric procedures, risk factors, abnormal conditions,
  congenital anomalies, the multiple-cause condition arrays — is not included.
  Everything omitted remains in the source files, whose per-year layouts are in
  `code/layouts/nchs_layouts.csv`.

## Sentinel values

Numeric columns map documented "not stated" sentinels to NULL rather than letting
them land as values: 99 for ages and counts, 9999 for birth weight, 99 and 00 for
gestation, 88 where the year's layout uses it for "not on certificate".

The death detail-age code is unit-prefixed — the leading digit is the unit (years,
months, days, hours, minutes) and reading it as a number turns a five-day-old
infant into a four-digit age. `age_years` decodes it: records whose unit is years
yield that value, every other unit is an infant death and yields 0, and unknown
ages yield NULL. A value portion of all nines is "not stated" within its unit —
the 2019 code `1999` is "age in years unknown", not a 999-year-old. `age_detail_code`
retains the raw published code.

## Disclosure

These are individual-level health records, so the published tables were checked
before release rather than after.

**Nothing is added relative to the source.** Both tables are a strict column
subset of the NCHS public-use file with the same rows. No column is a direct
identifier, every column traces to a published source variable, and the only
derived columns — `icd_revision` (a function of year), `age_years` (a decode of
the published `age_detail_code`) and `year` — create no information the source
did not already carry. Disclosure risk is therefore exactly the source's.

**The source itself is re-identifiable in the county years.** Share of birth
records unique on (year, month, state, county, mother's age, race, sex,
plurality, gestation):

| Year | Geography published | Records unique on those fields |
|---|---|---|
| 1975 | state | 9.1% |
| 1990 | state + county | 27.1% |
| 2000 | state + county | 29.8% |
| 2003 | state | 2.0% |
| 2008 | none | 0.8% |

Deaths are higher still — 85.1% in 1990 and 86.6% in 2000 on the equivalent
fields, falling to 23.2% once geography ends.

NCHS's own disclosure control is visible in that table: removing sub-state
geography drops uniqueness by roughly fortyfold, which is why they did it. The
residual concentrates in **1982–2002**, the only years carrying county FIPS.
Republishing does not add fields, but it does make those years far more
queryable than a fixed-width file on an FTP server.

## Dictionary coverage is partial by design

`dicionario` carries 184 verified value labels across 29 columns. The labels are
transcribed by hand from the NCHS user guides. An automatic parse of those guides
was built and rejected: the PDF-extracted text bleeds value blocks into one another
and carries OCR damage, and it produced demonstrably wrong labels — attendant
categories landing on `sex`, one label repeated across three distinct
`place_of_death` codes, "B1ack" for "Black". Publishing a wrong label onto a health
code is worse than publishing none, so only verified entries ship. Coded columns
outside that set carry `covered_by_dictionary = no`; their code sets are in the
per-year NCHS user guides linked from the dataset's raw data sources.
