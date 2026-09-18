# us_dot_fars — design notes

NHTSA's **Fatality Analysis Reporting System**: a census of every fatal motor
vehicle crash on a US public road since 1975, with crash, vehicle and person
records. Source, cleaning code and dbt models live under this directory and
`pipelines/datasets/us_dot_fars/`.

## Source

<https://www.nhtsa.gov/file-downloads?p=nhtsa/downloads/FARS/> — one CSV zip per
year, at a predictable URL:

```
https://static.nhtsa.gov/nhtsa/downloads/FARS/<year>/National/FARS<year>NationalCSV.zip
```

NHTSA has re-cut the whole series into CSV, so all 50 years download in one
shape. 618 MB in total.

| source file | clean table | rows | files |
|---|---|---:|---:|
| `accident.csv` | `crash` | 1,872,464 | 50 |
| `vehicle.csv` | `vehicle` | 2,798,349 | 50 |
| `person.csv` | `person` | 4,854,653 | 50 |
| — (derived) | `dicionario` | 26,525 | 1 |

Coverage is 1975-2024, the 50 states and the District of Columbia. Puerto Rico
and the other territories ship in separate FARS files and are **not** included:
all 51 `STATE` codes present are modern FIPS, so the directory join is exact.

Keys are unique in every year: `(year, state_id, case_number)` for `crash`, plus
`vehicle_number` for `vehicle`, plus `person_number` for `person`. Zero
duplicates across the whole corpus.

Licence: a work of the U.S. federal government, public domain.

Documentation: FARS Analytical User's Manual 1975-2024
(<https://crashstats.nhtsa.dot.gov/Api/Public/ViewPublication/813794>, 9.5 MB) and
the per-year release notes at
`https://static.nhtsa.gov/nhtsa/downloads/FARS/<year>/FARS<year> Release Notes.txt`.

## Where the dictionary comes from

The `dicionario` table is not transcribed from the PDF manual. It is built from
NHTSA's own machine-readable code sets, in two eras:

* **1975-2014** — the SAS release for each year
  (`FARS<year>NationalSAS.zip`) ships the `PROC FORMAT` source, and the
  `sas7bdat` column metadata binds each variable to its format name. Parsing the
  two together yields an authoritative per-year, per-variable code -> label map.
* **2015-2024** — the CSVs carry a `<VAR>NAME` label column beside every coded
  column, so the year documents itself.

Codes NHTSA published but never defined in any year — 3,435 of 26,525 rows, of
which 3,219 are city codes and 208 make-model codes — are recorded as
`Not documented by the source` rather than omitted. A dictionary that silently
drops what it cannot explain is worse than one that names the gap, and omitting
them would leave `custom_dictionary_coverage` failing with no way to satisfy it.

## The one thing to understand about this dataset

**Almost every coded variable's code set was redefined at least once.** Light
condition has four distinct code sets over the span, injury severity six, vehicle
body type eleven. That is why every coded column is `STRING` and why
`cobertura_temporal` is load-bearing rather than decorative: the same code means
different things in different years.

The sharpest case is `alcohol_test_result_code`, where code `95` is:

| years | meaning |
|---|---|
| 1975-2009 | Test Refused |
| 2010-2014 | Not Reported |
| 2015-2024 | 0.095 % BAC |

A flat code->label map would be wrong for most of the span, and pooling the
column across eras mixes a refusal with a real blood alcohol level.

## The traps

Each was measured over the whole corpus, not assumed. `verify_parquet.py`
re-checks the ones that can regress.

1. **The alcohol scale changed in 2015.** Through 2014 the code is BAC x 100 over
   0-94 with 95-99 reserved for refused / not given / unknown; from 2015 it is
   BAC x 1000 over 0-940 with 995-999 reserved. Reading the code as a number
   turns "96 = test not given" into a lethal 0.96 g/dL — in about 45% of person
   rows. `blood_alcohol_content` harmonises the two; the raw code stays in
   `alcohol_test_result_code` so the sentinels are not lost.

2. **Model year is two digits before 1998 and four digits after** — and 1998 and
   1999 already publish four digits, so the form has to be told apart by
   magnitude, not by file year. Keying on the year sends `9999` through the
   two-digit branch and yields model year 11899.

3. **1999 and 2000 store coordinates as unsigned integers with six implied
   decimals.** `32490935` is 32.490935 degrees, and a longitude of `72591700`
   means 72.591700 **west**. Read literally, the early era puts every crash 32
   million degrees north and the sign loss puts the rest in China. Scaling by a
   million also makes that era's fillers line up with the decimal era's
   (`88888888` -> 88.888888). A bounds check catches the rest, including the
   `-1000.677775` sitting in the 2001 file. Coordinates are published from 1999
   but are of uneven quality before roughly 2001.

4. **Sentinel bands move, and some values that look like sentinels are real
   top-codes.** `travel_speed` 98 means "98 mph or greater" through 2008 but "not
   reported" from 2009; 997 means "greater than 151 mph", which is a bound rather
   than a speed and is nulled. `age` moves from 99 to 998/999 in 2009.
   `survival_hours` 99 is a real 99-hour lag — only 999 is unknown. A single
   fixed band per column would either destroy real observations or invent
   impossible values.

5. **2021 and 2022 prefix the header line with a UTF-8 BOM.** Left in, it renames
   the first column and `STATE` silently goes missing for those two years.

6. **Death times are stored as 0 for survivors before 2010**, which is
   indistinguishable from midnight. The death block is gated on there being a
   death date.

7. **A block of roadway variables moved from crash level to vehicle level in
   2010** — speed limit, lane count, alignment, profile, surface condition and
   traffic control. Both placements are kept, each with its own temporal
   coverage, because they are different units of observation and cannot be
   pooled.

## Column scope

**Every column of `accident.csv`, `vehicle.csv` and `person.csv` is modelled** —
363 source columns across the three files, mapped to 343 output columns. Names
and descriptions come from NHTSA's own Data Element List in the FARS Analytical
User's Manual, 1975-2024, so a column is called what the publisher calls it.

Two source variables collapse into one output column only where the manual gives
them the same data element name **and** their year spans are disjoint — that is a
rename across eras (`TOWAWAY` → `TOWED`, `VEH_CF1` → `VEH_SC1`, `P_CF1` →
`P_SF1`). Same name with *overlapping* spans means genuinely parallel fields
(`WEATHER1`/`WEATHER2`, `CF1`/`CF2`/`CF3`, `GVWR_FROM`/`GVWR_TO`) and they stay
separate. `verify_spec2`-style checks assert that every source column is claimed
exactly once and that every declared alias and temporal coverage matches the real
50-year header matrix.

The one deliberate exclusion is the 213 `<VAR>NAME` companion columns the CSVs
carry from 2015. They are the label half of a code, and the `dicionario` already
carries that label for **every** year rather than only for 2015 on — so modelling
them would add 213 columns of duplication that are null for the first 40 years.

Because the tables now span 50 years of a changing form, many columns are
populated for only part of it: `DISPLACE` exists in 2011-2012 alone, `D_VISION1`
in 2009 alone. Each carries its own `temporal_coverage`, and the ones below the
0.05 non-null floor are listed in `architecture/sparse.json` and excluded from
`not_null_proportion_multiple_columns` — being empty outside their era is the
correct behaviour, not a defect.

`gen_architecture.py` plus `columns_extra.py` are the single source of truth: the
transform, the dbt models and the backend metadata are all generated from the
architecture CSVs.

### Performance

The transform is a per-cell Python pass over roughly 300 million cells. Rows are
built as lists aligned to the column order, with each column's source position
and cleaner resolved once into a plan rather than looked up per cell, and
`code()` drops leading zeros with `lstrip` rather than an `int` round-trip.
Together those take a year from about 200 seconds to about 25, and the change is
byte-identical on the output.

## Refresh

Annual, roughly a year in arrears, plus an Annual Report File that revises the
preceding year. `pipelines/datasets/us_dot_fars/flows.py` re-downloads and
re-cleans every year on each run, so a revision cannot be missed; the source poll
makes a scheduled run a no-op until a new year lands, and `force_run=True` picks
up a revision that does not move the max coverage date.

All three tables are `AllFree`: the BD Pro rolling window applies to tables
refreshed monthly or more often, and FARS is annual.
