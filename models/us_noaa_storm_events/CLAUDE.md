# us_noaa_storm_events — design notes

NOAA NCEI **Storm Events Database**: every severe weather and storm event recorded
by the National Weather Service in the United States and its coastal waters since
1950, with timing, location, deaths, injuries, and property and crop damage
estimates. Source, cleaning code and dbt models all live under this directory and
`pipelines/datasets/us_noaa_storm_events/`.

## Source

<https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/> — three gzipped
CSV families, one file per family per year, 1950 to the current year:

| family | clean table | rows | files |
|---|---|---:|---:|
| `StormEvents_details` | `event` | 2,041,816 | 77 |
| `StormEvents_fatalities` | `fatality` | 24,903 | 77 |
| `StormEvents_locations` | `event_location` | 1,817,621 | 77 |

363 MB gzipped in total. Coverage runs 1950-01 to 2026-05 (the `storm_events_database`
shell's description claiming the data ends 2020-02 was stale by six years and is
corrected as part of this onboarding).

The three families are linked by `EVENT_ID`, which is globally unique in `details`;
every fatality and location row has a matching event (0 orphans).

Documentation: `Storm-Data-Bulk-csv-Format.pdf` and `Storm-Data-Export-Format.pdf`
in the same directory, plus the `README`. **The format document is wrong about the
date format** — see trap 2.

Licence: US Government work, public domain. NCEI states it has reformatted the data
and standardised event types but altered no data values.

## Relationship to us_fema_openfema

The two are complementary and the descriptions say so. Storm Events is the
*meteorological* record — what the weather service observed, event by event, with
its own damage estimate. OpenFEMA is the *administrative* record — which events
became declared disasters and what assistance was paid. Neither substitutes for
the other, and the catalog would look redundant if that were not stated.

## The four traps

Each was measured over the whole corpus, not assumed. `verify_parquet.py`
re-checks the ones that can regress.

### 1. Damage fields are text with a magnitude suffix

`DAMAGE_PROPERTY` and `DAMAGE_CROPS` are published as `2.5K`, `1.50M`, `10.00B` —
and also, on 17,541 rows, as `.5K` / `.03K` with **no leading zero**. A naive
numeric cast silently yields NULL or `2.5` where the source meant `2,500`.

`parse_damage` decodes them to dollars; the published string is kept beside the
number in `damage_property_source` / `damage_crops_source` so the decoding is
auditable and nothing is lost. Suffix census over 2M rows:

| | 1950-1995 | 1996-2026 |
|---|---|---|
| `K` | 42,703 | 1,144,182 |
| `M` | 3,167 | 14,555 |
| `B` | 2 | 103 |
| none | 180,255 | 20,202 |

The bare (no-suffix) form looks alarming until you count it: **179,969 of the
180,255** pre-1996 bare values are literally `0`. There is no era where a bare
number means an undeclared unit.

49 rows are deliberately **not** decoded: `2h` / `5H` (5 rows), a lone `T` (1), and
a suffix with no number at all (43). Guessing at hundreds or trillions would invent
a value; they parse to NULL with the source text preserved.

Sanity check on the decoded totals — these reproduce the magnitudes NOAA publishes:
1953 $596M, 1996 $6.0B, 2005 $96.7B (Katrina), 2017 $80.4B (Harvey/Irma/Maria),
2024 $15.7B.

Values are **current dollars of the event's year**, not inflation-adjusted.

### 2. Event-type coverage is not uniform across eras

Only tornadoes are recorded 1950-1954. Tornado, thunderstorm wind and hail from
1955. The full vocabulary only from 1996. **A count of events per year across the
whole span measures the expansion of the register, not the occurrence of events**
— this is stated in the `event` table description and in the `event_type` column
observations, in all three languages.

The corpus carries **57 distinct event types**, not the 48 the NWS directive
10-1605 defines. Beyond the era effect, the vocabulary itself drifts:

- competing spellings: `Volcanic Ash` (1997-2006) and `Volcanic Ashfall` (2007-)
- `Hurricane (Typhoon)` (2,149 rows) and a bare `Hurricane` — 1 row, 2026, new
- outside the vocabulary entirely: `Northern Lights`, 8 rows, 2001 only
- many types start well after 1996: `Excessive Heat` and `Extreme Cold/Wind Chill`
  in 2000, `Marine Thunderstorm Wind` 2001, `Tsunami` and `Lakeshore Flood` 2006,
  `Sneakerwave` 2012

Values are preserved as published, never remapped. The `dicionario` table records
the year span of each one, which is what makes the drift visible.

### 3. The dates are in a format the source's own document contradicts

`BEGIN_DATE_TIME` is `DD-MON-YY hh:mm:ss` on **all 2,041,816 rows** — never the
`MM/DD/YYYY hh:mm:ss` the format PDF specifies. Its two-digit year cannot separate
1950 from 2050 in a corpus that spans both centuries.

So `begin_datetime` / `end_datetime` are built from the numeric `*_YEARMONTH`,
`*_DAY` and `*_TIME` fields instead, which are unambiguous. The fatalities file
then uses a *third* format, `MM/DD/YYYY hh:mm:ss`, for its own date column —
handled the same way, from `FAT_YEARMONTH` / `FAT_DAY` / `FAT_TIME`.

### 4. `CZ_FIPS` is only a county code when `CZ_TYPE = 'C'`

On `Z` rows it is an NWS public forecast zone number and on `M` rows a marine zone
number. Concatenating it with the state code unconditionally would mint 740,435
county ids that identify nothing.

`county_id` is therefore built only for `cz_type = 'C'` with a non-zero code
(1,301,381 of 2,041,816 events); `cz_fips`, `cz_type` and `cz_name` are all kept
so the zone rows stay usable.

Two further wrinkles found while checking it against `br_bd_diretorios_us`:

- **NWS uses its own pseudo-FIPS for the territories.** Puerto Rico is 99, Guam 98,
  American Samoa 97, Virgin Islands 96 — never the real 72/66/60/78, in any row.
  `state_id_for` remaps them, and the raw code is kept in `state_fips_nws`. Puerto
  Rico's municipio codes then resolve directly (99127 SAN JUAN → 72127, 4,706 rows
  recovered); Guam's and American Samoa's sub-state codes are an NWS scheme of
  their own and resolve to nothing, so they stay null.
- **State codes 81-95 are water, not land**: Gulf of Mexico, Atlantic North/South,
  E Pacific, the five Great Lakes, Lake St Clair, St Lawrence R, Gulf of Alaska,
  Hawaii Waters, Guam Waters. 51,642 rows. They have no FIPS code, so `state_id`
  and `county_id` are null there while `state_name` keeps the published value.

After the remap, `county_id` resolves against the directory on **99.51%** of the
rows that have one. The 6,379 that do not are historical or zone codes and are
kept as published rather than nulled — chiefly the **Connecticut counties**
09001-09015, which Storm Events still uses through 2026 although the directory
carries only the planning regions adopted in 2022. There is no `relationships`
test on `county_id` for that reason; the shortfall is documented in the column's
observations instead. `state_id` does carry the test, and passes.

## Two further decisions

**`LAT2` / `LON2` are dropped.** They are undocumented, unsigned, and packed in two
different encodings across eras — `DDMM` on older rows, `DDMMMMM` (degrees plus
thousandths of a minute) on newer ones, so `3543200` means 35.72°. They round-trip
to `LATITUDE` / `LONGITUDE` and are never present when those are absent, so they
carry no information; keeping them would add two columns that read as plain
numbers to anyone who did not know the encoding.

**`fatality_id` is not a primary key.** 914 identifiers repeat between distinct
deaths in different years — the source restarted the numbering, so a 2000 id
reappears in 2012 on an unrelated fatality. The table's key is
`(event_id, fatality_id)`, which is unique. `event_location`'s key is
`(event_id, location_index)`.

**`magnitude` carries no `measurement_unit`.** Its unit depends on the event type —
knots for wind, inches for hail diameter — so declaring one would be wrong for half
the rows. This is the single numeric column in the dataset without a unit, and
`gen_architecture.py` asserts that it is the only exception.

## Layout

```
models/us_noaa_storm_events/
├── code/
│   ├── architecture/sheet_{event,fatality,event_location}.csv   ← source of truth
│   ├── gen_architecture.py    writes the CSVs (edit the column defs here)
│   ├── gen_dbt.py             writes the .sql models and schema.yml from the CSVs
│   ├── common.py              scratch paths; re-exports the shared transform
│   ├── clean.py               one-shot: clean every year to parquet
│   ├── verify_parquet.py      row counts, key uniqueness, damage totals, null shares
│   └── upload.py              parquet -> basedosdados-dev staging
├── schema.yml                 generated
└── us_noaa_storm_events__*.sql  generated
```

The cleaning transform itself lives in `pipelines/datasets/us_noaa_storm_events/utils.py`
and is **imported** by `code/common.py`, never duplicated, so the one-shot bootstrap
and the recurring pipeline cannot drift.

Scratch data goes to `~/Downloads/us_noaa_storm_events_data/` (override with
`NOAA_STORM_EVENTS_DATA_DIR`) — never in the repo or in Dropbox.

## Recurring pipeline

`pipelines/datasets/us_noaa_storm_events/flows.py`, daily poll at 08:52 BRT.

Every run rebuilds **all** years. NCEI restates whole years in bulk — 71 of the 77
`details` files carried the same `c20260323` creation token, and all 74 older
`locations` files carried `c20260707`. An append-only refresh would miss every
correction and duplicate the years that were rewritten. Rebuilding everything is
affordable (363 MB, ~10 min) and removes the need for a per-year state store the
pipeline has nowhere to keep. BigQuery cost is unchanged either way: the models are
`materialized="table"` and rebuild in full from staging regardless.

A no-op run is cheap because `probe_source` reads the directory listing plus only
the newest year's `details` file (~12 MB) to get the max coverage date for the
poll. The full corpus is downloaded only after the poll says there is work.

The poll keys on coverage, so a release that only *corrects* earlier years without
adding a month does not trip it. NCEI adds the new month on essentially every
release so this is rare; `force_run=True` covers it when it is not.

Release day is not fixed — the 2026 creation tokens fall on the 1st, 19th, 23rd,
25th, 27th and 28th — hence a daily poll rather than a few chosen days.

**Coverage tier: `PartBdpro`, 6-month lag, on all three tables.** They refresh
monthly, so the house rule applies. Each run recomputes
`free_end = source_end - free_lag`, rewrites both `DateTimeRange`s and re-issues
the BigQuery Row Access Policies, so the window rolls on its own and the dbt
models are untouched.

The date column differs by table, and nulls drive the choice: the policy grants
`allUsers` `<date_col> <= free_end`, and a NULL fails that comparison, so a row
with no date would be paywalled forever.

| table | date column | why | window at 2026-05 |
|---|---|---|---|
| `event` | `begin_datetime` | non-null on all 2,041,816 rows | free ≤ 2025-11-30, pro 2025-12-01..2026-05-31 |
| `fatality` | `year` | `fatality_datetime` is NULL on 12 rows where the source gives no day | free ≤ 2025, pro 2026 |
| `event_location` | `year` | no date column at all beyond the partition | free ≤ 2025, pro 2026 |

The granularities differ at the boundary — `event`'s free window ends mid-month,
the other two at a year boundary — so a recent fatality can be free while its
event is not. That direction is harmless: it never exposes a row the pro window
is meant to cover.

Both Coverages (free `is_closed=False`, pro `is_closed=True`) and both
`DateTimeRange`s carry `is_closed` set to match, registered to exactly what the
pipeline recomputes so the site does not show a different window before the first
armed run. The pipeline never writes `is_closed` — `DateTimeRangeInput` has no
such field — so what is registered is what stays.

**Not verifiable before arming:** `apply_row_access_policies` issues real
BigQuery DDL with the worker's rights. The window arithmetic and the topology
guard are pure and are unit-checked; the policies themselves first run on the
first armed prod run.
