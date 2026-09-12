# us_dot_bts_ontime — onboarding plan

**Source:** Reporting Carrier On-Time Performance (1987-present), TranStats,
<https://www.transtats.bts.gov/DL_SelectFields.aspx?gnoyr_VQ=FGJ>
**Organization:** BTS (Bureau of Transportation Statistics, U.S. DOT) — new
organization, slug `bts`
**License:** `cc0`. A work of the US federal government is not subject to
copyright (17 U.S.C. §105), so this is public domain. Same treatment as
`us_fec_campaign_finance` and `us_cfpb_hmda`.
**Area:** `us` **Theme:** `infrastructure-transportation`
**Coverage:** 1987-10 → 2026-06, monthly, 465 months

One row per scheduled domestic US flight: reporting carrier, origin and
destination, scheduled and actual times, departure and arrival delay,
cancellation, diversion, and — from June 2003 — the attribution of delay to
carrier, weather, national air system, security or a late inbound aircraft.

## Source mechanics

The same 109 fields are published through two routes, and both are needed.

| Route | Covers | Mechanism |
|---|---|---|
| PREZIP | 1987-10..1989-12, 2000-01..present | Static monthly ZIP at a predictable URL |
| Download form | any month | ASP.NET WebForms page that builds the extract on request |

**The 1990s exist only through the form.** `PREZIP/` simply does not carry
1990-01..1999-12; the URL 404s for every month in that range. The form's year
dropdown offers 1987-2026, and a form request for 1995-06 returns 439,423 rows
with the same fields, so the decade is available — just not prezipped.

Three details about the form were each established by failing without them, and
are the reason a naive port of this code breaks:

1. **The `chkAllVars` postback must run first.** It is what ticks the 109 field
   checkboxes server-side. Posting the field names straight away is ignored and
   the request comes back as an error page.
2. **`chkDownloadZip` must be left off.** With it on, the form does not build
   anything — it redirects to the PREZIP URL, which 404s for exactly the years
   the form exists to fetch. This is what makes a wrong request look like
   "the 1990s are unavailable" rather than "the request was wrong".
3. **`cboPeriod` takes the month number**, not its name.

The form also returns the *internal* column names (`FL_DATE`,
`OP_UNIQUE_CARRIER`, ...) and renders dates as `M/D/YYYY 12:00:00 AM`. The 109
fields are the same and in the same order, so the rename is positional; the date
format is detected from the first non-null value.

Other parsing facts, verified against the files rather than the documentation:

- The prezipped CSV carries a **trailing empty column** from a stray delimiter —
  110 header fields for 109 documented columns. The last is dropped.
- **The schema is identical in 1987-10 and 2026-06**, field for field and in the
  same order. BTS harmonises retroactively, so no per-era mapping is needed.
- Month numbers in the PREZIP filename are **not zero-padded** (`_2003_6`).
- Encoding is latin-1.

## Architecture

`code/architecture/*.csv` is the source of truth for names, order and types.
`utils.py`, `gen_dbt.py` and the metadata step all read it; regenerate with
`code/architecture/build_architecture.py` rather than editing the CSVs.

English column names, `_id` suffix for identifiers, types by arithmetic meaning.

### Times are STRING, with derived TIME companions

The eight HHMM clock fields are **STRING**, not INT64. `1659 + 1` is not `1700`,
and INT64 silently drops the leading zero on `0937`. Four of them get a derived
TIME column, and scheduled departure additionally gets a DATETIME:

| Derived | From |
|---|---|
| `scheduled_departure_time_local` (TIME) | `CRSDepTime` |
| `departure_time_local` (TIME) | `DepTime` |
| `scheduled_arrival_time_local` (TIME) | `CRSArrTime` |
| `arrival_time_local` (TIME) | `ArrTime` |
| `scheduled_departure_datetime_local` (DATETIME) | `FlightDate` + `CRSDepTime` |

`2400` means midnight ending the day and is normalised to `00:00:00` without
advancing the date. **Scheduled departure is the only well-defined datetime in
the table**: the flight date is the scheduled departure date in origin local
time, whereas arrival clocks are in *destination* local time and may fall on the
following day. The source carries no timezone, so no arrival datetime is
derived, and none should be added later without a timezone lookup.

### Other typing decisions

- `Cancelled`, `Diverted`, `DepDel15`, `ArrDel15` and `DivReachedDest` ship as
  `0.00`/`1.00` floats but are booleans → STRING, dictionary-covered. The
  trailing `.00` is stripped so the dictionary keys (`0`, `1`) match.
- Delays, taxi and gate times, elapsed time and distance are genuine quantities →
  FLOAT64 with `minute` or `mile`.
- Airport ids, carrier codes, flight numbers, FIPS and world area codes are
  identifiers or labels → STRING, whatever they look like.
- Partition column `year` (INT64), range 1987–2031. Clustered by
  `reporting_carrier`, `origin`, `destination`.

### Coverage breaks — the reason columns are empty

Several columns are null for entire eras because the field did not exist, not
because nothing happened. Measured from the data (`output/coverage.json`) rather
than assumed, and recorded in `temporal_coverage` and the column notes:

| Columns | Present from |
|---|---|
| Taxi times, wheels off/on, air time, tail number | 1995 |
| Delay attribution (carrier / weather / NAS / security / late aircraft), cancellation code | 2003 |
| Diversion detail (`diversion_1_*`, `diversion_2_*`, gate times, `first_departure_time`) | 2008 |
| `diversion_3_*` | 2009, and sparse |
| `diversion_4_*`, `diversion_5_*` (16 columns) | **never** — no flight in 39 years was diverted four times |

A user aggregating `carrier_delay` across 1987-2026 without this reads the first
sixteen years as "no carrier delay".

Because `temporal_coverage` is not a writable column field on the backend, the
break is *also* stated in each column's `observations`, in all three languages —
otherwise it would live only in the architecture CSV and never reach the site.

## Tables

Row counts are the loaded and verified figures, not estimates.

| Table | Grain | Rows |
|---|---|---|
| `flight` | one scheduled flight | 234,378,366 |
| `airport` | one airport | 6,903 |
| `dicionario` | one coded value | 53,175 |

`flight` spans 1987-10-01 to 2026-06-30 across 40 years and 465 monthly files
(8.9 GB parquet). Rows by decade, which is also the check that the 1990s really
arrived through the form route:

| Decade | Rows | Years | With delay cause | With tail number |
|---|---|---|---|---|
| 1980s | 11,555,122 | 3 | 0 | 0 |
| 1990s | 52,694,390 | 10 | 0 | 27,003,866 |
| 2000s | 65,737,983 | 10 | 9,704,015 | 65,165,257 |
| 2010s | 62,561,043 | 10 | 11,608,288 | 62,384,681 |
| 2020s | 41,829,828 | 7 | 7,943,071 | 41,565,622 |

`airport` is parsed from `L_AIRPORT_ID`, whose description packs
`City, XX: Name`. `L_AIRPORT` (the airport *code* lookup) is deliberately not
joined into it: the only shared key is the description text and 74 descriptions
are duplicated across the two lookups, so the join is not sound. Airport codes
are covered by the dicionario instead.

Carrier and airport codes go to the dicionario rather than a directory because
no US airline or airport directory exists yet. Noted on the columns.

## The row key is not a global primary key

`[flight_date, reporting_carrier, flight_number, origin, destination,
scheduled_departure_time]` identifies a row across almost the whole series, but not
quite everywhere. Measured, not assumed:

| Year | Rows | Duplicate keys |
|---|---|---|
| 1987 | 1,311,826 | 135 (0.0103%) |
| 1995 | 439,423 | 0 |
| 2003 | 6,488,540 | 36 (0.0006%) |
| 2010 | 6,450,117 | 0 |
| 2019 | 7,422,037 | 0 |
| 2026 | 607,577 | 0 |

The cause is a source characteristic, not a cleaning fault: **a diverted flight is
recorded as two rows** — one leg flagged `diverted = 1` reaching a different
airport, one completing to the scheduled destination — with the same tail number.
Dropping `destination` from the key makes this far worse (2003 goes from 36 to
3,127 duplicates), which is how the two-segment shape was identified.

The dbt uniqueness test is scoped to the most recent year, where the count is
exactly zero, and the residual is stated in the table description rather than
tested away. Scoping is also a cost decision: an unscoped uniqueness test on a
232M-row, 114-column table would scan the whole table on every run.

## BD Pro

`flight` refreshes monthly, so it takes the house rolling window:
`PartBdpro(free_lag=6 months)`. `airport` and `dicionario` are `AllFree`.
Both a free and a pro Coverage must exist on `flight` before the pipeline runs,
or `assert_coverage_topology` hard-fails.

## Pipeline

Monthly, roughly a two-month publication lag (2026-06 landed 2026-08-12). The
recurring pipeline polls PREZIP only — the form route is onboarding-only, since
every month the pipeline will ever want is prezipped.

## Auxiliary files

Per-table bundles hold the BTS record layout (the authoritative definition of all
109 published fields) and the lookup tables that decode the categorical columns,
plus a README with citation, per-file provenance and download dates.

They are uploaded to **`basedosdados-dev`**, not the prod bucket: the prod bucket
refuses the local dev service account (`serviceusage.services.use` denied), and
registering the prod URL would advertise an object that does not exist. Copying
them to prod is part of the prod promotion, by someone holding prod credentials.

**The published links do not resolve for the public**, verified rather than
assumed — an anonymous fetch returns HTTP 400:

```xml
<Error><Code>UserProjectMissing</Code>
<Message>Bucket is a requester pays bucket but no user project provided.</Message></Error>
```

This affects every production table using the field, not just this dataset. The
fix is one bucket setting, not a per-dataset hosting decision.
