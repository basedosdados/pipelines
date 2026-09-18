# us_fdic_bankfind — onboarding notes

FDIC BankFind Suite: the institution directory plus quarterly Call Report
financials for every FDIC-insured institution.

* Source: <https://api.fdic.gov/banks> (the documented host; the older
  `banks.data.fdic.gov/api` redirects here)
* Data dictionaries: `api.fdic.gov/banks/docs/{risview,institution}_properties.yaml`
* Licence: no explicit statement. The FDIC is a US federal agency, so its work is
  public domain under 17 U.S.C. §105; recorded as `cc0`, matching what the repo
  already does for BLS, FEC and CMS.
* Coverage: 1984Q1 – 2026Q2, 170 quarters, 1,682,616 institution-quarters.
  27,834 institutions, of which 4,241 were active at extraction.

## Tables

| Table | Grain | Rows | Why it exists |
|---|---|---|---|
| `institution` | one per FDIC certificate | 27,834 | The master directory, closed and merged institutions included |
| `indicator` | one per line item | 2,326 | Decodes `financials_indicator`; carries each item's unit |
| `financials` | institution × quarter | 1.68M | Wide, 285 headline line items, one column each |
| `financials_indicator` | institution × quarter × line item | ~3.2B | Long, every reported line item |

### Why both a wide and a long financial table

The FDIC reports **2,378 fields** per institution-quarter, and the fields are
densely populated (0.73 of them in 1990, 0.87 recently). Neither shape serves
everyone:

* Wide-only would drop roughly 2,000 line items. Which 285 to keep is a judgment
  call that is wrong for somebody.
* Long-only makes the ordinary query — "assets, deposits and net income for these
  banks over these years" — a pivot over a 3.2-billion-row table.

So `financials` carries the headline items with one `FLOAT64` column each and its
own `measurement_unit`, and `financials_indicator` carries everything in long
form with the unit held per-indicator in `indicator`. `indicator.financials_column`
maps between them. The long table follows `us_sec_edgar__numeric_fact` and
`us_fed_fred__observation`, which are the same shape.

### How the 285 headline items were chosen

The backbone is the FDIC's own default field set — what `/financials` returns
with no `fields=` argument, i.e. its standard report. That set omits the loan and
deposit category aggregates any banking researcher wants (`LNRE`, `LNCI`,
`LNRERES`, `DEPNI`, …), so those were added explicitly, and the remainder was
filled from the balance-sheet, income, asset-quality and capital families,
ranked by how densely populated each field is in both 1995 and 2025. Binary
flags were then dropped: they are `STRING`-shaped by the house type rule and
would be the only non-measures in the table. They remain in the long table as
0/1.

## Units, and the thousands conversion

**The FDIC publishes dollar amounts in thousands.** Verified against a known
value: JPMorgan Chase Bank NA (CERT 628) reports `ASSET` = 3,788,551,000 for
2025Q2, which is $3.79 trillion, not $3.79 billion.

Cleaning multiplies dollar amounts by 1,000, so both financial tables are in
whole dollars and `measurement_unit` is the vocabulary's `USD`. This makes the
values directly comparable with the other monetary tables in the warehouse, at
the cost of differing from the FDIC's published figures by that factor — which
every column's `observations` records.

Classification into `USD` / `percent` / `unit` is done in `code/catalog.py` from
the published titles and descriptions. It is deliberately conservative: a field
is treated as a dollar amount only when nothing marks it as a ratio or a count.
Three bugs were caught while calibrating it, all of which would have silently
mis-scaled a column by 1,000, and all now covered by the assertion list in that
module's docstring:

* `DEP` ("Total deposits") was read as a count, because its *description*
  mentions "domestic offices". Count words are now matched on the title only.
* `DEPDOM` and `DEPUNINS` were read as counts for the same reason. "offices" and
  "branches" are no longer count words at all; a real count says "Number of".
* `BRANCH` was read as an amount, because only its description calls it a flag.
  Flags are matched on title and description.

## Download

The API caps a request at 500 rows once it asks for more than 250 fields, and at
10,000 rows otherwise. Fetching each quarter as ten field batches of ≤248 and
stitching on `CERT` therefore costs ~1,700 requests rather than ~34,000.

`code/download_and_clean.py` runs the quarters in a process pool, writes each
partition atomically, and skips quarters already on disk, so an interrupted run
resumes. Scratch data lives in `~/Downloads/us_fdic_bankfind_data/` and is
deleted at the end of the onboarding.

## Recurring pipeline

Quarterly, in `pipelines/datasets/us_fdic_bankfind/`. It rebuilds only the
trailing two quarters rather than re-downloading 170: each quarter writes
`year=YYYY/data_q<N>.parquet`, so a rebuild replaces that object instead of
adding a second copy, which makes the run idempotent on `dump_mode="append"`.
The window is two quarters, not one, because institutions file amended Call
Reports for several quarters afterwards.

All four tables are fully free. The BD Pro rolling window applies to tables
refreshing monthly or more often; Call Reports are quarterly.

## Open points

* `institution` is a current-state snapshot, not a history: one row per
  certificate carrying its latest attributes, stamped with `extraction_date`.
  The FDIC publishes structure-change history on `/history`, which is not
  onboarded here.
* Institutions recur across US financial datasets, so `cert` is a candidate for
  a directory table in `br_bd_diretorios_us`. Left as the dataset's own master
  for now; `financials.cert` joins to it through a dbt relationships test rather
  than a `directory_column`.
* `/failures`, `/sod` (Summary of Deposits) and `/locations` are further BankFind
  endpoints, not in scope here.

## Metadata

Registered on the **staging** backend (dev is down — see the repo memory), with
the dataset `under_review` and the four tables `published`, so the dataset stays
hidden from the production frontend until its PR merges.

| Record | Value |
|---|---|
| organization | `fdic` — created here; the FDIC had no organization record |
| dataset | `bankfind` |
| raw data sources | two: the institutions and the financials endpoints |
| tags | 5 existing + 2 created (`deposit`, `bank-supervision`) |
| coverage | `us`, free; `1984-03 .. 2026-06` on the two quarterly tables |

`code/register_metadata.py` does the whole registration and is idempotent. It
calls the databasis MCP server's functions in-process rather than through the
tool layer, because `financials` alone is 290 columns and passing that as a tool
argument means pushing 137 KB of JSON through one call.

Three backend behaviours it has to work around, all of which cost time the first
time round:

* **`create_update_*` is not idempotent for a table's child records.**
  Observation levels, cloud tables, coverages and updates get a brand-new row
  whenever `id` is omitted, so re-running the script multiplied them — eight
  observation levels and four coverages on `financials`. The script now prunes
  duplicates and passes existing ids.
* **Duplicate coverages then break `create_update_table`**, with
  `'TableForm' has no field named 'coverages_areas'` — an error that names
  nothing relevant. It only shows up on tables whose coverages carry a datetime
  range, which made it look table-specific rather than duplicate-specific.
* **`get_dataset` caps a table's columns at 200**, so on `financials` the
  partition column was simply absent from the listing and its `is_partition`
  flag silently went unset. Column ids come from `_fetch_table_columns`
  instead, whose ids are relay globals (`ColumnNode:<uuid>`).

The architecture CSVs are also mirrored to Drive for review:
<https://drive.google.com/drive/folders/1DF7OPwmR-zoTgDoL2Ojr-x0AQQqf0TKK>.
They are plain CSVs, not Sheets: Drive refused the CSV→Sheet conversion on
upload, and `bulk_upsert_columns(architecture_url=...)` only accepts a Sheet, so
columns are registered from `columns_json` instead.
