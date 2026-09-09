# us_census_cog — design notes

U.S. Census Bureau **Census of Governments**: the structure, employment and
finances of every state and local government in the United States. Backend slug
`census_governments`; source, cleaning code and dbt models live under this
directory and `pipelines/datasets/us_census_cog/`.

## Source

Three loosely-coupled file families on `www2.census.gov`, each with its own
directory tree, packaging and naming:

| family | clean tables | years | rows |
|---|---|---|---:|
| `gus/datasets/` — Government Units Survey | `government_unit` | 1997, 2012, 2017, 2021, 2022, 2024, 2025 | 645,684 |
| `apes/datasets/` — Public Employment & Payroll | `employment`, `employment_unit` | 1992–2024 | 6,541,411 |
| `gov-finances/datasets/` — State and Local Government Finances | `finance`, `finance_unit` | 1967, 1970–2018 | see below |

Licence: a work of the U.S. federal government, public domain.

## The identifier problem

**Three identifier schemes, and no single column is populated in every year.**
This is the one thing to understand before using the dataset.

| scheme | where | shape |
|---|---|---|
| GOVS, 14 characters | employment 1992–2024; finance 2013–2016 and 2018; government units 1997–2022 | GOVS state (2), type (1), GOVS county (3), unit (3), supplement (3), sub-code (2) |
| GOVS, 9 characters | finance 1967–2012 | the first nine positions of the above, padded here to 14 with zeros as the source itself does from 2013 |
| PID6, 6 digits | government units 2021–2025; employment 2021–2024; finance **2017 only** | `CENSUS_ID_PID6`, the Census Bureau's current stable identifier |

So the tables carry **both** `government_id` (PID6) and `government_id_govs`,
each populated where the source publishes it. Neither is back-derived by
crosswalk: the source documents both ID reuse and ID changes, and a silent
crosswalk would mis-attribute finances to the wrong government. The bridge
between the two eras ships as data instead — `government_unit` for 2021 and
2022 carries both columns, so a join across schemes is explicit and auditable.

**The GOVS state code is not FIPS.** It numbers the states alphabetically, so
California is 05 in GOVS and 06 in FIPS, and the GOVS county code is likewise
not the FIPS one (Los Angeles County is GOVS 019, FIPS 037). Every table carries
`state_id` and, where the source gives it, `county_id` in FIPS. The employment
data file publishes only the GOVS state, converted here through
`GOVS_TO_FIPS_STATE`.

## Traps, measured

1. **`www2.census.gov` answers HTTP 200 with a "Request Rejected" page** for an
   arbitrary subset of valid URLs, and caches the rejection against the exact
   URL. A 404 is a missing file; a 200 with an HTML body is always the firewall.
   `utils.fetch` retries with a cache-busting query string. Both the 2016 and
   2017 finance directory listings hit this during onboarding.

2. **Nothing about a year's file name is derivable from the year.** Employment
   ships as `<yy>cempst.zip` in census years, `<yy>empst.zip` otherwise,
   `<year>_downloadable_data.zip` for 2012–2013 and 2016, and
   `<year>_individual_unit_files.zip` from 2014 — except 2022, which is branded
   COG-E and has spaces in its name. 2004 publishes no standalone data file and
   2005 no standalone unit directory; both sit inside the year bundle **as
   nested zips**.

3. **`<yy>empest` and `<yy>emptot` are a different product.** They look like the
   individual unit files and sit beside them, but they are state and national
   estimates: 6,211 lines against 150,166 for 1995. Selecting on `emp` alone
   silently substitutes aggregates for microdata.

4. **The employment record layout changes five times.** Record length is the
   reliable discriminator: 84 (1992–2006, no data flags), 96 and 94 (2007–2018,
   flags on every measure), 72 (2019–2020), 80 (2021–2024, PID6 added).
   Part-time hours and full-time-equivalent employment were **dropped from the
   source in 2019** and are null from then on.

5. **The finance data changes shape twice.** 1967–2012 is a wide,
   comma-delimited record — one row per government, 529 finance columns split
   across three row-aligned files — and is melted here to one row per non-zero
   item. 2013 onwards is already long. Within the long era, fiscal 2017 alone
   uses a 32-character record keyed on PID6 while every other year uses 34
   characters keyed on the GOVS id.

6. **The wide file has an unescaped quote.** `"MORGAN COUNTY SOLID WASTE
   DISTRICT""` in fiscal 2012 makes its field absorb the next one and shifts
   every later value by a position — a record that parses without error and
   lands wrong data in every column. `_parse_wide_row` checks the field count on
   every record and repairs only what it can verify.

7. **Four finance columns are printed twice under one item code, and three of
   the four pairs disagree.** Measured on 30,000 records of fiscal 2012, they
   differ on 64, 637 and 76 records respectively, so neither copy is redundant.
   Any colliding key falls back to a slug of the column label; the Census code
   stays in `source_item_code` in the catalogue.

8. **The wide file fills unused character fields with repeated `B`,** not
   spaces. Left alone, 76,705 governments show a school level of `BB` in fiscal
   2012 alone. `_wide_value` maps them to null.

9. **`item_code` is not all one thing.** Of 529 finance variables, 279 are items
   the Census collects, 110 are subtotals over a family of codes and 140 are
   derived aggregates. Summing every row for one government double-counts. The
   `dicionario` records which is which; the long 2013–2018 files carry collected
   items only, so the two eras are not summable the same way.

10. **`unit_category` is not `government_type`.** The Government Units workbooks
    put dependent school systems and public pension systems on their own
    worksheets, and those rows carry their *parent's* type of government. The
    Census lists them but does not count them as governments. The worksheet is
    preserved as `unit_category` so the distinction survives.

11. **The place code is not always a place.** `99xxx` is the pseudo-code for a
    county area rather than an incorporated place; `place_id` is null there.

## What is deliberately not here

- **Government Units for 2002 and 2007.** Published as eight per-type `gid-*.zip`
  archives in a layout unrelated to the workbook every other year uses. Adding
  them means a second parser for two years.
- **Unit-level finance after 2018.** The Census stopped publishing it. Fiscal
  2019 onwards exists only as state-level aggregates, through the
  `timeseries/govslocalfin` API and the published tables.
- **State-by-type finance aggregates** (`<yy>statetypepu.txt`, 1992–2018). These
  are not derivable from the unit files in sample years, where state totals are
  weighted estimates rather than sums, so they are a genuine gap rather than a
  redundant one — a candidate for a later `finance_state_type` table.
- **Federal employment** appears in the employment files as government type 6
  with no state, and is kept rather than filtered: dropping it would make the
  national totals in the source irreproducible.

## Recurring pipeline

The Government Units Survey and the employment survey both publish annually, so
this dataset warrants a Prefect refresh. Finance does not — the source ended
unit-level publication at fiscal 2018.
