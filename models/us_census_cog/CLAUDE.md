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
| `gus/datasets/` — Government Units Survey | `government_unit` | 1997, 2012, 2017, 2021, 2022, 2024, 2025 | 644,893 |
| `apes/datasets/` — Public Employment & Payroll | `employment` / `employment_unit` | 1992–2024 | 5,643,839 / 897,572 |
| `gov-finances/datasets/` — State and Local Government Finances | `finance` / `finance_unit` | 1967, 1970–2018 | 116,497,008 / 1,912,739 |

Plus `dicionario`, 947 rows. **125,596,000 rows in total.**

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

## Checks against the published figures

- **Government count.** The 2022 `government_unit` rows in the three
  independent-government categories number **90,837**, which is the Census's own
  published 2022 count of local governments, exactly.
- **Employment internal consistency.** For 2010 and 2022 every unit's `000`
  total equals the sum of its own function rows, for both employment and, in
  2010, payroll — 10,481 and 79,255 units with zero mismatches. The 2022 payroll
  differs on 10,149 units, always by one dollar, which is rounding in the source.
- **Employment against the published summary.** Summed over units, 2022 gives
  14,921,207 full-time and 4,258,844 part-time employees against the published
  14,953,430 and 4,266,821 — 0.22% and 0.19% low. The per-state differences run
  in both directions (California is 6,161 high on full-time and 5,151 low on
  part-time), so this is the summary table and the microdata file carrying
  different revision vintages, not a parsing error.
- **Keys.** No duplicates at all on `(year, government_id_govs, item_code)` over
  114,845,732 finance rows, nor on the 2017 PID6 key. The only duplicates
  anywhere are 14 governments the 1992 employment directory lists twice.
- **Directories.** After splitting place from county subdivision, 0.3% of place
  codes and 0.5–0.8% of county codes are absent from the Data Basis US
  directories, which is geography churn across 58 years rather than a defect.

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

11. **The place code is not always a place.** The source writes one geography
    field for both municipalities and townships, but a municipality carries an
    incorporated-place code and a township a county-subdivision code. Measured
    on 2022: every one of the 16,214 township codes is absent from the place
    directory, against 29 of 19,491 municipal ones. They are split into
    `place_id` and `county_subdivision_id`; there is no county-subdivision
    directory, so the latter carries no foreign key. `99xxx` is a third thing —
    the pseudo-code for a county area — and is dropped.

12. **Seven employment function codes have no published label.** `212`, `312`,
    `412`, `512`, `612`, `712` and `812` appear from 1992 to 2000 and are in no
    code list the Census still publishes. The data settles what they are: across
    1992–1998, code `112` equals their sum for 47,541 of 47,545 units, so they
    are components of "Education - Elementary and Secondary Other". Which
    component each one is remains unknown, and the `dicionario` says so rather
    than guessing. The same treatment covers employment flags `I` and `S`,
    finance flags `M`, `N` and `S`, and worksheet codes `11`, `93`–`97` and `CC`.

13. **`finance_unit.data_flag` is not a code.** In the historical archive the
    field concatenates single-letter flags, giving 31 distinct values including
    `ED`, `GK` and `KLP`. It is deliberately left out of the dictionary
    coverage test.

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
