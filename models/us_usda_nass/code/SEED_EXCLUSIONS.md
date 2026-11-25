# us_usda_nass — first-pass seed and what it excludes

This onboarding is **curated and robustness-first**. The full QuickStats space is
tens of millions of rows across thousands of commodity×statistic combinations —
that breadth is what sank the earlier attempt. The first pass ingests a
high-value seed and logs every exclusion here so breadth can be widened later
without redesigning the schema (the schema already carries every dimension).

Source: the 5 QuickStats bulk sector files only (`qs.{animals_products,crops,
demographics,economics,environmental}_YYYYMMDD.txt.gz`). These are the whole
QuickStats database; both tables are a `SOURCE_DESC` filter on them. The separate
`qs.censusYYYY*` files are redundant (their rows are already in the sector files)
and are **not** downloaded. Zip-code-level census (`qs.census*zipcode`) is out of
scope.

## What is INCLUDED

| Dimension | Kept |
|---|---|
| Source → tables | `SURVEY` → `survey_{national,state,agricultural_district,county}`; `CENSUS` → `census_of_agriculture_{national,state,county}` (split by `agg_level_desc`; census has no ASD grain) |
| Commodity (16) | CORN, SOYBEANS, WHEAT, COTTON, SORGHUM, RICE, HAY, BARLEY, OATS, PEANUTS, CATTLE, HOGS, MILK, CHICKENS, EGGS, TURKEYS |
| Geography (`agg_level_desc`) | NATIONAL, STATE, COUNTY, AGRICULTURAL DISTRICT |
| Frequency (`freq_desc`) | ANNUAL |
| Statistic (`statisticcat_desc`) | rows whose value **starts with** one of: PRODUCTION, YIELD, AREA PLANTED, AREA HARVESTED, AREA BEARING, AREA NON-BEARING, AREA GROWN, PRICE RECEIVED, STOCKS, INVENTORY, SALES |
| Domain (`domain_desc`) | **all** (no filter) — census cross-tabs are kept |

`statisticcat_desc` is compound in the source ("INVENTORY OF MILK COWS", "SALES OF
HOGS"), so the statistic filter is a **prefix** match on the headline set, not an
equality. For crops this keeps ~99.9% of rows (YIELD/PRODUCTION/AREA are nearly
all of it); for livestock it cleanly drops non-headline series.

## What is EXCLUDED (and why)

**Commodities** — every commodity outside the 16 above (vegetables, fruit & tree
nuts, other field crops, other livestock/poultry, forestry, horticulture, etc.).
Reason: first-pass curation. Widening the commodity set is a config change to
`SEED_COMMODITIES` in `pipelines/datasets/us_usda_nass/constants.py`, no schema
change.

**Geography levels** — REGION (single- and multi-state program regions),
WATERSHED, ZIP CODE, CONGRESSIONAL DISTRICT. Reason: no BD directory, low demand,
and ZIP/congressional-district explode row counts. `state`, `county` and
`agricultural district` cover the standard research grains.

**Frequency** — everything except ANNUAL (WEEKLY, MONTHLY, POINT IN TIME,
etc.). Reason: the table is year-partitioned and the headline series are annual.
Weekly/monthly series (e.g. weekly crop progress, monthly milk production, cattle
on feed) are a natural follow-up as separate frequency handling.

**Statistics** — every `statisticcat_desc` not matching a headline prefix.
Examples dropped: (livestock) CONDEMNED*, HATCHED, EGGS SET, EGGS IN INCUBATORS,
PLACEMENTS*, LOSS*, FAT TEST, MILKFAT, RATE OF LAY, SLAUGHTERED, PIG CROP, CALF
CROP, FARROWED, LITTER RATE, GROSS INCOME, CONCENTRATION; (crops) SAMPLES, POD/EAR/
BOLL COUNT, PLANT POPULATION, ROW WIDTH, PRICE REACTION*, USAGE, AREA GRAZED, SEED
FOR PLANTING; (environmental) APPLICATIONS, TREATED, PEST MGMT (chemical-use is a
different topic); (demographics) OPERATORS/PRODUCERS counts. Reason: headline
production/price/area/stocks/inventory/sales are the high-value core the user
approved. Widen via `SEED_STAT_PREFIXES`.

**Raw columns dropped** from the 39 (not carried into the 25-column schema):
STATE_ANSI, COUNTY_ANSI (duplicate the FIPS codes), REGION_DESC, ZIP_5,
WATERSHED_CODE, WATERSHED_DESC, CONGR_DISTRICT_CODE, COUNTRY_CODE, COUNTRY_NAME
(always UNITED STATES), LOCATION_DESC (redundant concat of geography), BEGIN_CODE,
END_CODE (intra-year period codes; `reference_period` carries the human form),
WEEK_ENDING (weekly series excluded), LOAD_TIME (source ingestion timestamp),
SOURCE_DESC (implied by which table the row is in), AGG_LEVEL_DESC / the derived
`geography_level` column (implied by which per-grain table the row is in — the
tables are split by geography grain, so each carries only its grain's geo
columns).

## Deviations from the original task wording (approved in the plan)

- **`covered_by_dictionary = no`** on the descriptive dimensions. NASS values are
  already human-readable labels (`CORN`, `PRODUCTION`, `TOTAL`), and BD's
  `dicionario` is a `chave → valor` (code → label) map, so covering them would be
  a no-op (`CORN → CORN`). Only `value_suppression_flag` is dictionary-covered.
- **`value` carries no column-level `measurement_unit`** — the unit varies per
  row and lives in the `unit` column (documented in the column `observations`).
