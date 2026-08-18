# us_sec_edgar — Onboarding Plan

**Dataset:** `us_sec_edgar` (GCP dataset id; prod slug decided at metadata stage).
**Organization:** U.S. Securities and Exchange Commission (SEC).
**Source:** SEC *Financial Statement Data Sets* — quarterly bulk ZIPs.
- Landing page: https://www.sec.gov/data-research/sec-markets-data/financial-statement-data-sets
- File pattern: `https://www.sec.gov/files/dera/data/financial-statement-data-sets/<YYYY>q<Q>.zip`
- Documentation: https://www.sec.gov/files/financial-statement-data-sets.pdf
- Companion API (reference only, not ingested): https://www.sec.gov/edgar/sec-api-documentation

**License:** work of the U.S. federal government → **public domain** (17 U.S.C. §105).
SEC's website policy places its content in the public domain and imposes no
redistribution restriction. → **AllFree** (no BD Pro tier). SEC *does* impose a
fair-access rule on automated retrieval: every request must declare a descriptive
`User-Agent` carrying a contact address, and clients must stay under 10 requests
per second. Both are honoured in `code/sec.py` (`USER_AGENT`, per-request sleep).

**Commercial value:** high — this is the canonical structured feed of US public-company
financial statements (finance, accounting, and corporate-finance research; screening and
factor construction). Flagged for the team; the dataset itself stays free-tier because the
source is public domain and republishes only quarterly (BD Pro's rolling window applies to
tables refreshed monthly or more often).

## Coverage

January 2009 – March 2026 (69 quarterly releases, `2009q1` … `2026q1`). `2009q1.zip`
is a header-only file with no rows — the SEC ships it so that every year has four ZIPs.
A quarter's ZIP holds every XBRL submission *filed* in that quarter, so the fiscal
periods it reports on reach back years before the release quarter.

## Tables

The four source files map one-to-one onto four tables, plus the standard dictionary.

| DB table | Source file | Grain | ~rows/quarter (2025Q1) |
|---|---|---|---|
| `submission` | `sub.txt` | one EDGAR XBRL submission (`accession_number`) | 6.2k |
| `numeric_fact` | `num.txt` | one numeric fact: submission × tag × period × unit × segment | 3.66M |
| `tag` | `tag.txt` | one taxonomy tag: `tag` × `version` | 91k |
| `presentation` | `pre.txt` | one statement line: submission × report × line | 751k |
| `dicionario` | — | code → label for every dictionary-covered column | — |

Verified totals across all 69 releases (local parquet = BigQuery staging = published,
exact match at every step):

| Table | Rows |
|---|---|
| `numeric_fact` | 181,351,169 |
| `presentation` | 44,995,388 |
| `tag` | 4,784,009 |
| `submission` | 426,003 |
| `dicionario` | 495 |

**Stacked by release quarter.** Every table carries the release partition
(`year`, `quarter`) and the rows of a given quarter are exactly that quarter's ZIP.
This makes each quarter independently reproducible and makes the recurring pipeline
idempotent — appending a quarter never touches an earlier one. The cost is that `tag`
(and, to a lesser extent, `presentation`) repeats rows across quarters: a standard tag
used every quarter appears once per quarter. Users wanting a single tag dimension
should `select distinct tag, version, ... from tag`. This is recorded in the table
descriptions.

## Column design (English names — the data and its documentation are English)

Source field names are terse abbreviations (`adsh`, `stprba`, `bas1`, `nciks`). They are
expanded to readable snake_case English; the raw name is kept in `original_name` in every
architecture CSV.

- Partition: `year` INT64 + `quarter` INT64 — the **release** quarter (which ZIP the row
  came from), not the fiscal period. BigQuery partitions on `year`; `quarter` is a plain
  column, and each table is clustered on its highest-cardinality join key.
- Dates: `ddate`/`period`/`filed`/`changed` are `yyyymmdd` → **DATE**. `accepted` is a
  timestamp → **DATETIME**. `fye` is a `mmdd` month-day, not a date → **STRING**.
- Quantities (INT64/FLOAT64, each with a unit): `year`, `quarter`, `fiscal_year`,
  `quantity_quarters` (`qtrs`), `quantity_ciks` (`nciks`), and `value` (FLOAT64).
- `value` mixes units row by row (USD, CAD, shares, pure, percent…). Per the us_bea /
  world_wb_wdi precedent its `measurement_unit` is blank and the per-row unit lives in
  `unit_of_measure`.
- Everything else is STRING, including the numeric-looking columns where arithmetic is
  meaningless: `sic`, `cik`, `ein`, `report`, `line`, and every 0/1 boolean flag
  (`wksi`, `prevrpt`, `detail`, `custom`, `abstract`, `inpth`, `negating`). The flags and
  the coded columns carry `covered_by_dictionary = yes`; their labels live in `dicionario`.

### Directory links — what is and is not linked

- **`cik` (company) has no directory.** A US public-company / CIK directory does not exist
  under `br_bd_diretorios_us`. `cik` stays STRING with `covered_by_dictionary = no`, and
  the intended FK (`br_bd_diretorios_us.company:cik`) is recorded in `observations`.
  Building that directory is its own onboarding.
- **`sic` has no directory either.** Industry is a shared entity and should eventually own
  one (`br_bd_diretorios_us.sic`); until it does, the ~440 SEC SIC codes are carried in
  this dataset's `dicionario` and `sic` is `covered_by_dictionary = yes`.
- **State/province columns are not linked.** `stprba`/`stprma`/`stprinc` mix US state
  codes with Canadian province codes (`BC`, `ON` appear in `state_incorporation`), so they
  do not resolve against `br_bd_diretorios_us.state`.
- **Country columns** are ISO 3166-1 alpha-2 in practice (the documentation's "alpha-3"
  for `countryinc` is wrong — the data carries `US`, `KY`, `CA`, `BM`). They link to
  `br_bd_diretorios_mundo.pais:sigla_iso2` only if every observed value resolves; this is
  verified against the directory during validation and dropped if it does not.

## Source inconsistencies, and how the tests handle them

The data is published "as filed" and carries the filers' own errors. Three were
measured on the full export; the first two are tolerated explicitly rather than
silently, so a *new* occurrence still fails.

1. **`numeric_fact`'s documented key is not unique** — 212 key collisions
   (434 rows of 181,351,169, or 0.0002%). They occur where a filer reuses one
   free-text member identifier in `segments` for two different contracts, so the
   two rows share the key and carry *different* values. Tested with
   `custom_unique_combinations_of_columns` at `proportion_allowed_failures: 0.0001`.
2. **Four 2013Q1 accession numbers appear in `num.txt` with no row in that
   quarter's `sub.txt`** — 1,953 rows, confirmed against the raw SEC ZIP, and
   absent from `submission` in every other quarter too. Tested with
   `custom_relationships` and those four values in `ignore_values`, so the FK
   still fails on any orphan the SEC introduces later.
3. **174 implausible `period_end_date` values** (4 before 1900, 170 after 2035) —
   filer typos in the reported period. Left as filed; no test.

## Scratch data

`~/Downloads/us_sec_edgar_data/` (`input/` ZIPs, `output/` parquet). One quarter is
downloaded, cleaned, and its ZIP deleted before the next is fetched, so peak disk stays
near a single quarter. Deleted entirely at step 14.

## Recurring pipeline (step 12)

The source republishes quarterly, roughly five weeks after quarter end (2026Q1 was posted
2026-04-09). The pipeline polls the landing page for a new `<YYYY>q<Q>.zip`, downloads and
appends that one partition, and re-runs dbt. All tables are `AllFree`.
