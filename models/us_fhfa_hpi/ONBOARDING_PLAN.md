# us_fhfa_hpi — Onboarding Plan

**Source:** FHFA House Price Index® (HPI), https://www.fhfa.gov/data/hpi/datasets
**GCP dataset id:** `us_fhfa_hpi` · **backend slug:** `house_price_index` · **org:** `fhfa` (to create)
**License:** `cc0` — work of the US federal government, 17 U.S.C. §105 (public domain).
FHFA publishes no reuse restriction; "FHFA House Price Index®" is a registered trademark,
which constrains branding, not redistribution of the numbers.
**Naming:** English column names, partition `year` (INT64) — matches `us_bls_oes` / `us_bls_cpi`.
Metadata (names/descriptions) in PT/EN/ES.
**Cadence:** master file refreshed **monthly**; quarterly series gain a period each quarter;
annual developmental indexes are released once a year (late March) → recurring Prefect pipeline (step 12).

---

## 1. Source facts (verified against fhfa.gov, 2026-08-26)

Two distinct products, with different methods, frequencies and geographies.

### (a) Master file — the flagship HPI

`https://www.fhfa.gov/hpi/download/monthly/hpi_master.csv` — 186,011 rows, 12 columns,
appends every monthly and quarterly series FHFA publishes. Verified coverage: **1975Q1–2026Q2**
quarterly, **1991M01–2026M06** monthly.

| Field | Content |
|---|---|
| `hpi_type` | traditional (177,642), non-metro (5,922), distress-free (1,988), developmental (247), manufactured (212) |
| `hpi_flavor` | all-transactions (89,791), expanded-data (66,882), purchase-only (29,338) |
| `frequency` | quarterly (181,751), monthly (4,260) |
| `level` | MSA (145,480), State (30,912), USA or Census Division (9,372), Puerto Rico (247) |
| `place_id` | 2-letter state abbreviation, 5-digit CBSA/MSAD code, `USA`, `DV_*`, `PR` |
| `index_nsa` / `index_sa` | index, first period of the series = 100 |
| `rstderr` | relative standard error — **only** the expanded-data MSA rows (58,220) |
| `note` | 451 MSA rows; every other row carries a literal `\t`, which is cleaned to NULL |

Only all-transactions runs before 1991. `index_sa` is null wherever FHFA publishes no
seasonally adjusted variant (e.g. all all-transactions series).
`(frequency, hpi_type, hpi_flavor, level, place_id, yr, period)` is unique — 0 duplicates.

### (b) Annual developmental indexes

`https://www.fhfa.gov/hpi/download/annual/*` — all-transactions, not seasonally adjusted,
**1975–2025**, one release a year (this vintage: *Last updated March 31, 2026*). Built from the
methods in FHFA working papers 16-01, 16-02 and 16-04. Every file shares one schema:
geography key(s), `Year`, `Annual Change (%)`, `HPI`, `HPI with 1990 base`, `HPI with 2000 base`.

| File | Rows | Places | Key format |
|---|---|---|---|
| `hpi_at_national.xlsx` | 51 | — | — |
| `hpi_at_state.xlsx` | 2,601 | 51 | name + 2-letter abbreviation + 2-digit FIPS |
| `hpi_at_cbsa.xlsx` | 43,766 | 966 | 5-digit CBSA **or** 2-digit state FIPS for "(non CBSA areas)" |
| `hpi_at_county.xlsx` | 106,252 | 2,795 | 5-digit county FIPS, **current** vintage (CT planning regions `091xx`) |
| `hpi_at_zip3.xlsx` | 42,050 | 879 | 3-digit USPS ZIP prefix |
| `hpi_at_zip5.xlsx` | 689,533 | 19,024 | 5-digit USPS ZIP |
| `hpi_at_tract.csv` | 2,179,042 | 63,930 | 11-digit census tract GEOID |

Every `.xlsx` carries a 5-row title preamble; the header is row 6 (0-indexed 5).

---

## 2. Tables — 11 data tables + `dicionario`

The master file is split by geography level so each table has one observation level and a
dense column set; the annual product is split the same way. This follows `us_bea`
(`regional_state` / `regional_metro` / `regional_county`) rather than stacking grains.

| Table | Source | Grain | Rows |
|---|---|---|---|
| `monthly_national` | master, `frequency=monthly` | place × year × month × type × flavor | 4,260 |
| `quarterly_national` | master, `level='USA or Census Division'` | place × year × quarter × type × flavor | 5,112 |
| `quarterly_state` | master, `level in ('State','Puerto Rico')` | state × year × quarter × type × flavor | 31,159 |
| `quarterly_metro` | master, `level='MSA'` | CBSA/MSAD × year × quarter × type × flavor | 145,480 |
| `annual_national` | `hpi_at_national` | year | 51 |
| `annual_state` | `hpi_at_state` | state × year | 2,601 |
| `annual_cbsa` | `hpi_at_cbsa` | CBSA × year | 43,766 |
| `annual_county` | `hpi_at_county` | county × year | 106,252 |
| `annual_zip3` | `hpi_at_zip3` | ZIP3 × year | 42,050 |
| `annual_zip5` | `hpi_at_zip5` | ZIP5 × year | 689,533 |
| `annual_tract` | `hpi_at_tract` | tract × year | 2,179,042 |
| `dicionario` | — | code → label for `index_type`, `index_flavor` | ~9 |

**Total: 3,249,324 rows** (3,249,306 data rows plus 18 dictionary entries). Puerto Rico is folded into `quarterly_state`: its `place_id` is `PR`,
it is absent from the 51 rows at `level='State'` (50 states + DC), and it is a state-equivalent.

### Column naming

| Source | Column | Notes |
|---|---|---|
| `yr` | `year` INT64 | partition, FK `br_bd_diretorios_data_tempo.ano:ano` |
| `period` | `month` / `quarter` INT64 | split by frequency; `month` FK `…tempo.mes:mes` |
| `hpi_type` | `index_type` STRING | dictionary-covered |
| `hpi_flavor` | `index_flavor` STRING | dictionary-covered |
| `index_nsa`, `index_sa` | unchanged, FLOAT64 | unit `index` |
| `rstderr` | `relative_standard_error` FLOAT64 | `quarterly_metro` only |
| `HPI` | `index_nsa` FLOAT64 | annual tables; base = first year of the series |
| `HPI with 1990/2000 base` | `index_nsa_1990_base`, `index_nsa_2000_base` | |
| `Annual Change (%)` | `annual_change_percent` FLOAT64 | unit `percent` |

`index_nsa` / `index_sa` keep FHFA's own field names: they are the standard terms and keep the
annual and master tables commensurable.

### Directory links

| Column | Directory FK | |
|---|---|---|
| `year` | `br_bd_diretorios_data_tempo.ano:ano` | all tables |
| `month` | `br_bd_diretorios_data_tempo.mes:mes` | `monthly_national` |
| `state_abbreviation` (`quarterly_state`) | `br_bd_diretorios_us.state:abbreviation` | verified: 52 values ⊂ directory's 60 |
| `state_id` (`annual_state`) | `br_bd_diretorios_us.state:id_state` | 2-digit FIPS |
| `county_id` (`annual_county`) | `br_bd_diretorios_us.county:id_county` | vintage matches; dbt `relationships` validates |

**Not linked, and why:**

- `quarterly_metro.cbsa_id` — 37 of 410 codes are **Metropolitan Division** (MSAD) codes,
  which `br_bd_diretorios_us.cbsa_2023` does not carry. 373 of 410 do resolve.
- `annual_cbsa.cbsa_id` — all 922 five-digit codes resolve, but 44 further codes are 2-digit
  state FIPS standing for that state's "(non CBSA areas)" remainder, so the column is not
  a clean CBSA key.
- `zip_code_3`, `zip_code_5` — USPS ZIP codes, not ZCTAs; `br_bd_diretorios_us.zcta_2020`
  is a different universe and linking it would be wrong.
- `census_tract_id` — FHFA builds the tract index on 2010 tract boundaries (WP 16-04);
  the only tract directory is `census_tract_2020`.

---

## 3. BD Pro tier — per table

Data Basis paywalls a rolling recent window on any table refreshed monthly or more often.

| Table | Refresh | Tier |
|---|---|---|
| `monthly_national` | monthly | `PartBdpro(free_lag=6 months)` |
| `quarterly_*` | quarterly | `AllFree` |
| `annual_*` | annual | `AllFree` |
| `dicionario` | — | no coverage spec |

Only `monthly_national` needs a pro Coverage (`is_closed=True`) created before the pipeline
first runs, or `assert_coverage_topology` hard-fails.

---

## 4. Steps

1–2 context + architecture · 3–4 download + clean → partitioned parquet under
`~/Downloads/us_fhfa_hpi_data/` · 5 upload to `basedosdados-dev` (all-STRING staging parquet)
· 6 dbt models + `schema.yml` · 6b auxiliary files (dictionary + specifications workbooks)
· 7 dbt test · 8–9 metadata in staging, dataset `under_review` → publish on staging
· **checkpoint** · 10 prod metadata · 11 PR · 12 monthly Prefect pipeline · 13 publish prod
· 14 delete `~/Downloads/us_fhfa_hpi_data/`.
