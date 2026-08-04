# au_abs_cpi — Consumer Price Index, Australia (ABS)

Onboarding plan and design record. Org `au_abs`, dataset slug `cpi`, GCP dataset
`au_abs_cpi`, licence CC BY 4.0.

Source: Australian Bureau of Statistics, *Consumer Price Index, Australia*
(former catalogue 6401.0).
Latest release landing page:
https://www.abs.gov.au/statistics/economy/price-indexes-and-inflation/consumer-price-index-australia/latest-release

## The reform that shapes the design

Australia's CPI is mid-transition from quarterly to monthly:

- **Quarterly** since the September quarter **1948** — the long, continuous,
  ABS-native series. This is the flagship.
- An experimental **Monthly CPI Indicator** ran from 2017-09 (published from
  Oct 2022) to Sep 2025.
- The **complete monthly CPI** began with the **October 2025** reference month
  (first published Nov 2025). ABS moves the release to the 4th Wednesday of each
  month from Feb 2027.

Consequence: the dataset carries **two frequencies as two tables** — a deep
`quarterly` table (1948→) and a shallow `monthly` table (2017/2024→). The
recurring pipeline refreshes **monthly** (the current publication cadence);
the quarterly table is refreshed each quarter by the same flow.

## Scope (locked with the user)

1. Frequencies: **quarterly + monthly** (two fact tables).
2. Geography: **all 8 capital cities + "Australia"** (the ABS national aggregate,
   = weighted average of the eight capital cities). 9 regions.
3. Measures: **core** — original (not seasonally adjusted) index number, plus
   period-on-period and year-on-year percentage change. Seasonally adjusted,
   trend, and points-contribution series are out of scope for v1.

Longest series = quarterly All groups CPI, 1948 Q3 → present (Table 17).

## Source files (ABS time-series spreadsheets)

Stable URL pattern (release slug = latest reference period, e.g. `jun-2026`):
`.../consumer-price-index-australia/<slug>/<file>.xlsx`

| File | ABS table | Feeds | Grain | History |
|------|-----------|-------|-------|---------|
| 6401017.xlsx | T17 Quarterly All Groups | `quarterly` | all-groups × 9 regions | 1948-09 → (Darwin 1980-09) |
| 6401018.xlsx | T18 Quarterly hierarchy, WA8CC | `quarterly` | 124 items × Australia | 1948/1972/1980 → |
| 640101.xlsx  | T1 Monthly All Groups | `monthly` | all-groups × 9 regions | 2024-04 → |
| 640103.xlsx  | T3 Monthly hierarchy, WA8CC | `monthly` | 124 items × Australia | 2017-09 / 2024-04 → |
| 6401010.xlsx | T10 Monthly hierarchy by city | `monthly` | 124 items × 8 cities | 2017-09 / 2024-04 → |

ABS time-series workbook layout (all files): an `Index` sheet cataloguing every
series (`Data Item Description`, `Series Type`, `Series ID`, start/end, …) and
one or more `Data*` sheets with a metadata block (rows 1–10: Unit, Series Type,
Frequency, Series ID, …) and dates in column A from row 11.

Data Item Description parses as `"<measure> ; <item> ; <region> ;"`.

We keep **only the `Index Numbers` measure** from each table as the backbone and
**compute** both percentage changes from the index within each (region, item)
series. This is the `us_bls_cpi` precedent: one uniform, reproducible method,
full coverage even where ABS publishes index-only (Table 10, by city). Computed
WA8CC changes are QA-checked against ABS-published values (Tables 1/3) within a
tolerance.

## Table design (tidy long, partition by `year`)

Both fact tables share the same shape; only the sub-annual period column differs.

### `quarterly` (1948 Q3 → latest)
`year` · `quarter` (1–4) · `region` · `serie_id` · `index_name` ·
`index_number` · `percentage_change_period` (q/q) · `percentage_change_year` (y/y)

### `monthly` (2017-09 → latest)
`year` · `month` (1–12) · `region` · `serie_id` · `index_name` ·
`index_number` · `percentage_change_period` (m/m) · `percentage_change_year` (y/y)

Notes on modelling choices:
- **No dictionary table.** Every column is either numeric or a readable label
  (`region`, `index_name`) or an identifier (`serie_id`); nothing is a code, so
  `covered_by_dictionary = no` throughout and no `dicionario` is needed
  (matching `br_ibge_ipca`).
- **No adjustment column.** All series are `Original`; SA/trend excluded in v1.
- **No base-period column.** ABS re-references all series to one common base, so
  the base is uniform per table and stated in the table description, not a column.
- **`region` value "Australia"** is the ABS national CPI aggregate (weighted
  average of the eight capital cities); documented in the column observations.
- Logical key: (`year`, period, `serie_id`); `serie_id` is unique per
  (item, region) for the index measure. Also test (`year`, period, `region`,
  `index_name`).

## BD Pro tiering (per-table, by refresh frequency)

- `monthly` refreshes monthly → **PartBdpro** (free_lag = 6 months), rolling
  window enforced by BigQuery Row Access Policies.
- `quarterly` refreshes quarterly (less than monthly) → **AllFree**.

## Step status

- [x] 1 context — source, org, licence, coverage
- [x] 2 architecture — this doc + code/architecture/*.csv
- [x] 3 download — input/*.xlsx (5 tables)
- [ ] 4 clean — pipelines/datasets/au_abs_cpi/utils.py (shared) + code/clean_data.py
- [ ] 5 upload (dev) · 6 dbt · 7 validate · 8 discover · 9 metadata (dev)
- [ ] — verification checkpoint —
- [ ] 10 metadata (prod) · 11 PR · 12 monthly Prefect pipeline · 13 publish
