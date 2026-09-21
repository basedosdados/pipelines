# au_abs_prices_inflation

Australian price indexes and inflation (ABS), covering the whole
[Price indexes and inflation](https://www.abs.gov.au/statistics/economy/price-indexes-and-inflation)
topic. **One fact table per ABS release**, and **one flow per release** — each
release has its own cadence, its own landing page and its own poll, so a release
that fails to parse cannot block the others.

Every ABS release ships the full history in its time-series spreadsheets, so each
run is a **full replace**, not an incremental append. The source poll
short-circuits a run until ABS publishes a newer period, which makes a
scheduled run a cheap no-op between releases.

## Tables
Every table is partitioned by `year` (int64). Only `cpi_monthly` is BD Pro; the
rolling window applies to tables refreshed monthly or more often, and every
other release here is quarterly.

| table | ABS release | cat | rows | coverage | tier |
|---|---|---|---|---|---|
| `cpi_quarterly` | Consumer Price Index | 6401.0 | 23,662 | 1948Q3– | AllFree |
| `cpi_monthly` | Consumer Price Index | 6401.0 | 66,294 | 2017-09– | PartBdpro (6 months) |
| `wage_price_index` | Wage Price Index | 6345.0 | 52,458 | 1997Q3– | AllFree |
| `producer_price_index` | Producer Price Indexes | 6427.0 | 76,058 | 1966Q3– | AllFree |
| `international_trade_price_index` | International Trade Price Indexes | 6457.0 | 53,897 | 1974Q3– | AllFree |
| `living_cost_index` | Selected Living Cost Indexes | 6467.0 | 18,810 | 1998Q2– | AllFree |
| `dwelling_value` | Total Value of Dwellings | 6432.0 | 8,316 | 2002Q1– | AllFree |

## Refresh cadence
One flow per release, each polling its own landing page. Poll windows follow
each release's published calendar; the minutes are deliberately distinct so the
flows do not compete for BigQuery slots.

| flow | cron (America/Sao_Paulo) | ABS release window |
|---|---|---|
| `au_abs_prices_inflation_cpi` | `15 16 22-28 * *` | last week of each month (4th Wednesday from Feb 2027) |
| `..._wage_price_index` | `17 14 16-22 2,5,8,11 *` | mid Feb/May/Aug/Nov |
| `..._producer_price_index` | `27 14 27-31 1,4,7,10 *` | end Jan/Apr/Jul/Oct |
| `..._international_trade_price_index` | `32 14 26-31 1,4,7,10 *` | a day before Producer |
| `..._living_cost_index` | `37 14 1-8 2,5,8,11 *` | first week of Feb/May/Aug/Nov |
| `..._dwelling_value` | `42 14 1-12 3,6,9,12 *` | first fortnight of Mar/Jun/Sep/Dec |

Staging upload: dump mode **`append`**, source format `parquet`. Not
`overwrite`: that calls `tb.delete(mode="all")`, which drops the materialized
**production** table and fires from the dev half too, so a dev-only validation
run would delete prod and return without rebuilding it. `append` ends in
`st.upload(if_exists="replace")` — same semantics for a full-history rebuild,
no delete.

## Where the code lives
- `pipelines/datasets/au_abs_prices_inflation/` — `constants.py` (URLs, per-release
  config, vocabularies), `timeseries.py` (the generic ABS time-series workbook
  reader, shared by every release), `cpi.py` (the CPI transform, which predates
  the generic reader and keeps its own wide shape), `releases.py` (the other five
  transforms), `tasks.py` (Prefect wrappers), `flows.py` (every flow plus its
  inline schedule).
- `models/au_abs_prices_inflation/` — dbt models and `schema.yml`. The architecture
  CSVs under `code/architecture/` are the schema source of truth.

No Prefect imports live in `cpi.py`. The one-shot onboarding bootstrap
(`code/clean_data.py`) and the recurring pipeline both import those functions, so
the cleaning transform lives in exactly one place.

## Design notes
- The CPI tables carry the `cpi_` release prefix; the *frequency* keys inside
  `constants.py` (`quarterly` / `monthly`) stay semantic because they drive
  `PERIOD_COL` and `YOY_LAG`. `constants.TABLE_ID` maps one to the other.
- **Descriptions split on `" ; "`, never on a bare `;`.** Total Value of
  Dwellings has measures containing one (`Value of dwelling stock; Owned by All
  Sectors`), so a bare-semicolon split mislabels every dwelling-stock series.
- **The Wage Price Index reorders its description parts between workbooks**, so
  `releases.py` classifies each part by shape against a vocabulary rather than
  reading it by position. Position-based parsing mislabels rows silently.
- **The Wage Price Index is the only release with a varying Series Type.** Its
  Table 1 publishes nine descriptions three times over (Original, Seasonally
  Adjusted, Trend) with the type only in the workbook metadata, so it has a
  `series_type` column and the others do not. `build_release` raises if another
  release ever gains a non-Original series.
- **Producer and International Trade read `index_type` from the table title.**
  Output vs Input, and Import vs Export, appear nowhere in the description;
  without them `All groups` collides 21 and 6 ways respectively.
- The Monthly CPI Indicator (cat 6484.0) **ceased** at September 2025 and is
  already folded into `cpi_monthly`; there is nothing separate to onboard.
- Deploy: `.github/scripts/deploy_flows.py` auto-discovers every flow defined in
  `flows.py`; the dev pool ignores the schedules, the prod pool activates them.

## Operating reminders
- A `COMPLETED` run is not proof of an ingest: the source poll returns early and
  still completes. Check the logs, or run
  `uv run python -m pipelines.diagnostics health`.
- The dev materialization runs only when `materialize_to_prod=False`. That is the
  pre-arm validation path; an armed run goes straight to prod.
- Validate with
  `{"materialize_to_prod": false, "update_metadata": false, "force_run": true}`
  on the dev pool, and remember the PR needs the `deploy-flow` label to deploy at
  all.
