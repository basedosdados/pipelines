# au_abs_prices_inflation

Australian price indexes and inflation (ABS), covering the whole
[Price indexes and inflation](https://www.abs.gov.au/statistics/economy/price-indexes-and-inflation)
topic. **One fact table per ABS release**, and **one flow per release** — each
release has its own cadence, its own landing page and its own poll, so a release
that fails to parse cannot block the others.

Every ABS release ships the full history in its time-series spreadsheets, so each
run is a **full replace** (``dump_mode="overwrite"``), not an incremental append.
The source poll short-circuits a run until ABS publishes a newer period, which
makes a scheduled run a cheap no-op between releases.

## Tables
| table | ABS release | cat | partition | coverage tier | flow |
|---|---|---|---|---|---|
| `cpi_quarterly` | Consumer Price Index | 6401.0 | `year` (int64) | AllFree | `au_abs_prices_inflation_cpi` |
| `cpi_monthly` | Consumer Price Index | 6401.0 | `year` (int64) | PartBdpro (6 months) | `au_abs_prices_inflation_cpi` |

## Refresh cadence
- `au_abs_prices_inflation_cpi` — `15 16 22,23,24,25,26,27,28 * *`
  (16:00 America/Sao_Paulo). ABS publishes the monthly CPI in the last week of
  each month, moving to the 4th Wednesday from Feb 2027.

Staging upload: dump mode `overwrite`, source format `parquet`.

## Where the code lives
- `pipelines/datasets/au_abs_prices_inflation/` — `constants.py` (URLs, per-release
  table config), `cpi.py` (pure download + cleaning transform for the CPI
  release), `tasks.py` (Prefect wrappers), `flows.py` (every flow plus its inline
  schedule).
- `models/au_abs_prices_inflation/` — dbt models and `schema.yml`. The architecture
  CSVs under `code/architecture/` are the schema source of truth.

No Prefect imports live in `cpi.py`. The one-shot onboarding bootstrap
(`code/clean_data.py`) and the recurring pipeline both import those functions, so
the cleaning transform lives in exactly one place.

## Design notes
- The CPI tables carry the `cpi_` release prefix; the *frequency* keys inside
  `constants.py` (`quarterly` / `monthly`) stay semantic because they drive
  `PERIOD_COL` and `YOY_LAG`. `constants.TABLE_ID` maps one to the other.
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
