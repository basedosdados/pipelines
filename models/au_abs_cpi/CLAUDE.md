# au_abs_cpi

Australian Consumer Price Index (ABS). Every release ships the full history in the time-
series spreadsheets, so each run is a **full replace** (``dump_mode="overwrite"``), not
an incremental append. A single flow downloads the current release, rebuilds the
quarterly and monthly tables, and materializes them. The source poll short-circuits the
run until ABS publishes a newer month, which makes a scheduled run a cheap no-op between
releases.

## Refresh cadence
- `0 16 22,23,24,25,26,27,28 * *` — 16:00 America/Sao_Paulo, day 22,23,24,25,26,27,28

Staging upload: dump mode `overwrite`, source format `parquet`.

## Tables
| table | partition | materialization | coverage tier | cast columns |
|---|---|---|---|---|
| `monthly` | `year` (int64) | table | PartBdpro | 9 |
| `quarterly` | `year` (int64) | table | AllFree | 9 |

## Where the code lives
- `pipelines/datasets/au_abs_cpi/` — `constants.py` (URLs, table list), `utils.py`
  (pure download + cleaning transform), `tasks.py` (Prefect wrappers),
  `flows.py` (the flow + its inline schedule).
- `models/au_abs_cpi/` — dbt models and `schema.yml`.
  The architecture CSVs under `models/au_abs_cpi/code/architecture/` are the
  schema source of truth.

## Source
- https://www.abs.gov.au/statistics/economy/price-indexes-and-inflation/

## Design notes
Deploy: ``.github/scripts/deploy_flows.py`` auto-discovers ``au_abs_cpi_flow``; the dev
pool ignores the schedule, the prod pool activates it.

No Prefect imports here. The one-shot onboarding bootstrap
(models/au_abs_cpi/code/clean_data.py) and the recurring Prefect pipeline both import
these functions, so the cleaning transform lives in exactly one place.

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

<!-- Generated from constants.py / flows.py / the dbt models. Extend by hand with
     source-specific gotchas as they are discovered. -->
