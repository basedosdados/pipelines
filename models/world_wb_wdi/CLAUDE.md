# world_wb_wdi

World Bank World Development Indicators (WDI). The bulk WDI_CSV.zip carries the full
history on every release, so each run is a **full replace** (dump_mode="overwrite"), not
an incremental append. A single flow downloads once and rebuilds all six tables.

## Refresh cadence
- `0 16 15 3,6,9,12 *` — 16:00 America/Sao_Paulo, day 15, Mar/Jun/Sep/Dec

Staging upload: dump mode `overwrite`, source format `parquet`.
Worker sizing: `"memory": "16Gi"`

## Tables
| table | partition | materialization | coverage tier | cast columns |
|---|---|---|---|---|
| `country_indicator` | — | table | NonHistorical | 3 |
| `data` | `year` (int64) | table | — | 4 |
| `dicionario` | — | table | NonHistorical | 5 |
| `footnote` | `year` (int64) | table | — | 4 |
| `indicator_time` | — | table | — | 3 |
| `indicators` | — | table | NonHistorical | 19 |

## Where the code lives
- `pipelines/datasets/world_wb_wdi/` — `constants.py` (URLs, table list), `utils.py`
  (pure download + cleaning transform), `tasks.py` (Prefect wrappers),
  `flows.py` (the flow + its inline schedule).
- `models/world_wb_wdi/` — dbt models and `schema.yml`.
  The architecture CSVs under `models/world_wb_wdi/code/architecture/` are the
  schema source of truth.

## Source
- https://databank.worldbank.org/data/download/WDI_CSV.zip

## Design notes
The data is annual: the source poll compares the latest year in the source against what
is registered and short-circuits the run when the World Bank has not published a new
year, which makes a scheduled run a cheap no-op between the yearly updates. WDI is CC BY
4.0 and fully open, so every table is AllFree — no BD Pro paywall (the rolling-window
paywall applies only to monthly-or-faster tables).

Deploy: `.github/scripts/deploy_flows.py` auto-discovers `world_wb_wdi_flow`; the dev
pool ignores the schedule, the prod pool activates it.

Pure functions (no Prefect) so they are importable and unit-testable. The recurring
pipeline wraps them in @task (see tasks.py); the bootstrap imports ``clean_all``
directly. Column order and BigQuery types come from the architecture CSVs (the single
source of truth), never from the raw headers.

Staging output is **all-STRING** by Data Basis convention: ``gcs.dump_header``
stringifies the one-row header BigQuery infers the staging schema from, so typed parquet
is rejected. The dbt model ``safe_cast``s every column to its real type. Raw source
value strings are preserved verbatim (never round-tripped through float), so no
precision is lost and a NULL never becomes the literal ``"nan"``.

World Bank World Development Indicators (WDI). The bulk ``WDI_CSV.zip`` carries the full
history on every release, so each run is a full replace (``dump_mode="overwrite"``). See
models/world_wb_wdi/ for the one-shot bootstrap.

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
