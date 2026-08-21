# au_abs_labour_force

Labour Force, Australia (ABS cat. 6202.0), monthly. Both sources ship the full history
every release — the SDMX ``all`` query returns every period, and each ABS Excel
spreadsheet carries the whole series — so every run is a **full replace**
(``dump_mode="overwrite"``), not an incremental append. A single flow downloads once and
rebuilds all four tables. The source poll short-circuits the run until the ABS publishes
a newer reference month, so a scheduled run is a cheap no-op between releases.

## Refresh cadence
- `0 6 14-27 * *` — 06:00 America/Sao_Paulo, day 14-27

Staging upload: dump mode `overwrite`, source format `parquet`.
Worker sizing: `"memory": "6Gi"`

## Tables
| table | partition | materialization | coverage tier | cast columns |
|---|---|---|---|---|
| `hours_worked` | `year` (int64) | table | — | 8 |
| `labour_force_status` | `year` (int64) | table | — | 20 |
| `status_in_employment` | `year` (int64) | table | — | 8 |
| `underutilisation` | `year` (int64) | table | — | 10 |

## Where the code lives
- `pipelines/datasets/au_abs_labour_force/` — `constants.py` (URLs, table list), `utils.py`
  (pure download + cleaning transform), `tasks.py` (Prefect wrappers),
  `flows.py` (the flow + its inline schedule).
- `models/au_abs_labour_force/` — dbt models and `schema.yml`.
  The architecture CSVs under `models/au_abs_labour_force/code/architecture/` are the
  schema source of truth.

## Source
- https://data.api.abs.gov.au/rest/data
- https://www.abs.gov.au/statistics/labour/employment-and-unemployment/

## Design notes
Every table refreshes monthly, so each carries the BD Pro rolling window
(``PartBdpro``): the most recent six months are pro-only, older data is free, and the
window rolls forward on its own each run.

Deploy: ``.github/scripts/deploy_flows.py`` auto-discovers ``au_abs_labour_force_flow``;
the dev pool ignores the schedule, the prod pool activates it (paused until armed).

Pure functions (no Prefect) so they are importable and unit-testable. Schema and column
order come from the architecture CSVs (the single source of truth).

Sources ------- - SDMX-CSV (ABS Data API), ``labels=both`` so each cell is ``"code:
label"``: * ``labour_force_status`` <- ``LF`` (states, age total) + ``LF_AGES``
(national, all ages, adds Not-in-labour-force + Civilian population). *
``underutilisation`` <- ``LF_UNDER`` (state and age). - ABS time-series Excel
spreadsheets (curated API does not serve these): * ``hours_worked`` <- Table 18
(national, by sex). * ``status_in_employment`` <- Table 19 (national) + SEM1 (states
pivot).

ABS reports counts in thousands and hours in thousands of hours; every value is scaled
by ``10 ** UNIT_MULT`` (SDMX) or by the Index unit (Excel) to absolute persons / hours,
so the ``person`` / ``hour`` measurement units are truthful. Rates are left untouched.
National totals come from the national sources; states from the state sources — ABS
benchmarks national separately, so national is never derived by summing states.

- the ABS Data API (SDMX-CSV) for the status and underutilisation cubes — one ``all``
query returns the full monthly history, so the pipeline query is month-agnostic; - the
ABS time-series Excel spreadsheets for the hours-worked distribution and status-in-
employment, which the curated API does not serve; these live under a month-stamped
release path.

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
