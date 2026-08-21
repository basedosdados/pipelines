# us_bls_cpi

US Consumer Price Index (BLS), CPI-U + CPI-W. The BLS flat files carry the full history
every month, so each run is a **full replace** (dump_mode="overwrite"), not an
incremental append. A single flow downloads once and rebuilds all four tables. Schedule
targets the BLS monthly release window (~2nd week).

## Refresh cadence
- `0 16 10,11,12,13,14,15 * *` — 16:00 America/Sao_Paulo, day 10,11,12,13,14,15

Staging upload: dump mode `overwrite`, source format `parquet`.
Worker sizing: `"memory": "8Gi"`

## Tables
| table | partition | materialization | coverage tier | cast columns |
|---|---|---|---|---|
| `annual` | `year` (int64) | table | AllFree | 8 |
| `dicionario` | — | table | — | 5 |
| `monthly` | `year` (int64) | table | PartBdpro | 10 |
| `semiannual` | `year` (int64) | table | AllFree | 9 |

## Where the code lives
- `pipelines/datasets/us_bls_cpi/` — `constants.py` (URLs, table list), `utils.py`
  (pure download + cleaning transform), `tasks.py` (Prefect wrappers),
  `flows.py` (the flow + its inline schedule).
- `models/us_bls_cpi/` — dbt models and `schema.yml`.
  The architecture CSVs under `models/us_bls_cpi/code/architecture/` are the
  schema source of truth.

## Source
- https://download.bls.gov/pub/time.series

## Design notes
Deploy: `.github/scripts/deploy_flows.py` auto-discovers `us_bls_cpi_flow`; the dev pool
ignores the schedule, the prod pool activates it.

Pure functions (no Prefect) so they are importable and unit-testable. The recurring
pipeline wraps them in @task (see tasks.py); the bootstrap CLI imports `clean_all`
directly. Schema/column order come from the architecture CSVs (the single source of
truth).

US Consumer Price Index (BLS), CPI-U (`cu`) + CPI-W (`cw`) time.series flat files. See
models/us_bls_cpi/ONBOARDING_PLAN.md for the full design.

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
