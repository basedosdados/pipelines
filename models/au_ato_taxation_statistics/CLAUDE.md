# au_ato_taxation_statistics

ATO Taxation Statistics (data.gov.au). The ATO reissues the whole collection once a year
and revises earlier years in place, so each run refetches every in-scope release and
does a **full replace** (``dump_mode="overwrite"``) rather than appending the newest
year.

## Refresh cadence
- `0 16 20 * *` — 16:00 America/Sao_Paulo, day 20

Staging upload: dump mode `overwrite`, source format `parquet`.
Worker sizing: `"memory": "8Gi"`

## Tables
| table | partition | materialization | coverage tier | cast columns |
|---|---|---|---|---|
| `company_industry` | `year` (int64) | table | — | 8 |
| `dicionario` | — | table | — | 5 |
| `gst_industry` | `year` (int64) | table | — | 8 |
| `individuals_income_state` | `year` (int64) | table | — | 11 |
| `individuals_industry` | `year` (int64) | table | — | 8 |
| `individuals_postcode` | `year` (int64) | table | — | 9 |

## Where the code lives
- `pipelines/datasets/au_ato_taxation_statistics/` — `constants.py` (URLs, table list), `utils.py`
  (pure download + cleaning transform), `tasks.py` (Prefect wrappers),
  `flows.py` (the flow + its inline schedule).
- `models/au_ato_taxation_statistics/` — dbt models and `schema.yml`.
  The architecture CSVs under `models/au_ato_taxation_statistics/code/architecture/` are the
  schema source of truth.

## Source
- https://data.gov.au/data/api/3/action/package_search
- https://data.gov.au/data/api/3/action/package_show

## Design notes
Deploy: `.github/scripts/deploy_flows.py` auto-discovers
`au_ato_taxation_statistics_flow`; the dev pool ignores the schedule, the prod pool
activates it.

No Prefect imports here: this module is shared verbatim between the one-shot onboarding
bootstrap (``models/au_ato_taxation_statistics/code``) and the recurring Prefect flow.

The ATO publishes one CKAN package per financial year, each holding ~96 Excel workbooks.
Every detailed table has the same shape: a few leading dimension columns followed by
measure columns that come in ``<item> no.`` / ``<item> $`` pairs. The transform melts
those pairs into a long ``item`` / ``record_count`` / ``amount`` triple.

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
