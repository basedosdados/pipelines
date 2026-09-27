# world_cricsheet

Cricsheet global cricket. The full-history bundle ``all_csv2.zip`` ships the entire
history on every (near-daily) release, so each run is a **full replace**
(``dump_mode="overwrite"``), not an incremental append — this sidesteps the duplicate-
on-append problem the overlapping recent windows would otherwise cause. A single flow
downloads once, rebuilds all four tables, and materializes them. Because each run re-
ingests the whole 11.4M-row history, it is scheduled **weekly** (not daily).

## Refresh cadence
- `0 6 * * 1` — 06:00 America/Sao_Paulo, Mon

Staging upload: dump mode `overwrite`, source format `parquet`.
Worker sizing: `"memory": "12Gi"`

## Tables
| table | partition | materialization | coverage tier | cast columns |
|---|---|---|---|---|
| `deliveries` | `year` (int64) | table | PartBdpro | 28 |
| `match_players` | `year` (int64) | table | AllFree | 5 |
| `matches` | `year` (int64) | table | PartBdpro | 33 |
| `people` | — | table | — | 22 |

## Where the code lives
- `pipelines/datasets/world_cricsheet/` — `constants.py` (URLs, table list), `utils.py`
  (pure download + cleaning transform), `tasks.py` (Prefect wrappers),
  `flows.py` (the flow + its inline schedule).
- `models/world_cricsheet/` — dbt models and `schema.yml`.
  The architecture CSVs under `models/world_cricsheet/code/architecture/` are the
  schema source of truth.

## Source
- https://cricsheet.org/downloads/all_csv2.zip
- https://cricsheet.org/register/people.csv

## Design notes
Deploy: ``.github/scripts/deploy_flows.py`` auto-discovers ``world_cricsheet_flow``; the
dev pool ignores the schedule, the prod pool activates it (paused until armed).

Pure functions (no Prefect) so they are importable and unit-testable. The recurring
pipeline wraps them in @task (see tasks.py); the bootstrap CLI imports ``clean_all``
directly. Column order / types come from the architecture CSVs (the single source of
truth) for ``deliveries`` and ``matches``; ``match_players`` and ``people`` were renamed
in the dbt models via SELECT aliases, so their **staging** column names (what the dbt
``from staging`` reads) differ from the architecture's final names — see STAGING_COLUMNS
below.

Staging parquet is written **all-STRING** (values pass through the real types first,
then cast via arrow): ``upload_to_gcs`` infers the staging schema from a one-row header
that ``gcs.dump_header`` stringifies, so typed parquet is rejected by BigQuery. This
differs from the onboarding upload, which used typed parquet. See
[[project_dump_header_parquet_bug]]. The dbt model ``safe_cast``s every column back to
its real type, so nothing downstream changes.

Cricsheet global cricket. The full-history bundle ``all_csv2.zip`` ships the entire
history on every (near-daily) release, so each run is a full replace
(``dump_mode="overwrite"``). Because that rebuilds the whole 11.4M-row dataset, the
pipeline runs **weekly** rather than daily (see ``flows.py`` schedule). See
models/world_cricsheet/ for the onboarding design and the architecture CSVs (the schema
source of truth).

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
