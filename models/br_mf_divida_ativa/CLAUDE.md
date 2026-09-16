# br_mf_divida_ativa

PGFN "Dados Abertos da Dívida Ativa da União": three quarterly tables (SIDA /
previdenciário / FGTS). Each release adds ONE new quarter as an immutable snapshot, so
the pipeline is **incremental append**, not full replace — only quarters newer than the
registered ``RawDataSource.Update`` boundary are downloaded and appended to staging
(partitioned by ano/trimestre), which keeps each quarter's partition from being ingested
twice. All three tables paywall their most recent two quarters (``PartBdpro``, free_lag
6 months = 2 quarters); the rolling window and its BigQuery Row Access Policies are re-
applied on every prod run by ``register_table_materialization_task``.

## Refresh cadence
- No cron literal found in `flows.py` — check `deploy_schedules` before assuming it is scheduled.

Staging upload: dump mode `append`, `overwrite`, source format `parquet`.
Worker sizing: `"memory": "8Gi"`

## Tables
| table | partition | materialization | coverage tier | cast columns |
|---|---|---|---|---|
| `fgts` | `ano` (int64) | table | — | 17 |
| `nao_previdenciario` | `ano` (int64) | table | — | 15 |
| `previdenciario` | `ano` (int64) | table | — | 15 |

## Where the code lives
- `pipelines/datasets/br_mf_divida_ativa/` — `constants.py` (URLs, table list), `utils.py`
  (pure download + cleaning transform), `tasks.py` (Prefect wrappers),
  `flows.py` (the flow + its inline schedule).
- `models/br_mf_divida_ativa/` — dbt models and `schema.yml`.
  The architecture CSVs under `models/br_mf_divida_ativa/code/architecture/` are the
  schema source of truth.

## Source
- (none literal in `constants.py`)

## Design notes
Deploy: `.github/scripts/deploy_flows.py` auto-discovers ``br_mf_divida_ativa_flow``
(defined at module level here); the dev pool ignores the schedule, the prod pool
activates it (paused until armed).

Pure functions (no Prefect), importable and unit-testable, shared by the recurring
pipeline (``tasks.py``/``flows.py``) and the one-shot onboarding bootstrap under
``models/br_mf_divida_ativa/code/`` (which imports from here). The source publishes one
quarterly ZIP per system (SIDA / PREV / FGTS); each ZIP holds several ``;``-delimited,
Latin-1 CSV parts. The SIDA (nao_previdenciario) table is ~40-50M rows per quarter, so
every part is processed in row chunks and streamed to Parquet — the whole table is never
held in memory.

Schema and column order come from the architecture CSVs (the single source of truth, at
``constants.ARCHITECTURE_DIR``). Staging Parquet is all-STRING by Data Basis convention;
the dbt model ``safe_cast``s each column to its real type.

PGFN "Dados Abertos da Dívida Ativa da União" — quarterly stock of active-debt
registrations across three systems (SIDA / previdenciário / FGTS). See
models/br_mf_divida_ativa/ONBOARDING_PLAN.md for the full design.

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
