# us_bea

US Bureau of Economic Analysis (BEA) economic accounts, pulled from the BEA REST API.
BEA benchmark revisions rewrite historical values, so each run re-fetches the full
history and overwrites staging (``dump_mode="overwrite"``), rather than appending. A
single flow downloads once and rebuilds all six tables.

## Refresh cadence
- `0 16 25,26,27,28 * *` — 16:00 America/Sao_Paulo, day 25,26,27,28

Staging upload: dump mode `overwrite`, source format `parquet`.
Worker sizing: `"memory": "8Gi"`

## Tables
| table | partition | materialization | coverage tier | cast columns |
|---|---|---|---|---|
| `dicionario` | — | table | — | 5 |
| `gdp_by_industry` | `year` (int64) | table | AllFree | 9 |
| `nipa` | `year` (int64) | table | AllFree | 13 |
| `regional_county` | `year` (int64) | table | AllFree | 13 |
| `regional_metro` | `year` (int64) | table | AllFree | 12 |
| `regional_state` | `year` (int64) | table | AllFree | 14 |

## Where the code lives
- `pipelines/datasets/us_bea/` — `constants.py` (URLs, table list), `utils.py`
  (pure download + cleaning transform), `tasks.py` (Prefect wrappers),
  `flows.py` (the flow + its inline schedule).
- `models/us_bea/` — dbt models and `schema.yml`.
  The architecture CSVs under `models/us_bea/code/architecture/` are the
  schema source of truth.

## Source
- https://apps.bea.gov/api/
- https://apps.bea.gov/api/data/

## Design notes
Coverage tier: all six tables are ``AllFree``. ``nipa`` is a mixed-frequency table
(annual/quarterly rows have a NULL ``month``), so a monthly Row Access Policy would
paywall those rows forever (``DATE(year, month, 1)`` is NULL) — it is therefore NOT
paywalled. ``nipa``'s ``(year, month)`` coverage still drives the monthly source poll;
it just does not gate access. ``dicionario`` has no date column, so it takes no coverage
spec. If a rolling BD Pro paywall is wanted later, add an end-of-period ``date`` column
to ``nipa`` and key the policy on it.

Deploy: ``.github/scripts/deploy_flows.py`` auto-discovers ``us_bea_flow``; the dev pool
ignores the schedule, the prod pool activates it (paused).

Pure functions (no Prefect) so they are importable and unit-testable. The recurring
pipeline wraps them in @task (see tasks.py); the bootstrap
``models/us_bea/code/clean.py`` imports the same row-builders and fetch helpers, so the
two cannot drift apart.

- ``STAGING_SCHEMAS`` (here) — the RAW STAGING schema this pipeline writes: ``year``
INT64, ``value`` FLOAT64, everything else STRING (including ``quarter``/``month``), with
staging column names ``table_name``/``series_code``. - the architecture CSVs under
``models/us_bea/code/architecture/`` — the FINAL post-dbt schema
(``table_id``/``series_id``, ``quarter``/``month`` INT64). The dbt models rename and
recast staging into that shape.

The bootstrap uploads STAGING_SCHEMAS as TYPED parquet (the one-shot onboarding path
accepts it). The recurring pipeline instead writes ALL-STRING parquet: the
``dump_header`` parquet bug makes ``upload_to_gcs`` infer every staging column as
STRING, so typed parquet is rejected on read. Values pass through the typed staging
schema FIRST (so ``year`` serializes as ``"1959"`` not ``"1959.0"``) and are only then
cast to string via arrow — never ``astype(str)``, which would turn NULL into the literal
``"nan"`` and defeat the dbt ``safe_cast``.

US Bureau of Economic Analysis (BEA) economic accounts, pulled directly from the BEA
REST API (https://apps.bea.gov/api/). Six tables: nipa, gdp_by_industry, regional_state,
regional_county, regional_metro, dicionario.

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
