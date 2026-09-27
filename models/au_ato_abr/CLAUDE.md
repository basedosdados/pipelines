# au_ato_abr

Australian Business Register "ABN Bulk Extract" (data.gov.au, ATO). The source
republishes a **full snapshot weekly**. We stack snapshots (CNPJ model): each run
uploads the new snapshot to staging with ``dump_mode="overwrite"`` and the
**incremental** dbt models append its ``extraction_date`` partition to the prod tables,
so history accumulates.

## Refresh cadence
- `0 16 * * 1,2,3,4` — 16:00 America/Sao_Paulo, Mon/Tue/Wed/Thu

Staging upload: dump mode `overwrite`, source format `parquet`.
Worker sizing: `"memory": "8Gi"`

## Tables
| table | partition | materialization | coverage tier | cast columns |
|---|---|---|---|---|
| `dgr` | `extraction_date` (date) | incremental | — | 5 |
| `dicionario` | — | table | — | 5 |
| `entity` | `extraction_date` (date) | incremental | — | 15 |
| `other_name` | `extraction_date` (date) | incremental | — | 5 |

## Where the code lives
- `pipelines/datasets/au_ato_abr/` — `constants.py` (URLs, table list), `utils.py`
  (pure download + cleaning transform), `tasks.py` (Prefect wrappers),
  `flows.py` (the flow + its inline schedule).
- `models/au_ato_abr/` — dbt models and `schema.yml`.
  The architecture CSVs under `models/au_ato_abr/code/architecture/` are the
  schema source of truth.

## Source
- https://data.gov.au/data/api/3/action/package_show?id=abn-bulk-extract
- https://data.gov.au/data/dataset/5bd7fcab-e315-42cb-8daf-50b7efc2027e/

## Design notes
The run polls cheaply first (an HTTP HEAD on the ZIPs, compared against
``Table.Update.latest``) and only downloads the ~1 GB payload when the source has
actually republished — so a scheduled run is a cheap no-op between weekly releases.

Deploy: `.github/scripts/deploy_flows.py` auto-discovers ``au_ato_abr_flow``; the dev
pool ignores the schedule, the prod pool activates it (deployed paused).

Pure functions (no Prefect) so they are importable and unit-testable. The recurring
pipeline wraps them in @task (see tasks.py); the bootstrap CLI imports ``clean_all`` /
``download_zips`` directly.

Records are streamed straight out of the two source ZIPs with ``lxml.iterparse`` (no
full extraction to disk). Output is **all-STRING** hive-partitioned parquet:
``upload_to_gcs`` infers the staging schema from a stringified header, so typed parquet
is rejected; the dbt models ``safe_cast`` every column back to its real type.
``extraction_date`` is encoded in the path only, never in the file body. Dates are built
as real ``date32`` first, then arrow-cast to string (so NULLs stay NULL rather than
becoming the literal ``"nan"``).

Australian Business Register "ABN Bulk Extract" (data.gov.au, ATO). The source
republishes a full snapshot **weekly**; we stack each snapshot, partitioned by
``extraction_date`` (see models/au_ato_abr/ONBOARDING_PLAN.md). Each run uploads the new
snapshot to staging with ``dump_mode="overwrite"`` and the incremental dbt models append
the new ``extraction_date`` partition to the prod tables.

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
