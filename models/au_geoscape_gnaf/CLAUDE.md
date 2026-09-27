# au_geoscape_gnaf

Geoscape **G-NAF** (Geocoded National Address File, data.gov.au). The source republishes
a **full snapshot quarterly** (Feb/May/Aug/Nov). We stack snapshots (CNPJ model): each
run uploads the new snapshot to staging with ``dump_mode="overwrite"`` and the
**incremental** dbt models append its ``snapshot_date`` partition to the prod tables, so
history accumulates.

## Refresh cadence
- `0 16 14,17,20,23,26 2,5,8,11 *` — 16:00 America/Sao_Paulo, day 14,17,20,23,26, Feb/May/Aug/Nov

Staging upload: dump mode `overwrite`, source format `parquet`.
Worker sizing: `"memory": "16Gi"`

## Tables
| table | partition | materialization | coverage tier | cast columns |
|---|---|---|---|---|
| `address_detail` | `snapshot_date` (date) | incremental | — | 44 |
| `dicionario` | — | table | — | 5 |
| `locality` | `snapshot_date` (date) | incremental | — | 14 |
| `street_locality` | `snapshot_date` (date) | incremental | — | 16 |

## Where the code lives
- `pipelines/datasets/au_geoscape_gnaf/` — `constants.py` (URLs, table list), `utils.py`
  (pure download + cleaning transform), `tasks.py` (Prefect wrappers),
  `flows.py` (the flow + its inline schedule).
- `models/au_geoscape_gnaf/` — dbt models and `schema.yml`.
  The architecture CSVs under `models/au_geoscape_gnaf/code/architecture/` are the
  schema source of truth.

## Source
- https://data.gov.au/data/api/3/action/package_show

## Design notes
G-NAF is Open-G-NAF/CC-BY, so every table is ``AllFree`` — no BD Pro rolling window, no
Row Access Policies. The quarterly cadence is well below the monthly-or-more paywall
threshold.

The run resolves the current release from the CKAN API and polls cheaply first (the
resolved ``snapshot_date`` vs the free ``Coverage``), only downloading the ~1.6 GB
payload when a newer quarterly snapshot has actually been published — so a scheduled run
is a cheap no-op between quarterly releases.

Deploy: `.github/scripts/deploy_flows.py` auto-discovers ``au_geoscape_gnaf_flow``; the
dev pool ignores the schedule, the prod pool activates it (deployed paused).

Shared by the recurring pipeline (wrapped in ``@task`` in ``tasks.py``) and the one-shot
bootstrap in ``models/au_geoscape_gnaf/code/clean.py`` (which imports ``clean_all``
directly). Pure functions, no Prefect imports, so they are importable and unit-testable.

Each quarterly G-NAF release is a full snapshot. The per-state PSV tables are read
straight out of the downloaded all-states zip (no full extraction); the default geocode,
locality/street points, and ABS mesh-block codes are folded into the three backbone
tables; output is written as parquet partitioned by ``snapshot_date`` + ``id_state``
(CNPJ-style stacking).

- ``stringify=False`` — typed parquet (dates/floats/int). Used by the one-shot
bootstrap, which uploads with an explicit typed hive schema. - ``stringify=True`` — all-
STRING parquet + a 0-row ``00_header.parquet`` guard per partition. Required by the
pipeline: ``upload_to_gcs`` infers the staging schema from a stringified header, so
typed parquet is rejected, and the dbt models ``safe_cast`` every column back to its
real type. The raw PSV values are already ISO dates / plain decimals / small ints, so
the string path keeps them verbatim (empty -> NULL); it never round-trips through float,
so no ``"1959.0"`` / ``"nan"`` artifacts appear.

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
