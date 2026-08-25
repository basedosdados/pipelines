# us_fed_fred

FRED (Federal Reserve Bank of St. Louis) public-domain economic series. FRED serves the
full history of each series on every call (latest revision only), so each run is a
**full replace** (``dump_mode="overwrite"``), not an incremental append. A single flow
downloads the seed series, rebuilds both tables, and materializes them. The source poll
short-circuits the run when no series has a newer observation, making a scheduled daily
run a cheap no-op between releases.

## Refresh cadence
- `0 21 * * *` — 21:00 America/Sao_Paulo, daily

Staging upload: dump mode `overwrite`, source format `parquet`.

## Tables
| table | partition | materialization | coverage tier | cast columns |
|---|---|---|---|---|
| `observation` | `year` (int64) | table | PartBdpro | 4 |
| `series` | — | table | NonHistorical | 14 |

## Where the code lives
- `pipelines/datasets/us_fed_fred/` — `constants.py` (URLs, table list), `utils.py`
  (pure download + cleaning transform), `tasks.py` (Prefect wrappers),
  `flows.py` (the flow + its inline schedule).
- `models/us_fed_fred/` — dbt models and `schema.yml`.
  The architecture CSVs under `models/us_fed_fred/code/architecture/` are the
  schema source of truth.

## Source
- https://api.stlouisfed.org/fred

## Design notes
``observation`` is high-frequency (daily/weekly series), so it carries the BD Pro
rolling window: the most recent 6 months are pro-only, everything older is free.
``series`` is a metadata catalog and stays fully free (``NonHistorical`` coverage from
the table's last-modified time).

Deploy: ``.github/scripts/deploy_flows.py`` auto-discovers ``us_fed_fred_flow``; the dev
pool ignores the schedule, the prod pool activates it.

Pure functions (no Prefect) so they are importable and unit-testable. The recurring
pipeline wraps them in ``@task`` (see ``tasks.py``); the bootstrap CLI imports
``download_all``/``clean_all`` directly. Schema/column order come from the architecture
CSVs (the single source of truth).

``download_all(input_dir)`` fetch each seed series' metadata + observations, apply the
public-domain license gate, and persist the kept series as raw JSON under ``input_dir``.
``clean_all(input_dir, out)`` read that raw JSON and write the two tables as all-STRING
partitioned parquet under ``out``.

Splitting them lets ``clean_all`` be re-run (and transform-parity-tested) against a
cached ``input/`` without re-hitting the FRED API.

License gate (both applied at download): 1. Source allowlist — keep only U.S.-federal-
agency sources (public domain). 2. "Copyright" in ``/series`` notes — FRED's own marker
for restricted series. Every dropped series is logged to ``input_dir/_excluded.csv``.

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
