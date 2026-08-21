# au_rba_statistical_tables

Reserve Bank of Australia statistical tables. Every CSV carries the full history of its
series, so each run is a **full replace** (``dump_mode="overwrite"``), not an
incremental append. A single flow downloads all ~220 CSVs once and rebuilds all four
tables.

## Refresh cadence
- `0 9 * * *` — 09:00 America/Sao_Paulo, daily

Staging upload: dump mode `overwrite`, source format `parquet`.
Worker sizing: `"memory": "4Gi"`

## Tables
| table | partition | materialization | coverage tier | cast columns |
|---|---|---|---|---|
| `data` | `year` (int64) | table | AllFree | 5 |
| `dicionario` | — | table | — | 5 |
| `series` | — | table | AllFree | 12 |
| `series_break` | — | table | AllFree | 6 |

## Where the code lives
- `pipelines/datasets/au_rba_statistical_tables/` — `constants.py` (URLs, table list), `utils.py`
  (pure download + cleaning transform), `tasks.py` (Prefect wrappers),
  `flows.py` (the flow + its inline schedule).
- `models/au_rba_statistical_tables/` — dbt models and `schema.yml`.
  The architecture CSVs under `models/au_rba_statistical_tables/code/architecture/` are the
  schema source of truth.

## Source
- (none literal in `constants.py`)

## Design notes
Deploy: `.github/scripts/deploy_flows.py` auto-discovers
`au_rba_statistical_tables_flow`; the dev pool ignores the schedule, the prod pool
activates it.

No Prefect imports here — this module is the single source of truth for the transform,
shared by the recurring pipeline (``tasks.py``) and the one-shot onboarding bootstrap
under ``models/au_rba_statistical_tables/code/``.

The RBA publishes each statistical table as a CSV with a metadata block keyed by row
label (``Title``, ``Description``, ``Frequency``, ``Type``, ``Units``, ``Source``,
``Publication date``, ``Series ID``) above a dated data block. Each data column is one
series; the transform pivots that wide block into long ``(table_id, series_id, date,
value)`` rows and lifts the metadata block into a series catalogue.

1. ``series_id`` is NOT globally unique. 228 mnemonics appear in both the ``b13.1.2-*``
and ``b13.2.1-*`` tables carrying different values (827 of 834 overlapping cells
differ). The key is therefore ``(table_id, series_id)``.

2. Four file families are not ``(series_id, date)`` time series at all and are excluded
— see ``NON_TIMESERIES_PREFIXES``.

Licence gate: only series every one of whose named sources publishes under CC BY 4.0
(RBA, APRA, ABS) are kept. See ``models/au_rba_statistical_tables/LICENCE.md``.

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
