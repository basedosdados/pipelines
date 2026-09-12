# br_cgu_sancoes

CGU sanctions registries (CEIS, CNEP, CEPIM, Acordos de Leniência) from the Portal da
Transparência. Each registry is an on-demand live snapshot with no historical archive,
so every run fetches the latest available snapshot and does a full replace
(``dump_mode="overwrite"``), stamping ``data_extracao``. A single flow downloads all
registries once and rebuilds all six tables.

## Refresh cadence
- `0 8 * * 1,2` — 08:00 America/Sao_Paulo, Mon/Tue

Staging upload: dump mode `overwrite`, source format `parquet`.

## Tables
| table | partition | materialization | coverage tier | cast columns |
|---|---|---|---|---|
| `acordos_leniencia` | `data_extracao` (date) | table | PartBdpro | 12 |
| `acordos_leniencia_efeitos` | `data_extracao` (date) | table | AllFree | 4 |
| `ceis` | `data_extracao` (date) | table | PartBdpro | 25 |
| `cepim` | `data_extracao` (date) | table | AllFree | 6 |
| `cnep` | `data_extracao` (date) | table | PartBdpro | 26 |
| `dicionario` | — | table | — | 5 |

## Where the code lives
- `pipelines/datasets/br_cgu_sancoes/` — `constants.py` (URLs, table list), `utils.py`
  (pure download + cleaning transform), `tasks.py` (Prefect wrappers),
  `flows.py` (the flow + its inline schedule).
- `models/br_cgu_sancoes/` — dbt models and `schema.yml`.

## Source
- https://portaldatransparencia.gov.br/download-de-dados/{registry}/{date}

## Design notes
BD Pro rolling window: the high-value compliance tables (ceis, cnep, acordos_leniencia)
paywall their recent window keyed on the *sanction start* date (``data_inicio_sancao`` /
``data_inicio_acordo``), so a sanction ages out of Pro six months after it starts. The
remaining tables (cepim, acordos_leniencia_efeitos) stay fully free (coverage keyed on
the snapshot date, their only date column); ``dicionario`` has no date column and takes
no spec.

Deploy: `.github/scripts/deploy_flows.py` auto-discovers `br_cgu_sancoes_flow`; the dev
pool ignores the schedule, the prod pool activates it.

Pure functions (no Prefect) shared by the recurring pipeline (wrapped in @task in
tasks.py) and the one-shot bootstrap in ``models/br_cgu_sancoes/code/clean.py`` (which
imports the column specs and transform from here). This module is the schema source of
truth for the dataset — there are no architecture CSVs; the per-column ``(name, kind)``
specs below define the output column order and types, and the dbt models ``safe_cast``
to those types.

The registries are cumulative on-demand snapshots (each snapshot contains the full past
+ active history), so the pipeline keeps a single current snapshot per table
(``dump_mode="overwrite"``) with ``data_extracao`` as the freshness stamp.

CGU sanctions registries published by the Portal da Transparência: CEIS, CNEP, CEPIM and
Acordos de Leniência (which ships two CSVs, Acordos + Efeitos). Each registry is an on-
demand live snapshot behind AWS WAF: requesting ``/download-de-
dados/<registry>/<YYYYMMDD>`` triggers generation and 302-redirects to the S3 zip once
ready. There is no historical archive — only the current snapshot is retrievable — so
every run fetches the latest available snapshot and overwrites
(``dump_mode="overwrite"``), stamping ``data_extracao`` with the snapshot date.

The cleaning transform and column schema live in ``utils.py`` (the schema source of
truth for this dataset, since there are no architecture CSVs); the one-shot bootstrap in
``models/br_cgu_sancoes/code/clean.py`` imports the same functions.

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
