# br_senado_dados_abertos

Senado Federal legislative open data. One flow refreshes all 18 tables from the public
Legislative Open Data API each day: the ten dimensions in full, the eight time-series
tables (votacao, votacao_parlamentar, votacao_orientacao_bancada, processo, relatoria,
votacao_comissao, votacao_comissao_parlamentar, discurso) for the recent window only —
uploaded with ``dump_mode="append"``, which replaces just those ``ano=`` partitions and
leaves history in place. Like the Câmara pipeline, there is no source-poll gate:
legislative activity changes continuously, so a daily run is always meaningful.

## Refresh cadence
- `0 8 * * *` — 08:00 America/Sao_Paulo, daily

Staging upload: dump mode `append`, source format `parquet`.
Worker sizing: `"memory": "4Gi"`

## Tables
| table | partition | materialization | coverage tier | cast columns |
|---|---|---|---|---|
| `bloco` | — | table | — | 5 |
| `comissao` | — | table | — | 9 |
| `discurso` | `ano` (int64) | table | PartBdpro | 16 |
| `lideranca` | — | table | — | 16 |
| `mesa` | — | table | — | 9 |
| `partido` | — | table | — | 5 |
| `processo` | `ano` (int64) | table | PartBdpro | 24 |
| `relatoria` | `ano` (int64) | table | PartBdpro | 18 |
| `senador` | — | table | — | 12 |
| `senador_cargo` | — | table | — | 7 |
| `senador_comissao` | — | table | — | 7 |
| `senador_filiacao` | — | table | — | 5 |
| `senador_mandato` | — | table | — | 10 |
| `votacao` | `ano` (int64) | table | PartBdpro | 27 |
| `votacao_comissao` | `ano` (int64) | table | PartBdpro | 19 |
| `votacao_comissao_parlamentar` | `ano` (int64) | table | PartBdpro | 8 |
| `votacao_orientacao_bancada` | `ano` (int64) | table | PartBdpro | 10 |
| `votacao_parlamentar` | `ano` (int64) | table | PartBdpro | 8 |

## Where the code lives
- `pipelines/datasets/br_senado_dados_abertos/` — `constants.py` (URLs, table list), `utils.py`
  (pure download + cleaning transform), `tasks.py` (Prefect wrappers),
  `flows.py` (the flow + its inline schedule).
- `models/br_senado_dados_abertos/` — dbt models and `schema.yml`.

## Source
- (none literal in `constants.py`)

## Design notes
Deploy: `.github/scripts/deploy_flows.py` auto-discovers `br_senado_dados_abertos_flow`;
the dev pool ignores the schedule, the prod pool activates it.

Builds the same all-STRING partitioned parquet the one-shot onboarding produces, reusing
the cleaning transform in `senado_clean`. Shared by the recurring pipeline (`tasks.py`)
and the onboarding bootstrap (`models/.../code/`).

Senado Federal legislative open data (senators, votes, bills, committees, parties,
blocs, leaderships, Directing Board), sourced from the public Legislative Open Data API.
See models/br_senado_dados_abertos/ for the design and the architecture source of truth
(code/architecture_spec.py).

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
