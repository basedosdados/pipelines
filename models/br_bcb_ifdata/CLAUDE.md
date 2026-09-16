# br_bcb_ifdata

Compartilhado entre o onboarding estático (`models/br_bcb_ifdata/code/`) e o pipeline
recorrente trimestral. Sem imports do Prefect.

## Refresh cadence
- `0 16 1,2,3,4,5 * *` — 16:00 America/Sao_Paulo, day 1,2,3,4,5

Staging upload: dump mode `overwrite`, source format `parquet`.
Worker sizing: `"memory": "4Gi"`

## Tables
| table | partition | materialization | coverage tier | cast columns |
|---|---|---|---|---|
| `coluna` | `ano` (int64) | table | — | 10 |
| `dicionario` | — | table | — | 5 |
| `instituicao` | `ano` (int64) | table | — | 15 |
| `relatorio` | `ano` (int64) | table | — | 5 |

## Where the code lives
- `pipelines/datasets/br_bcb_ifdata/` — `constants.py` (URLs, table list), `utils.py`
  (pure download + cleaning transform), `tasks.py` (Prefect wrappers),
  `flows.py` (the flow + its inline schedule).
- `models/br_bcb_ifdata/` — dbt models and `schema.yml`.
  The architecture CSVs under `models/br_bcb_ifdata/code/architecture/` are the
  schema source of truth.

## Source
- (none literal in `constants.py`)

## Design notes
A API documentada do IF.data (Olinda OData, `servico/IFDATA`) respondia 500 em todas as
chamadas de dados em 2026-08-18, então o download usa a API do próprio aplicativo
IF.data (`www3.bcb.gov.br/ifdata/rest/`). Ela não é documentada, mas é a que o site usa.
O formato é decodificado assim:

trel<p>_<rel>.json -> c[] -> ifd --(info[].id)--> info entry info entry.ty == 0 -> valor
vem do cadastro, coluna `c<lid>` info entry.ty == 1 -> valor vem de dados<p>_<n>.json,
célula `lid` info entry.lid == -1 -> coluna não disponível nesta competência/tipo

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
