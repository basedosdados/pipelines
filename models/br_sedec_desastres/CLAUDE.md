# br_sedec_desastres

Relatório "Reconhecimentos vigentes" do S2ID (SEDEC/MIDR): os reconhecimentos federais
de situação de emergência e estado de calamidade pública em vigor.

## Refresh cadence
- `0 9 1 * *` — 09:00 America/Sao_Paulo, day 1

Staging upload: dump mode —, source format `parquet`.
Worker sizing: `"memory": "4Gi"`

## Tables
| table | partition | materialization | coverage tier | cast columns |
|---|---|---|---|---|
| `reconhecimentos_vigentes` | `data_extracao` (date) | table | — | 8 |

## Where the code lives
- `pipelines/datasets/br_sedec_desastres/` — `constants.py` (URLs, table list), `utils.py`
  (pure download + cleaning transform), `tasks.py` (Prefect wrappers),
  `flows.py` (the flow + its inline schedule).
- `models/br_sedec_desastres/` — dbt models and `schema.yml`.

## Source
- https://s2id.mi.gov.br/paginas/relatorios/

## Design notes
A tabela é uma série de retratos — cada execução grava o conjunto vigente na data da
extração. As decisões de desenho e o que ainda está aberto estão no README do diretório.

**Este flow não roda hoje.** O S2ID barra o IP de saída do cluster por geolocalização e
a raspagem morre no download. Decidido em 2026-08-10, com a supervisão: o retrato é
gerado na máquina de quem mantém a base, pelo `run_local.py` deste diretório, e
promovido por PR com a label `table-approve`. A receita mensal está no README. O flow
fica aqui porque o código é o mesmo — se o IP for liberado, basta devolver o
`deploy_schedules`.

Deploy: `.github/scripts/deploy_flows.py` descobre `br_sedec_desastres_flow`
automaticamente, desde que o flow esteja definido neste arquivo (o script filtra por
`obj.fn.__code__.co_filename`). O pool de dev ignora o schedule; o de prod o ativa, mas
entra pausado.

Funções puras: nada aqui importa Prefect no nível do módulo. Quem embala em `@task` é o
`tasks.py`. O `log()` vem de `pipelines.utils.utils` e resolve o logger do Prefect só em
tempo de chamada — dentro de uma task ele escreve no log do run, fora dela cai no
`logging` padrão —, então o módulo segue importável sem o Prefect instalado.

O schema (ordem das colunas, tipos, nome de origem) vem de `constants.COLUNAS`, não de
arquivo — a planilha de arquitetura fica fora do repo, em `task_davi/`.

Relatório "Reconhecimentos vigentes" do S2ID (Sistema Integrado de Informações sobre
Desastres), mantido pela SEDEC, vinculada ao MIDR.

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
