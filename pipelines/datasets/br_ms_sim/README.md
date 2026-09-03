# br_ms_sim — pipeline

Carga dos microdados de óbitos não fetais (CID-10) do SIM/DATASUS, do FTP até a
materialização em `basedosdados.br_ms_sim.microdados`.

Contexto da base, investigações de qualidade e decisões de tratamento estão em
[`models/br_ms_sim/README.md`](../../../models/br_ms_sim/README.md).

## Sem schedule, por quê

O DATASUS republica o SIM duas vezes por ano, sem data fixa. O flow é deployado
sem `deploy_schedules`: o deployment existe, aceita execução avulsa e não dispara
sozinho. Para armar depois, basta acrescentar a lista de crons em `flows.py`.

## Definitivo e preliminar

A fonte serve o mesmo ano em dois diretórios:

| Diretório | Conteúdo |
|---|---|
| `SIM/CID10/DORES/` | definitivo, publicado cerca de um ano após o fechamento |
| `SIM/PRELIM/DORES/` | preliminar, ainda sujeito a revisão |

`resolve_year_source` dá precedência ao definitivo. Reprocessar um ano depois do
fechamento troca o dado e muda `dado_preliminar` de `1` para `0` — não há passo
manual para a virada, só rodar o flow com aquele ano.

## Parâmetros

| Parâmetro | Padrão | Efeito |
|---|---|---|
| `ano` | vazio | Vazio pega o ano mais recente da fonte. Preenchido é backfill: o flow pula o poll e não mexe no metadado da fonte |
| `materialize_after_dump` | `True` | Sobe para prod e materializa lá |
| `update_metadata` | `True` | Registra a cobertura materializada |
| `force_run` | `False` | Materializa mesmo sem novidade na fonte |

Execução de teste no pool de dev, sem tocar em produção:

```json
{"materialize_after_dump": false, "update_metadata": false, "force_run": true}
```

Os padrões escrevem em **produção**, mesmo saindo do pool de teste.

## Formato da staging

A staging é CSV desde a carga original. O particionado sai em
`ano=<ano>/sigla_uf=<UF>/microdados.csv` e sobe com `dump_mode="append"`: os
caminhos são fixos, então reenviar um ano sobrescreve aquele ano e preserva o
resto da série. `overwrite` apagaria o prefixo inteiro, com ele 1996 em diante.

Trocar para parquet exigiria recriar a tabela externa e, com ela, recarregar
toda a série.

## Carga manual

`utils.py` não importa Prefect, então a transformação roda fora do flow:

```python
from pipelines.datasets.br_ms_sim import utils

source = utils.resolve_year_source(2025)
utils.download_table("microdados", 2025, source)
utils.clean_table("microdados", 2025, source)
```

## Pontos de atenção

- `dado_preliminar` é coluna nova no modelo. Em `dump_mode="append"` o schema da
  tabela externa só é ampliado por `_sync_staging_schema`
  (`pipelines/utils/tasks.py`), que abre o cliente do BigQuery sem credencial e
  responde `403 bigquery.tables.update`. É o primeiro caso em que esse caminho é
  de fato exercitado.
- O dicionário do conjunto vem de uma staging própria, alimentada por
  `models/br_ms_sim/code/update_dicionario.py`. As linhas de `dado_preliminar`
  precisam ser acrescentadas por lá.
- Os scripts em `models/br_ms_sim/code/microdados/` são a carga anterior e não
  conhecem `dado_preliminar`: gravam CSV com uma coluna a menos do que o modelo
  espera. Usar o flow.
