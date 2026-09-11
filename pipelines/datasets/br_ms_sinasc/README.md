# br_ms_sinasc — pipeline

Carga dos microdados de nascidos vivos do SINASC/DATASUS, do FTP até a
materialização em `basedosdados.br_ms_sinasc.microdados`.

Contexto da base, anomalias conhecidas e decisões de tratamento estão em
[`models/br_ms_sinasc/README.md`](../../../models/br_ms_sinasc/README.md).

## Sem schedule, por quê

O DATASUS republica o SINASC uma vez por ano, sem data fixa, e revisa anos já
fechados. O flow é deployado sem `deploy_schedules`: o deployment existe, aceita
execução avulsa e não dispara sozinho. Para armar depois, basta acrescentar a
lista de crons em `flows.py`.

## Só o diretório definitivo

A fonte serve os anos fechados em `SINASC/1996_/Dados/DNRES/` e o ano corrente
em um diretório preliminar à parte. O flow lê apenas o definitivo, como fazia a
carga local, e a tabela não tem coluna que distinga a origem — diferente de
`br_ms_sim`, que carrega os dois e marca `dado_preliminar`.

Passar a carregar o preliminar exige acrescentar a coluna ao modelo e ao
dicionário antes de mexer aqui.

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

## Qual ano entra

Sem `ano`, o flow carrega o ano mais recente que existe na fonte, que aqui é o
último ano fechado: o flow lê só o diretório definitivo.

O poll compara esse ano com o fim da cobertura da tabela. Depois que a cobertura
alcança o ano, as execuções seguintes encerram sem carregar nada, e as revisões
que o DATASUS publicar em anos já fechados não entram. Para trazê-las, executar
com `ano` preenchido ou com `force_run`.

## Formato da staging

A staging é CSV desde a carga original. O particionado sai em
`ano=<ano>/sigla_uf=<UF>/data.csv` e sobe com `dump_mode="append"`: os caminhos
são fixos, então reenviar um ano sobrescreve aquele ano e preserva o resto da
série. `overwrite` apagaria o prefixo inteiro, com ele 1996 em diante.

Trocar para parquet exigiria recriar a tabela externa e, com ela, recarregar
toda a série.

## Carga manual

`utils.py` não importa Prefect, então a transformação roda fora do flow:

```python
from pipelines.datasets.br_ms_sinasc import utils

utils.download_table("microdados", 2024)
utils.clean_table("microdados", 2024)
```

## Diferenças em relação à carga local

O flow substitui `models/br_ms_sinasc/code/br_ms_sinasc_etl.py`, com três
mudanças de comportamento:

- **`id_municipio_mae`** era convertido duas vezes — primeiro de 6 para 7
  dígitos, depois cruzado de novo contra o código de 6 —, e o segundo cruzamento
  zerava a coluna. A conversão agora é uma só, por valor: código de 6 dígitos
  vira 7, o de 7 passa intacto.
- **`data_registro_cartorio`** não estava na lista de datas e chegava à staging
  como `DDMMAAAA`, que o `safe_cast(... as date)` do modelo nulifica. Passou a
  ser convertida como as demais. O `DNRES` não traz o bloco de cartório em 2018
  nem em 2024, então `cartorio`, `registro_cartorio` e `data_registro_cartorio`
  saem nulas.
- **Coluna nova na fonte** era detectada comparando o cabeçalho com o do ano
  anterior, o que exigia ter os dois anos baixados, e a coluna sem mapeamento
  era descartada em silêncio. Agora a limpeza levanta `ValueError` nomeando a
  coluna fora de `RENAME`.

## Pontos de atenção

- `sequencial_nascimento` não é chave: reinicia por lote estadual e repete entre
  reprocessamentos. Não há combinação de colunas que garanta unicidade, e o
  modelo não tem teste de unicidade.
- O dicionário do conjunto tem staging própria e não é alimentado por este flow.
