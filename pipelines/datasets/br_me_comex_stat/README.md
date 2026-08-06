# Documentação do Conjunto de Dados: Comex Stat (Estatísticas de Comércio Exterior)

Este documento registra o contexto e as decisões da base do Comex Stat, para quem
for mexer no pipeline depois.

---

## Sobre a fonte

O Comex Stat é a base de estatísticas de comércio exterior do MDIC, montada a
partir das declarações de importação e exportação processadas pela Receita
Federal.

- [Página de base de dados bruta (é o que o flow raspa)](https://www.gov.br/mdic/pt-br/assuntos/comercio-exterior/estatisticas/base-de-dados-bruta)
- [Diretório dos arquivos CSV](https://balanca.economia.gov.br/balanca/bd/comexstat-bd)

Quatro tabelas, dois recortes:

| Tabela | Arquivo na fonte | Grão | Partições |
|---|---|---|---|
| `ncm_exportacao` | `ncm/EXP_<ano>.csv` | NCM × país × UF × via × URF | `ano`, `mes` |
| `ncm_importacao` | `ncm/IMP_<ano>.csv` | idem | `ano`, `mes` |
| `municipio_exportacao` | `mun/EXP_<ano>_MUN.csv` | SH4 × país × município | `ano`, `mes`, `sigla_uf` |
| `municipio_importacao` | `mun/IMP_<ano>_MUN.csv` | idem | `ano`, `mes`, `sigla_uf` |

As quatro são `part_bdpro` com `free_lag` de 6 meses (o padrão de `PartBdpro`):
os últimos 6 meses ficam atrás do BD Pro e o resto é livre. A janela rola
sozinha a cada run, via `sync_table_coverage_task`.

## Particularidades que mandam no desenho do pipeline

### A fonte publica o ano inteiro, sempre

Cada arquivo é o ano corrente completo, não o mês novo. Três consequências, todas
boas:

1. **Não existe backfill a fazer.** Qualquer run refaz janeiro–mês corrente. Se o
   flow perdeu um mês, a run seguinte traz o mês perdido junto — não precisa de
   loop de meses como o `br_me_caged` nem de listagem de FTP como o CNES.
2. **É idempotente.** `to_partitions` grava `data.csv` com nome fixo por partição
   (`pipelines/utils/utils.py`), então re-subir o ano sobrescreve os mesmos
   objetos no GCS. `dump_mode="append"` não duplica.
3. **Revisão de mês passado entra de graça.** O MDIC revisa meses anteriores
   dentro do mesmo arquivo anual, e como o ano inteiro é reprocessado, a revisão
   chega sem tratamento especial.

Na virada de ano o `parse_last_date` devolve `YYYY-01` e o download passa a
buscar só o arquivo novo — dezembro do ano anterior já está materializado das
runs anteriores.

### A data da fonte vem de um `<h2>` raspado, não dos arquivos

`parse_last_date` (`pipelines/crawler/me_comex_stat/tasks.py`) lê o seletor
`#parent-fieldname-text > h2` da página de base bruta:

```text
'Últimos dados disponíveis: janeiro - julho de 2026'  →  '2026-07'
```

Duas fragilidades para ter em mente:

- O texto passa por um dicionário `{"julho de 2026": "2026-07", ...}`. Se o MDIC
  mudar a redação do título, é `KeyError` — falha alta, não silenciosa, o que é
  o comportamento desejado.
- **Esse título fala pelas quatro tabelas ao mesmo tempo.** Se um dia os arquivos
  de município passarem a ter defasagem maior que os de NCM, o gate vai liberar
  as tabelas de município todo dia sem nada novo para trazer (a run é inofensiva,
  mas reprocessa e roda dbt à toa). Em 2026-08-06 conferi mês a mês os quatro
  arquivos: todos com `CO_MES` até 7, sem divergência. Se aparecer churn diário
  em `municipio_*`, é aqui que se olha.

### A validação contra os totais oficiais derruba a run

`validate_table` (`pipelines/crawler/me_comex_stat/utils.py`) baixa o arquivo de
conferência do MDIC (`*_TOTAIS_CONFERENCIA*.csv`) e compara, para o ano de
referência, as somas de `valor_fob_dolar`, `peso_liquido_kg` e
`quantidade_estatistica` mais a contagem de linhas. Qualquer divergência levanta
`ValueError`.

Isso roda dentro do clean, **antes de qualquer upload**, então uma falha nunca
suja o BigQuery. E como o `Table.Update` só avança no fim do flow, a run seguinte
tenta de novo sozinha. O cenário esperado é o MDIC publicar o arquivo de dados
antes de atualizar o de conferência: a run do dia quebra, a do dia seguinte passa.

### Uma única fonte para as quatro tabelas

As quatro tabelas apontam para o mesmo `RawDataSource` no backend
(`347bae65-…`, a página de base bruta). Ou seja, o `RawDataSource.Update` é um
**ponteiro compartilhado**: a primeira das quatro que rodar no dia o avança, e as
outras três encontram ele já em dia.

Isso não atrapalha o gate, porque o gate compara contra o `Table.Update`, que é
por tabela. Mas explica uma coisa que confunde ao olhar o site: as quatro exibem
a mesma "última atualização na fonte original" mesmo tendo coberturas diferentes.

---

## Pipeline: a armadilha que já congelou este conjunto

### O gate antigo comparava data de cobertura com relógio

Até 2026-08, o flow usava `poll_source_for_update_task`, cujo gate é:

```python
source_max_date > client.get_table_update_latest(...)
```

O lado esquerdo é uma **data de cobertura** (`2026-07-01` = "a fonte tem dados até
julho"). O lado direito é um **relógio**: `register_table_materialization_task`
grava ali o `bq.last_modified()`, o horário da última materialização.

Como o dado do mês M sai no começo de M+1, a data de cobertura fica sempre cerca
de um mês atrás do relógio. O gate só passa quando faz mais de um mês desde a
última materialização — na prática, o conjunto ingeria **mês sim, mês não**.

Estado em 2026-08-06, que é o retrato exato do problema:

| Tabela | `Table.Update` (relógio) | `RawSource.Update` (cobertura) | Cobertura publicada |
|---|---|---|---|
| `ncm_exportacao` | 2026-07-03 | 2026-06-01 | …2026-06 |
| `ncm_importacao` | 2026-07-03 | 2026-06-01 | …2026-06 |
| `municipio_exportacao` | 2026-06-26 | 2026-06-01 | …2026-05 |
| `municipio_importacao` | 2026-06-26 | 2026-06-01 | …2026-05 |

Com julho na fonte, a conta do gate ficava `2026-07-01 > 2026-07-03` para NCM —
falso, travado. As tabelas de município já tinham perdido junho pelo mesmo
motivo (`2026-06-01 > 2026-06-26` é falso), e é por isso que a cobertura delas
parou em maio.

O sintoma no Prefect é traiçoeiro: a run termina `COMPLETED`, o `Poll` fica em
dia e o log diz apenas `Não há novas atualizações na fonte original`. Uma
pipeline morta é indistinguível de uma saudável pelo estado da run.

### O modelo novo, adotado aqui em 2026-08

O flow passou para o modelo de reconciliação de cobertura
(`pipelines/utils/metadata/poll.py`), o mesmo do CNES e do
`br_me_caged`:

```python
register_source_coverage_task(...)          # grava o Poll e avança RawSource.Update
check_source_is_ahead_of_table_task(...)    # gate: RawSource.Update > Table.Update
...
sync_table_coverage_task(...)               # cobertura + RAPs + Table.Update como cobertura
```

Agora os dois lados da comparação são data de cobertura. O `commit_source_update_task`
sumiu: quem grava o ponteiro da fonte é o `register_source_coverage_task`, no
começo do flow. O commit deferido deixou de ser necessário porque o gate passou a
depender do `Table.Update`, que só avança no `sync_table_coverage_task`, no fim —
uma falha no meio não trava a run seguinte.

O `register_source_coverage_task` fica **fora** do `if not force_run`: o Poll é
gravado em toda run, inclusive nas forçadas. Só o gate é pulado.

### Migrar exige semear o `Table.Update` — não é opcional

Esta é a parte que congelou o CNES de 2026-06-17 a 2026-08-03 e que o diff do
`br_me_caged` não cobre. O `Table.Update` que já está gravado é **relógio**, e o
gate novo o interpreta como cobertura. Sem semear, ele compara
`2026-07-01 > 2026-07-03` e devolve falso para sempre — o modelo novo herda o
congelamento do antigo.

Nada no código valida isso nem corrige sozinho: o único escritor do valor certo
(`sync_table_coverage_task`) está **depois** do gate que o valor errado bloqueia.

Duas rotas equivalentes:

```python
# 1. semear a cobertura já materializada
from pipelines.utils.metadata.client import MetadataClient
import datetime

client = MetadataClient(env="prod")
seeds = {
    "ncm_exportacao": datetime.date(2026, 6, 1),
    "ncm_importacao": datetime.date(2026, 6, 1),
    "municipio_exportacao": datetime.date(2026, 5, 1),
    "municipio_importacao": datetime.date(2026, 5, 1),
}
for table_id, latest in seeds.items():
    client.upsert_table_update("br_me_comex_stat", table_id, latest=latest)
```text

```
# 2. disparar cada deployment uma vez com force_run
parameters = {"force_run": true}
```

Conferir depois com `client.get_table_update_latest(...)`: tem que voltar data de
cobertura (dia 1º, meia-noite), nunca timestamp com hora. Se voltar hora
diferente de meia-noite, é relógio e o gate está travado.

**O seed tem que chegar junto com o deploy do código novo.** Enquanto o flow
antigo estiver no ar, todo run bem-sucedido dele chama
`register_table_materialization_task`, que regrava o `Table.Update` como relógio
— desfazendo o seed. Pior: o seed *destrava* o gate antigo também (ele compara
contra o mesmo campo), então semear e não fazer o deploy provoca exatamente o run
que apaga o seed. Se isso acontecer, é só semear de novo com a cobertura
materializada mais recente.

---

## Ambiente e execução

Os flows escrevem em **prod por padrão** (`materialize_after_dump=True`,
`update_metadata=True`, `target="prod"`). Uma run disparada com `{}` sobe dado em
prod, reescreve cobertura e reaplica as Row Access Policies. Para testar sem
tocar em prod:

```json
{"materialize_after_dump": false, "update_metadata": false, "force_run": true}
```

Crons (America/Sao_Paulo), em `flows.py`:

| Flow | Cron |
|---|---|
| `ncm_exportacao`, `ncm_importacao` | `0 8,17 * * 1-5` |
| `municipio_importacao` | `0 20 * * 1-5` |
| `municipio_exportacao` | `0 21 * * 1-5` |

As tabelas de NCM rodam duas vezes por dia. Com o gate novo, a primeira run que
detectar novidade materializa e a segunda encontra o `Table.Update` já em dia e
encerra.

### Verificação local não alcança este flow

`load_flows_from_file` não importa `flows.py` no venv 3.10 do repo, porque
`pipelines/utils/metadata/domain.py` usa `StrEnum` (Python 3.11+). Não é um
defeito deste conjunto — vale para todo flow que importa `domain.py`, CNES e
CAGED inclusive. A imagem de execução é `prefecthq/prefect:3-python3.12`, onde
funciona. Para checar a descoberta localmente, importe com um shim:

```python
import enum
if not hasattr(enum, "StrEnum"):
    class StrEnum(str, enum.Enum): pass
    enum.StrEnum = StrEnum
```

Lembrando que `load_flows_from_file` **engole ImportError** e devolve `{}` — um
import quebrado vira ausência silenciosa de deploy, não erro.
