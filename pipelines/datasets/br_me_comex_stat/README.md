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

`parse_last_date` (`pipelines/datasets/br_me_comex_stat/tasks.py`) lê o seletor
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

`check_totals_against_validation_file` (`pipelines/datasets/br_me_comex_stat/utils.py`) baixa o arquivo de
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

## Testes

Os testes ficam em duas camadas: checagens em Python, que rodam no clean antes do
upload, e testes do dbt, que rodam no `build_and_promote`.

### Checagens antes do upload

Rodam dentro de `clean_br_me_comex_stat` (`pipelines/datasets/br_me_comex_stat/tasks.py`).
O `upload_to_gcs` só começa depois que o clean termina, então qualquer erro
interrompe a run antes de subir arquivo.

1. `check_year_matches_filename` (`utils.py` do conjunto) confere que toda linha
   tem o `ano` do nome do arquivo (`EXP_2026.csv`, `IMP_2026_MUN.csv`). Roda logo
   depois do rename, antes de gravar as partições.
2. `check_totals_against_validation_file` compara somas e contagem de linhas com o arquivo de
   conferência do MDIC, como descrito em "A validação contra os totais oficiais
   derruba a run". Reprova arquivo sem nenhuma linha de dado; nos outros, o
   passo 1 garante um `ano` só.
3. `check_rows_written` (`utils.py` do conjunto) roda depois de todos os
   arquivos e compara as linhas lidas com as linhas dos `data.csv` gravados nas
   partições. Detecta linha perdida (coluna de partição vazia) e linha a mais (o
   `to_partitions` grava em modo de acréscimo).

### Testes do dbt

Ficam em `models/br_me_comex_stat/schema.yml`. Nas quatro tabelas de dado, todos
rodam só no mês mais recente (`where: __most_recent_year_month__`). Uma falha
interrompe o `build_and_promote`, e o mês não chega a prod.

| Teste | Colunas | Detalhe |
|---|---|---|
| `dbt_utils.unique_combination_of_columns` | Chave de cada tabela | |
| `not_null_proportion_multiple_columns` | Todas | `at_least: 0.90` |
| `relationships` | `id_sh4` | Diretório do sistema harmonizado |
| `relationships` | `id_municipio` | Diretório de municípios |
| `relationships` | `id_unidade` | Diretório de NCM |
| `custom_relationships` | `sigla_uf`, `sigla_uf_ncm` | Diretório de UF, campo `sigla`; ignora `EX` |
| `custom_relationships` | `sigla_pais_iso3` | Diretório de países; ignora `ANT`, `CSK`, `SCG` e `PCZ` |
| `custom_relationships` | `id_ncm` | Diretório de NCM; ignora `85492100`, `90189097`, `26011220` e `29159070` |
| `custom_dictionary_coverage` | `id_pais` nas quatro tabelas; `id_via` e `id_urf` nas duas de NCM | `dicionario` |
| `dbt_utils.accepted_range` | `peso_liquido_kg`, `valor_fob_dolar`, `quantidade_estatistica`, `valor_frete`, `valor_seguro`, onde existirem | Mínimo 0 |

- Em 2026 o maior percentual de nulos foi 0,52%, em `sigla_uf_ncm` da
  `ncm_exportacao`, onde o modelo transforma o código `ND` em nulo.
- Os testes de UF ignoram o código `EX`, que a fonte usa e o diretório de UF não
  tem.
- O `custom_relationships` reprova com um único valor fora do diretório.
- O teste aceita zero e reprova valores negativos.

No `dicionario` os testes rodam sem filtro de mês: unicidade,
`not_null_proportion_multiple_columns` (`at_least: 0.90`) e `accepted_values`:

| Coluna | Valores aceitos |
|---|---|
| `id_tabela` | `municipio_exportacao`, `municipio_importacao`, `ncm_exportacao`, `ncm_importacao` |
| `nome_coluna` | `id_pais`, `id_via`, `id_urf` |

Não há teste de `ano` nem de `mes` no dbt: com o filtro do mês mais recente, o
teste só veria valores válidos. Linha com `ano` vazio reprova no
`check_year_matches_filename`; linha com `mes` vazio não passa por nenhuma
checagem.

### Manutenção do dicionário

Nenhum flow atualiza o `dicionario`. Código novo de país, via ou URF reprova o
`custom_dictionary_coverage` até ser incluído à mão no arquivo da staging de dev,
gravado por cima do mesmo arquivo:

```text
gs://basedosdados-dev/staging/br_me_comex_stat/dicionario/dicionario.csv
```

Prod recebe pelo `table-approve`, que só sincroniza a staging de modelo cujo
`.sql` mudou na PR.

Formato das entradas:

- Na staging a coluna se chama `coluna`; o modelo renomeia para `nome_coluna`.
- O rótulo vem das tabelas auxiliares do MDIC
  ([`PAIS.csv`](https://balanca.economia.gov.br/balanca/bd/tabelas/PAIS.csv),
  [`URF.csv`](https://balanca.economia.gov.br/balanca/bd/tabelas/URF.csv)), sem o
  código na frente, com a inicial de cada palavra em maiúscula e sem acento:
  `0812003 - DRF TAUBATE` vira `Drf Taubate`.
- `cobertura_temporal` é `(1)`.

### Diretório de NCM

Os quatro códigos em `ignore_values` do `id_ncm` são códigos que a fonte usa e o
diretório de NCM (`br_bd_diretorios_mundo.nomenclatura_comum_mercosul`) não tem.
Nenhum flow atualiza esse diretório: ele é carregado à mão por
`models/br_bd_diretorios_mundo/code/download_sh4_ncm.py`, a partir do
[`NCM.csv`](https://balanca.economia.gov.br/balanca/bd/tabelas/NCM.csv) do MDIC.
NCM novo do MDIC reprova o `custom_relationships` do `id_ncm` até o diretório ser
recarregado ou o código entrar no `ignore_values`.

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

    class StrEnum(str, enum.Enum):
        pass

    enum.StrEnum = StrEnum
```

Lembrando que `load_flows_from_file` **engole ImportError** e devolve `{}` — um
import quebrado vira ausência silenciosa de deploy, não erro.
