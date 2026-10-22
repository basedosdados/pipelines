# Documentação do Conjunto de Dados: br_inmet_bdmep (Banco de Dados Meteorológicos do INMET)

Este documento registra o contexto e as decisões do conjunto `br_inmet_bdmep`, para quem for
mantê-lo depois.

---

## Sobre o Sistema

O INMET publica os dados horários das estações meteorológicas automáticas em
<https://portal.inmet.gov.br/dadoshistoricos/>, com um ZIP por ano, de 2000 em diante:

```text
https://portal.inmet.gov.br/uploads/dadoshistoricos/<ano>.zip
```

O ZIP do ano corrente é republicado ao longo do ano, à medida que os meses fecham. Um ano que
ainda não existe responde 404.

| Tabela | Conteúdo | Carga |
|---|---|---|
| `microdados` | Uma linha por estação, data e hora (UTC), com precipitação, pressão, radiação, temperatura, umidade e vento | Pipeline recorrente |
| `estacao` | Cadastro das estações: município, nome, data de fundação, localização e altitude | Lida de `br_inmet_bdmep_staging.estacao`, sem flow |

## Estrutura no repositório

| Arquivo | Papel |
|---|---|
| `pipelines/datasets/br_inmet_bdmep/flows.py` | `check_update` e `extract_and_load` do `microdados` |
| `pipelines/datasets/br_inmet_bdmep/tasks.py` | Tasks que chamam as funções do `utils.py` |
| `pipelines/datasets/br_inmet_bdmep/utils.py` | Download do ZIP, leitura de cada estação e gravação do CSV do ano |
| `pipelines/datasets/br_inmet_bdmep/constants.py` | URL, User-Agent, padrões de renomeação, ordem das colunas e regra BD Pro (`PartBdpro`) |
| `models/br_inmet_bdmep/` | Modelos dbt do `microdados` e do `estacao` |

## Acesso à fonte

O site do INMET derruba a conexão quando o User-Agent é o padrão do curl ou do httpx. O do
`requests` e o de navegador passam. O download usa o User-Agent de navegador definido em
`HEADERS`, no `constants.py`.

No começo de janeiro, o ZIP do ano novo pode ainda não existir. Quando a URL do ano corrente
responde 404, `download_year_zip` baixa o do ano anterior.

## Formato dos arquivos

Cada CSV do ZIP traz os dados de uma estação, com nome no padrão
`INMET_<região>_<UF>_<código>_<nome>_<início>_A_<fim>.CSV` e datas em `DD-MM-AAAA`:

```text
INMET_CO_DF_A001_BRASILIA_01-01-2026_A_30-09-2026.CSV
```

O arquivo vem em latin-1 (`ISO-8859-1`), com separador `;` e vírgula decimal. Antes da tabela há
8 linhas de cabeçalho:

```text
REGIAO:;CO
UF:;DF
ESTACAO:;BRASILIA
CODIGO (WMO):;A001
LATITUDE:;-15,78944444
LONGITUDE:;-47,92583332
ALTITUDE:;1160,96
DATA DE FUNDACAO:;07/05/00
```

Na leitura (`read_station_file`):

- o `id_estacao` vem do `CODIGO (WMO)`, na quarta linha;
- os nomes de coluna passam para minúsculas, perdem o acento e são renomeados pelos padrões de
  `RENAME_PATTERNS`, aplicados em ordem;
- o `;` no fim de cada linha gera uma coluna sem nome, que fica de fora do `select`;
- a data é aceita nos formatos `AAAA/MM/DD` e `AAAA-MM-DD`;
- a hora vem como `0000 UTC` e é gravada como `00:00:00`;
- `-9999` marca valor ausente e vira nulo.

A data de referência do `check_update` é a maior data final presente nos nomes dos arquivos
(`get_max_date`).

## Duas versões de cada estação no ZIP

O ZIP do ano corrente traz cada estação em dois arquivos: um até o fim do mês anterior e outro
até o fim do mês mais recente. O `2026.zip` de outubro de 2026 tinha 1.276 CSVs, 639 até
31/08/2026 e 637 até 30/09/2026:

```bash
unzip -Z1 2026.zip | sed -E 's/.*_([0-9]{2}-[0-9]{2}-[0-9]{4})\.CSV$/\1/' | sort | uniq -c
```

O pipeline carrega os dois arquivos, e o modelo descarta a linha repetida com
`qualify row_number() over (partition by data, hora, id_estacao) = 1`.

## Gravação do ano

O `extract_and_load` grava um único CSV por ano, em `ano=<ano>/microdados_<ano>.csv`, lendo e
escrevendo uma estação por vez (`build_year_csv`). Com o `2026.zip` de outubro de 2026
(7.762.080 linhas), o pico de memória fica em 239 MiB. Concatenar o ano inteiro antes de gravar
leva o pico a 4.137 MiB, acima dos 4Gi do pod.

A staging é `append` e o nome do arquivo é fixo por ano, então o envio de cada mês substitui o
arquivo do ano no bucket. Com um nome diferente, os dois arquivos ficariam na mesma partição.

O modelo `microdados` é incremental, com `merge` em `data`, `hora` e `id_estacao`, e lê da staging
só as linhas a partir da maior `data` já carregada.

## BD Pro

O `microdados` é `PartBdpro` (`COVERAGE`, no `constants.py`): os meses mais recentes ficam
restritos a assinantes. O `pre_hook` do modelo apaga as row access policies a cada `dbt run`, e
elas só voltam quando o pipeline registra a materialização. Um `dbt run` feito à mão em prod deixa
a tabela sem as políticas até o próximo run do pipeline.

## Referências

- Dados históricos do INMET: <https://portal.inmet.gov.br/dadoshistoricos/>
