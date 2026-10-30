# Documentação do Conjunto de Dados: br_stn_capag (CAPAG)

Capacidade de Pagamento (CAPAG) de municípios e estados, apurada pela Secretaria do
Tesouro Nacional (STN) a partir do Siconfi: três indicadores (endividamento, poupança
corrente e liquidez), a nota de cada um e a nota final.

- Fonte: Tesouro Transparente, API CKAN (`capag-municipios` e `capag-estados`)
- Código: `pipelines/datasets/br_stn_capag/` e `models/br_stn_capag/`

## A fonte

Cada pacote CKAN lista um recurso por arquivo publicado. A API também devolve outros
formatos (o PDF de metadados, por exemplo), e só CSV e XLSX entram (`DATA_FORMATS`).

| pacote | tabela | formato | recursos |
|---|---|---|---|
| `capag-municipios` | `municipios` | XLSX | 2018 a 2026; em 2024, 2025 e 2026 há mais de um recorte por ano |
| `capag-municipios` | `municipios_base_calculo` | XLSX (aba `Datalake`) | os de 2024 em diante (os anteriores não têm a aba) |
| `capag-estados` | `estados` | CSV (`;`, UTF-8 com BOM) | 2018 a 2025, com uma revisão de 2024 |

`municipios_base_calculo` vem da aba `Datalake` do mesmo arquivo de `municipios`: valores
da DCA, RGF e RREO que servem de base ao cálculo dos indicadores, em formato longo (um
valor por município, ano, demonstrativo e conta). A aba só existe nos recursos de 2024
em diante (`MIN_YEAR`), e cada arquivo traz uma janela móvel de anos (ver
[Base de Cálculo](#base-de-cálculo)), então todos são lidos, e o `data_referencia` distingue
as versões do mesmo valor. O cabeçalho de cada coluna junta ano, conta e detalhe numa string só;
`_parse_datalake_header` separa as partes, e os prefixos do RGF/RREO ficam em
`CONTAS_DATALAKE`. Cabeçalho novo que não bate com nenhum formato derruba a execução.

O servidor responde `429` a downloads em sequência: o download espera
`DOWNLOAD_PAUSE_SECONDS` entre arquivos e repete com espera crescente (ou o
`Retry-After`) nos status de `RETRY_STATUS_CODES`.

### Data de referência do recurso

É a data que identifica o recurso, usada para escolher o que baixar e para o
`check_update` comparar com o que já foi carregado (`_resource_reference_date`):

1. a data no fim do nome, quando há (`Capag Municípios 2025 - 10/06/2025` -> `2025-06-10`);
2. senão, 1º de janeiro do ano do nome (`CAPAG Municípios 2023` -> `2023-01-01`).

Como vários recursos podem ter a mesma data, `select_resources` devolve todos os de uma
data, e a unicidade da tabela é `ano` + `data_referencia` + chave.

### Arquivos de revisão

Dois recursos têm "revisão" no nome (`revisao` em `list_resources`): municípios 2022 e
estados 2024. Valem duas regras:

- **Entre arquivos:** num ano com revisão, ela substitui o recurso que só traz o ano
  no nome (`select_resources` tira o segundo). Os dois teriam a mesma
  `data_referencia`, e a chave `ano` + `data_referencia` + id se repetiria.
- **Colunas de revisão:** a planilha de municípios 2022 traz colunas `*_Revisão` ao lado
  das originais, e elas vão para a tabela como `*_revisao` (`COLUNAS_REVISAO`), sem
  sobrepor as originais: `indicador_endividamento_revisao`, `nota_endividamento_revisao`,
  as de poupança corrente e de liquidez e `nota_capag_revisao`. Fora da tabela de municípios 2022,
  ficam nulas. As tabelas de `estados` não possuem colunas de revisão (o arquivo de 2024 é completo).
  O ponto de atenção é a forma de uso dos dados revisados: não há certeza se eles substituem 
  integralmente aqueles não revisados; há também a coluna de "Precisa de Revisão" na origem e valores
  `n.d.` na revisão viram nulo, então não se distingue "sem revisão" de "revisado, sem
  dado". A fonte também traz `CAPAG_Oficial`, que não foram levadas.

Limite: o `append` de uma revisão sobre um ano já carregado duplica a chave no staging, e
o `check_update` não a percebe (a data é a mesma do recurso do ano). Para as duas
revisões que existem, a carga com `historico` resolve.

A STN não documenta a regra de revisão de 2022 (nem o que é `CAPAG_Oficial`), então a
tabela guarda as duas versões e quem consome decide qual usar.

### Achar a tabela dentro do arquivo

Os arquivos não têm um formato só, então a leitura não usa posição fixa:

- **Aba:** a partir de 2024 o resultado fica na aba `Prévia da CAPAG` (a lista
  `PREFERRED_SHEETS` é lida primeiro), e as demais abas (`CAPAG Ano Base 2023`,
  `Datalake`, `DCA_*`, `RGF_*`...) são cálculo de apoio. Antes de 2024 o arquivo tem uma
  aba só, com nome livre (`capag_2018`, `CAPAG-Municípios (4)`, `capag 23 ano base 22`).
- **Cabeçalho:** não está sempre na linha 1. Nos XLSX de 2024 em diante está na linha 3
  (as duas de cima são índices de fórmulas), e no CSV de estados de 2019 está na linha
  2. Em cada aba, as primeiras `HEADER_SCAN_ROWS` linhas são varridas atrás da que
  contém todas as `COLUNAS_OBRIGATORIAS`.
- **Nomes das colunas:** o cabeçalho é normalizado (sem acento, minúsculo, `_`) e casado
  com `COLUNAS_FONTE`, que lista as variações de cada versão:

| coluna | variações na fonte |
|---|---|
| `id_municipio` | `Cod.IBGE`, `Código Município Completo` |
| `sigla_uf` | `UF` |
| `populacao` | `População` (só até 2023) |
| `indicador_endividamento` | `Indicador 1`, `Indicador_1` |
| `nota_endividamento` | `Nota 1`, `Nota_1` |
| `indicador_poupanca_corrente` | `Indicador 2`, `Indicador_2` |
| `nota_poupanca_corrente` | `Nota 2`, `Nota_2` |
| `indicador_liquidez` | `Indicador 3`, `Indicador_3` |
| `nota_liquidez` | `Nota 3`, `Nota_3` |
| `nota_capag` | `Classificação_CAPAG`, `Classificação da CAPAG`, `CAPAG_2022`, `CAPAG_Oficial`, `CAPAG` |

Uma versão nova da fonte com outro nome de coluna faz o tratamento parar com
`cabeçalho não encontrado`; basta acrescentar a variação em `COLUNAS_FONTE`.

### Armadilhas dos arquivos

- **Dimensão da aba errada:** o XLSX de municípios 2021 declara a aba com 1 linha, e o
  `openpyxl` em modo de leitura obedece. `sheet.reset_dimensions()` resolve.
- **Fórmulas:** a aba `Prévia da CAPAG` é toda fórmula; a leitura usa `data_only=True`,
  que traz o último valor calculado pelo Excel.
- **Sem dado:** `n.d.`, `n.e.`, `-` e vazio viram nulo (`NULL_TOKENS`). Em 2024 em
  diante, municípios sem nota final vêm como `n.e.` (a prévia não pôde ser calculada).
- **Percentual:** estados 2019 e 2020 trazem `0,8287` (vírgula decimal), os demais `82,07%`. Tudo vira proporção (`0.8207`), a mesma escala dos municípios.
- **Colunas que não existem:** `populacao` de 2024 em diante, e as notas dos
  indicadores de estados em 2018, ficam nulas.
- **Rodapés:** linhas sem código IBGE (municípios) ou sem sigla de 2 letras (estados)
  são descartadas.
- **Validação:** o tratamento levanta erro com menos de `MIN_ROWS` linhas (5.000 em
  municípios, 26 em estados) ou com chave repetida no recurso.

### Colunas de saída

Em `municipios`: `ano`, `data_referencia`, `sigla_uf`, `id_municipio`, `populacao`,
`indicador_endividamento`, `nota_endividamento`, `indicador_poupanca_corrente`,
`nota_poupanca_corrente`, `indicador_liquidez`, `nota_liquidez`, `nota_capag`, `ano_base` e as sete
colunas `*_revisao`. Em `estados` são as mesmas sem `id_municipio`, `populacao` e as de
revisão. Em `municipios_base_calculo`: `id_municipio`, `ano_base`, `demonstrativo`
(`dca`, `rgf` ou `rreo`), `tipo` (só DCA: `DO`, `RO` ou `RI`), `codigo_conta` (só DCA),
`conta`, `detalhe`, `valor` e `data_referencia`.

`ano` (o ano do nome do recurso) é a partição Hive: o CSV sai em
`tmp/<tabela>/output/ano=AAAA/<nome do recurso>.csv`, sem a coluna `ano`. Cada recurso é
um arquivo, então os vários recortes de um ano convivem na mesma partição. Em
`municipios_base_calculo` o `ano` do arquivo também é a partição Hive, mas ele não vira
coluna do modelo: a tabela final é particionada por `ano_base`.

## Como o flow funciona

Cada tabela (`municipios`, `estados` e `municipios_base_calculo`) tem um par de flows (`CheckThenExtractLoadPipeline`, ver `flows.py`):

1. **`check_update`** lista os recursos no CKAN, sem baixar nada, e compara a maior data
   de referência com a cobertura registrada no backend. Se há recurso mais novo,
   dispara o `_download`.
2. **`_download`** (`extract_and_load`) baixa só o recurso daquela data de referência
   (`download_table(table_id, reference_date)`), trata, escreve o CSV particionado e
   faz o upload para o GCS (staging), no `run_extract_and_load` do framework. Depois
   o `build_and_promote` roda o dbt e promove a partição.

A cobertura é `AllFree`, com `data_referencia` como coluna de data.

`municipios_base_calculo` segue o mesmo par de flows e os mesmos `download_params`, mas só
considera os recursos com aba `Datalake` (2024 em diante): `historico` baixa todos eles, e a
carga normal é `append` do recurso de `reference_date`.

## Parâmetros

O `check_update` não tem parâmetros. O `_download` recebe um `download_params`:

| chave | o que é |
|---|---|
| `reference_date` | data de referência do recurso, como `"2026-09-01"`; o `check_update` preenche, e um disparo manual precisa informar |
| `historico` | opcional, `true` baixa **todos** os recursos e a carga **substitui** a tabela (`overwrite`); sem ele, só o recurso de `reference_date` entra, em `append` |

Em `municipios_base_calculo` os parâmetros valem como acima, restritos aos recursos de 2024
em diante.


## O dbt

`models/br_stn_capag/` tem um modelo por tabela (`br_stn_capag__municipios`,
`br_stn_capag__estados` e `br_stn_capag__municipios_base_calculo`). `municipios` e
`estados` são particionados por `ano` (2018 a 2030); `municipios_base_calculo` é
particionado por `ano_base` (2018 a 2030) e clusterizado por `id_municipio`. O
`schema.yaml` traz:

- `unique_combination_of_columns`: `ano` + `data_referencia` + `id_municipio` (ou
  `sigla_uf`, em estados);
- `relationships` de `id_municipio` com `br_bd_diretorios_brasil__municipio` e de
  `sigla_uf` com `br_bd_diretorios_brasil__uf`;
- `not_null` em `ano`, `data_referencia` e nas chaves.

Em `municipios_base_calculo`, a unicidade é `ano_base` + `data_referencia` + `id_municipio` +
`demonstrativo` + `tipo` + `codigo_conta` + `conta` + `detalhe`, com `accepted_values` em `demonstrativo`
(`dca`, `rgf`, `rreo`) e em `tipo` (`DO`, `RO`, `RI`), `relationships` de `id_municipio`
e `not_null` em `ano_base`, `demonstrativo`, `conta`, `detalhe`, `valor` e `data_referencia`.

## Pendências e decisões em aberto

- **Mais de um recorte por ano:** a tabela guarda todos (`ano` + `data_referencia`).
  Quem quiser só o vigente precisa filtrar a maior `data_referencia` de cada `ano`;
  não há coluna que marque isso.
- **Ano base:** `ano_base` (só municípios) vem de `Ano_Base` (2022 revisão e 2023) ou de
  `Origem da Nota Final` (2024 em diante, texto `CAPAG Ano Base 2023`, lido com regex).
  Municípios 2018 a 2021 e 2022 sem revisão não trazem, e ficam nulos. Estados não tem.

## Base de Cálculo
[Demonstrativos Fiscais](https://www.gov.br/tesouronacional/pt-br/contabilidade-e-custos/uniao/demonstrativos-fiscais)

### Relatório de Gestão Fiscal (RGF)
O Relatório de Gestão Fiscal (RGF) é um dos instrumentos de Transparência da Gestão Fiscal criados pela Lei de Responsabilidade Fiscal (LRF). Especificamente, o RGF objetiva o controle, o monitoramento e a publicidade do cumprimento, por parte dos entes federativos, dos limites estabelecidos pela LRF: Despesas com Pessoal, Dívida Consolidada Líquida, Concessão de Garantias e Contratação de Operações de Crédito. Todos esses limites são definidos em percentuais da Receita Corrente Líquida (RCL), que é apurada em demonstrativo próprio elaborado e publicado pela Secretaria do Tesouro Nacional (STN). 

### Relatório Resumido da Execução Orçamentária (RREO)

O Relatório Resumido da Execução Orçamentária (RREO) é um documento mensal disponibilizado pelo Tesouro Nacional que permite o acompanhamento e análise do desempenho das ações governamentais estabelecidas na Lei de Diretrizes Orçamentárias (LDO) e na Lei Orçamentária Anual (LOA).

O RREO abrange os órgãos da administração direta e entidades da administração indireta de todos os poderes, que recebam recursos dos orçamentos fiscal e da seguridade social, inclusive sob a forma de subvenções para pagamento de pessoal ou de custeio em geral ou de capital, excluídos, no último caso, aqueles provenientes de aumento de participação acionária.

### Declaração de Contas Anuais (DCA)

A Declaração de Contas Anuais (DCA) é o demonstrativo em que os municípios informam ao
Tesouro, pelo Siconfi, o balanço do exercício já encerrado: receitas e despesas
orçamentárias, por conta. Na `Datalake` aparece com os tipos `DO` (despesa orçamentária),
`RO` (receita orçamentária) e `RI` (receita intraorçamentária).

### Qual demonstrativo alimenta qual indicador

| demonstrativo | contas usadas | indicador |
|---|---|---|
| RGF | `Dívida consolidada` e `Receita corrente líquida` (RCL) | Indicador 1, endividamento (dívida consolidada ÷ RCL) |
| DCA | `DO` despesas empenhadas (correntes); `RO` e `RI` receitas brutas realizadas e as deduções (FUNDEB, transferências constitucionais e outras) | Indicador 2, poupança corrente (despesas correntes ÷ receita corrente ajustada) |
| RGF | `Disponibilidade de caixa bruta`, `Disponibilidade de caixa líquida` (por vinculação) e as obrigações financeiras (restos a pagar e demais obrigações) | Indicador 3, liquidez |
| RREO | despesas e receitas correntes (cada uma com a parte intraorçamentária), até o 6º bimestre | nenhum diretamente: nas abas `CAPAG Ano Base` aparece como "RREO revisão" |

### Dicionário para decodificar as colunas

Os arquivos com `Datalake` trazem o dicionário que liga o nome codificado ao nome legível:

- Nas abas `CAPAG Ano Base AAAA`, a linha 3 (`Nome alternativo`) é o cabeçalho cru da `Datalake`
  e a linha 4 é o nome legível (`2023 - Receitas Correntes - Deduções FUNDEB`). A linha 2 é a posição da
  coluna na `Datalake` e a linha 1 é o papel dela no cálculo (`DPC-2`, `RCA-0`, `Caixa Bruto`...).
  Os valores dessas abas são `VLOOKUP` na `Datalake` do mesmo arquivo.
- A aba `ChavesBusca` lista, por aba, indicador e ano, o rótulo e a chave da `Datalake`.

Os formatos do cabeçalho: na DCA, `<ano><DO|RO|RI><código><conta><código> - <grupo>`; no
RGF e no RREO, `<ano><NomeColado><coluna do demonstrativo><linha>`. `_parse_datalake_header`
reconhece todos os cabeçalhos dos arquivos de 2024 a 2026.

### Janela móvel e versões

Cada arquivo traz só os últimos anos de cada demonstrativo, e a janela avança com os
arquivos:

| demonstrativo | mai/2024 | set/2026 |
|---|---|---|
| DCA | 2020 a 2023 | 2021 a 2025 |
| RGF | 2022 e 2023 | 2023 a 2025 |
| RREO | 2023 | 2025 |

Por isso se leem todos os arquivos com `Datalake`: DCA 2020, RGF 2022 e o RREO de 2023 e 2024
só existem nos arquivos mais antigos. O mesmo valor aparece em vários arquivos, em geral
idêntico (96 a 99,7% iguais entre mai/2024 e set/2026, conforme o demonstrativo e o ano), mas a STN
pode corrigi-lo, e os arquivos de início de ano trazem menos municípios (RGF 2024 em fev/2025:
3.767; em jun/2025: 5.414). Cada linha guarda o `data_referencia` do arquivo, e a versão vigente de
um `ano_base` é a de maior `data_referencia` em que o valor aparece. Linhas sem valor na fonte
não são gravadas.

