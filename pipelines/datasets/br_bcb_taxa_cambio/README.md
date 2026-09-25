# br_bcb_taxa_cambio

Cotações e paridades de dez moedas contra o real, publicadas pelo Banco Central no
sistema PTAX. São cinco boletins por dia útil e por moeda: abertura, três
intermediários e fechamento.

- **API:** `https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/`
- **Página da fonte:** <https://dadosabertos.bcb.gov.br/dataset/taxas-de-cambio-todos-os-boletins-diarios>
- **Especificação dos campos:** <https://www.bcb.gov.br/conteudo/dadosabertos/BCBDepin/gnastportal-dados-abertostaxas-de-cambio---todos-os-boletins-diarios.pdf>
- **Tabela:** `basedosdados.br_bcb_taxa_cambio.taxa_cambio`
- **Série:** começa em 1984-11-29; o euro entra em 1998-12-31

## Estrutura

A coleta fica separada do flow, em `crawler/`:

```text
pipelines/crawler/bcb_taxa_cambio/
├── constants.py   URLs da API e da planilha de arquitetura
├── utils.py       download, limpeza e escrita particionada
└── tasks.py       @task envolvendo utils

pipelines/datasets/br_bcb_taxa_cambio/
├── flows.py       o @flow: ordem das etapas e horário
└── README.md      este arquivo

models/br_bcb_taxa_cambio/
├── br_bcb_taxa_cambio__taxa_cambio.sql modelo dbt
└── schema.yaml                         testes
```

## Como o flow funciona

Roda todo dia às 8h40 de São Paulo.

Primeiro pergunta ao PTAX qual foi a última data publicada, consultando só o dólar
numa janela de quinze dias — as dez moedas saem no mesmo boletim, então a data do
dólar vale para a tabela. Se essa data não for mais nova que o fim da cobertura
registrada, o flow encerra sem baixar nada. **A execução termina em verde nesse
caso**, então saber se houve ingestão exige ler o log.

Havendo novidade, consulta `/Moedas` e baixa, para cada moeda, o ano corrente
inteiro — de 1º de janeiro até hoje. Não há download incremental.

A saída é particionada por `ano` e sobe com `dump_mode="append"`, primeiro para
`basedosdados-dev` e depois para `basedosdados`. O arquivo gravado tem sempre o
mesmo nome, então a partição do ano corrente é substituída a cada execução. Carregar
a mesma partição com um nome de arquivo diferente soma as linhas em vez de
substituí-las.

O parâmetro `anos` recarrega anos específicos, um de cada vez, e sobe tudo num
upload só no fim — cada ano tem sua própria partição no mesmo diretório. Nesse modo
o flow não consulta a fonte nem grava Poll ou Update, porque os anos pedidos estão
no passado e o poll encerraria a execução antes do download.

### A arquitetura é lida em tempo de execução

`treat_currency_df` chama `apply_architecture_to_dataframe` apontando para uma
planilha do Google, cujo id está em `constants.ARCHITECTURE_URL`. A ordem e os nomes
das colunas vêm de lá, não do repositório: editar a planilha muda o que o flow
grava, sem passar por código, e a planilha indisponível interrompe a execução.

### O modelo dbt

`materialized="table"`, sem particionamento — a tabela é recriada inteira a cada
execução, com cerca de 804 mil linhas e 10 MB.

## Metadados

Dataset de slug `taxa_cambio` sob a organização `bcb`, registrado em prod, com
dataset e tabela em `published`.

Três níveis de observação, cada um ligado à sua coluna: `day` em `data_cotacao`,
`time` em `hora_cotacao` e `other` em `tipo_moeda`. Sem esse vínculo o site mostra
"Não informado".

Uma única fonte original, "Dados Abertos BCB". A tabela precisa continuar com uma
só: `_raw_source_id` levanta erro quando encontra mais de uma, e isso interrompe o
poll.

A janela recente fica atrás do BD Pro e o histórico é livre — `PartBdpro` com
defasagem de seis meses no `flows.py`. São duas Coverages, a livre com
`is_closed=False` e a paga com `is_closed=True`, cada uma com seu intervalo de
datas, também marcado. As faixas não se sobrepõem: a livre termina num dia e a paga
começa no seguinte.

O corte se move sozinho. A cada execução, `register_table_materialization` lê a data
máxima no BigQuery, recalcula o fim da faixa livre, reescreve os dois intervalos e
reemite as Row Access Policies no BigQuery. O modelo dbt não participa disso.

A primeira execução armada é a primeira vez que essas políticas são aplicadas nesta
tabela — até lá ela segue sem nenhuma, apesar de marcada como paga.

### Unidades de medida

`cotacao_compra` e `cotacao_venda` estão em `brl`: a especificação do BCB as define
como `unidade monetária corrente/[moeda]`, e a unidade corrente é o real.

`paridade_compra` e `paridade_venda` ficam sem unidade de propósito. São razões
contra o dólar cuja orientação depende de `tipo_moeda` — `USD/[moeda]` no tipo A,
`[moeda]/USD` no tipo B —, então nenhum valor fixo descreveria a coluna. As três
colunas envolvidas trazem essa observação nos três idiomas.

## Limitações conhecidas

**Linhas repetidas vindas da fonte.** Em 1984–1988, 1993, 1996, 1997, 2000, 2001,
2003 e 2004, a API do PTAX devolve mais de um registro para a mesma data, hora, moeda
e tipo de boletim, de 9 a 1.465 por ano. Parte repete os valores e parte traz
cotações diferentes. A tabela guarda os registros como a fonte publica, e quem
precisa de unicidade deduplica por essa combinação.

**As descrições do BigQuery podem divergir da API.** O `check_metadata` compara os
dois textos sem tolerância, e o lado do BigQuery só é regravado por `dbt run` —
`dbt test` não toca nele. Depois de editar descrição no `schema.yaml` ou na API, é
preciso rodar o modelo antes de abrir PR.
