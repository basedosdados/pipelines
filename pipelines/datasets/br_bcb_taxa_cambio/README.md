# br_bcb_taxa_cambio

Cotações e paridades de dez moedas contra o real, publicadas pelo Banco Central no
sistema PTAX. São cinco boletins por dia útil e por moeda: abertura, três
intermediários e fechamento.

- **API:** `https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/`
- **Página da fonte:** <https://dadosabertos.bcb.gov.br/dataset/taxas-de-cambio-todos-os-boletins-diarios>
- **Especificação dos campos:** <https://www.bcb.gov.br/conteudo/dadosabertos/BCBDepin/gnastportal-dados-abertostaxas-de-cambio---todos-os-boletins-diarios.pdf>
- **Tabela:** `basedosdados.br_bcb_taxa_cambio.taxa_cambio`
- **Série:** começa em 1984-11-29; o euro entra em 2002-01-02

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

Roda todo dia às 8h de São Paulo.

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

A tabela inteira está atrás do BD Pro — `AllBdpro` no `flows.py`, com uma só
Coverage, marcada `is_closed=True`. Não existe Row Access Policy, porque
`needs_row_access_policy` vale apenas para `PartBdpro`.

### Unidades de medida

`cotacao_compra` e `cotacao_venda` estão em `brl`: a especificação do BCB as define
como `unidade monetária corrente/[moeda]`, e a unidade corrente é o real.

`paridade_compra` e `paridade_venda` ficam sem unidade de propósito. São razões
contra o dólar cuja orientação depende de `tipo_moeda` — `USD/[moeda]` no tipo A,
`[moeda]/USD` no tipo B —, então nenhum valor fixo descreveria a coluna. As três
colunas envolvidas trazem essa observação nos três idiomas.

## Divergência entre as duas staging

O modelo lê staging diferente conforme o target: `dev` lê `basedosdados-dev` e
`prod` lê `basedosdados-staging`. O flow sobe para as duas de forma independente, e
elas não batem em dois anos:

| ano | `basedosdados-dev` | `basedosdados-staging` |
| --- | --- | --- |
| 2023 | 9.280 | 20.470, das quais 12.390 distintas |
| 2024 | 100 | 12.560 |

Esses dois anos respondem por toda a diferença entre os totais, 780.601 contra
804.251. Os demais anos batem linha a linha.

Em prod, o 2023 tem um `data.parquet` de 2023-08-24 convivendo com o `data.csv` de
2023-12-29, e a tabela externa lê os dois. Em dev, o 2023 é um `data.csv` parado em
2023-09-27, e o 2024 nunca foi carregado além de 100 linhas.

Isso importa na hora do merge: o `table-approve` espelha `basedosdados-dev` por cima
de `basedosdados`, apagando o prefixo de destino antes de copiar
(`push_table_to_bq`, em `.github/workflows/scripts/prefect_run_dbt.py`). Enquanto
dev estiver assim, uma PR com essa etiqueta substitui os dois anos de prod pelos de
dev. Corrigir dev com o parâmetro `anos` resolve os dois lados de uma vez, porque o
espelhamento também remove o arquivo órfão.

## Pendências

- [ ] Recarregar 2023 e 2024 em dev, com `anos: [2023, 2024]` e
      `materialize_after_dump: false`.
- [ ] Os anos de 1984 a 1988 têm linhas repetidas em ambas as staging, entre 108 e
      1.465 por ano, e 1993, 1996, 1997, 1999, 2000, 2001, 2003 e 2004 têm entre 9 e
      51. Falta apurar se vêm da fonte ou da carga original.
- [ ] O `Update` da fonte está com `latest` vazio e não há nenhum `Poll`. O flow
      grava os dois, mas só em execução com `update_metadata` e
      `materialize_after_dump` ligados. A entidade `month` desse Update é o valor
      que `upsert_raw_source_update` fixa para qualquer fonte.
- [ ] A Coverage está marcada como paga e o intervalo de datas dentro dela como
      livre. São campos separados e precisam coincidir.
- [ ] Trocar `AllBdpro` por `PartBdpro`. A convenção libera o histórico e cobra só a
      janela recente em tabela que atualiza mensalmente ou mais; no repositório, 63
      flows usam `PartBdpro` e 4 usam `AllBdpro`. A Coverage livre precisa existir
      antes da troca, com o início da série, senão `assert_coverage_topology`
      interrompe a execução — e o pipeline nunca escreve o início da faixa livre.
- [ ] A tabela não tem `auxiliary_files_url`. A especificação dos campos é o
      documento a empacotar.
- [ ] A descrição de `tipo_boletim` lista três tipos; a especificação lista quatro,
      incluindo "Fechamento Interbancário".
- [ ] As descrições de `cotacao_compra` e `cotacao_venda` terminam com ponto, que a
      convenção não usa.
- [ ] O horário é `0 8 * * *`. O minuto 0 concentra execuções no mesmo instante e
      disputa slot do BigQuery.
