# br_bcb_taxa_cambio

Cotações e paridades diárias de 10 moedas contra o real, publicadas pelo Banco
Central. Cinco boletins por dia útil e por moeda: abertura, três intermediários e
fechamento.

- **API:** `https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/`
- **Página da fonte:** `https://dadosabertos.bcb.gov.br/dataset/taxas-de-cambio-todos-os-boletins-diarios`
- **Tabela:** `basedosdados.br_bcb_taxa_cambio.taxa_cambio`
- **Série:** 1984-11-28 em diante (o euro só a partir de 2002-01-02)

## Estrutura

O conjunto segue o layout antigo, com o código de coleta separado do flow:

```text
pipelines/crawler/bcb_taxa_cambio/
├── constants.py   URLs da API e da planilha de arquitetura
├── utils.py       download, limpeza e escrita particionada
└── tasks.py       @task envolvendo utils

pipelines/datasets/br_bcb_taxa_cambio/
├── flows.py       o @flow: ordem das etapas + schedule
└── README.md      este arquivo

models/br_bcb_taxa_cambio/
├── br_bcb_taxa_cambio__taxa_cambio.sql modelo dbt
└── schema.yaml                         testes
```

## Como o flow funciona

Agendado para 8h (São Paulo), todo dia.

Cada execução consulta `/Moedas` para descobrir as moedas disponíveis e, para cada
uma, baixa o **ano corrente inteiro** — 1º de janeiro até hoje. Não há download
incremental nem consulta ao que já está carregado.

A saída é particionada por `ano` e sobe com `dump_mode="append"` primeiro para
`basedosdados-dev` e depois para `basedosdados`. Como o arquivo gravado tem sempre o
mesmo nome, a partição do ano corrente é **substituída** a cada execução, não somada.
Carregar a mesma partição com outro nome de arquivo duplicaria as linhas — foi o que
aconteceu com 2023 (ver "Pendências").

O modelo dbt é `materialized="table"`, sem particionamento. A tabela tem ~804 mil
linhas e cerca de 10 MB, então recriá-la inteira a cada execução é barato e
particionar não traria ganho.

### A arquitetura é lida em tempo de execução

`treat_currency_df` chama `apply_architecture_to_dataframe` apontando para uma
planilha do Google (o id está em `constants.ARCHITECTURE_URL`). A ordem e os nomes
das colunas vêm de lá, não do repo: mudar a planilha muda o que o flow grava, sem
alteração de código, e a planilha ficar indisponível quebra a execução.

## Metadados

Registrados em prod, dataset de slug **`taxa_cambio`** sob a organização `bcb`, com
dataset e tabela `published`.

Três grupos de observação, ligados às colunas do grão: `day` → `data_cotacao`,
`time` → `hora_cotacao`, `other` → `tipo_moeda`. Sem esse vínculo o site exibe
"Não informado".

Uma fonte original registrada, "Dados Abertos BCB". A tabela tem que ficar com
**uma só** — `_raw_source_id` levanta erro quando a consulta casa mais de um nó, e
isso quebraria o poll.

A tabela está **inteiramente atrás do BD Pro** (`AllBdpro` no `flows.py`, uma única
Coverage com `is_closed=True`). Não há Row Access Policy: `needs_row_access_policy`
só vale para `PartBdpro`.

## As duas staging divergem

O modelo dbt lê staging diferente conforme o target: `dev` lê
`basedosdados-dev`, `prod` lê `basedosdados-staging`. O flow sobe para as duas de
forma independente, e hoje elas não batem:

| Ano | `basedosdados-dev` | `basedosdados-staging` |
| --- | --- | --- |
| 2023 | 9.280 linhas, sem repetição | 20.470 linhas, 12.390 distintas |

Ou seja: **prod tem 2023 duplicado e dev tem 2023 incompleto**. Rodar o modelo em dev
e conferir o resultado não prova nada sobre prod.

## Pendências

- [ ] **Duplicata de 2023 em prod.** 20.470 linhas para 12.390 combinações distintas
      de data + hora + moeda + boletim. Sobra de recarga com outro nome de arquivo.
      Conserto é limpar o prefixo de 2023 em `gs://basedosdados/staging/` e recarregar
      o ano. Os anos 1984-1988 têm sobra menor e 1993, 1996, 1997, 1999, 2000, 2001,
      2003 e 2004 têm de 9 a 51 linhas — essas podem ser da própria fonte.
- [ ] **2023 incompleto em dev.** 9.280 linhas contra 12.390 distintas em prod.
- [ ] **O flow não consulta a fonte antes de baixar.** Faltam
      `poll_source_for_update_task` e `commit_source_update_task`. Consequências: o
      ano corrente inteiro é rebaixado todo dia mesmo sem novidade, e não há registro
      de quando o BCB publicou nem de quando olhamos — o Update da fonte está com
      `latest` vazio e não existe nenhum Poll.
- [ ] **O Update da fonte está como mensal.** A série é diária; a entidade tem que
      ser `day`.
- [ ] **Cobertura marcada como paga, intervalo de datas dentro dela marcado como
      livre.** Os dois campos são separados e precisam bater (`is_closed=True` nos
      dois).
- [ ] **`AllBdpro` numa tabela diária.** A regra da casa para atualização mensal ou
      mais frequente é liberar o histórico e cobrar só a janela recente
      (`PartBdpro`). No repo, 63 flows usam `PartBdpro` e 4 usam `AllBdpro`. Trocar
      exige criar a Coverage livre antes, senão `assert_coverage_topology` derruba a
      execução.
- [ ] **Colunas sem unidade de medida.** `cotacao_compra`, `cotacao_venda`,
      `paridade_compra` e `paridade_venda` são FLOAT64 sem unidade. As descrições de
      `cotacao_compra` e `cotacao_venda` terminam com ponto, que a convenção não usa.
