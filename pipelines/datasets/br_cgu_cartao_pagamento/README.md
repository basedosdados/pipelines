# br_cgu_cartao_pagamento (Cartão de Pagamento do Governo Federal)

Dados mensais do Cartão de Pagamento do Governo Federal, publicados pela CGU no Portal da Transparência. Este README traz só o que é específico do conjunto. A lógica de checagem da fonte, de escolha do mês e de retry é comum aos conjuntos CGU e está em [`pipelines/shared/cgu/README.md`](../../shared/cgu/README.md). Leia esse antes de mexer no pipeline.

## Tabelas

Cada tabela baixa o próprio ZIP, de forma independente, e tem um par de flows (`check_update` e `extract_and_load`).

| Tabela | Fonte | Cron (America/Sao_Paulo) |
|--------|-------|--------------------------|
| `microdados_governo_federal` | [`cpgf`](https://portaldatransparencia.gov.br/download-de-dados/cpgf/) | 20:00 |
| `microdados_defesa_civil` | [`cpdc`](https://portaldatransparencia.gov.br/download-de-dados/cpdc/) | 20:30 |
| `microdados_compras_centralizadas` | [`cpcc`](https://portaldatransparencia.gov.br/download-de-dados/cpcc/) | 21:00 |

## Particularidades

- **Um ZIP por mês e por tabela.** A URL é `{URL}{ano}{mes:02d}/`, montada por `build_urls`. Não há subsistemas, então a regra "tudo ou nada" entre subsistemas não se aplica: cada mês tem uma URL só.
- **Particionamento** por `ANO_EXTRATO` e `MES_EXTRATO` (`partition_data`, em formato CSV).
- **`dicionario` não faz parte do pipeline.** `br_cgu_cartao_pagamento__dicionario` é uma tabela estática, lida de `br_cgu_cartao_pagamento_staging.dicionario` no dbt. O teste `custom_dictionary_coverage` das três tabelas referencia ela via `ref()`, mas ela não tem entrada em `TABELAS` nem flows. Ver `constants.py`.
- **`RELATIVE_MONTH = 1`**: cada execução carrega um mês, em ordem.

## Onde mexer

| O quê | Onde |
|-------|------|
| IDs das tabelas e `RELATIVE_MONTH` | `constants.py` |
| URLs, pastas de entrada e saída | `TABELAS` em `pipelines/shared/cgu/constants.py` |
| Fiação dos flows e crons | `flows.py` |
| `get_latest_update` e `extract_load_data` | `tasks.py` |

## Mudanças no dicionário

Códigos sem rótulo na fonte entram no `dicionario` com o rótulo `Código <chave>: sem significado nas fontes oficiais (nota da BD)`, conforme o [manual da equipe de dados](https://basedosdados.github.io/manual-equipe-dados/governanca/tratamento-de-dados/como-fazer/como-tratar-codigo-sem-rotulo/). Quando o órgão responder, o rótulo é trocado pelo significado informado.

| Tabela.coluna | Chave | Rótulo | Primeira competência | Linhas | Fontes consultadas | Pedido LAI |
|---------------|-------|--------|----------------------|--------|--------------------|------------|
| `microdados_defesa_civil.transacao` | `PAGAMENTO DCC-LIQUIDACAO TOTAL ANTECIP.` | Código PAGAMENTO DCC-LIQUIDACAO TOTAL ANTECIP.: sem significado nas fontes oficiais (nota da BD) | 2025-09 | 221 | [Dicionário de dados do CPDC](https://portaldatransparencia.gov.br/dicionario-de-dados/cpdc) | Protocolo `00106.028186/2026-21` / issue [2370](https://github.com/orgs/basedosdados/projects/17/views/13?filterQuery=assignee%3Aluizavboas&pane=issue&itemId=319580496&issue=basedosdados%7Cpipelines%7C2370) |
| `microdados_defesa_civil.transacao` | `ESTORNO DE PAGAMENTO - LIQUID.ANT.SAQUE` | Código ESTORNO DE PAGAMENTO - LIQUID.ANT.SAQUE: sem significado nas fontes oficiais (nota da BD) | 2025-09 | 3 | [Dicionário de dados do CPDC](https://portaldatransparencia.gov.br/dicionario-de-dados/cpdc) | Protocolo `00106.028186/2026-21` / issue [2370](https://github.com/orgs/basedosdados/projects/17/views/13?filterQuery=assignee%3Aluizavboas&pane=issue&itemId=319580496&issue=basedosdados%7Cpipelines%7C2370) |
| `microdados_defesa_civil.transacao` | `COMPRA A/V - R$ - REVRS APRES` | Código COMPRA A/V - R$ - REVRS APRES: sem significado nas fontes oficiais (nota da BD) | 2025-09 | 3 | [Dicionário de dados do CPDC](https://portaldatransparencia.gov.br/dicionario-de-dados/cpdc) | Protocolo `00106.028186/2026-21` / issue [2370](https://github.com/orgs/basedosdados/projects/17/views/13?filterQuery=assignee%3Aluizavboas&pane=issue&itemId=319580496&issue=basedosdados%7Cpipelines%7C2370) |

Os três códigos aparecem só nos dados de setembro de 2025.
