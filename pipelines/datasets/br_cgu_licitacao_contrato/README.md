# br_cgu_licitacao_contrato (Licitações e Contratos do Governo Federal)

Dados mensais de licitações e contratos do Governo Federal, publicados pela CGU no Portal da Transparência. Este README traz só o que é específico do conjunto. A lógica de checagem da fonte, de escolha do mês e de retry é comum aos conjuntos CGU e está em [`pipelines/shared/cgu/README.md`](../../shared/cgu/README.md). Leia esse antes de mexer no pipeline.

## Tabelas e fontes

São 7 tabelas, vindas de duas fontes. O ZIP mensal de cada fonte traz os CSVs de várias tabelas do mesmo grupo.

| Fonte | URL | Tabelas |
|-------|-----|---------|
| Licitações | [`licitacoes`](https://portaldatransparencia.gov.br/download-de-dados/licitacoes/) | `licitacao`, `licitacao_participante`, `licitacao_item`, `licitacao_empenho` |
| Compras | [`compras`](https://portaldatransparencia.gov.br/download-de-dados/compras/) | `contrato_compra`, `contrato_item`, `contrato_termo_aditivo` |

## Particularidades

- **Cada tabela baixa o ZIP por conta própria**, mesmo quando outra tabela da mesma fonte já baixou o mesmo arquivo. Não há dependência real de ordem entre elas no dbt (nenhum `ref()` cruzado), então o download repetido é um custo aceito. Cada tabela lê o CSV dela (`READ` em `TABELAS`, por exemplo `_Licitação.csv`).
- **Um ZIP por mês e por fonte.** A URL é `{URL}{ano}{mes:02d}/`. Não há subsistemas, então a regra "tudo ou nada" entre subsistemas não se aplica.
- **Particionamento** por `ano` e `mes` (`partition_data`, em formato CSV).
- **Crons** das tabelas ficam entre 20:15 e 21:00 (America/Sao_Paulo), em horários distintos para escalonar a carga. Ver `flows.py`.
- **`RELATIVE_MONTH = 1`**: cada execução carrega um mês, em ordem.

## Onde mexer

| O quê | Onde |
|-------|------|
| IDs das tabelas e `RELATIVE_MONTH` | `constants.py` |
| URLs, pastas de entrada e saída, CSV de cada tabela | `TABELAS` em `pipelines/shared/cgu/constants.py` |
| Fiação dos flows e crons | `flows.py` |
| `get_latest_update` e `extract_load_data` | `tasks.py` |
