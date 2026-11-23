# br_cgu_beneficios_cidadao (Benefícios ao Cidadão)

Dados mensais de benefícios sociais, publicados pela CGU no Portal da Transparência. Este README traz só o que é específico do conjunto. A lógica de checagem da fonte, de escolha do mês e de retry é comum aos conjuntos CGU e está em [`pipelines/shared/cgu/README.md`](../../shared/cgu/README.md). Leia esse antes de mexer no pipeline.

## Tabelas

Cada tabela tem URL e ZIP próprios, e um par de flows (`check_update` e `extract_and_load`).

| Tabela | Fonte | Cron (America/Sao_Paulo) |
|--------|-------|--------------------------|
| `novo_bolsa_familia` | [`novo-bolsa-familia`](https://portaldatransparencia.gov.br/download-de-dados/novo-bolsa-familia/) | 19:00 |
| `garantia_safra` | [`garantia-safra`](https://portaldatransparencia.gov.br/download-de-dados/garantia-safra/) | 19:15 |
| `bpc` | [`bpc`](https://portaldatransparencia.gov.br/download-de-dados/bpc/) | 19:30 |

Os outros modelos dbt em `models/br_cgu_beneficios_cidadao/` (`auxilio_brasil`, `bolsa_familia_pagamento`, `auxilio_emergencial`) não têm fonte configurada e ficam fora deste pipeline.

## Particularidades

- **Um ZIP por mês e por tabela.** A URL é `{URL}{ano}{mes:02d}/`. No download, a barra final é removida (`rstrip("/")`) e a requisição envia `User-Agent` de Chrome. É o único conjunto em que o `download_file` faz as duas coisas.
- **A fonte não expõe data de última atualização.** Saber se há mês novo exige consultar a URL mês a mês, que é o que `next_available_date` faz.
- **Particionamento** em Parquet, por `read_and_partition_beneficios_cidadao`:
  - `novo_bolsa_familia`: por ano e mês de competência (`coluna1=.../coluna2=...`);
  - `garantia_safra` e `bpc`: por mês (`coluna1=...`).
- **`RELATIVE_MONTH = 1`**: cada execução carrega um mês, em ordem.

## Onde mexer

| O quê | Onde |
|-------|------|
| IDs das tabelas e `RELATIVE_MONTH` | `constants.py` |
| URLs, pastas de entrada e saída | `TABELAS` em `pipelines/shared/cgu/constants.py` |
| Fiação dos flows e crons | `flows.py` |
| `get_latest_update` e `extract_load_data` | `tasks.py` |
| Leitura e particionamento | `read_and_partition_beneficios_cidadao` e `partition_data_beneficios_cidadao` em `pipelines/shared/cgu` |
