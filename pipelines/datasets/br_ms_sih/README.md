# Documentação do Conjunto de Dados: SIH (Sistema de Informações Hospitalares do SUS)

Este documento registra o contexto e as decisões do conjunto `br_ms_sih`, para quem for
mantê-lo depois.

---

## Sobre o Sistema

O SIH registra as internações hospitalares pagas pelo SUS. O DATASUS publica os dados
mensalmente no FTP público, em arquivos `.dbc` por grupo e por UF, sob
`dissemin/publicos/SIHSUS/200801_/Dados/`.

| Tabela | Grupo no FTP |
|---|---|
| `aihs_reduzidas` | `RD` |
| `servicos_profissionais` | `SP` |

O nome do arquivo traz o grupo, a UF e a competência no formato `AAMM`: `RDSP2507.dbc` é
o grupo RD de SP em 2025-07.

## Estrutura no repositório

| Arquivo | Papel |
|---|---|
| `pipelines/datasets/br_ms_sih/flows.py` | Flows e agendamentos das duas tabelas |
| `pipelines/datasets/br_ms_sih/tasks.py` | Listagem do FTP, download e conversão para parquet |
| `pipelines/datasets/br_ms_sih/constants.py` | Ids das tabelas e `RELOAD_WINDOW_MONTHS`, o tamanho da janela de recarga |
| `pipelines/shared/datasus/tasks.py` | Tasks de FTP, descompressão e conversão, compartilhadas com o SIA e o SINAN |
| `models/br_ms_sih/` | Modelos dbt das duas tabelas e do `dicionario` |

## Nome dos arquivos parquet

A conversão para parquet é feita por `read_dbf_save_parquet_chunks`, função
compartilhada pelo SIH, pelo SIA e pelo SINAN. O `tasks.py` do conjunto passa
`name_by_source_file=True`, então cada parquet leva o nome do arquivo do FTP de onde
saiu:

```text
ano=2025/mes=7/sigla_uf=SP/aihs_reduzidas_RDSP2507_0.parquet
```

O número final é o pedaço do arquivo: a conversão grava de 100 mil em 100 mil linhas.

A staging é carregada com `dump_mode="append"`, que só acrescenta arquivos e nunca apaga.
Com o nome tirado do arquivo do FTP, recarregar um mês sobrescreve os parquets que já
estão lá. Se o nome dependesse da posição do arquivo na lista do FTP, como no padrão
antigo, uma lista diferente para o mesmo mês geraria nomes novos, e o mês ficaria em
dobro na staging.

O nome pelo arquivo não cobre dois casos, em que os arquivos antigos continuam na staging
ao lado dos novos:

- **UF republicada com outro nome de arquivo**: os parquets do nome anterior
  continuam na staging.
- **Mês no padrão antigo**: os meses carregados antes desta convenção estão como
  `<tabela>_<posição>_<pedaço>.parquet`.

Na staging de dev, o flow apaga a partição de cada arquivo antes de subir os novos. Na
staging de prod, a pasta da partição precisa ser apagada à mão antes da recarga (ver
"Cuidados em prod", abaixo).

## Recarga dos meses recentes e backfill

O `check_update` só dispara o `extract_and_load` quando aparece no FTP uma competência
nova, a seguinte à última da tabela. Nesse caso, o `extract_and_load` carrega uma janela
de 3 competências que termina na nova: a nova e as 2 anteriores. O tamanho da janela é
`RELOAD_WINDOW_MONTHS`, em `constants.py`. O DATASUS publica um arquivo preliminar por UF
e competência, publica UFs atrasadas depois das demais e reescreve os arquivos depois.
Baixar de novo as 2 competências anteriores traz o que foi completado nesse meio-tempo. O
que for reescrito depois de sair da janela só entra por backfill.

Para cada arquivo carregado, o flow apaga a partição (`ano=…/mes=…/sigla_uf=…`) na
staging de dev (`gs://basedosdados-dev/staging/br_ms_sih/<tabela>/`) antes de subir os
novos.

Os modelos `br_ms_sih__aihs_reduzidas` e `br_ms_sih__servicos_profissionais` são
`incremental`, com `incremental_strategy="insert_overwrite"` e partição por `ano`. Cada
`dbt run` refaz por inteiro, a partir da staging, as partições dos anos `>= max(ano) - 1`
e deixa as anteriores como estão. Por isso rodar de novo não faz o mesmo mês entrar duas
vezes. Com `--vars '{br_ms_sih_min_ano: 2024}'`, o run refaz os anos `>= 2024`.

São dois anos porque há competências reescritas no FTP até cerca de um ano depois de
publicadas: em janeiro, o ano anterior ainda precisa ser refeito. A janela de recarga de 3
competências sempre cai dentro desses dois anos.

Dois detalhes do filtro reduzem o custo de cada run:

- O `max(ano)` vem dos metadados de partição (`INFORMATION_SCHEMA.PARTITIONS`), que não
  custam quase nada. Um `select max(ano)` leria a coluna inteira, cerca de 20 GB em
  `servicos_profissionais`.
- O filtro compara `ano` com um valor fixo (`ano >= '2025'`), e não com uma subconsulta.
  Assim o BigQuery lê na staging só as pastas `ano=…` desses anos. Com subconsulta, ele lê
  a staging inteira.

O teste `custom_uf_month_completeness` (`tests-dbt/generic/`), aplicado às duas tabelas,
falha quando uma UF tem zero linhas num mês, ou menos de 50% da mediana dos 12 meses
anteriores da mesma UF. Ele olha os últimos 24 meses e ignora as 2 competências mais
recentes da tabela. Com a janela de 3, cada competência é baixada três vezes (quando sai
e nos dois runs seguintes) antes de o teste olhar para ela. Se a UF continuar incompleta,
o `dbt test` de dev do `build_and_promote` falha, nada é promovido para prod, e o run se
repete a cada dia com a mesma falha até alguém rodar o backfill.

O teste de unicidade da `aihs_reduzidas` (`dbt_utils.unique_combination_of_columns`) olha só
`ano >= extract(year from current_date()) - 2`, para não ler a tabela inteira a cada run.
Esse corte cobre os anos que o run reescreve, até em janeiro, quando o ano mais recente da
tabela ainda é o anterior. Uma duplicata em ano mais antigo só aparece numa consulta à parte.

### Backfill

Para recarregar competências antigas, dispare à mão o deployment do `extract_and_load` da
tabela, `br_ms_sih_aihs_reduzidas_download` ou `br_ms_sih_servicos_profissionais_download`
(em dev, com o prefixo `dev-`), com o parâmetro:

```json
{"download_params": {"year_months": "2403 2404 2405"}}
```

Cada competência pode vir como `"2403"`, `"202403"` ou `"2024-03"`, separadas por espaço
ou vírgula. Com `year_months`, a janela não é usada: o flow carrega só as competências
pedidas, de todas as UFs, com a mesma limpeza da staging de dev, e depois o
`build_and_promote` roda como num run normal.

### Cuidados em prod (run normal e backfill)

1. **Mês em dobro na staging de prod.** A promoção copia os arquivos das pastas de
   partição da staging de dev para a de prod (`gs://basedosdados/staging/br_ms_sih/<tabela>/`)
   sem apagar o que já está lá: arquivo de mesmo nome é sobrescrito, os outros ficam.
   Sobram arquivos velhos, e o mês fica em dobro em prod, em três casos: (a) mês carregado
   no padrão antigo de nomes; (b) UF republicada com outro nome de arquivo; (c) arquivo
   reescrito que gera menos pedaços que a versão anterior, cujos pedaços a mais continuam
   lá. Em (a) e (b), apague à mão a pasta da partição em prod antes da recarga. O caso (c)
   pode acontecer num run normal da janela.
2. **Ano antigo.** O `build_and_promote` roda o dbt sem `--vars`, então uma competência de
   ano anterior a `max(ano) - 1` não chega à tabela. Depois do backfill, rode o dbt em
   prod pelo deployment `run_dbt_model_flow` (flow "BD template: Executa DBT model") com
   `--vars '{br_ms_sih_min_ano: AAAA}'` em `flags`. Esse run executa o `pre_hook` dos dois
   modelos, que apaga todas as row access policies da tabela. Elas só voltam com o
   registro da materialização (`register_table_materialization_task`, no fim do
   `build_and_promote` de prod). Até lá, a parte paga da tabela fica aberta.
