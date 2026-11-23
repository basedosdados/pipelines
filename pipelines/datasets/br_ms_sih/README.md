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

A UF da partição sai do nome do arquivo, e o FTP tem um arquivo com nome em minúsculo:
`sppa1001.dbc`, o grupo SP do Pará em 2010-01. Por isso o `year_month_sigla_uf_parser`
passa a UF para maiúsculo, e o modelo da `servicos_profissionais` aplica `upper()` em
`sigla_uf`. Sem isso, esse mês entrava na tabela com `sigla_uf = 'pa'`.

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
`incremental`, com `incremental_strategy="insert_overwrite"` e partição mensal na coluna
`data_competencia`, o primeiro dia do mês da competência. Filtro em `ano` ou `mes` não poda
partição; só filtro em `data_competencia`.

Cada `dbt run` refaz por inteiro, a partir da staging, as partições da penúltima
competência da tabela em diante, e deixa as anteriores como estão. Por isso rodar de novo
não faz o mesmo mês entrar duas vezes. Quando o dbt roda depois de um `extract_and_load`
com competência nova, a tabela ainda não a tem, e o run refaz as 3 competências que o flow
acabou de recarregar. Se `RELOAD_WINDOW_MONTHS` mudar, o `interval 1 month` do filtro dos
dois modelos tem de mudar junto (ele vale `RELOAD_WINDOW_MONTHS` − 2). Com
`--vars '{"br_ms_sih_most_recent_competencia": "2024-03"}'`, o run refaz de 2024-03 em
diante.

Dois detalhes do filtro reduzem o custo de cada run:

- A penúltima competência vem dos metadados de partição (`INFORMATION_SCHEMA.PARTITIONS`),
  que não custam quase nada. Um `select max(data_competencia)` leria a coluna inteira.
- O filtro compara `ano` e `mes` da staging com valores fixos
  (`ano > '2026' or (ano = '2026' and mes in ('7', …, '12'))`), e não com uma subconsulta.
  Assim o BigQuery lê na staging só as pastas `ano=…/mes=…` dessas competências. Com
  subconsulta, ele lê a staging inteira.

O teste `custom_uf_month_completeness` (`tests-dbt/generic/`), aplicado às duas tabelas,
falha quando uma UF tem zero linhas num mês, ou menos de 50% da mediana dos 12 meses
anteriores da mesma UF. Ele olha os últimos 24 meses e ignora as 2 competências mais
recentes da tabela. Com a janela de 3, cada competência é baixada três vezes (quando sai
e nos dois runs seguintes) antes de o teste olhar para ela. Se a UF continuar incompleta,
o `dbt test` de dev do `build_and_promote` falha, nada é promovido para prod, e o run se
repete a cada dia com a mesma falha até alguém rodar o backfill.

Os outros testes das duas tabelas olham só as 3 competências mais recentes
(`where: __most_recent_months__(data_competencia, 3)`, em
`macros/custom_get_where_subquery.sql`), que são as que o run acabou de refazer. A
competência mais recente vem de `INFORMATION_SCHEMA.PARTITIONS`, e o corte em
`data_competencia` poda as partições, então o teste não lê a tabela inteira a cada run. Um
problema em competência mais antiga só aparece numa consulta à parte.

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
2. **Competência antiga.** O `build_and_promote` roda o dbt sem `--vars`, então uma
   competência anterior à penúltima da tabela não chega a ela. Depois do backfill, rode o
   dbt em prod pelo deployment `run_dbt_model_flow` (flow "BD template: Executa DBT model")
   com `--vars '{"br_ms_sih_most_recent_competencia": "AAAA-MM"}'` em `flags`, usando a
   competência mais antiga do backfill.
