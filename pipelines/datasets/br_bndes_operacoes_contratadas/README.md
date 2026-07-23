# br_bndes_operacoes_contratadas — Operações Indiretas Automáticas

Contexto e decisões do pipeline da tabela `operacoes_indiretas_automaticas`
(conjunto `br_bndes_operacoes_contratadas`; slug de backend do conjunto: `operacoes_contratadas`).

## O que é

Operações de financiamento **contratadas** pelo BNDES na forma **indireta automática** (menor
valor, repassadas por instituições financeiras credenciadas). Grão = **uma operação contratada**
(não há identificador único de operação na fonte). Cobertura nacional, **2002-01 a 2026-05**, ~2,36
milhões de linhas. Não inclui Cartão BNDES nem operações com pessoas físicas (o documento do
cliente é sempre CNPJ).

## Fonte

CSV consolidado do **Portal de Dados Abertos do BNDES** (CKAN), recurso
`612faa0b-b6be-4b2c-9317-da5dc2c0b901` (`;`-delimitado, cp1252, ~1,1 GB, série inteira em um
arquivo). Sinal de atualização = **`last_modified`** do recurso, via
`GET /api/3/action/resource_show?id=612faa0b-b6be-4b2c-9317-da5dc2c0b901`.

## Estrutura

- Crawler (Prefect 3): `pipelines/crawler/bndes/{constants,utils,tasks,flows}.py`.
- Wrapper `@flow` + schedule: `pipelines/datasets/br_bndes_operacoes_contratadas/flows.py`
  (cron **semanal**, segunda 06h BRT).
- Poll **deferido** (`poll_source_for_update` + `commit_source_update`): grava o Poll ao
  detectar novidade, mas só comita o Update **depois** de materializar — evita adiantar o
  Update e travar runs futuras se o flow falhar no meio.
- DBT: `models/br_bndes_operacoes_contratadas/` (`.sql` com `safe_cast` por coluna, partição
  `ano`; `schema.yml`).

## Decisões de modelagem

- **Partição só por `ano`** (INT64), derivado de `data_contratacao`.
- **Staging é 100% STRING** — o `clean` grava Parquet todo string; a tipagem fica a cargo do
  `safe_cast` no dbt. (Parquet tipado quebrava o `dbt run`: `INT32 does not match target
  STRING_PIECE`.)
- **`id_municipio` sentinelas → NA:** `"0"` e `"9999999"` (município não informado) viram NA,
  pra não criar FK quebrado contra `br_bd_diretorios_brasil.municipio` (nulo passa no teste).
- **CNAE não vira FK** (classificação própria do BNDES; CNAE 2.2 ≠ `cnae_2` do diretório).
- **`has_sensitive_data = no`** (CNPJ mascarado na origem; varredura confirmou zero CPF).
- **Nome da tabela `operacoes_indiretas_automaticas`** — paralelo à irmã `operacoes_nao_automaticas`,
  sem redundância com o conjunto ("Operações Contratadas") e ≤3 palavras (manual de estilo). O
  nome inicial gerado por IA (`operacoes_contratadas_forma_indireta_automatica`) foi ajustado em review.
- **Observation level = `transaction`** (grão de operação; a BD não tem entidade "operação").
- DBT sem `unique_combination` (grão-operação sem PK). Os testes de `relationships` são
  escopados a `__most_recent_year__` (tabela grande).

## Limitações conhecidas

- **Precisão do poll (data, não datetime).** O `last_modified` do CKAN (com microssegundos) é
  truncado para **data** no poll (`SOURCE_DATE_FORMAT = "%Y-%m-%d"` em `flows.py`). Se a fonte
  publicar **duas vezes no mesmo dia**, a 2ª revisão teria a mesma data da 1ª e seria pulada.
  **Impacto desprezível** com o cron **semanal**: cada run pega a versão mais recente (o CSV é
  regenerado/cumulativo) e o framework de poll compartilhado coerce para data de qualquer
  forma — o fix teria que ser no framework, não aqui. Documentado por indicação de code review.
- **Workspace em `/tmp` fixo.** `INPUT_PATH`/`OUTPUT_PATH` são compartilhados; o `download_csv`
  retoma via `Range` e o `clean` faz `rmtree` do output antes de reescrever. Runs concorrentes
  se atropelariam — mas o flow é semanal/single-run e isso segue a convenção dos outros
  crawlers do repo. O `download_csv` valida o tamanho final (falha alto em vez de corromper
  silenciosamente).
- **Datas inválidas em `data_contratacao`** virariam `ano` nulo e **não** entram no Parquet
  (não dá pra particionar por nulo). Hoje são **0** (CSV == xlsx verificado ao centavo), e o
  `clean` **loga e descarta explicitamente** essas linhas quando ocorrem (não é mais silencioso).

## Metadados

Registrados **direto em produção** (o backend de dev foi desativado durante a onboarding): no
conjunto existente `operacoes_contratadas`, tabela em status **`under_review`** (aguardando code
review para promover a `published`). Descrições PT/EN/ES, coverage **2002-01 a 2026-05** (ano-mês,
refletindo a atualização mensal da fonte), cloud table em `basedosdados.br_bndes_operacoes_contratadas`.
A raw source (nome = nome da tabela) tem o Update mensal preenchido; o Poll é gravado na 1ª run.
