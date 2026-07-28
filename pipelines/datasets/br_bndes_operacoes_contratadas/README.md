# br_bndes_operacoes_contratadas

Conjunto de **operações de financiamento contratadas pelo BNDES**, a partir do
**Portal de Dados Abertos do BNDES** (CKAN). Slug de backend do conjunto:
`operacoes_contratadas`.

Tabelas:

| Tabela | Grão | Pipeline recorrente |
|---|---|---|
| `operacoes_indiretas_automaticas` | operação (forma indireta automática) | ✅ |
| `operacoes_administracao_publica` | operação com ente da Administração Pública Direta | ✅ |
| `operacoes_nao_automaticas` | subcrédito (forma direta e indireta não automática) | ❌ (só modelo dbt) |

## Estrutura (compartilhada)

- **Crawler (Prefect 3):** `pipelines/crawler/bndes/{constants,utils,tasks,flows}.py`. Cada
  tabela com pipeline tem sua própria config (`constants` / `constants_administracao_publica`),
  seu transform + `clean` e seu `_run` em `flows.py`. Funções genéricas (`download_csv`,
  `get_source_last_modified`) são compartilhadas entre as tabelas.
- **Wrapper `@flow` + schedule por tabela:** `pipelines/datasets/br_bndes_operacoes_contratadas/flows.py`
  (cron **semanal**, segunda 06h BRT).
- **Poll deferido** (`poll_source_for_update` + `commit_source_update`): grava o Poll ao detectar
  novidade, mas só comita o Update **depois** de materializar — evita adiantar o Update e travar
  runs futuras se o flow falhar no meio.
- **Staging 100% STRING:** o `clean` grava Parquet todo string; a tipagem fica a cargo do
  `safe_cast` no dbt. (Parquet tipado quebra o upload: `... does not match target STRING_PIECE`.)
  Partição por `ano`.
- **DBT:** `models/br_bndes_operacoes_contratadas/` (um `.sql` por tabela + `schema.yml` único).
- **Observation level = `transaction`** (grão de operação/subcrédito; a BD não tem entidade
  "operação").

## operacoes_indiretas_automaticas

### O que é

Operações de financiamento **contratadas** pelo BNDES na forma **indireta automática** (menor
valor, repassadas por instituições financeiras credenciadas). Grão = **uma operação contratada**
(não há identificador único de operação na fonte). Cobertura nacional, **2002-01 a 2026-05**, ~2,36
milhões de linhas. Não inclui Cartão BNDES nem operações com pessoas físicas (o documento do
cliente é sempre CNPJ).

### Fonte

CSV consolidado do Portal de Dados Abertos do BNDES (CKAN), recurso
`612faa0b-b6be-4b2c-9317-da5dc2c0b901` (`;`-delimitado, cp1252, ~1,1 GB, série inteira em um
arquivo). Sinal de atualização = **`last_modified`** do recurso, via
`GET /api/3/action/resource_show?id=612faa0b-b6be-4b2c-9317-da5dc2c0b901`.

### Decisões de modelagem

- **Partição só por `ano`** (INT64), derivado de `data_contratacao`.
- **`id_municipio` sentinelas → NA:** `"0"` e `"9999999"` (município não informado) viram NA,
  pra não criar FK quebrado contra `br_bd_diretorios_brasil.municipio` (nulo passa no teste).
- **CNAE não vira FK** (classificação própria do BNDES; CNAE 2.2 ≠ `cnae_2` do diretório).
- **`has_sensitive_data = no`** (CNPJ mascarado na origem; varredura confirmou zero CPF).
- **Nome da tabela** — paralelo à irmã `operacoes_nao_automaticas`, sem redundância com o conjunto
  ("Operações Contratadas") e ≤3 palavras (manual de estilo). O nome inicial gerado por IA
  (`operacoes_contratadas_forma_indireta_automatica`) foi ajustado em review.
- DBT sem `unique_combination` (grão-operação sem PK). Os testes de `relationships` são
  escopados a `__most_recent_year__` (tabela grande).

### Limitações conhecidas

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

### Metadados

Registrados **direto em produção** (o backend de dev foi desativado durante a onboarding): no
conjunto existente `operacoes_contratadas`, tabela em status **`under_review`** (aguardando code
review para promover a `published`). Descrições PT/EN/ES, coverage **2002-01 a 2026-05** (ano-mês,
refletindo a atualização mensal da fonte), cloud table em `basedosdados.br_bndes_operacoes_contratadas`.
A raw source (nome = nome da tabela) tem o Update mensal preenchido; o Poll é gravado na 1ª run.

## operacoes_administracao_publica

### O que é

Operações do BNDES com **entes da Administração Pública Direta** (União, Administração
Estadual, Administração Municipal). Grão = uma operação. Cobertura nacional, **1994–2026**,
~4,7 mil linhas (após o filtro CONTRATADA). Fonte: conjunto CKAN
`operacoes-com-entes-da-administracao-publica-direta`, recurso
`ea4e5da3-e586-4225-a460-c5aa09e36100` (~1,17 MB, `;` / cp1252). Sinal de atualização =
`last_modified` do recurso, mensal.

### Decisões de modelagem

- **Filtra só `nivel_atual == 'CONTRATADA'`** (95,1% das linhas). A fonte publica o funil
  inteiro (PERSPECTIVA → C/CONSULTA → EM ANÁLISE → APROVADA → CONTRATADA), mas o conjunto
  é "operações contratadas". O filtro fica isolado numa linha do `clean` e a coluna
  `nivel_atual` **permanece no schema** (constante), pra que remover o filtro depois não
  exija mudança de estrutura.
- **Valores em REAIS, não em milhares — `measurement_unit = BRL`, sem ×1000.** O dicionário
  de dados oficial do BNDES (PDF) descreve `valor_da_operacao_historico_em_reais`,
  `valor_desembolsado_em_reais` e `saldo_a_liberar_atualizado_em_reais` como "em milhares
  de reais", mas isso é **boilerplate errado**: o nome das colunas diz "em reais" e a ordem
  de grandeza confirma reais — a maior operação bruta é 3.605.000.000 (Plano de Mobilidade
  de SP), coerente com **R$3,6 bi**; em milhares seria R$3,6 **trilhões** numa única
  operação (impossível). **Não multiplicar por 1000.** (Decisão Davi + revisão, contra a
  doc oficial.)
- **Geografia pelo diretório:** `sigla_uf` (de `uf`, `-`→NA) e `id_municipio` resolvido no
  dbt por join normalizado (nome+UF) contra `br_bd_diretorios_brasil.municipio` — o nome do
  município **não** é guardado na tabela final (manual de estilo). 98,4% dos municípios
  casam automaticamente; ~8 variantes de grafia/renomeação entram por CASE; `DIVERSOS`/`SEM
  MUNICIPIO`/`-` viram NA. O staging mantém `nome_municipio` (all-string) só para essa
  resolução.
- `saldo_a_liberar` vem tipado como `string` no dicionário, mas é 100% numérico nos dados →
  `FLOAT64`, BRL.
- `has_sensitive_data = no` (entes públicos, sem CPF/CNPJ de pessoa). Cobertura pública →
  `AllFree`, sem paywall BD Pro.

## operacoes_nao_automaticas

Operações contratadas na forma **direta e indireta não automática**; cada contrato pode ter um
ou mais subcréditos e **cada linha é um subcrédito** (grão = subcrédito). Existe como **modelo
dbt** (arquivo `.sql` e entrada no `schema.yml` em `models/br_bndes_operacoes_contratadas/`),
onboardada anteriormente e **fora** do crawler deste repo — **não tem pipeline recorrente** aqui.
Contexto detalhado não documentado neste README.
