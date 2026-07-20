# Documentação do Conjunto de Dados: br_rf_cno (Cadastro Nacional de Obras)

## Sobre o Sistema

O **CNO (Cadastro Nacional de Obras)** é o banco de dados da Secretaria Especial da
Receita Federal do Brasil (RFB) com as informações cadastrais de obras de construção
civil e de seus responsáveis. É pré-requisito para o contribuinte cumprir obrigações
tributárias e obter a certidão de regularidade fiscal da obra.

- **Fonte do download:** um único ZIP (~306 MB) em
  `https://arquivos.receitafederal.gov.br/public.php/dav/files/gn672Ad4CF8N6TK/Dados/Cadastros/CNO/cno.zip`.
- **Data de referência (`data_extracao`):** lida do header `Last-Modified` do `cno.zip` via
  `HEAD` (httpx), em `check_need_for_update` (`pipelines/crawler/rf/tasks.py`). **Não** é
  derivada do conteúdo dos dados. Antes usava `PROPFIND` (WebDAV), abandonado por causa do WAF
  (ver *Acesso à fonte e o WAF*).
- **RawDataSource registrada** aponta para a página do portal de dados abertos
  (`dados.gov.br/dados/conjuntos-dados/cadastro-nacional-de-obras-cno`), com atualização
  diária. O dicionário oficial de dados (SERPRO) descreve os arquivos e códigos.

## Acesso à fonte e o WAF

A fonte (servidor WebDAV ownCloud da Receita) passou a ficar atrás de um **WAF (F5 BIG-IP)**
que interfere no acesso automatizado. Dois efeitos, com os respectivos contornos no código:

- **Bloqueia o método `PROPFIND`** (responde `HTTP 200` + página HTML `"Request Rejected"`).
  Por isso `check_need_for_update` não lista mais o diretório via `PROPFIND`; lê a data pelo
  header `Last-Modified` do `cno.zip` via `HEAD`.
- **Bloqueia User-Agents "crus"** (ex.: o default `python-httpx`). Por isso o download
  (`download_file_async`/`download_chunk` em `pipelines/crawler/rf/utils.py`) envia headers de
  browser (constante `_BROWSER_HEADERS`) tanto no `HEAD` (que lê o `content-length`) quanto nos
  `GET` de range, com `follow_redirects=True`.
- **É errático:** o mesmo endpoint alterna `200`/`404` e pode fazer rate-limit por IP. O `HEAD`
  do download valida o `content-length` e falha com mensagem clara se ele não vier; a task
  `crawl` tem `retries=2` para picos transitórios.

## Estrutura dos Flows

O diretório do dataset (`pipelines/datasets/br_rf_cno/flows.py`) é só um wrapper fino:
cada tabela tem um flow que chama `_run_rf` do módulo compartilhado
`pipelines/crawler/rf/flows.py`. Sequência de `_run_rf`:

1. `check_need_for_update` → data da fonte (header `Last-Modified` via `HEAD`).
2. `poll_source_for_update_task` (poll **deferido**) — grava só o `Poll`, retorna se há
   novidade; **não** grava o `Update`.
3. download → `process_file` (parquet particionado por `data=<data_extracao>`) → upload staging dev.
4. `run_dbt(run/test, target=dev)`.
5. se `materialize_after_dump`: upload staging prod → `run_dbt(run/test, target=prod)`.
6. se `update_metadata`: `register_table_materialization_task` + `commit_source_update_task`
   (grava o `RawDataSource.Update`) — **só depois** da materialização.

**Poll deferido (PR #1637, commit `8b65f1b2`):** o `Update` da fonte só avança depois de a
materialização + testes passarem. Isso evita o "congelamento" do poll eager antigo, mas tem
uma consequência importante — ver *Causa raiz* abaixo.

**Parâmetros default:** `chunksize=100000`, `materialize_after_dump=True`, `dbt_alias=True`,
`update_metadata=True`, `target="prod"`, `force_run=False`.

**Schedules (cron, `America/Sao_Paulo`, dias úteis):** `microdados` 04:05 · `vinculos` 04:15 ·
`areas` 04:25 · `cnaes` 04:35.

## Tabelas e Particularidades

Mapeamento arquivo→tabela (`TABLES_RENAME`/`COLUMNS_RENAME` em `pipelines/crawler/rf/constants.py`):

| Arquivo (ZIP) | Tabela | Publicada? |
|---|---|---|
| `cno.csv` | `microdados` | sim |
| `cno_vinculos.csv` | `vinculos` | sim |
| `cno_areas.csv` | `areas` | sim |
| `cno_cnaes.csv` | `cnaes` | sim |
| `cno_totais.csv` | `totais` | **não** (tem rename, mas sem flow e sem modelo dbt) |
| — | `dicionario` | sim (modelo estático `materialized="table"`) |

Todas as tabelas de dados são **incrementais**, particionadas por `data_extracao` (DATE) —
cada execução acrescenta um snapshot completo do CNO naquela data.

### `br_rf_cno__microdados`
Tabela cadastral básica da obra. Pontos de atenção:
- O campo de origem **`Estado` (Texto, 50)** é renomeado para `sigla_uf`
  (`constants.py:58`). É **texto livre de abrangência mundial**, não uma sigla de UF — a
  fonte legitimamente traz obras no exterior (há `Código/Nome do País`, `Caixa postal no
  exterior`, `CEP somente Brasil`). Por isso o modelo faz `left join` ao diretório
  `br_bd_diretorios_brasil.uf` e nula os valores que não são UF brasileira válida
  (ver *Histórico de Correções* #1).
- `id_municipio` (IBGE 7 dígitos) é derivado por `left join` do `id_municipio_rf` (código
  TOM/RF, 4 dígitos) ao diretório de município.
- `situacao` e `qualificacao_responsavel` têm os zeros à esquerda removidos (`ltrim(...,'0')`)
  e são cobertos pelo `dicionario`. Códigos do dicionário oficial: situação
  01=NULA/02=ATIVA/03=SUSPENSA/14=PARALISADA/15=ENCERRADA.
- A coluna de origem `Código de localização` (→ `id_localizacao`) **não** é selecionada pelo
  modelo (descartada).

### `br_rf_cno__vinculos`
Vínculos (responsáveis) da obra. `data_fim` usa a sentinela `9999-*` para "sem data de fim"
(vínculo em aberto), e há registros com data digitada errada — valores fora do range do
diretório de datas são nulados (ver *Histórico de Correções* #2).

**Duplicatas (PENDENTE — requer aprovação de superior):** ao contrário de `areas`/`microdados`,
`vinculos` **não tem** teste `unique_combination_of_columns`, então duplicatas passam batido —
há **~10.656 linhas 100% duplicadas** na partição mais recente (~2,5%; a própria fonte traz
linhas idênticas). Verificar com `COUNT(*) - COUNT(DISTINCT TO_JSON_STRING(t))`. Resolução
**não executada** (mexe em dado público de prod → depende de OK de um superior):

- **Forward:** `select distinct` no modelo (espelha a `areas`) + teste
  `unique_combination_of_columns` em `[id_cno, id_responsavel, data_inicio, data_fim,
  qualificacao_contribuinte]` (chave verificada: **0 colisões** após o distinct), escopado em
  `__most_recent_date_cno__`. Obs.: o teste só fica verde quando o partition máximo estiver
  limpo (partição nova pós-fix, ou a limpeza de histórico abaixo).
- **Histórico:** **NÃO usar `--full-refresh`** — o staging tem só **126 das 292 partições**, então
  full-refresh reconstruiria do staging e **perderia ~166 partições**. Limpar in-place com
  `CREATE OR REPLACE TABLE \`basedosdados.br_rf_cno.vinculos\` PARTITION BY data_extracao AS
  SELECT DISTINCT * FROM \`basedosdados.br_rf_cno.vinculos\`` (dropa as Row Access Policies →
  reaplicar). Sem isso, só o distinct no modelo limpa os partitions novos; o histórico mantém os dupes.

### `br_rf_cno__areas`
Áreas da obra. **Uma obra (`id_cno`) tem VÁRIAS áreas** (grão = uma área de uma obra, não
uma obra). A chave natural é `[id_cno, categoria, destinacao, tipo_area, tipo_area_complementar]`.
A fonte ainda traz linhas 100% duplicadas (ruído), removidas com `select distinct`
(ver *Histórico de Correções* #3).

### `br_rf_cno__cnaes`
CNAE(s) da obra. Sem particularidade. `cnaes` e `vinculos` são as tabelas **sem** teste
`unique_combination_of_columns`; a `cnaes` nunca foi afetada pelo databug de testes (não tinha
teste de qualidade que reprovasse).

## Modelagem incremental e testes

- Testes rodam só no partition mais recente, via `where: __most_recent_date_cno__`
  (macro `macros/custom_get_where_subquery.sql` → `data_extracao = max(data_extracao)`).
- `run_dbt` (`pipelines/utils/tasks.py`) **trata falha de `dbt test` como fatal** (`raise`):
  se um teste reprova, o flow aborta antes de materializar em prod.
- **Ao validar em dev, use `--full-refresh`.** O modelo é incremental e o staging de dev
  costuma estar parado no mesmo `data_extracao` máximo da tabela; sem `--full-refresh`, o
  filtro `where data > max(...)` não reprocessa nada e o teste roda sobre dado antigo
  (falso negativo). Ex.: `uv run dbt build --select br_rf_cno__areas --full-refresh`.

## Causa raiz do congelamento (jan–jul/2026)

Após a migração Prefect 0→3, com o `dbt test` fatal, **três tabelas passaram a reprovar
testes de qualidade** no partition mais recente, abortando os flows já no `dbt test` de dev
— antes de materializar prod e antes de gravar metadados. Resultado: **todas as tabelas do
conjunto ficaram congeladas em `2026-01-28`** (dev e prod), por ~5,5 meses.

**Divergência de metadados ("data de atualização" × "última extração"):** como o poll é
deferido, o `Poll` (verificação) avança todo dia (mostra data recente no site), mas o
`Update`/coverage só avançam após materializar — o que não acontecia. Além disso, o poll
*eager* antigo (pré-#1637) chegou a adiantar a "última atualização na fonte" para a data do
arquivo mesmo sem materializar (resíduo). **A correção dos testes resolve os dois problemas:**
com os flows voltando a materializar, coverage e `RawDataSource.Update` ressincronizam
sozinhos na primeira execução bem-sucedida em prod.

**Blocker final (partition-date):** mesmo com WAF e testes resolvidos, o prod continuou
congelado porque a pasta de partição era gravada com hora (`data=YYYY-MM-DD 00:00:00`), e
`SAFE_CAST('...00:00:00' AS DATE)=NULL` fazia o filtro incremental nunca inserir — ver
*Histórico de Correções* 2026-07-17.

## Metadados no backend (migração `br_me_cno` → `br_rf_cno`)

No lado dos **dados/pipeline**, o conjunto foi migrado do GCP dataset antigo **`br_me_cno`**
(Ministério da Economia) para **`br_rf_cno`** (Receita Federal), com renome de tabelas
(`microdados_cnae`→`cnaes`, `microdados_vinculo`→`vinculos`). Mas o **backend do site havia
ficado apontando para o `br_me_cno`**, que está **vazio e congelado desde 2021-08-11** — o site
mostrava os metadados mas as *cloud tables* serviam tabelas vazias (usuário copiava o caminho BQ
e pegava 0 linha). A capa mostrava cobertura/tamanho certos porque vêm dos *registros* de
metadado (atualizados pelo flow), não da cloud table.

Reconciliado em **2026-07-17** (via MCP `databasis` + GraphQL, em prod):

- **Cloud tables** das 5 tabelas repontadas para `basedosdados.br_rf_cno.*`.
- **Colunas** alinhadas ao BQ/`schema.yml` (o backend tinha o schema antigo do `br_me_cno`:
  `ni_responsavel`→`id_responsavel`, `cnae_2`→`cnae_2_subclasse`, faltavam `data_extracao`/
  `nome_pais`; descrições velhas em `id_cno`/`id_cno_vinculado`/`nome_empresarial`).
- **`areas` registrada do zero** (não existia no backend): tabela + 8 colunas + cloud table +
  observation level (Obra/Construção) + cobertura BD Pro (free `2024-08-05..2026-01-17`; pro
  `2026-01-18..2026-07-17`, espelhando a `microdados`).
- Verificado: as 5 tabelas com cloud em `br_rf_cno` e conjunto + descrições de coluna idênticos
  ao BQ.

Aprendizados de MCP/backend (para a próxima vez):

- **`check_metadata.py`** compara descrição do **BQ (persist_docs do `schema.yml`)** × **API**,
  por nome de coluna — lê o `br_rf_cno` (do `schema.yml`), não a cloud table. Ele deriva as
  tabelas a validar dos **arquivos de modelo** mudados no PR; num PR que **não toca modelo** (só
  crawler/flow), não acha tabela e **crasha** com `ValueError: No objects to concatenate`. Isso
  **não** é divergência de metadado — não faz sentido a label `check-metadata` em PR de
  crawler/flow.
- **Rename de coluna não existe** no MCP/GraphQL: `update_column`/`CreateUpdateColumn` (mesmo com
  `id`) atualizam descrição/flags mas **ignoram troca de `name`** → renomear = `delete_column` +
  criar de novo. `table`/`bigqueryType` querem **UUID puro** (não `TableNode:uuid`).
  `upload_columns_from_sheet` **cria** (não faz upsert por nome) — duplica se a coluna já existe,
  e não seta `is_partition` (setar depois). `update_column` com `directory_column_name` só
  *preserva* um diretório existente; para *criar* o link, GraphQL com `directoryPrimaryKey`.
- **RAP (Row Access Policy):** o worker de **dev** não tem `bigquery.rowAccessPolicies.setIamPolicy`
  (limitação geral do dev, afeta toda tabela BD Pro com o `pre_hook DROP ALL ROW ACCESS POLICIES`),
  então `dbt run --target dev` falha no pre_hook; o worker de **prod** tem a permissão. Validar
  este tipo de flow no de prod, ou conceder a permissão à SA de dev por tabela.
- Nomes de exibição ainda são da era ME ("Microdados CNAE"/"Microdados Vínculo") — cosmético;
  renomear o **slug** quebraria URLs públicas.

## Histórico de Correções

### 2026-07-14 — destravamento dos testes dbt (branch `fix/br_rf_cno`)
Correções apenas nos modelos dbt (sem tocar em flow), validadas em dev com `--full-refresh`:

1. **`microdados` — `relationships sigla_uf → uf`** (reprovava com 8 linhas): obras no
   exterior (`CHILE`, `CHUBUT`, `BUENO ARIES`) e lixo textual (`estado`, `SÃO PAULO`, `EX`).
   `left join` ao diretório `uf` e `sigla_uf = uf.sigla` (nula o que não casa). `PASS=10`.
2. **`vinculos` — `relationships data_fim → data`** (reprovava com 3 linhas): sentinelas
   `9999-01-31`/`9999-03-30` e o typo `9202-02-14`. `case` que nula `data_fim` fora do range
   do diretório (`0001-01-01`..`5000-12-31`). `PASS=6`.
3. **`areas` — `unique_combination_of_columns`** (reprovava com 834.728): `[id_cno]` não é o
   grão (obra tem N áreas) e há ~18,5k linhas 100% duplicadas. `select distinct` + chave
   trocada para `[id_cno, categoria, destinacao, tipo_area, tipo_area_complementar]`. `PASS=3`.

`cnaes` nunca foi afetada (não tem teste de unicidade). Ao subir para prod, as quatro tabelas
voltam a materializar e os metadados ressincronizam.

### 2026-07-16 — acesso à fonte quebrado pelo WAF (PR #1681)
Com os testes destravados, os flows passaram a falhar no **acesso à fonte** — o WAF (F5) da
Receita bloqueando `PROPFIND` e User-Agents crus (ver *Acesso à fonte e o WAF*). Correções no
crawler compartilhado `rf`:

1. `check_need_for_update`: `PROPFIND` → `HEAD` + header `Last-Modified` (via httpx), reusando o
   mesmo parse de data. Type hint corrigido para `-> date`.
2. `download_file_async`: o `HEAD` passou a mandar `_BROWSER_HEADERS` + `follow_redirects=True`, e
   valida o `content-length` (erro claro se ausente). Headers de browser extraídos para a
   constante `_BROWSER_HEADERS`, reusada pelos GETs de chunk.

### 2026-07-17 — partition-date destravou o prod (branch `fix/br_rf_cno_utils`)
Com WAF e testes resolvidos, o flow rodou end-to-end mas o **prod continuava congelado em
`2026-01-28`**: o `dbt run` logava "OK" mas inseria **0 linhas**. Causa: `process_file`
(`crawler/rf/tasks.py`) fazia `datetime.strptime(...)` (um **datetime**) e `process_chunk`
(`crawler/rf/utils.py`) montava a pasta como `f"data={partition_date}"` → `data=2026-07-17
00:00:00`. No staging (append) **todas** as partições ficavam com `data`="...00:00:00" e
`SAFE_CAST(data AS DATE)=NULL`, então o filtro incremental `where safe_cast(data as date) >
(select max(data_extracao) from {{this}})` nunca inseria. **Fix (data-only):** `.date()` no
`process_file` + `f"data={partition_date:%Y-%m-%d}"` no `process_chunk`. Após o fix, o prod
avançou `2026-01-28`→`2026-07-17` e o `dbt test` de prod passou (a partição nova entra limpa,
com o `left join` de `sigla_uf` já aplicado). Não precisou `--full-refresh` nem mexer em RAP.

### 2026-07-17 — migração de metadados no backend (`br_me_cno` → `br_rf_cno`)
As cloud tables/colunas do backend apontavam para o `br_me_cno` (vazio); reconciliadas para o
`br_rf_cno` e a `areas` registrada do zero. Ver a seção *Metadados no backend* acima.
