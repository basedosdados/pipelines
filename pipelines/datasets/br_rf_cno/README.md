# Documentação do Conjunto de Dados: br_rf_cno (Cadastro Nacional de Obras)

## Sobre o Sistema

O **CNO (Cadastro Nacional de Obras)** é o banco de dados da Secretaria Especial da
Receita Federal do Brasil (RFB) com as informações cadastrais de obras de construção
civil e de seus responsáveis. É pré-requisito para o contribuinte cumprir obrigações
tributárias e obter a certidão de regularidade fiscal da obra.

- **Fonte do download:** um único ZIP (~306 MB) em
  `https://arquivos.receitafederal.gov.br/public.php/dav/files/gn672Ad4CF8N6TK/Dados/Cadastros/CNO/cno.zip`.
- **Data de referência (`data_extracao`):** obtida por um `PROPFIND` (WebDAV) no diretório
  do CNO — pega o `getlastmodified` mais recente entre os arquivos. **Não** é derivada do
  conteúdo dos dados. Ver `check_need_for_update` em `pipelines/crawler/rf/tasks.py`.
- **RawDataSource registrada** aponta para a página do portal de dados abertos
  (`dados.gov.br/dados/conjuntos-dados/cadastro-nacional-de-obras-cno`), com atualização
  diária. O dicionário oficial de dados (SERPRO) descreve os arquivos e códigos.

## Estrutura dos Flows

O diretório do dataset (`pipelines/datasets/br_rf_cno/flows.py`) é só um wrapper fino:
cada tabela tem um flow que chama `_run_rf` do módulo compartilhado
`pipelines/crawler/rf/flows.py`. Sequência de `_run_rf`:

1. `check_need_for_update` → data da fonte (`getlastmodified`).
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

### `br_rf_cno__areas`
Áreas da obra. **Uma obra (`id_cno`) tem VÁRIAS áreas** (grão = uma área de uma obra, não
uma obra). A chave natural é `[id_cno, categoria, destinacao, tipo_area, tipo_area_complementar]`.
A fonte ainda traz linhas 100% duplicadas (ruído), removidas com `select distinct`
(ver *Histórico de Correções* #3).

### `br_rf_cno__cnaes`
CNAE(s) da obra. Sem particularidade — é a única tabela sem teste de unicidade, por isso
nunca foi afetada pelo databug.

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
