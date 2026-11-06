# Checagem de schema no `upload_to_gcs`

> **Estado:** implementado e testado (unitário, mocks e GCP de dev). **Pendente:** validar o evento `SchemaWarning` no Prefect e criar a Automation (§8). Melhorias em aberto no §10.

## 1. Objetivo

Ao subir dados para o GCS via `upload_to_gcs`, comparar as colunas do arquivo com o schema da tabela externa de staging no BigQuery. O resultado é **warning** (segue e avisa) ou **erro** (aborta antes de tocar na staging ou no bucket). Os dois chegam à equipe de dados no Discord via Automation do Prefect.

## 2. Contexto: o que a lib `basedosdados` faz

- **Tipos inferidos: sempre `STRING`.** `_get_columns_from_data` devolve `STRING` para todas as colunas, em CSV e Parquet. O schema inferido pela lib não serve para checar tipo.
- **CSV:** `tb.create` monta `ExternalConfig("CSV")` com `schema` explícito e `autodetect=False`. A tabela é posicional e toda `STRING`.
- **Parquet:** o `ExternalConfig("PARQUET")` não recebe schema; o BigQuery o deriva do `header.parquet` que o `dump_header` sobe antes, com `.astype(str)`. **A staging Parquet é sempre all-STRING**, e `get_table().schema` reflete o header, não os arquivos de dados. Por convenção do pipeline, os arquivos de dados também são all-STRING (o dbt faz `safe_cast`).
- **`allow_jagged_rows=False`** (default de `Table.create`): um CSV com menos colunas que o schema faz o BigQuery recusar a leitura ("too few columns").
- Os chamadores da task (`stage_dispatch.py`, `materialize_prod/flows.py`) ignoram o retorno.

## 3. Regras de classificação

O schema de referência é a staging atual. O **schema final esperado** é a staging mais as colunas novas, no fim.

| Situação | CSV | Parquet |
|---|---|---|
| Sem mudanças | segue | segue |
| a1: coluna a mais | **warning** se estiver no fim do arquivo; no meio, **erro** (desalinha) | **warning** |
| a2: coluna faltando | **erro** (D1), no fim ou no meio | **erro** (D1) |
| a3: ordem diferente | **erro** | warning informativo (casa por nome) |
| a4: tipo incompatível | não se aplica (tudo `STRING`) | **erro** |
| a5: layout de partição diferente | **erro** | **erro** |

No `overwrite`, todos os erros viram warning (ADR-6).

- **CSV (a1 + a2 + a3):** o header tem que ser prefixo do schema final, posição por posição. Só a coluna nova **no fim** vira warning.
- **Renomeação** aparece como a1 + a2 (uma some, outra surge): é erro no `append` e warning no `overwrite`.
- **Parquet, a4:** aliases normalizados (`INTEGER`=`INT64`, `FLOAT`=`FLOAT64`, `BOOLEAN`=`BOOL`) e igualdade estrita entre o tipo do arquivo (pyarrow) e o da staging. Na prática dispara quando o arquivo viola a convenção all-STRING (ADR-3).
- **Nomes:** comparados normalizados com `bq_safe_column_name`, dos dois lados.

## 4. Módulo `pipelines/utils/schema_check.py`

De baixo para cima:

- **Modelo:** `SourceFormat` e `Severity` (`StrEnum`); `Finding(severity, message)`; `Column` (nome e tipo normalizados; `data_type=None` quando desconhecido, como em CSV); `TableSchema(columns, partition_keys)`. `columns` **não inclui** as chaves de partição; `partition_keys` é `()` sem partição e `None` quando não foi possível ler.
- **Leitura dos schemas:**
  - `UploadSchema.from_files(data_path, source_format)`: lê o primeiro arquivo. CSV: header (`pd.read_csv(nrows=0)`); Parquet: nomes e tipos (pyarrow → BigQuery). Chaves de partição das pastas `chave=valor` relativas a `data_path`.
  - `StagingSchema.from_table(table)`: lê `table.schema`, tira as chaves de partição (que o BigQuery inclui no fim) e as lê de `partitionDefinition` (ADR-9).
- **Fatos:** `SchemaDiff.between(staging, upload)` calcula colunas novas, faltando, ordem, tipos diferentes e layout de partição, sem juízo de severidade.
- **Regras:** `_csv_findings`, `_parquet_findings`, `_layout_findings`; `_for_overwrite` rebaixa erros a warning.
- **Resultado:** `SchemaCheckResult` (`findings`, `added_columns`, `warnings`, `errors`, `render()`, `raise_if_errors()`).
- **Entrada única:** `check(staging, upload, source_format, overwrite)`.

## 5. Fluxo na task

```
_upload_to_gcs
 ├─ append + staging existe
 │    ├─ _check_staging: StagingSchema.from_table + UploadSchema.from_files + check
 │    ├─ errors ──► raise SchemaMismatchError            (nada é alterado)
 │    ├─ _sync_staging_schema(result.added_columns)      # só se não houve erro
 │    └─ st.upload
 ├─ append + staging não existe → cria, sem comparação (primeiro upload)
 └─ overwrite                   → §6
```

- `_upload_to_gcs` retorna o `SchemaCheckResult` (ou `None`).
- Com `warnings`, a task `upload_to_gcs` faz `log(..., "warning")` e retorna `Completed(name="SchemaWarning", message=<texto>)`. O estado continua `COMPLETED`.
- **Retry:** `SchemaMismatchError` é tipo próprio e fica fora do retry (`retry_condition_fn`, ADR-7). Qualquer outra falha, inclusive da checagem, é retentada (ADR-8).
- **Fail-closed (ADR-8):** se a checagem não consegue rodar (ex.: erro de API do BigQuery), o upload é abortado.
- `_sync_staging_schema` recebe `added_columns` (nomes normalizados, com o tipo do arquivo; em CSV, `STRING`) e altera a tabela no lugar com `update_table(["schema"])`. É só aditivo: nunca remove nem reordena, e não toca no prefixo do GCS.

## 6. Caso `dump_mode="overwrite"`

A staging e o prefixo são recriados do zero a partir do arquivo novo: não há histórico para desalinhar. A checagem **não bloqueia**, mas avisa, porque o dbt a jusante pode quebrar (coluna renomeada ou removida).

1. **Antes** de `tb.delete`/`st.delete_table`, ler o schema da staging atual (se existir).
2. Comparar com o arquivo novo pelas regras do §3.
3. Todas as divergências viram **warning**; nenhuma levanta `SchemaMismatchError`.
4. A mensagem diz que é um overwrite e que a staging foi substituída ("Revisar modelos dbt dependentes").

## 7. ADRs

Formato: contexto, opções, decisão, consequências.

### ADR-1: onde ficam as regras de schema do upload

- **Contexto:** o `TableSchemaValidator` (`schema_validator.py`) compara só conjuntos de nomes e levanta `ValueError` para qualquer diferença. Precisamos de ordem, tipo, partição e severidades. Seu único uso é `crawler/bcb/utils.py` (4 chamadas).
- **Opções:**
  1. Usar como está e capturar o `ValueError`. Não distingue coluna a mais de faltando, não vê ordem nem tipo, e a exceção carrega só texto.
  2. Estender o validador no mesmo arquivo. Foi a primeira tentativa, mas o código novo não reutilizava nada do antigo; só misturava dois contratos.
  3. Módulo novo (`schema_check.py`), deixando o `schema_validator.py` intacto.
- **Decisão:** opção 3.
- **Consequências:** o `bcb` não entra no diff e não corre risco de regressão. Cada módulo tem um contrato. A unificação dos dois fica fora do escopo.

### ADR-2: ordem das colunas (a3)

- **Contexto:** a tabela de CSV é posicional, então ordem trocada coloca dados na coluna errada sem erro. Em Parquet o BigQuery casa por nome.
- **Opções:** (1) ignorar; (2) warning sempre; (3) erro sempre; (4) erro em CSV e warning em Parquet; (5) reordenar o arquivo automaticamente.
- **Decisão:** opção 4. A opção 5 fica de fora porque altera o dado enviado ao bucket.
- **Consequências:** a regra de CSV é "header igual ao schema final esperado, posição por posição", o que também pega coluna nova ou faltando no meio.

### ADR-3: tipo das colunas (a4)

- **Contexto:** a staging Parquet é all-STRING (§2) e os arquivos devem ser all-STRING. O BigQuery **não converte** tipos na leitura: um arquivo `INT64` numa staging `STRING` falha com `Got non-null value in column n of parquet type INT64. Parquet column requires appropriate ConvertedType to load` (teste `test_bigquery_recusa_ler_arquivo_tipado_em_staging_string`). O caso inverso está documentado em `models/us_dot_fars/code/upload.py`.
- **Opções:**
  1. Comparar o schema inferido pela lib: inútil, sempre `STRING`.
  2. Checar tipo também em CSV: sem sentido, pelo mesmo motivo.
  3. Parquet: ler o tipo do arquivo (pyarrow) e comparar com `get_table().schema`, com igualdade estrita após normalizar aliases.
  4. Igual à 3, com lista de conversões permitidas.
  5. Deixar tipo fora da v1.
  6. Aceitar qualquer tipo de arquivo quando a staging é `STRING`: **descartada**, esconderia um erro de leitura real.
- **Decisão:** opção 3, sem lista de conversões.
- **Consequências:** tipo incompatível em Parquet é erro; na prática, dispara quando o arquivo viola a convenção all-STRING, e o erro aparece antes do upload em vez de no dbt. CSV não tem checagem de tipo.

### ADR-4: coluna a mais (a1)

- **Contexto:** o `_sync_staging_schema` já acrescentava colunas novas ao fim do schema, sem avisar.
- **Opções:** (1) erro; (2) acrescentar ao fim e avisar; (3) acrescentar sem avisar.
- **Decisão:** opção 2.
- **Consequências:** em CSV, só vale se a coluna estiver no fim do arquivo; no meio, o arquivo desalinha da tabela e vira erro de ordem (ADR-2). A checagem roda antes do sync.

### ADR-5: coluna faltando (a2) — D1

- **Contexto:** com `allow_jagged_rows=False`, um CSV com menos colunas pode ser recusado pelo BigQuery; coluna faltando no meio desloca os dados seguintes sem erro. Em Parquet a coluna seria lida como NULL.
- **Opções:**
  1. Erro sempre. Chegou a ser adotada e foi revertida.
  2. Warning sempre.
  3. Warning, exceto CSV com coluna faltando no meio, que é erro de ordem.
  4. Warning e habilitar `allow_jagged_rows=True` nas staging de CSV.
- **Decisão:** opção 1, erro sempre em `append` (CSV e Parquet). No `overwrite`, só warning (ADR-6).
- **Consequências:** a fonte que deixar de trazer uma coluna precisa de um `overwrite` ou de ajuste deliberado da staging. Habilitar `allow_jagged_rows=True` fica como evolução, se o erro de coluna faltando no fim ficar frequente.

### ADR-6: `dump_mode="overwrite"` — D2

- **Contexto:** a staging é recriada do zero. Sem checagem, a mudança de schema passa em silêncio e pode quebrar o dbt.
- **Opções:** (1) não checar; (2) aplicar as regras do append, com erros; (3) checar e **só avisar**.
- **Decisão:** opção 3, lendo o schema antigo **antes** de `tb.delete`/`st.delete_table`.
- **Consequências:** nunca bloqueia. A mensagem diz que o schema foi substituído e pede revisão dos modelos dbt.

### ADR-7: erro de schema e retries

- **Contexto:** a task tem `retries=3`; repetir não corrige divergência de schema.
- **Opções:** (1) exceção comum, com 3 tentativas inúteis; (2) `SchemaMismatchError` próprio, excluído do retry com `retry_condition_fn`.
- **Decisão:** opção 2.
- **Consequências:** o erro aborta antes de alterar a staging ou o bucket, e o alerta chega na primeira tentativa.

### ADR-8: falha da própria checagem

- **Contexto:** a checagem depende do BigQuery e da leitura do arquivo, que podem falhar.
- **Opções:** (1) fail-closed, abortando o upload; (2) fail-open, com warning no log.
- **Decisão:** opção 1.
- **Consequências:** nada sobe sem validação. Uma instabilidade do BigQuery passa a derrubar flows. A falha usa um erro **diferente** de `SchemaMismatchError`, então mantém os retries normais (pode ser transitória).

### ADR-9: tabelas particionadas (Hive)

- **Contexto:** `to_partitions` grava `ano=2024/mes=01/data.csv` e remove as chaves das colunas do arquivo. Verificado em staging reais e em dev:
  - `table.schema` **inclui as chaves de partição**, no fim. Sem tratamento, a D1 bloquearia todo upload particionado.
  - `table._properties["partitionDefinition"]["partitionedColumn"]` traz as chaves, em ordem (`[{'field': 'ano'}]`). Em tabela não particionada, vale `None`.
  - `external_data_configuration.hive_partitioning` é `None` na não particionada e um objeto na particionada.
- **Opções para obter as chaves da staging:** (1) listar um blob no GCS e ler as pastas `k=v`; (2) ler `partitionDefinition`.
- **Decisão:** opção 2, isolada em `StagingSchema.from_table` (usa o `get_table` que já existe, sem chamada ao storage).
- **Regras:**
  1. As chaves ficam fora da comparação de colunas, dos dois lados.
  2. **Layout (a5):** as chaves do upload (em ordem, lidas do caminho) precisam ser iguais às da staging. Diferença é **erro** no `append` e **warning** no `overwrite`.
  3. `_properties` é privado (o client não expõe `partitionDefinition`). Se a chave faltar mas `hive_partitioning` existir, as chaves ficam **desconhecidas** (`None`): o layout não é verificado e a mensagem traz um warning explícito.
- **Consequências:** valores novos de partição (`ano=2025`) não são mudança de schema. O acesso a um atributo privado pode mudar com a versão do client; o teste de integração fixa o formato atual.

## 8. Automation do Prefect + Discord

**Dois triggers**, um para cada resultado:

1. **Warning:** evento de estado customizado.
   ```json
   {
     "type": "event",
     "match": { "prefect.resource.id": "prefect.task-run.*" },
     "expect": ["prefect.task-run.SchemaWarning"],
     "posture": "Reactive",
     "threshold": 1,
     "within": 0
   }
   ```
2. **Erro:** evento `prefect.task-run.Failed`, filtrado pela task `upload_to_gcs` e pelo erro `SchemaMismatchError`, para não alertar qualquer falha do upload.

Ação: *Send a notification* com um `CustomWebhookNotificationBlock` (`method=POST`, `json_data={"content": "{{body}}"}`, formato do Discord). Corpo em Jinja:
```
{{ event.payload.validated_state.message }}
Flow run: {{ flow_run|ui_url }}
```

## 9. Testes

Três níveis. Rodar: `uv run pytest pipelines/utils/tests/<arquivo>`.

### 9.1 Unitários — `test_schema_check.py` (35)

Sem rede; usam schemas montados à mão e arquivos em `tmp_path`.

| Grupo | O que cobre |
|---|---|
| CSV (8) | sem mudança; coluna nova no fim (warning) e no meio (erro); faltando no fim e no meio (erro); ordem trocada; renomeação; nome normalizado |
| Parquet (7) | coluna nova em qualquer posição (warning, tipo em `added_columns`); faltando; ordem (warning); tipo incompatível; aliases; tipo desconhecido fora da checagem; formato inválido |
| Partição (6 casos) | layout igual; 4 variações de layout diferente (erro); chaves desconhecidas (warning) |
| Overwrite (3) | erro vira warning e `raise_if_errors` não levanta; sem mudança não avisa; nota do dbt |
| Resultado (2) | `raise_if_errors` com título; `render` truncado |
| Leitura (8) | `UploadSchema` em CSV (com e sem partição), Parquet (mapeamento de tipos), pasta vazia; `StagingSchema.from_table` com mock (3 casos) |
| Retry (2) | `SchemaMismatchError` não é retentado; outras falhas são |

### 9.2 Mocks nas bordas — `test_upload_to_gcs.py` (21)

Só `bd.Table`, `bd.Storage` e o client do BigQuery são falsos (`create_autospec`), anexados a um mock pai que registra a **ordem** das chamadas. Arquivos, schemas e regras são reais.

| # | Garantia | Origem |
|---|---|---|
| 1 | `append` com erro de schema levanta `SchemaMismatchError` antes de `update_table`, `st.upload` e qualquer deleção | ADR-7 |
| 2 | `append` com coluna nova: `update_table` roda antes de `st.upload`, com as colunas e tipos de `added_columns` | §5 |
| 3 | `append` sem staging: cria a tabela e não chama `check` | §5 |
| 4 | `overwrite`: o schema é lido antes de `st.delete_table` e `tb.delete`; divergência não bloqueia | §6, ADR-6 |
| 5 | Falha da checagem (erro de API) propaga e não é `SchemaMismatchError` | ADR-8 |
| 6 | Staging some entre `table_exists` e `get_table`: `RuntimeError` | §5 |
| 7 | `dump_mode` inválido: `ValueError`, sem efeito colateral | — |

Também cobre partição (chaves ignoradas na comparação; layout diferente é erro), Parquet (tipo da coluna nova; tipo incompatível), a tradução do resultado na task (`SchemaWarning` ou `None`) e o **retry com o engine do Prefect** (`prefect_test_harness`): `SchemaMismatchError` roda 1 vez, outras falhas rodam 4.

## 10. Funcionalidades futuras e pontos em aberto

### 10.1 Chave de partição no header do arquivo

- **Problema:** as chaves Hive existem só como nome de pasta. Se uma chave aparecer também no header, a tabela teria duas colunas com o mesmo nome, e o BigQuery recusa.

### 10.2 Outros pontos

- **Primeiro upload sem checagem.** Sem staging, não há comparação; um Parquet tipado no primeiro upload passa e a leitura falha depois (ADR-3). 
- **Header e chaves de partição de todos os arquivos**, não só do primeiro. As chaves vêm do caminho (barato, sem abrir arquivo): o conjunto de tuplas tem de ter um único elemento. Em Parquet, `pq.read_schema` lê só o rodapé e é barato. Em CSV exigiria abrir cada arquivo (custo × risco, a decidir).
- **Fallback por blob** para as chaves da staging, se `partitionDefinition` faltar e `hive_partitioning` existir: ler um blob em `source_uri_prefix` (`list_blobs(max_results=1)`). Hoje as chaves ficam `None`, com warning.

