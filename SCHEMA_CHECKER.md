# Plano: checagem de schema no `upload_to_gcs`

> **Estado:** decisões D1 (coluna faltando = erro no append), D2 (overwrite só avisa) e ADR-8 (fail-closed) fechadas. O módulo `pipelines/utils/schema_check.py` foi reestruturado (§4, §11): `TableSchema` com as variações `UploadSchema` e `StagingSchema` e um único ponto de entrada, `check`. `bq_safe_column_name` está em `utils.py`. **Pendente:** integração em `tasks.py` (§5; ainda usa a API antiga e não importa) e testes em `pipelines/utils/tests/`. Particionamento (ADR-9) resolvido com `partitionDefinition`. Em aberto: validação em dev (evento do Prefect, tipos Parquet), chave de partição repetida no header e header de todos os arquivos.

## 1. Objetivo

Ao subir dados para o GCS via `upload_to_gcs`, comparar as colunas do arquivo com o schema da tabela externa de staging no BigQuery. O resultado é classificado em **warning** (segue e avisa) ou **erro** (aborta antes de tocar na staging ou no bucket). Os dois chegariam à equipe de dados no Discord via Automation do Prefect.

## 2. Estado atual

### Pipelines

`pipelines/utils/tasks.py`:
- `upload_to_gcs` (task, `retries=3`) chama `_upload_to_gcs`.
- `append` com staging existente: chama `_sync_staging_schema`, que compara **conjuntos** de nomes normalizados (normalização com `_bq_safe_column_name`) e só **acrescenta** colunas novas ao fim da tabela.
- `overwrite`: apaga a staging e o prefixo (com as funções `tb.delete`, `st.delete_table`) e recria a partir do header do arquivo. Nada é comparado.
- Os chamadores (`stage_dispatch.py:510`, `materialize_prod/flows.py:195`) ignoram o retorno da task.

### Biblioteca basedosdados

Verificado no código da lib `basedosdados` (`basedosdados/upload/`):
- **Tipos inferidos: sempre `STRING`.** `_get_columns_from_data` devolve `{"name": col, "type": "STRING"}` para todas as colunas, em CSV e Parquet, e `header()` só lê nomes. Logo o schema que `_load_staging_schema_from_data` devolve **não serve** para checar tipo.
- **CSV:** `tb.create` monta `ExternalConfig("CSV")` com `schema` explícito e `autodetect=False`. Ou seja, a tabela é posicional e todas as colunas são `STRING`. Tipo não é problema em CSV.
- **Parquet:** o `ExternalConfig("PARQUET")` **não recebe schema**. O BigQuery deriva o schema dos arquivos, então os tipos reais estão em `client.get_table(...).schema`. Para comparar tipo, é preciso ler o schema do próprio arquivo Parquet (pyarrow) e mapear para tipos do BigQuery.
- **`allow_jagged_rows=False`** (default de `Table.create`). Com isso, um CSV com **menos** colunas que o schema faz o BigQuery recusar a leitura ("too few columns"), em vez de preencher NULL.

### Validade schema
Atualmente, há uma implementação simples de uma classe validadora. 

- **`validate_schema` / `TableSchemaValidator`:** único uso é `pipelines/crawler/bcb/utils.py` (4 chamadas, linhas 305, 358, 406, 416), sempre comparando contra colunas "esperadas" por conjunto, com falha dura.

## 3. Regras de classificação

O schema de referência é a staging atual. O **schema final esperado** é a staging atual mais as colunas novas, acrescentadas no fim.

O esquema de decisões de classificação proposto para cada situação de mudança de schema:

| Situação | CSV | Parquet |
|---|---|---|
| Sem mudanças | segue | segue |
| a1: coluna a mais | **warning** se estiver no fim do arquivo. Se estiver no meio, **erro** (pois, desalinha) | **warning** |
| a2: coluna faltando | **erro** (D1), no fim ou no meio do schema (no meio, desloca as colunas seguintes) | **erro** (D1) |
| a3: ordem diferente | **erro** | warning informativo, já que o formato casa as colunas por nome |
| a4: tipo incompatível | não se aplica (tudo `STRING`) | **erro** |

Como cada regra é aplicada:
- **CSV, a1 + a3 + a2 num único teste:** o header do arquivo tem que ser igual ao schema final esperado, posição por posição. Divergência de posição é erro (coluna nova no meio, ordem trocada ou coluna faltando). Vira warning apenas a coluna nova **no fim**.
- **Renomeação de coluna** aparece como a1 + a2 ao mesmo tempo (uma some, outra surge). Como coluna faltando é erro (D1), a renomeação é erro em CSV e em Parquet no `append`; no `overwrite` vira warning. A mensagem lista as duas colunas juntas, para a equipe reconhecer o padrão.
- **Parquet, a4:** normalizar aliases (`INTEGER`=`INT64`, `FLOAT`=`FLOAT64`, `BOOLEAN`=`BOOL`) e exigir igualdade estrita entre o tipo do arquivo (pyarrow) e o da staging (`get_table().schema`). Uma lista de conversões permitidas (por exemplo `INT64→FLOAT64`) só entra se houver falso positivo.
- **Nomes:** comparar sempre normalizados com `bq_safe_column_name`, dos dois lados (`Column.of` faz isso na construção).

## 4. Módulo da checagem

O código novo vive em `pipelines/utils/schema_check.py`. O `schema_validator.py` **não foi alterado** e segue servindo ao crawler do `bcb` (ADR-1).

Estrutura do módulo, de baixo para cima:
- **Modelo:**
  - `SourceFormat` e `Severity` (`StrEnum`); `Finding(severity, message)`.
  - `Column`, que normaliza nome (`bq_safe_column_name`) e tipo (aliases) uma única vez; `data_type` é `None` quando o tipo não é conhecido (CSV no upload).
  - `TableSchema(columns, partition_keys)`: um lado da comparação. `columns` **não inclui** as chaves de partição; `partition_keys` é `()` sem partição e `None` quando não foi possível ler.
- **Leitura dos schemas** (subclasses de `TableSchema`):
  - `UploadSchema.from_files(data_path, source_format)`: lê o primeiro arquivo. Em CSV, o header (`pd.read_csv(nrows=0)`, como o `dump_header`); em Parquet, nomes e tipos (pyarrow → BigQuery, `f.type`). As chaves de partição vêm das pastas `chave=valor` relativas a `data_path`.
  - `StagingSchema.from_table(table)`: lê `table.schema`, tira as chaves de partição (o BigQuery as inclui no fim) e lê as chaves em `partitionDefinition` (ADR-9). `hive_partitioning` presente sem `partitionDefinition` dá `partition_keys=None`.
- **Fatos:** `SchemaDiff.between(staging, upload)` calcula colunas novas, faltando, ordem, tipos diferentes e as chaves de partição dos dois lados (`layout_unknown`, `layout_changed`), sem juízo de severidade.
- **Regras:** `_csv_findings`, `_parquet_findings` (tabela do §3) e `_layout_findings` (a5); `_for_overwrite` rebaixa os erros a warning (§6).
- **Resultado:** `SchemaCheckResult` guarda os `findings` e as `added_columns` (colunas do upload ausentes na staging, com o tipo do arquivo); `warnings` e `errors` são derivados; `render()` trunca a mensagem; `raise_if_errors()` levanta `SchemaMismatchError`.
- **Entrada única:** `check(staging, upload, source_format, overwrite)`, usada pela task e pelos testes.

Ordem do arquivo: modelo → leitura dos schemas → `SchemaDiff` → regras → resultado → `check` e `schema_title`.

## 5. Fluxo na task

> **Estado:** este fluxo descreve o alvo. O `tasks.py` atual ainda usa `LocalDataSchema` e `check_staging_schema`, que não existem mais; a integração com a API nova é a parte 3 da implementação.

```
_upload_to_gcs
 ├─ append + staging existe
 │    ├─ StagingSchema.from_table(get_table) e UploadSchema.from_files
 │    ├─ check(staging, upload, fmt)  ── errors ──► raise SchemaMismatchError   (nada é alterado)
 │    ├─ _sync_staging_schema(result.added_columns)   # só se não houve erro
 │    └─ st.upload
 ├─ append + staging não existe  → cria, sem comparação (primeiro upload)
 └─ overwrite                    → ver §6
```

- `_upload_to_gcs` passa a retornar o resultado da validação (ou `None`).
- A task `upload_to_gcs`, havendo `warnings`: `log(..., "warning")` e `return Completed(name="SchemaWarning", message=<texto>)`. O estado continua `COMPLETED`, então retries e dependências não mudam.
- **Erro sem retry inútil:** `SchemaMismatchError` é um tipo próprio. A task tem `retries=3`, e repetir não resolve divergência de schema. Usar `retry_condition_fn` para não retentar esse tipo.
- **Fail-closed (ADR-8):** falha da própria checagem (ex.: erro de API do BigQuery) aborta o upload. Ela não é `SchemaMismatchError`, então a task a retenta normalmente; só `SchemaMismatchError` fica fora do retry.
- `_sync_staging_schema` passa a receber `result.added_columns` (já normalizadas e tipadas), sem reler o header nem depender do tipo da lib.
- **Tipo da coluna nova:** em Parquet, o tipo lido do arquivo (pyarrow, o mesmo mapeamento do a4); em CSV, `data_type` é `None` e o sync usa `STRING`. Conferir em dev se atualizar `table.schema` de uma externa Parquet se comporta como esperado.
- Um helper da task (`_check_staging`) concentra o bloco repetido em `append` e `overwrite`: lê os dois schemas e chama `check`.
- `bq_safe_column_name` já está em `utils.py` (feito), sem import circular com `schema_check.py`.

## 6. Caso `dump_mode="overwrite"`

Hoje a staging antiga é apagada antes de qualquer leitura, então a mudança de schema passa em silêncio. No overwrite a tabela e o prefixo são **recriados do zero a partir do arquivo novo**: não há histórico para desalinhar e a ordem do CSV define a nova tabela. Por isso a checagem não deve **bloquear** o upload, mas o dbt a jusante pode quebrar (coluna renomeada ou removida) e vale avisar.

Regra proposta:
1. **Antes** do `tb.delete`/`st.delete_table`, ler o schema da staging atual (`get_table`, tolerando `NotFound` e falha de API).
2. Comparar com o header do arquivo novo usando a mesma lógica do §3.
3. **Todas** as divergências viram **warning**, inclusive ordem (a3), coluna faltando (a2) e tipo (a4). Nenhuma levanta `SchemaMismatchError`.
4. A mensagem diz explicitamente que é um overwrite e que o schema da staging **foi substituído**, por exemplo: "Overwrite: staging recriada com o schema do arquivo. Revisar modelos dbt dependentes."

A leitura do schema antigo tem que ficar **antes** da deleção.

## 7. Decisões

- **D1 (decidido): coluna faltando é erro em `append`, em CSV e Parquet.** Em CSV, coluna faltando no meio desloca todas as colunas seguintes (dados na coluna errada, sem erro do BigQuery) e, no fim, o BigQuery pode recusar a leitura das linhas curtas (`allow_jagged_rows=False`, mantido). Em Parquet a coluna seria lida como NULL, mas fica como erro por consistência. Se a fonte tirar uma coluna de propósito, o caminho é um `overwrite`.
  - Habilitar `allow_jagged_rows=True` nas staging de CSV (atualizando o `external_data_configuration`) fica como evolução, caso erro de coluna faltando no fim passe a ser frequente.
- **D2 (decidido): `overwrite` nunca bloqueia, só avisa (§6).** Vale para a1, a2, a3 e a4.

## 8. ADR: registro das decisões e alternativas

Formato: contexto, opções consideradas, decisão e consequências. **Status** "decidido" significa decidido na discussão; "a confirmar" depende de teste em dev.

### ADR-1: onde ficam as regras de schema do upload

- **Contexto:** o `TableSchemaValidator` compara só conjuntos de nomes e levanta `ValueError` para qualquer diferença. Precisamos de ordem, tipo, partição e severidades (warning vs erro). O único uso do validador atual é `crawler/bcb/utils.py` (4 chamadas).
- **Opções:**
  1. Usar como está e capturar o `ValueError`. Não distingue coluna a mais de faltando, não vê ordem nem tipo, e a exceção carrega só texto.
  2. Estender o validador no mesmo arquivo. Foi o que se tentou primeiro, mas o código novo não reutilizava nada do antigo; só dividia o arquivo, misturando dois contratos diferentes.
  3. Módulo novo (`schema_check.py`), deixando o `schema_validator.py` intacto.
- **Decisão:** opção 3. **Status:** decidido (revisa a decisão inicial pela opção 2).
- **Consequências:** o `bcb` não entra no diff e não corre risco de regressão. Cada módulo tem um contrato: o validador rígido dos crawlers e a checagem com severidade do upload. A possível unificação dos dois fica fora do escopo.

### ADR-2: ordem das colunas (a3)

- **Contexto:** a tabela externa de CSV é posicional, então ordem trocada coloca dados na coluna errada sem erro. Em Parquet o BigQuery casa por nome.
- **Opções:**
  1. Ignorar a ordem.
  2. Warning sempre.
  3. Erro sempre.
  4. Erro em CSV e warning informativo em Parquet.
  5. Reordenar o arquivo automaticamente para casar com a staging.
- **Decisão:** opção 4. **Status:** decidido. A opção 5 fica fora da v1, porque altera o dado enviado ao bucket.
- **Consequências:** a regra de CSV é "header igual ao schema final esperado, posição por posição". Isso também pega coluna nova no meio e coluna faltando no meio.

### ADR-3: tipo das colunas (a4)

- **Contexto:** a lib `basedosdados` infere todas as colunas como `STRING`, em CSV e Parquet. Em CSV a tabela é toda `STRING`. Em Parquet o BigQuery deriva os tipos dos arquivos.
- **Opções:**
  1. Comparar o schema inferido pela lib com a staging. Inútil: sempre `STRING`.
  2. Checar tipo também em CSV. Sem sentido, pelo mesmo motivo.
  3. Parquet: ler o schema do arquivo (pyarrow) e comparar com `get_table().schema`, com igualdade estrita após normalizar aliases.
  4. Igual à 3, com lista de conversões permitidas (por exemplo `INT64→FLOAT64`).
  5. Deixar tipo fora da v1.
- **Decisão:** opção 3, sem lista de conversões; a lista entra só se houver falso positivo. **Status:** decidido; o mapeamento pyarrow→BigQuery é a confirmar em dev.
- **Consequências:** tipo incompatível em Parquet é erro. CSV não tem checagem de tipo.

### ADR-4: coluna a mais (a1)

- **Contexto:** o `_sync_staging_schema` já acrescenta colunas novas ao fim do schema.
- **Opções:** 
  1. erro;
  2. acrescentar ao fim e avisar;
  3. acrescentar sem avisar (comportamento atual).
- **Decisão:** opção 2. 
- **Status:** decidido.
- **Consequências:** em CSV, só vale se a coluna nova estiver no fim do arquivo. Se estiver no meio, o schema (que recebe no fim) e o arquivo desalinham, e vira erro de ordem (ADR-2). A checagem roda antes do sync.

### ADR-5: coluna faltando (a2)

- **Contexto:** com `allow_jagged_rows=False`, um CSV com menos colunas que o schema pode fazer o BigQuery recusar a leitura das linhas curtas. Coluna faltando no meio desloca os dados seguintes sem erro. Em Parquet a coluna é lida como NULL.
- **Opções:**
  1. Erro sempre. Chegou a ser adotada e foi revertida.
  2. Warning sempre.
  3. Warning, exceto CSV com coluna faltando no meio, que é erro de ordem.
  4. Warning e habilitar `allow_jagged_rows=True` nas staging de CSV.
- **Decisão:** opção 1, erro sempre em `append` (CSV e Parquet). No `overwrite` continua só warning (ADR-6).
- **Status:** decidido.
- **Consequências:** a fonte que deixar de trazer uma coluna precisa de um `overwrite` ou de ajuste deliberado da staging.

### ADR-6: `dump_mode="overwrite"`

- **Contexto:** a staging e o prefixo são recriados do zero a partir do arquivo novo, então não há histórico para desalinhar. Hoje a mudança de schema passa em silêncio e pode quebrar o dbt a jusante.
- **Opções:**
  1. Não checar (situação atual).
  2. Aplicar as mesmas regras do append, com erros.
  3. Checar e **só avisar**, para qualquer divergência.
- **Decisão:** opção 3, lendo o schema antigo **antes** de `tb.delete`/`st.delete_table`. 
- **Status:** decidido.
- **Consequências:** nunca bloqueia. A mensagem diz que o schema foi substituído e pede revisão dos modelos dbt dependentes.

### ADR-7: erro de schema e retries

- **Contexto:** a task tem `retries=3`. Repetir não corrige divergência de schema.
- **Opções:** 
  1. exceção comum, com 3 tentativas inúteis;
  2. `SchemaMismatchError` próprio, excluído do retry com `retry_condition_fn`.
- **Decisão:** opção 2.
- **Status:** decidido.
- **Consequências:** o erro aborta antes de alterar a staging ou o bucket, e o alerta chega na primeira tentativa.

### ADR-8: falha da própria checagem

- **Contexto:** a checagem depende de chamadas ao BigQuery e à leitura do arquivo, que podem falhar.
- **Opções:** 
  1. fail-closed, abortando o upload; 
  2. fail-open, registrando warning no log e seguindo.
- **Decisão:** opção 1, fail-closed: se a checagem não conseguir rodar (por exemplo, erro de API do BigQuery ao ler a staging), o upload é abortado.
- **Status:** decidido.
- **Consequências:** nada sobe sem validação. Uma instabilidade do BigQuery passa a derrubar flows que hoje funcionam. Para esses casos, a falha deve usar um erro **diferente** de `SchemaMismatchError`, que mantém os retries normais da task (a falha pode ser transitória); só `SchemaMismatchError` fica fora do retry (ADR-7).


### ADR-9: tabelas particionadas (Hive)

- **Contexto:** `to_partitions` grava `ano=2024/mes=01/data.csv` e remove as chaves de partição das colunas do arquivo; elas existem só como nomes de pasta. Verificado em staging reais (`br_rf_cnpj.estabelecimentos`, particionada, e `br_camara_dados_abertos.deputado_ocupacao`, não particionada):
  - `table.schema` **inclui as chaves de partição**, no fim da lista. Sem tratamento, a D1 (coluna faltando = erro) bloquearia todo upload particionado.
  - `table._properties["partitionDefinition"]["partitionedColumn"]` traz as chaves, em ordem: `[{'field': 'data_referencia'}, {'field': 'sigla_uf'}]`. A ordem é a das pastas gravadas pelo pipeline (`data_referencia=.../sigla_uf=*/`). Em tabela não particionada, vale `None`.
  - `table.external_data_configuration.hive_partitioning` é `None` em tabela não particionada e um objeto na particionada.
- **Opções para obter as chaves da staging:** (1) listar um blob no GCS e ler as pastas `k=v`; (2) ler `partitionDefinition`. A opção 2 não faz chamada extra ao storage e usa o `get_table` que já existe.
- **Decisão:** opção 2, isolada em `StagingSchema.from_table`. **Status:** decidido e implementado.
- **Regras implementadas:**
  1. As chaves de partição da staging e do upload ficam fora da comparação de colunas, dos dois lados.
  2. **Layout (a5):** as chaves do upload (`UploadSchema.partition_keys`, em ordem, lidas do caminho relativo a `data_path`) precisam ser iguais, e na mesma ordem, às da staging. Chaves diferentes, ordem diferente ou particionada contra não particionada é **erro no `append`** e **warning no `overwrite`**.
  3. `_properties` é privado (o JSON cru da API; o client não expõe `partitionDefinition`). Se a chave faltar mas `hive_partitioning` existir, as chaves são tratadas como **desconhecidas**: o layout não é verificado e a mensagem traz um warning explícito, em vez de supor "sem partição".
- **Sem impacto:** valores novos de partição (`ano=2025`) não são mudança de schema; as chaves são sempre `STRING`, então ficam fora da checagem de tipo.
- **Ainda a avaliar:**
  - Chave de partição que também aparece no header do arquivo (o BigQuery recusa a coluna duplicada).
  - Comparar o header de **todos** os arquivos do upload, e não só o primeiro: `dump_header` e a lib leem só um arquivo, então um arquivo de outra partição com header diferente passa despercebido.
  - Um teste contra uma staging real em dev, para fixar o formato de `_properties` (o acesso a atributo privado pode mudar com a versão do client).

## 9. Proposta de Automation do Prefect + Discord

**Dois triggers**, um para cada resultado:

1. **Warning**: evento de estado customizado.
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
2. **Erro**: evento `prefect.task-run.Failed`, filtrado pela task `upload_to_gcs` e pelo tipo/mensagem do erro (`SchemaMismatchError`), para não alertar qualquer falha do upload.

Ação: *Send a notification* com um `CustomWebhookNotificationBlock` (`method=POST`, `json_data={"content": "{{body}}"}`, formato esperado pelo Discord). O corpo usa Jinja, por exemplo:
```
{{ event.payload.validated_state.message }}
Flow run: {{ flow_run|ui_url }}
```

## 10. Fora do escopo da v1

- Reordenar o arquivo automaticamente para casar com a staging (em vez de erro em a3). Altera o dado enviado ao bucket; fica fora da v1.
- Deduplicar alertas repetidos da mesma tabela, se houver ruído.

## 11. Revisão da estrutura do módulo

> **Estado:** implementada em `schema_check.py` (partes 1 e 2). Nenhuma regra do §3 mudou: severidades, D1, D2 e ADRs são os mesmos. Falta a parte 3 (integração em `tasks.py`) e a parte 4 (testes). O §4 descreve a estrutura final; este capítulo guarda o diagnóstico, as decisões tomadas e o que ainda está em aberto.

### 11.1 Diagnóstico (da estrutura anterior)

O módulo tem 540 linhas e quatro conceitos de entrada/saída que se sobrepõem. Pontos que pesam sem pagar o custo:

- **Dois pontos de entrada.** `check_schema` (pura) só é usada nos testes; a task usa `check_staging_schema`, que monta `Column`s e chama `_compare`. O caminho testado não é o caminho de produção.
- **`Column.coerce` / `ColumnLike`** existem só para os testes passarem `str` ou tupla.
- **`SchemaCheckResult.diff`** não é lido por ninguém fora do módulo (a task usa `has_changes` e `render`).
- **Duas listas** (`warnings`, `errors`) mais `_Findings`, e `for_overwrite` reescreve texto para rebaixar. A severidade poderia ser um dado de cada achado.
- **`ignore` + `layout`** como parâmetros de `_compare`: a partição entra como exceção em vez de fazer parte do modelo.
- **`_sync_staging_schema(tb, table, incoming, local)`** depende de `incoming` (tipo da lib) e de `local` só para o tipo da coluna nova.
- **`_upload_to_gcs`** repete o mesmo bloco (header, `incoming`, `get_table`, `LocalDataSchema.read`, `check_staging_schema`) em `append` e `overwrite`.

### 11.2 Estrutura adotada

Duas variações simétricas de `TableSchema` e uma função de comparação:

```python
@dataclass(frozen=True)
class TableSchema:
    columns: tuple[Column, ...]              # sem chaves de partição
    partition_keys: tuple[str, ...] | None   # None = não deu para ler

class UploadSchema(TableSchema):  from_files(data_path, source_format)
class StagingSchema(TableSchema): from_table(table)

def check(staging: StagingSchema, upload: UploadSchema, source_format, overwrite) -> SchemaCheckResult
```

O que ficou diferente da proposta inicial:
- **Subclasses** (`UploadSchema`, `StagingSchema`) em vez de duas fábricas na mesma classe: as assinaturas de `check` e `SchemaDiff.between` declaram o lado de cada argumento. Os dois usam a mesma ordem, `(staging, upload)`.
- **Header do CSV lido direto** em `from_files`, com a mesma chamada do `dump_header` (`pd.read_csv(nrows=0, dtype=str)`). Dispensa `tb._load_staging_schema_from_data`. O `dump_header` continua necessário para o `tb.create`.
- **`SchemaDiff` continua um tipo do módulo** (as regras o consomem), mas **não faz parte do resultado**.
- **Achado com severidade:** `Finding(severity, message)` numa lista só; `errors` e `warnings` são propriedades. No `overwrite`, `_for_overwrite` rebaixa os erros e acrescenta a nota.
- **Partição no modelo:** `columns` já vem sem as chaves, então o parâmetro `ignore` desapareceu. O layout (a5) é um fato do diff (`layout_unknown`, `layout_changed`).
- **Correção:** a leitura do tipo Parquet usa `f.type`; a versão anterior usava `f.data_type`, que não existe em `pyarrow.Field` e quebraria todo upload Parquet em `append` com staging existente.

Tamanho: o módulo tem hoje cerca de 520 linhas (eram 540). A estimativa inicial de ~300 não se confirmou: a estrutura ficou mais clara, mas o corte de linhas foi pequeno. Reduções possíveis, se valerem a pena: fundir `Severity`/`Finding`/`_warning`/`_error` em duas listas de texto, e juntar as funções `_added_message`, `_missing_message` e `_type_message` nas regras.

Pendente na task (parte 3): helper `_check_staging` com o bloco repetido de `append` e `overwrite`; `_sync_staging_schema(tb, table, added_columns)`; retorno da task com `SchemaWarning`.

### 11.3 Partições

**O que existe para recuperar o estado de particionamento de uma tabela externa** (verificado no `google-cloud-bigquery` 3.25.0 e na `basedosdados` instaladas):

| Fonte | O que dá | Chaves? |
|---|---|---|
| `table.external_data_configuration.hive_partitioning` (público) | `HivePartitioningOptions` ou `None`: `mode`, `source_uri_prefix`, `require_partition_filter` | Não |
| `table.time_partitioning` / `range_partitioning` | particionamento nativo; vazio em externa com Hive | Não se aplica |
| `table._properties["partitionDefinition"]["partitionedColumn"]` (privado) | lista de `{"field": ...}`, na ordem das pastas | **Sim** |
| `basedosdados.Table._is_partitioned()` | `bool`, a partir do arquivo local ou do cadastro de metadados da BD | Não |
| `basedosdados.Table._parser_blobs_to_partition_dict()` | chaves e valores lidos das pastas `k=v` dos blobs | Sim, com chamada ao storage |

Não há método público que devolva as chaves. O que fecha o desenho:

1. **Fonte primária:** `partitionDefinition`, lido em **um único lugar** (`StagingSchema.from_table`), para que uma mudança de versão do client tenha um só ponto de ajuste. **Feito.** O `external_data_configuration` é lido com `getattr`, então `None` conta como "sem partição" e não quebra.
2. **Fallback no lugar do warning (não implementado):** se `partitionDefinition` faltar e `hive_partitioning` existir, ler as chaves de um blob em `source_uri_prefix` (`list_blobs(max_results=1)`, pastas `k=v`). Só se isso também falhar vale `None` (layout não verificado, com warning).
3. **Chaves do upload de todos os arquivos (não implementado).** Hoje vêm só do primeiro arquivo. Ler o caminho de todos é barato (sem abrir arquivo): o conjunto de tuplas de chaves tem de ter um único elemento, senão é erro de layout. Resolve metade do item "header de todos os arquivos" do ADR-9.
4. **Chave de partição no header do arquivo (não implementado).** `columns` do upload não é filtrado, então uma chave de partição presente no header aparece hoje como coluna nova (aviso), em vez de erro. A regra desejada: se uma chave de partição aparecer no header do arquivo, é erro (o BigQuery recusa a coluna duplicada); em `overwrite`, warning.
5. **Header de todos os arquivos (não implementado):** em Parquet, `pq.read_schema` lê só o rodapé e é barato; comparar todos é viável. Em CSV exigiria abrir cada arquivo, então fica como opção a decidir (custo × risco).
6. **Teste contra staging real em dev**, para fixar o formato de `_properties` (já listado no ADR-9).

### 11.4 Decisões e pontos em aberto

Decididos:
- **Nova forma adotada:** `TableSchema` com as subclasses `UploadSchema` e `StagingSchema`, e `check` como entrada única.
- **Header do CSV lido direto** em `UploadSchema.from_files`, sem `_load_staging_schema_from_data`.

Em aberto:
- Item 5 de 11.3: checar o header de todos os CSVs ou só de todos os Parquets?
- Fallback por blob (item 2 de 11.3) entra na v1 ou espera a validação em dev?
- Regra da chave de partição no header (item 4 de 11.3) e leitura das chaves de todos os arquivos (item 3): implementar na parte 3 ou numa parte 2b?
