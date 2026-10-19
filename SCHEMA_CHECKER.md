# Plano: checagem de schema no `upload_to_gcs`

> **Estado:** decisões D1 (coluna faltando = erro no append), D2 (overwrite só avisa) e ADR-8 (fail-closed) fechadas. Implementado e integrado em `_upload_to_gcs` e na task `upload_to_gcs`: `pipelines/utils/schema_check.py`, `bq_safe_column_name` em `utils.py` e testes em `pipelines/utils/tests/`. Particionamento (ADR-9) resolvido com `partitionDefinition`. Em aberto: validação em dev (evento do Prefect, tipos Parquet), chave de partição repetida no header e header de todos os arquivos.

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
- **Nomes:** comparar sempre normalizados com `_bq_safe_column_name`, dos dois lados.

## 4. Módulo da checagem

O código novo vive em `pipelines/utils/schema_check.py`. O `schema_validator.py` **não foi alterado** e segue servindo ao crawler do `bcb` (ADR-1).

Estrutura do módulo, de baixo para cima:
- **Modelo:** `SourceFormat` (`StrEnum`) e `Column`, que normaliza nome (`bq_safe_column_name`) e tipo (aliases) uma única vez.
- **Fatos:** `SchemaDiff.between(source, expected)` calcula colunas novas, faltando, ordem e tipos diferentes, sem juízo de severidade.
- **Regras:** `_csv_findings` e `_parquet_findings` (tabela do §3), escolhidas por formato; `_Findings.for_overwrite` rebaixa tudo a warning (§6).
- **Resultado:** `SchemaCheckResult` guarda o `diff`, os `warnings` e os `errors`; `render()` trunca a mensagem; `raise_if_errors()` levanta `SchemaMismatchError`.
- **Arquivos locais:** `LocalDataSchema.read(data_path, source_format)` lê uma vez as chaves de partição (em ordem) e, em Parquet, os tipos do arquivo; `type_of(nome)` aceita o nome cru.
- **Entradas:** `check_staging_schema(staging, incoming, local, overwrite)` (usada pela task) e `check_schema(source, expected, ...)` (pura, usada nos testes).

## 5. Fluxo na task

```
_upload_to_gcs
 ├─ append + staging existe
 │    ├─ lê header do arquivo e schema da staging
 │    ├─ valida (§3)  ── errors ──► raise SchemaMismatchError   (nada é alterado)
 │    ├─ _sync_staging_schema(incoming já lido)   # só se não houve erro
 │    └─ st.upload
 ├─ append + staging não existe  → cria, sem comparação (primeiro upload)
 └─ overwrite                    → ver §6
```

- `_upload_to_gcs` passa a retornar o resultado da validação (ou `None`).
- A task `upload_to_gcs`, havendo `warnings`: `log(..., "warning")` e `return Completed(name="SchemaWarning", message=<texto>)`. O estado continua `COMPLETED`, então retries e dependências não mudam.
- **Erro sem retry inútil:** `SchemaMismatchError` é um tipo próprio. A task tem `retries=3`, e repetir não resolve divergência de schema. Usar `retry_condition_fn` para não retentar esse tipo.
- **Fail-closed (ADR-8):** falha da própria checagem (ex.: erro de API do BigQuery) aborta o upload. Ela não é `SchemaMismatchError`, então a task a retenta normalmente; só `SchemaMismatchError` fica fora do retry.
- Refatorar `_sync_staging_schema` para receber o `incoming` já calculado, evitando ler o header duas vezes.
- **Tipo da coluna nova em Parquet:** hoje o sync acrescenta a coluna com o tipo que a lib infere, sempre `STRING`. Em Parquet isso pode divergir do tipo real do arquivo. Ao integrar, usar o tipo lido do arquivo (pyarrow, o mesmo mapeamento do a4). Conferir em dev se atualizar `table.schema` de uma externa Parquet se comporta como esperado.
- Mover `_bq_safe_column_name` para `utils.py` (reimportar em `tasks.py`) para não criar import circular com `schema_check.py`.

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
  2. **Layout (a5):** as chaves do upload (`LocalDataSchema.partition_keys`, em ordem, lidas do caminho relativo a `data_path`) precisam ser iguais, e na mesma ordem, às da staging. Chaves diferentes, ordem diferente ou particionada contra não particionada é **erro no `append`** e **warning no `overwrite`**.
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
