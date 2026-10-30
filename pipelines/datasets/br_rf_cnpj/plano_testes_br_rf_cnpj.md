# Plano de implementação: testes de qualidade do `br_rf_cnpj`

## O que já existe e vale reaproveitar

- **Macros em `tests-dbt/generic/`:** `not_null_proportion_multiple_columns`, `custom_unique_combinations_of_columns` (tolera uma proporção de falhas), `is_row_count_increasing`, `custom_relationships` e `custom_dictionary_coverage`. O `dbt_utils` está na 1.1.1.
- **Filtro `__most_recent_date_cnpj__`:** o `custom_get_where_subquery.sql` filtra pela última `data_referencia`. Todos os testes novos devem usá-lo, porque as tabelas são grandes e particionadas.
- **Exceção:** `simples` é snapshot único (não histórica), então o placeholder não se aplica a ela.
- **Pontos do pipeline que importam:**
  - `process_csv_estabelecimentos` grava uma partição por `sigla_uf in UFS`. Se um UF vier fora da lista, as linhas somem sem erro. Esse é o risco do teste de soma das partições.
  - `id_municipio` sai vazio do Python e é preenchido por um join no dbt (`b.id_municipio_rf`).
  - `fill_left_zeros` usa `astype(str).str.zfill`, e o `cnpj` é montado por concatenação.

### Fase 1: testes de integridade do pipeline (Python)

Rodam **antes da carga**, em `pipelines/datasets/br_rf_cnpj/`, e devem falhar o flow.

1. **Soma das partições de UF = linhas de entrada.**
   - Em `process_csv_estabelecimentos`, acumular `rows_in += len(chunk)` e `rows_out += len(df_particao)` por chunk.
   - Ao final, comparar. Qualquer diferença gera erro, mas há possibilidade de tolerância implementada.
2. **Contagem de arquivos baixados vs. esperado.**
   - `get_table_files` já devolve a lista da fonte. Comparar `len(files)` com o esperado (10 por tabela segmentada) e com os arquivos efetivamente baixados e processados.
   - O número 10 deve ir para `constants.py` (ex.: `EXPECTED_FILES_PER_TABLE`).
   - Se a Receita mudar a quantidade de fragmentos, falhar com aviso em vez de carregar incompleto.
3. **Validação de schema (número e ordem das colunas vs. BQ).**
   - Isso será feito em um outro PR para todas as tabelas e datasets
4. **Testes unitários** com `pytest` e CSVs mínimos para (1) e (2), pois são lógica Python pura e não dependem do BQ.

## Testes dbt

1. Definir a convenção de severidade:
   - `error` para integridade, chaves e datas impossíveis;
   - `warn` para formato (e-mail, telefone), por exemplo
2. Medir antes de escrever o limiar. Para cada teste novo, rodar uma query exploratória no BigQuery na partição mais recente e só então fixar o `at_least` ou o `proportion_allowed_failures`. Isso evita testes que nascem quebrados.

### Fase 2: testes dbt simples, só em YAML (baixo risco)

Tudo em `models/br_rf_cnpj/schema.yml`, com `config.where: __most_recent_date_cnpj__`.

| Item | Como | Observações |
|---|---|---|
| Valores aceitos (seção "Extra") | `dbt_utils.accepted_values` por coluna | `simples.opcao_simples`/`opcao_mei`. Os valores estão como string nos modelos (`cast(... as string)`), então listar `'1','2'` com `quote: true`. Confirmar antes se `simples.opcao_*` é `S/N` ou `0/1`. |
| `socios`: `is_row_count_increasing` | Em `data_referencia` | Igual a `empresas`. |
| `id_municipio` não nulo se `sigla_uf` não é EX/BR | `dbt_utils.expression_is_true` ou `not_null` com `where` combinado | Combinar `__most_recent_date_cnpj__ and sigla_uf not in ('EX','BR')`. O macro faz `replace` simples e deve funcionar. Validar com `dbt compile`. |
| `simples`: filtro na unicidade | Avaliar `where` | Snapshot único. Manter sem filtro de partição, mas confirmar o custo da query. |

### Fase 3: testes de formato e datas

Criar **dois genéricos novos** em `tests-dbt/generic/` para evitar muitos testes repetidos:

- **`regex_or_null(column_name, pattern, severity)`:** cobre `cep`, `ddd_*`, `telefone_*`/`fax`, `cpf_representante_legal` e `email`.
  - `cep`: `^[0-9]{8}$`
  - `ddd`: `^[0-9]{2}$`
  - `telefone`/`fax`: `^[0-9]{6,9}$`
  - `cpf_representante_legal`: aceitar o padrão mascarado `***XXXXXX**` ou vazio. Definir o padrão a partir de uma amostra real.
  - `email`: regex frouxa com `severity: warn`.
- **`date_in_range(column_name, min_date='1900-01-01')`:** `col is null or col between date(min) and current_date()`. Existe um macro `validate_date_range.sql` em `macros/`. Ver se dá para reaproveitar a lógica antes de criar outro.

Aplicação das datas:

- **Colunas de data** (`data_inicio_atividade`, `data_situacao_cadastral`, `data_situacao_especial`, `data_entrada_sociedade`, `data_opcao_simples`, `data_exclusao_simples`, `data_opcao_mei`, `data_exclusao_mei`): aplicar `date_in_range` em cada uma.
- **Ordem entre datas:** `dbt_utils.expression_is_true` em nível de modelo, tolerando nulos, ex.: `(a is null or b is null or a <= b)`:
  - `data_inicio_atividade <= data_situacao_cadastral`
  - `data_exclusao_simples >= data_opcao_simples`
  - `data_exclusao_mei >= data_opcao_mei`
- **`socios.documento`:** `expression_is_true` com `case`:
  - `tipo='1'` exige CNPJ de 14 dígitos
  - `tipo='2'` exige CPF mascarado
  - `tipo='3'` fica livre. Confirmar com amostra o que vem em estrangeiros.
.

### Fase 4: unicidade e testes restantes

- **Caracteres inválidos (issue 1533):** teste `no_invalid_chars` para `razao_social`, `email`, `logradouro` etc. Antes de escrever o regex, ler a issue e levantar quais caracteres aparecem na prática (ex.: `�`, controles `\x00-\x1f`). Começar em `warn`.
- **`not_null_proportion_multiple_columns` nas 5 tabelas:** o macro marca como falha a coluna com proporção de nulos acima de `1 - at_least`. Para cada tabela, rodar com os padrões, listar as colunas que falham e decidir caso a caso se vão para `ignore_values` (colunas naturalmente vazias, como `complemento`, `fax`, `data_exclusao_*`) ou se indicam bug. Observação: o macro compara `column_name not in (...)`, então `ignore_values` é uma lista de **nomes de colunas**, apesar do nome
- **`socios`: chave de unicidade.** Não existe chave natural declarada. Candidatas a testar na partição recente:
  - `(cnpj_basico, tipo, documento, nome, qualificacao, data_entrada_sociedade)`
  - com e sem `cpf_representante_legal`
  - Como o CPF vem mascarado, haverá colisões legítimas. Por isso usar `custom_unique_combinations_of_columns` (tolera 5%) e **medir a taxa real antes de fixar o limiar**. 
- **Cruzamento por `cnpj_basico`:** usar `custom_relationships` ou `relationships` com `severity: warn`:
  - `estabelecimentos.cnpj_basico` → `empresas.cnpj_basico`
  - `socios.cnpj_basico` → `empresas.cnpj_basico`
  - `simples.cnpj_basico` → `empresas.cnpj_basico`
  - Atenção: `simples` é snapshot e `empresas` é particionada, então a comparação tem que ser com a partição mais recente de `empresas`. Isso pode pedir um genérico novo com `where` na tabela destino, ou um modelo/CTE auxiliar com `data_referencia = (select max ...)`. Verificar se `custom_relationships` aceita um `to` filtrado.


### Fase 5: CNPJ alfanumérico (investigação, antes de qualquer código)

A partir de 2026 o CNPJ passa a aceitar letras. Isso afeta:

- **`fill_left_zeros`:** `astype(str).str.zfill` funciona com letras, mas `astype(str)` sobre `NaN` produz a string `"nan"`, que vira `"00000nan"`. Já é um risco hoje.
- **Modelos SQL:** `lpad(safe_cast(cnpj_basico as string), 8, '0')` e `lpad(cnpj, 14, "0")` funcionam com letras, mas qualquer `cast(... as int64)` em CNPJ quebraria. Fazer `grep` por casts de CNPJ nos modelos e em `br_bd_diretorios_brasil.empresa`.
- **Dígito verificador:** o cálculo muda (módulo 11 sobre valores ASCII − 48). Se algum teste ou rotina valida o DV, precisa ser ajustado.
- **Novos testes:** formato `^[A-Z0-9]{12}[0-9]{2}$` para `cnpj` e `^[A-Z0-9]{8}$` para `cnpj_basico`.

**Resultado da investigação (2026-10-28):**

- **Código:** o Python lê os CSVs com `dtype=str` e os modelos só usam `lpad(safe_cast(... as string), N, '0')`. Não há `cast` de CNPJ para número no `br_rf_cnpj` nem nos consumidores (CVM, SICOR, `br_bd_diretorios_brasil__empresa`). Nenhuma rotina ou teste valida o DV.
- **Dado:** na partição 2026-10-01 de `estabelecimentos` (73,8M linhas) há 0 CNPJs com letras e 0 violações dos formatos alfanuméricos em `cnpj`, `cnpj_basico`, `cnpj_ordem` e `cnpj_dv`. Também 0 casos com `nan`.
- **Testes adicionados:** `regex_or_null` com `severity: error` em `cnpj` (`^[A-Z0-9]{12}[0-9]{2}$`), `cnpj_basico` (`^[A-Z0-9]{8}$`, nas 4 tabelas), `cnpj_ordem` (`^[A-Z0-9]{4}$`) e `cnpj_dv` (`^[0-9]{2}$`). Em `simples` não há `where`, por ser snapshot único.

### Decisões tomadas

1. **Datas: manter o teste `date_in_range` e não aplicar `validate_date_range` nos modelos.**
   - **Contexto:** os modelos de `br_rf_cnpj` só usam `safe_cast(... as date)`, que zera formato inválido mas deixa passar datas bem formadas e absurdas (ex.: `1850-01-01`, datas futuras). A macro `validate_date_range` hoje só é usada em `br_tse_*` e `br_ms_sia`.
   - **Decisão:** manter o teste, que detecta e avisa sem alterar o dado publicado.
   - **Alternativa descartada por ora:** aplicar a macro nos modelos. Ela corrigiria o dado, mas em silêncio (vira `NULL`, sem alerta se a fonte mudar) e exigiria reprocessar as tabelas.
   - **Quando reabrir:** se o teste falhar por valores sentinela da fonte (ex.: `0000-00-00`, `9999-12-31`), avaliar tratar no SQL do modelo. Se a macro for adotada, manter o `not_null_proportion_multiple_columns` para pegar aumento súbito de nulos.
   - **Pendente:** medir na partição mais recente se existem sentinelas antes de subir a severidade de `warn` para `error`, e decidir sobre `data_modificacao` (hoje fora dos testes por omissão).

2. **Ordem das datas em `simples`: tolerar uma taxa mínima de violações em vez de exigir zero.**
   - **Contexto:** `data_exclusao_simples < data_opcao_simples` e `data_exclusao_mei < data_opcao_mei` ocorrem na origem (RFB). Medido uma vez em `basedosdados.br_rf_cnpj.simples` (snapshot único, sem coluna de partição): 3 casos em ~25,6M linhas comparáveis (Simples) e 3 em ~20,1M (MEI), cerca de 1e-7. Total da tabela: ~50,8M linhas.
   - **Decisão:** trocar os `dbt_utils.expression_is_true` estritos por `custom_matching_rows_limit` com `max_share: 0.000001` (≈50 linhas), mantendo `severity: error`. O limite dá folga de ~10x sobre a taxa observada: ruído da fonte passa, mas um aumento real de violações falha.
   - **Alternativas descartadas:** manter o teste estrito (falharia sempre por um problema que não controlamos) e corrigir o dado no modelo (alteraria o publicado em silêncio e exigiria reprocessar).
   - **Observação:** o denominador é o total de linhas da tabela, não só as comparáveis. Isso deixa o teste um pouco mais permissivo, mas a diferença é irrelevante nessa escala.
   - **Quando reabrir:** se a taxa observada se aproximar do limite, ou se a RFB corrigir a origem (aí dá para voltar ao teste estrito).

3. **CNPJ alfanumérico: só testes de formato; não validar o DV por enquanto.**
   - **Contexto:** ver o resultado da investigação na Fase 5. A base ainda não tem letras, e nada no código quebra com elas.
   - **Decisão:** testes de formato em `error`, já alfanuméricos, para pegar também o `"00000nan"` que o `fill_left_zeros` produziria com célula vazia.
   - **Alternativa descartada por ora:** teste do DV (módulo 11 sobre `ASCII − 48`). Exigiria um genérico novo rodando em ~74M linhas, sem benefício enquanto não houver letras.
   - **Quando reabrir:** no primeiro CNPJ com letra na base, implementar o teste do DV. Avaliar blindar o `fill_left_zeros` contra `NaN`, com teste `pytest`.

4. **Ordem das datas em `estabelecimentos`: tolerar uma taxa mínima de violações em vez de exigir zero.**
   - **Contexto:** `data_inicio_atividade > data_situacao_cadastral` ocorre em 1065 linhas da partição mais recente (~74M), cerca de 1,4e-5. Em uma amostra de 15 CNPJs (primeiros casos do arquivo bruto, não aleatória), as datas no BigQuery são idênticas às do arquivo da Receita, e o mapeamento de colunas do código confere. A inversão vem da fonte. Detalhes no item 8 do `relatorio.md`.
   - **Decisão:** trocar o `dbt_utils.expression_is_true` estrito por `custom_matching_rows_limit` com `max_share: 0.0001` (≈7.400 linhas), mantendo `severity: error` e `where: __most_recent_date_cnpj__`. A folga é de ~7x sobre a taxa observada.
   - **Alternativas descartadas:** manter o teste estrito (falha sempre por problema da fonte) e corrigir o dado no modelo (alteraria o publicado em silêncio e exigiria reprocessar).
   - **Observação:** o limite é mais folgado que o de `simples` (1e-6) porque a taxa observada é cerca de 100x maior.
   - **Pendente:** contagem completa no arquivo bruto e classificação dos casos por situação cadastral. Se a taxa se aproximar do limite, reabrir.

5. **Relacionamento por `cnpj_basico` com `empresas`: `custom_relationships` em `warn`, contra a partição mais recente do pai.**
   - **Contexto:** o `custom_relationships` fazia `from {{ to }}` sem filtro, ou seja, compararia contra todas as partições de `empresas`. Isso sairia caro e mascararia órfãos (CNPJ presente só em partição antiga). Taxas medidas uma vez nas partições mais recentes (`simples` é snapshot): `simples` 2 em 50,8M (~4e-8), `socios` 33 em 28,5M (~1,2e-6), `estabelecimentos` 4134 em 73,8M (~5,6e-5).
   - **Decisão:** novo parâmetro opcional `to_where` no macro (padrão `None`, sem efeito nos demais datasets) e `custom_relationships` em `severity: warn` nas três tabelas, com `to_where` apontando para `max(data_referencia)` de `empresas`. Limiares com ~10x de folga: `simples` 1e-6, `socios` 1e-5, `estabelecimentos` 5e-4.
   - **Alternativas descartadas:** `relationships` nativo (compara com todas as partições do pai e não tolera falhas) e criar um macro novo (duplicaria a lógica).
   - **Observação:** os órfãos não foram investigados (ex.: empresas baixadas ou removidas da base de empresas). Se `empresas` e a tabela filha tiverem `data_referencia` diferentes, o teste pode dar falso positivo.
   - **Pendente:** verificar a renderização do Jinja em `to_where` na primeira execução. Se os órfãos forem relevantes, investigar a causa antes de subir para `error`.

### Decisões em aberto

1. **Chave de `socios`:** medir as combinações candidatas no BQ (consulta de leitura, com custo) antes de fechar o teste?
