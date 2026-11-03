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
   - **Revisão:** todos os testes do `schema.yml` foram colocados em `severity: error` (ver decisão 8); a convenção `warn` não está mais em uso.
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

Criar em `tests-dbt/generic/` para evitar muitos testes repetidos:

- **`regex_or_null(column_name, pattern, severity)`:** cobre `cep` e `cpf_representante_legal`. `ddd_*`, `telefone_*`/`fax` e `email` ficaram sem teste de formato (ver decisão 6).
  - `cep`: `^[0-9]{8}$`
  - `cpf_representante_legal`: aceitar o padrão mascarado `***XXXXXX**` ou vazio. Definir o padrão a partir de uma amostra real.
- **`date_in_range(column_name, min_date='1900-01-01')`:** `col is null or col between date(min) and current_date()`. Existe um macro `validate_date_range.sql` em `macros/`. Ver se dá para reaproveitar a lógica antes de criar outro.

Aplicação das datas:

- **Colunas de data** (`data_inicio_atividade`, `data_situacao_cadastral`, `data_situacao_especial`, `data_entrada_sociedade`, `data_opcao_simples`, `data_exclusao_simples`, `data_opcao_mei`, `data_exclusao_mei`): aplicar `date_in_range` em cada uma.
- **Ordem entre datas:** `dbt_utils.expression_is_true` em nível de modelo, tolerando nulos, ex.: `(a is null or b is null or a <= b)`:
  - `data_inicio_atividade <= data_situacao_cadastral`
  - `data_exclusao_simples >= data_opcao_simples`
  - `data_exclusao_mei >= data_opcao_mei`
- **`socios.documento`:** `expression_is_true` com `case`:
  - `tipo='1'` exige 8 dígitos (a fonte traz o `cnpj_basico`, não o CNPJ de 14 dígitos)
  - `tipo='2'` exige CPF mascarado (`***999999**`)
  - `tipo='3'` exige `documento` nulo (a amostra mostrou que é sempre nulo)

### Fase 4: unicidade e testes restantes

- **Caracteres inválidos (issue 1533):** teste `no_invalid_chars` para `razao_social`, `email`, `logradouro` etc. Antes de escrever o regex, ler a issue e levantar quais caracteres aparecem na prática (ex.: `�`, controles `\x00-\x1f`). 
- **`not_null_proportion_multiple_columns` nas 5 tabelas:** o macro marca como falha a coluna com proporção de nulos acima de `1 - at_least`. Decidir caso a caso se vão para `ignore_values` 
- **`socios`: chave de unicidade.** Não existe chave natural declarada. Candidatas a testar na partição recente:
  - `(cnpj_basico, tipo, documento, nome, qualificacao, data_entrada_sociedade)`
  - com e sem `cpf_representante_legal`
  - Como o CPF vem mascarado, haverá colisões legítimas. Por isso usar `custom_unique_combinations_of_columns` (tolera 5%) e **medir a taxa real antes de fixar o limiar**. 
- **Cruzamento por `cnpj_basico`:** usar `custom_relationships` ou `relationships` com `severity: error`:
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

- **Código:** o Python lê os CSVs com `dtype=str` e os modelos só usam `lpad(safe_cast(... as string), N, '0')`. Não há `cast` de CNPJ para número no `br_rf_cnpj` nem nos consumidores (CVM, SICOR, `br_bd_diretorios_brasil__empresa`). 
- **Dado:** na partição 2026-10-01 de `estabelecimentos` (73,8M linhas) há 0 CNPJs com letras e 0 violações dos formatos alfanuméricos em `cnpj`, `cnpj_basico`, `cnpj_ordem` e `cnpj_dv`. Também 0 casos com `nan`.
- **Testes adicionados:** `regex_or_null` com `severity: error` em `cnpj` (`^[A-Z0-9]{12}[0-9]{2}$`), `cnpj_basico` (`^[A-Z0-9]{8}$`, nas 4 tabelas), `cnpj_ordem` (`^[A-Z0-9]{4}$`) e `cnpj_dv` (`^[0-9]{2}$`). Em `simples` não há `where`, por ser snapshot único.

### Decisões tomadas

1. **Datas: manter o teste `date_in_range` e não aplicar `validate_date_range` nos modelos.**
   - **Contexto:** os modelos de `br_rf_cnpj` só usam `safe_cast(... as date)`, que zera formato inválido mas deixa passar datas bem formadas e absurdas (ex.: `1850-01-01`, datas futuras). A macro `validate_date_range` hoje só é usada em `br_tse_*` e `br_ms_sia`.
   - **Decisão:** manter o teste, que detecta e avisa sem alterar o dado publicado.
   - **Alternativa descartada por ora:** aplicar a macro nos modelos. Ela corrigiria o dado, mas em silêncio (vira `NULL`, sem alerta se a fonte mudar) e exigiria reprocessar as tabelas.
   - **Quando reabrir:** se o teste falhar por valores sentinela da fonte (ex.: `0000-00-00`, `9999-12-31`), avaliar tratar no SQL do modelo. Se a macro for adotada, manter o `not_null_proportion_multiple_columns` para pegar aumento súbito de nulos.
   - **Medição (rodada de 31 testes):** os 8 `date_in_range` passaram (então ainda em `warn`), ou seja, não há datas fora de 1850 até o ano seguinte nem `9999-12-31`. Sentinelas inválidas (ex.: `0000-00-00`) viram `NULL` no `safe_cast` e só seriam pegas pelo `not_null_proportion_multiple_columns`.
   - **Severidade:** subida para `error` (decisão 8).

2. **Ordem das datas em `simples`: tolerar uma taxa mínima de violações em vez de exigir zero.**
   - **Contexto:** `data_exclusao_simples < data_opcao_simples` e `data_exclusao_mei < data_opcao_mei` ocorrem na origem (RFB). Medido uma vez em `basedosdados.br_rf_cnpj.simples` (snapshot único, sem coluna de partição): 3 casos em ~25,6M linhas comparáveis (Simples) e 3 em ~20,1M (MEI), cerca de 1e-7. Total da tabela: ~50,8M linhas.
   - **Decisão:** trocar os `dbt_utils.expression_is_true` estritos por `custom_matching_rows_limit` com `max_share: 0.000001`, mantendo `severity: error`. 
   - **Alternativas descartadas:** manter o teste estrito (falharia sempre por um problema que não controlamos) e corrigir o dado no modelo (alteraria o publicado em silêncio e exigiria reprocessar).
   - **Observação:** o denominador é o total de linhas da tabela, não só as comparáveis. Isso deixa o teste um pouco mais permissivo, mas a diferença é irrelevante nessa escala.

3. **CNPJ alfanumérico: só testes de formato**
   - **Contexto:** ver o resultado da investigação na Fase 5. A base ainda não tem letras, e nada no código quebra com elas.
   - **Decisão:** testes de formato em `error`, já alfanuméricos, para pegar também o `"00000nan"` que o `fill_left_zeros` produziria com célula vazia.

4. **Ordem das datas em `estabelecimentos`: tolerar uma taxa mínima de violações em vez de exigir zero.**
   - **Contexto:** `data_inicio_atividade > data_situacao_cadastral` ocorre em 1065 linhas da partição mais recente (~74M), cerca de 1,4e-5. Em uma amostra de 15 CNPJs (primeiros casos do arquivo bruto, não aleatória), as datas no BigQuery são idênticas às do arquivo da Receita, e o mapeamento de colunas do código confere. A inversão vem da fonte. Detalhes no item 8 do `relatorio.md`.
   - **Decisão:** trocar o `dbt_utils.expression_is_true` estrito por `custom_matching_rows_limit` com `max_share: 0.0001` (≈7.400 linhas), mantendo `severity: error` e `where: __most_recent_date_cnpj__`. A folga é de ~7x sobre a taxa observada.
   - **Alternativas descartadas:** manter o teste estrito (falha sempre por problema da fonte) e corrigir o dado no modelo (alteraria o publicado em silêncio e exigiria reprocessar).
   - **Observação:** o limite é mais folgado que o de `simples` (1e-6) porque a taxa observada é cerca de 100x maior.
   - **Verificação no arquivo bruto: dispensada.** O teste passa com o limite atual e o limite é o controle contínuo; contar o arquivo inteiro não mudaria a decisão. Se a taxa se aproximar do limite, reabrir.

5. **Relacionamento por `cnpj_basico` com `empresas`: `custom_relationships`, contra a partição mais recente do pai.**
   - **Contexto:** o `custom_relationships` fazia `from {{ to }}` sem filtro, ou seja, compararia contra todas as partições de `empresas`. Isso sairia caro e mascararia órfãos (CNPJ presente só em partição antiga). Taxas medidas uma vez nas partições mais recentes (`simples` é snapshot): `simples` 2 em 50,8M (~4e-8), `socios` 33 em 28,5M (~1,2e-6), `estabelecimentos` 4134 em 73,8M (~5,6e-5).
   - **Decisão:** novo parâmetro opcional `to_where` no macro (padrão `None`, sem efeito nos demais datasets) e `custom_relationships` em `severity: warn` (hoje `error`) nas três tabelas, com `to_where` apontando para `max(data_referencia)` de `empresas`. Limiares com ~10x de folga: `simples` 1e-6, `socios` 1e-5, `estabelecimentos` 5e-4.
   - **Alternativas descartadas:** `relationships` nativo (compara com todas as partições do pai e não tolera falhas) e criar um macro novo (duplicaria a lógica).

6. **Colunas de contato: sem teste de formato em DDD, telefone, fax e e-mail; CEP corrigido no modelo.**
   - **Contexto:** o relatório mostrou placeholders (`0`, espaços) e zeros à esquerda em DDD e telefone, e e-mails digitados livremente na fonte (~38,9 mil avisos). No CEP, 82 mil linhas perdiam o zero à esquerda.
   - **Decisão:** não aplicar teste em `ddd_*`, `telefone_*`, `fax` nem regex em `email` (o `no_invalid_chars` de `email` permanece). O CEP é corrigido no modelo (`lpad` para 8 dígitos e valores só de zeros viram `NULL`) e o `regex_or_null` de `cep` ficava em `warn` (hoje `error`, decisão 8).
   - **Pendente:** o teste de CEP seguiu com 84.999 resultados após "renovar a partição" porque o modelo é incremental (filtra `data_referencia > max(data_referencia)` da tabela e faz `merge ... on false`, só insere). A partição já existente não é reescrita; é preciso apagá-la antes do `dbt run` ou usar `--full-refresh`.

7. **`cpf_representante_legal`: `regex_or_null` em `error`.**
   - **Decisão:** `^\*{3}[0-9]{6}\*{2}$` com `where: __most_recent_date_cnpj__`. A amostra mostrou só nulo e `***999999**`.

8. **Severidade: todos os testes do `schema.yml` em `error`.**
   - **Decisão:** os 12 testes que estavam em `warn` passaram a `error`: os 8 `date_in_range` de datas, os 3 `custom_relationships` de `cnpj_basico` com `empresas` e o `regex_or_null` de `cep`.
   - **Efeito:** na rodada de 31 testes só o `cep` falhava (84.999 linhas, por a partição não ter sido reconstruída). Com `error` ele passa a reprovar a execução até o modelo ser reprocessado.
   - **Risco:** os órfãos de `cnpj_basico` e as datas futuras agora derrubam o `dbt test`. Os limiares de `custom_relationships` têm ~10x de folga sobre a taxa medida.

9. **`not_null_proportion_multiple_columns` em `empresas`, `socios`, `simples` e `estabelecimentos`.**
   - **Contexto:** o macro reprova colunas com mais de 95% de nulos (`at_least` padrão 0,05). Nulos medidos na partição 2026-10-01 (`simples`: tabela inteira).
   - **Decisão:** `ignore_values` (nomes de colunas) por tabela, com `where: __most_recent_date_cnpj__` (exceto `simples`, sem filtro e sem colunas ignoradas):
     - `empresas`: `ente_federativo` (99,9%).
     - `socios`: `id_pais` (99,7%), `cpf_representante_legal` e `nome_representante_legal` (97,3%).
     - `estabelecimentos`: `nome_cidade_exterior` (99,9%), `id_pais` (96,9%), `situacao_especial` e `data_situacao_especial` (99,95%).
     - `simples`: nenhuma; a coluna mais vazia é `data_exclusao_mei` (60%).

10. **CEP: o que não tem exatamente 8 dígitos depois da limpeza vira `NULL` no modelo.**
   - **Contexto:** depois do `lpad`, o teste de CEP ainda reprovava 1 linha com `92200-35`. Não é um CEP válido e parece truncado na fonte (o valor real seria algo como `92200-350`, sem como saber).
   - **Decisão:** no `case` de `cep` de `br_rf_cnpj__estabelecimentos.sql`, o `else` deixa de devolver o valor cru: só passa o que casa `^[0-9]{8}$`; o restante vira `NULL`. O teste `regex_or_null` de `cep` fica estrito, sem exceção.
   - **Alternativas descartadas:** (1) remover o traço: deixaria 7 dígitos e o `lpad` fabricaria `09220035`, um CEP inexistente com aparência válida; (2) tolerar o traço no teste: o regex `5-3` não aceita essa linha, e aceitar `5-2` enfraqueceria o teste para acomodar um caso.
   - **Efeito:** altera o dado publicado em poucas linhas (observada 1 em 73,8M), de valor malformado para `NULL`, consistente com o tratamento dos CEPs só de zeros.
   - **Pendente:** apagar a partição `2026-10-01` no dev, rodar `dbt run --select br_rf_cnpj__estabelecimentos` e repetir o teste.

11. **`socios`: `select distinct *`**
   - **Contexto:** não existe chave natural declarada. Na partição 2026-10-01 (28.495.961 linhas), os candidatos testados deram de 293 a 490 linhas duplicadas (~1e-5 a 1,7e-5), e nenhum chegou a zero. Com a chave `cnpj_basico, tipo, documento, nome, qualificacao, data_entrada_sociedade` são 126 grupos e 424 linhas (298 excedentes).
   - **Decisão:** o select final de `br_rf_cnpj__socios.sql` passa a `select distinct *`, removendo linhas totalmente idênticas. Não será criado teste de unicidade para `socios`; a decisão em aberto sobre a chave fica encerrada.
   - **Alternativas descartadas:** `custom_unique_combinations_of_columns` com tolerância (o macro calcula a taxa por grupo duplicado, não a soma das duplicatas, e quase nunca reprovaria) e teste estrito com a chave ampliada (candidato C).
   - **Limitações:**
     - O `distinct` não une os ~5 casos do tipo 3 cujas colunas do representante diferem; essas linhas permanecem.
     - O modelo é incremental: só vale para partições novas. As partições já publicadas mantêm as duplicatas até serem apagadas e reprocessadas.