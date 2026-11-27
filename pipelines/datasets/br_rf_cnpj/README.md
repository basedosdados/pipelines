# Documentação do Conjunto de Dados: CNPJ (Cadastro de Pessoa Jurídica)

Pipeline de dados de CNPJ (empresas, estabelecimentos, sócios e simples) da
Receita Federal. Este README documenta as mudanças estruturais feitas na
migração desta pipeline (antes `br_me_cnpj`), o motivo de cada uma e seus
impactos no reprocessamento histórico.

## 1. Correção de encoding na leitura dos CSVs

Os arquivos CSV de origem da Receita Federal são publicados em `latin1`
(`ISO-8859-1`), não em `utf-8`. A pipeline anterior lia esses arquivos com o
encoding incorreto, o que inseria caracteres inválidos em colunas de
texto livre — por exemplo, `razao_social`, `nome_fantasia` e demais campos com
acentuação.

A correção lê o CSV de origem como `latin1` e regrava o CSV intermediário como
`utf-8`, preservando a acentuação corretamente (ver `process_csv_*` em
`pipelines/shared/rf_cnpj/utils.py`).

## 2. Necessidade de reprocessamento desde 2023

Como o bug de encoding afeta todo dado ingerido pela pipeline antiga, os dados
de referência precisaram ser reprocessados do zero para corrigir os caracteres já gravados incorretamente em BigQuery. 

**Data Range:** 2023-06-10 a 2026-05-10

**Colunas afetadas:**
- br_me_cnpj.estabelecimentos : bairro, complemento, email, logradouro, nome_fantasia, numero, tipo_logradouro

- br_me_cnpj.empresas: razao_social

- br_me_cnpj.socios: nome

## 3. Migração de `br_me_cnpj` para `br_rf_cnpj`

O dataset estava indexado sob a organização ME (Ministério da Economia), que
não é a fonte dos dados. A fonte real é a Receita Federal (RF), então o dataset
foi renomeado/migrado para `br_rf_cnpj` para refletir corretamente a
organização de origem. Toda referência à organização e aos metadados do
dataset (incluindo joins em `br_bd_diretorios_brasil__empresa`) foi atualizada
de `br_me_cnpj` para `br_rf_cnpj`.

## 4. Particionamento por data de referência (`folder_date`, não `last_modified_date`)

O particionamento passou a ser feito pela **data de referência do arquivo na
fonte** (`folder_date` — o mês/competência a que os dados dizem respeito), e
não mais pela `last_modified_date` (data em que o arquivo foi modificado/gerado
pela Receita Federal). Isso alinha o particionamento ao período que os dados de
fato representam.


## 5. Pipeline recorrente de dicionário

A tabela `br_rf_cnpj__dicionario` traduz os códigos usados nas demais tabelas
(qualificação do responsável, motivo da situação cadastral, porte, etc.) para seus valores legíveis
(`chave` - `valor`). Ela é materializada como `table`, não incremental,
apenas o snapshot mais atual dos códigos.

O conteúdo vem de duas origens, unidas no modelo:

- **Arquivos de dicionário publicados pela própria Receita Federal** junto
  com a publicação mensal: arquivo de `Qualificacoes`, `Paises` e `Motivos`.
  Esses arquivos são baixados
  e processados por `process_csv_dicionario`
  (`pipelines/shared/rf_cnpj/utils.py`), que lê cada CSV `chave;valor` e adiciona `id_tabela`/`nome_coluna` para identificar a qual tabela e coluna cada código pertence.
- **Entradas manuais** (`dicionario_not_found`, no modelo dbt), cobrindo
  chaves que aparecem nos dados reais mas não constam nos arquivos oficiais
  da Receita Federal (ex.: códigos `36`, `994`/`393`, `8`/`9`/`32` sem
  correspondência na fonte). Sem essas entradas, os valores ficam sem
  tradução no dicionário.

CNAE (principal e secundária), natureza jurídica e município não entram no
dicionário: são resolvidos pelos diretórios de `br_bd_diretorios_brasil`,
declarados como `directory_column` das respectivas colunas.

As chaves de cada relacionamento vêm de `get_table_unique_keys`, que consulta a
tabela **materializada** (`basedosdados-dev.br_rf_cnpj.<tabela>`), não a
staging. A staging é tabela externa sobre CSV acumulado por competência, onde
ler uma coluna custa o arquivo inteiro de todos os meses; a materializada é
nativa e particionada, e cobra só a coluna consultada. O dicionário passa
portanto a depender da materialização das demais tabelas, e seu flow roda às
14:35, depois das quatro.

O projeto que fatura essas consultas é definido por `BD_BILLING_PROJECT_ID`,
com `basedosdados` como padrão. Execuções fora do worker precisam apontá-la
para `basedosdados-dev`, onde a credencial de desenvolvimento tem permissão de
criar job.

Como `simples` e `dicionario` não têm cobertura temporal por competência
(`NonHistorical`), o polling dessas tabelas compara contra `Table.Update`
(quando rodamos pela última vez), e não contra `Coverage`, ao decidir se há
dado novo a processar.

## 6. Tabelas legado (`*_legado`) e materialização full-refresh

Foram criadas tabelas `_legado` (`empresas_legado`, `estabelecimentos_legado`,
`socios_legado`) em staging, contendo os dados históricos migrados do
`br_me_cnpj`. Os modelos principais (`br_rf_cnpj__empresas`,
`br_rf_cnpj__estabelecimentos`, `br_rf_cnpj__socios`) são incrementais e, na
materialização **full-refresh** (primeira execução / quando não há
`is_incremental()`), fazem `union all` com os dados das tabelas `_legado` para
reconstruir o histórico completo (dados até 2023-04-30). Em execuções
incrementais normais, apenas os dados novos vindos da staging atual
(`empresas`, `estabelecimentos`, `socios`) são adicionados.


## 7. Alerta: houve erro na divulgação pela Receita Federal. A representação do documento de sócio pessoa jurídica passou de 14 para 8 caracteres a partir do mês de agosto de 2026.

Na tabela de sócios, para registros de sócio **pessoa jurídica** (`tipo = "1"`),
a coluna `documento` armazenava historicamente o **CNPJ completo (14
caracteres)** do sócio PJ. Em **agosto de 2026** a própria Receita Federal passou a
publicar esse campo com **apenas 8 caracteres** (o `cnpj_basico`, sem
filial/dígito verificador), reduzindo a granularidade da identificação do
sócio PJ na fonte.

Essa é uma mudança **na fonte**, não introduzida por esta pipeline.

## 8. Robustez do download (zips corrompidos, timeouts e retries)

Os arquivos de `estabelecimentos`, `empresas` e `socios` são grandes (o
`Estabelecimentos0.zip` passa de 2 GB) e baixados em chunks paralelos com
`Range`. Em outubro de 2026 o flow de estabelecimentos falhou porque zips
chegaram corrompidos (`Error -3 while decompressing data`) e, nas tentativas
seguintes, a listagem da Receita Federal estourou o timeout de 30 s. Execuções
anteriores do mesmo dia já mostravam o padrão (`empresas` com chunk falhando após
5 tentativas, `estabelecimentos` com o mesmo erro de descompressão). A hipótese
mais provável é um chunk gravado com conteúdo errado no seu offset, que só
aparecia na extração; ela não foi confirmada, então os avisos abaixo ajudam a
identificá-la caso volte.

O que mudou:

- **Validação por chunk:** a resposta precisa ter exatamente os bytes do range
  pedido; um `200` em offset maior que 0 também é rejeitado. Resposta inválida
  cai no retry do chunk (aviso `Falha no download do chunk ...`).
- **Validação do zip:** `testzip()` antes de extrair, tratando também
  `zlib.error`. Se o zip chegar corrompido, só aquele arquivo é baixado de novo
  (até `DOWNLOAD_ZIP_MAX_ATTEMPTS`, hoje 3) e, esgotadas as tentativas, a task
  falha com `RuntimeError`. Antes, o `BadZipFile` era só logado e os dados
  daquele arquivo se perdiam em silêncio.
- **Listagem WebDAV (`PROPFIND`):** timeout de `PROPFIND_TIMEOUT` (120 s) e até
  `PROPFIND_MAX_ATTEMPTS` (4) tentativas com backoff de 10, 20 e 40 s, tanto no
  check de atualização quanto em `get_table_files`.
- **Retry sem refazer o que já ficou pronto:** a task `download_and_process_files` tem `retries=3` e
  recomeça do início. Cada arquivo processado deixa um marcador em
  `tmp/br_rf_cnpj/done/<tabela>/<folder_date>_<i>` (fora de `input` e `output`), 
  e o retry pula os arquivos já feitos. O marcador guarda quantas linhas aquele
  arquivo mandou gravar, e o retry soma esse número à contagem da seção 9.
  Como a escrita de `estabelecimentos` é 
  em modo append, as sobras `estabelecimentos_<i>.csv` do arquivo que estava em 
  processamento são apagadas  antes de reprocessá-lo, para não duplicar linhas.

Os marcadores moram no `tmp/` do pod; como cada execução roda em um pod novo,
não há risco de marcador de uma competência anterior.

## 9. Conferência das linhas gravadas antes do upload

No fim de `download_and_process_files`, `check_rows_written` (`pipelines/utils/data_tests/rows.py`)
compara as linhas que `process_csv_*` e `process_manual_dictionaries` passaram
ao `to_csv` com as linhas dos CSVs em `tmp/br_rf_cnpj/output/<tabela>`, a pasta
que vai para o upload. Se os números não batem, a task falha com `ValueError`
antes de subir qualquer arquivo.

As linhas "lidas" vêm do que cada `process_csv_*` devolve. Em `estabelecimentos`
é o total de linhas lidas do CSV (`rows_in`), não o que foi gravado: assim, uma
linha que some porque a `sigla_uf` não está em `UFS` deixa de bater com a
pasta de saída e a task falha. Não há tolerância de perda de linhas. Nas
demais tabelas, a contagem é feita logo antes de cada escrita, então só o que o
dicionário remove no `dropna` e no `drop_duplicates` fica fora da conta.

A conferência supõe que a pasta de saída só tem o que a execução atual gravou,
o que vale no pod novo de cada execução. Numa execução local, uma competência
anterior que ficou em `output/<tabela>` faz a conferência falhar. O `data.csv`
do `dicionario` é escrito em modo append por várias tabelas na mesma execução;
por isso `download_and_process_files` o apaga uma vez, antes do loop das tabelas,
e um retry recomeça o dicionário do zero em vez de repetir linhas.

## 10. Implementação de testes

### Testes de integridade do pipeline (Python)

Rodam **antes da carga**, em `pipelines/datasets/br_rf_cnpj/`, e devem falhar o flow.

1. **Soma das partições de UF = linhas de entrada.**
   - Abarcada pela validação do item 9.
2. **Contagem de arquivos baixados vs. esperado.**
   - `get_table_files` já devolve a lista da fonte. Comparar `len(files)` com o esperado (10 por tabela segmentada) e com os arquivos efetivamente baixados e processados.
   - O número 10 deve ir para `constants.py` (ex.: `EXPECTED_FILES_PER_TABLE`).
   - Se a Receita mudar a quantidade de fragmentos, falhar com aviso em vez de carregar incompleto.

### Testes dbt

#### Testes dbt simples

| Item | Como | Observações |
|---|---|---|
| Valores aceitos (seção "Extra") | `dbt_utils.accepted_values` por coluna | `simples.opcao_simples`/`opcao_mei`. Os valores estão como string nos modelos (`cast(... as string)`), então listar `'1','2'` com `quote: true`. Confirmar antes se `simples.opcao_*` é `S/N` ou `0/1`. |
| `socios`: `is_row_count_increasing` | Em `data_referencia` | Igual a `empresas`. |
| `id_municipio` não nulo se `sigla_uf` não é EX/BR | `dbt_utils.expression_is_true` ou `not_null` com `where` combinado | Combinar `__most_recent_date_cnpj__ and sigla_uf not in ('EX','BR')`. O macro faz `replace` simples e deve funcionar. Validar com `dbt compile`. |
| `simples`: filtro na unicidade | Avaliar `where` | Snapshot único. Manter sem filtro de partição, mas confirmar o custo da query. |

#### Testes de formato e datas

- **`regex_or_null(column_name, pattern, severity)`:** cobre `cep` e `cpf_representante_legal`. `ddd_*`, `telefone_*`/`fax` e `email` ficaram sem teste de formato (ver decisão 4).
  - `cep`: `^[0-9]{8}$`
  - `cpf_representante_legal`: aceitar o padrão mascarado `***XXXXXX**` ou vazio. Definir o padrão a partir de uma amostra real.
- **`date_in_range(column_name, min_date='1900-01-01')`:** `col is null or col between date(min) and current_date()`. Aplicado em `data_inicio_atividade`, `data_situacao_cadastral`, `data_situacao_especial`, `data_entrada_sociedade`, `data_opcao_simples`, `data_exclusao_simples`, `data_opcao_mei`, `data_exclusao_mei`
- **Ordem entre datas:** `dbt_utils.expression_is_true` em nível de modelo, tolerando nulos, ex.: `(a is null or b is null or a <= b)`. Tolerar uma taxa mínima de violações em vez de exigir zero. Ver decisão 2.
  - `data_inicio_atividade <= data_situacao_cadastral`
  - `data_exclusao_simples >= data_opcao_simples`
  - `data_exclusao_mei >= data_opcao_mei`

- **`socios.documento`:** `expression_is_true` com `case`:
  - `tipo='1'` exige 8 dígitos (a fonte traz o `cnpj_basico`, não o CNPJ de 14 dígitos)
  - `tipo='2'` exige CPF mascarado (`***999999**`)
  - `tipo='3'` exige `documento` nulo (a amostra mostrou que é sempre nulo)

#### Unicidade e testes restantes

- **Caracteres inválidos (issue 1533):** teste `no_invalid_chars` para `razao_social`, `email`, `logradouro` etc (ex.: `�`, controles `\x00-\x1f`). 
- **`not_null_proportion_multiple_columns` nas 5 tabelas:** o macro marca como falha a coluna com proporção de nulos acima de `1 - at_least` 
- **`socios`: chave de unicidade.** Não existe chave natural declarada. Candidatas a testar na partição recente:
  - `(cnpj_basico, tipo, documento, nome, qualificacao, data_entrada_sociedade)`
  - com e sem `cpf_representante_legal`
  - Como o CPF vem mascarado, haverá colisões legítimas. Por isso usar `custom_unique_combinations_of_columns` (tolera 5%) e **medir a taxa real antes de fixar o limiar**. 

### Decisões tomadas

1. **Datas: manter o teste `date_in_range` e não aplicar `validate_date_range` nos modelos.**
   - **Contexto:** os modelos de `br_rf_cnpj` só usam `safe_cast(... as date)`, que zera formato inválido mas deixa passar datas bem formadas e absurdas (ex.: `1850-01-01`, datas futuras). A macro `validate_date_range` hoje só é usada em `br_tse_*` e `br_ms_sia`.
   - **Decisão:** manter o teste, que detecta e avisa sem alterar o dado publicado.
   - **Medição:** os 8 `date_in_range` passaram (então ainda em `warn`), ou seja, não há datas fora de 1850 até o ano seguinte nem `9999-12-31`. Sentinelas inválidas (ex.: `0000-00-00`) viram `NULL` no `safe_cast` e só seriam pegas pelo `not_null_proportion_multiple_columns`.

2. **Ordem das datas em `simples` e `estabelecimentos` : tolerar uma taxa mínima de violações em vez de exigir zero.**
   - **Contexto:** `data_exclusao_simples < data_opcao_simples` e `data_exclusao_mei < data_opcao_mei` ocorrem na origem (RFB). Medido uma vez em `basedosdados.br_rf_cnpj.simples`: 3 casos em ~25,6M linhas comparáveis (Simples) e 3 em ~20,1M (MEI), cerca de 1e-7. Total da tabela: ~50,8M linhas.  `data_inicio_atividade > data_situacao_cadastral` ocorre em 1065 linhas da partição mais recente (~74M) de `estabelecimentos`, cerca de 1,4e-5. Em uma amostra de 15 CNPJs (primeiros casos do arquivo bruto, não aleatória), as datas no BigQuery são idênticas às do arquivo da Receita, e o mapeamento de colunas do código confere. A inversão vem da fonte.
   - **Decisão:** trocar os `dbt_utils.expression_is_true` estritos por `custom_matching_rows_limit` com `max_share: adaptado a cada tabela, mantendo `severity: error` e `where: __most_recent_date_cnpj__`, no caso de `estabelecimentos`. 
   - **Alternativas descartadas:** manter o teste estrito (falharia sempre por um problema que não controlamos) e corrigir o dado no modelo (alteraria o publicado em silêncio e exigiria reprocessar).
   - **Observação:** o denominador é o total de linhas da tabela, não só as comparáveis. Isso deixa o teste um pouco mais permissivo, mas a diferença é irrelevante nessa escala.

3. **CNPJ alfanumérico: só testes de formato**
   - **Contexto:**A partir de 2026 o CNPJ passa a aceitar letras. Isso afeta:

        - **`fill_left_zeros`:** `astype(str).str.zfill` funciona com letras, mas `astype(str)` sobre `NaN` produz a string `"nan"`, que vira `"00000nan"`. Já é um risco hoje.
       - **Modelos SQL:** `lpad(safe_cast(cnpj_basico as string), 8, '0')` e `lpad(cnpj, 14, "0")` funcionam com letras, mas qualquer `cast(... as int64)` em CNPJ quebraria.
       - **Dígito verificador:** o cálculo muda (módulo 11 sobre valores ASCII − 48). Se algum teste ou rotina valida o DV, precisa ser ajustado.
       - **Novos testes:** formato `^[A-Z0-9]{12}[0-9]{2}$` para `cnpj` e `^[A-Z0-9]{8}$` para `cnpj_basico`.

       **Investigação (2026-10-28):**
       - **Código:** o Python lê os CSVs com `dtype=str` e os modelos só usam `lpad(safe_cast(... as string), N, '0')`. Não há `cast` de CNPJ para número no `br_rf_cnpj` nem nos consumidores (CVM, SICOR, `br_bd_diretorios_brasil__empresa`). 
       - **Dado:** na partição 2026-10-01 de `estabelecimentos` (73,8M linhas) há 0 CNPJs com letras e 0 violações dos formatos alfanuméricos em `cnpj`, `cnpj_basico`, `cnpj_ordem` e `cnpj_dv`. Também 0 casos com `nan`.
       - **Testes adicionados:** `regex_or_null` com `severity: error` em `cnpj` (`^[A-Z0-9]{12}[0-9]{2}$`), `cnpj_basico` (`^[A-Z0-9]{8}$`, nas 4 tabelas), `cnpj_ordem` (`^[A-Z0-9]{4}$`) e `cnpj_dv` (`^[0-9]{2}$`). 
   - **Decisão:** testes de formato em `error`, já alfanuméricos, para pegar também o `"00000nan"` que o `fill_left_zeros` produziria com célula vazia.

4. **Colunas de contato: sem teste de formato em DDD, telefone, fax e e-mail; CEP corrigido no modelo.**
   - **Contexto:** o relatório mostrou placeholders (`0`, espaços) e zeros à esquerda em DDD e telefone, e e-mails digitados livremente na fonte (~38,9 mil avisos). No CEP, 82 mil linhas perdiam o zero à esquerda.
   - **Decisão:** não aplicar teste em `ddd_*`, `telefone_*`, `fax` nem regex em `email` (o `no_invalid_chars` de `email` permanece). O CEP é corrigido no modelo (`lpad` para 8 dígitos e valores só de zeros viram `NULL`) e o `regex_or_null` de `cep` ficava em `warn` (hoje `error`, decisão 6).

5. **`cpf_representante_legal`: `regex_or_null` em `error`.**
   - **Decisão:** `^\*{3}[0-9]{6}\*{2}$` com `where: __most_recent_date_cnpj__`. A amostra mostrou só nulo e `***999999**`.

6. **Severidade: todos os testes do `schema.yml` em `error`.**
   - **Decisão:** os 12 testes que estavam em `warn` passaram a `error`: os 8 `date_in_range` de datas e o `regex_or_null` de `cep`.

7. **`not_null_proportion_multiple_columns` em `empresas`, `socios`, `simples` e `estabelecimentos`.**
   - **Contexto:** o macro reprova colunas com mais de 95% de nulos (`at_least` padrão 0,05). Nulos medidos na partição 2026-10-01 (`simples`: tabela inteira).
   - **Decisão:** `ignore_values` (nomes de colunas) por tabela, com `where: __most_recent_date_cnpj__` (exceto `simples`, sem filtro e sem colunas ignoradas):
     - `empresas`: `ente_federativo` (99,9%).
     - `socios`: `id_pais` (99,7%), `cpf_representante_legal` e `nome_representante_legal` (97,3%).
     - `estabelecimentos`: `nome_cidade_exterior` (99,9%), `id_pais` (96,9%), `situacao_especial` e `data_situacao_especial` (99,95%).
     - `simples`: nenhuma; a coluna mais vazia é `data_exclusao_mei` (60%).

8. **CEP: o que não tem exatamente 8 dígitos depois da limpeza vira `NULL` no modelo.**
   - **Contexto:** depois do `lpad`, o teste de CEP ainda reprovava 1 linha com `92200-35`. Não é um CEP válido e parece truncado na fonte (o valor real seria algo como `92200-350`, sem como saber).
   - **Decisão:** no `case` de `cep` de `br_rf_cnpj__estabelecimentos.sql`, o `else` deixa de devolver o valor cru: só passa o que casa `^[0-9]{8}$`; o restante vira `NULL`. O teste `regex_or_null` de `cep` fica estrito, sem exceção.
   - **Alternativas descartadas:** (1) remover o traço: deixaria 7 dígitos e o `lpad` fabricaria `09220035`, um CEP inexistente com aparência válida; (2) tolerar o traço no teste: o regex `5-3` não aceita essa linha, e aceitar `5-2` enfraqueceria o teste para acomodar um caso.
   - **Efeito:** altera o dado publicado em poucas linhas (observada 1 em 73,8M), de valor malformado para `NULL`, consistente com o tratamento dos CEPs só de zeros.

9. **`socios`: `select distinct *` no modelo e nenhum teste de unicidade.**
   - **Contexto:** não existe chave natural declarada. Na partição 2026-10-01 (28.495.961 linhas), os candidatos testados deram de 293 a 490 linhas duplicadas (~1e-5 a 1,7e-5), e nenhum chegou a zero. Com a chave `cnpj_basico, tipo, documento, nome, qualificacao, data_entrada_sociedade` são 126 grupos e 424 linhas (298 excedentes): 98 grupos de tipo 1 / qualificação 20 (61 empresas, até 16 linhas por grupo, idênticas nas demais colunas medidas) e 27 de tipo 3 / qualificação 37 (`documento` sempre nulo; em 5 grupos as colunas do representante diferem).
   - **Decisão:** Não será criado teste de unicidade para `socios`; a decisão em aberto sobre a chave fica encerrada.
   - **Alternativas descartadas:** `custom_unique_combinations_of_columns` com tolerância (o macro calcula a taxa por grupo duplicado, não a soma das duplicatas, e quase nunca reprovaria) e teste estrito com a chave ampliada.

10. **`estabelecimentos`: sem limpeza de caracteres de controle.** Chegou a
    haver `regexp_replace(col, r'[\x00-\x1f]+', ' ')` com `trim` e `nullif` em
    `nome_fantasia`, `logradouro`, `numero`, `complemento` e `bairro`, mas foi
    removida: as ocorrências são proporcionalmente poucas e a limpeza aumentaria o
    processamento de uma tabela grande. Medido no staging (73.809.379 linhas), por
    coluna: `nome_fantasia` 10, `logradouro` 10, `numero` 21, `complemento` 18,
    `bairro` 3 e `cep` 0. No conjunto das cinco colunas são 61 linhas
    (~8,3e-7), e só 7 têm `\n` ou `\r`. As colunas voltam a ser só
    `safe_cast(... as string)`, como na `main`. O teste `no_invalid_chars` cobre só U+FFF0–U+FFFF, não os
    caracteres de controle. Se eles passarem a causar problema (por exemplo, quebra
    de linha em CSV), reabrir. Como o modelo é incremental, a mudança só vale para
    as partições novas: qualquer partição de dev construída com a limpeza precisa
    ser apagada e reprocessada.

## Resumo
* Correção de encoding: os CSVs de origem da Receita Federal eram lidos incorretamente, gerando caracteres inválidos em campos de texto (ex.: razão social, nome fantasia). 
* Reprocessamento histórico: dados de referência a partir de 2023 foram reprocessados para corrigir os registros afetados pelo erro de encoding.
* Correção da organização de origem: dataset migrado de br_me_cnpj (Ministério da Economia) para br_rf_cnpj, refletindo corretamente a Receita Federal como fonte dos dados.
* Particionamento por data de referência: a partição das tabelas passou a ser baseada na data de referência do arquivo na fonte (competência dos dados), em vez da data de modificação do arquivo.
* Preservação do histórico: dados anteriores a maio/2023, hoje indisponíveis na fonte original, foram preservados em tabelas legado e continuam incluídos na base a cada atualização completa da tabela.
* Alerta de fonte: o documento do sócio pessoa jurídica (tabela `socios`, `tipo = "1"`) passou a ser publicado pela Receita Federal com 8 caracteres em vez dos 14 caracteres do CNPJ completo, a partir da competência 2026-08.
* Robustez do download: chunks e zips são validados, o zip corrompido é baixado de novo só para aquele arquivo, a listagem da Receita Federal tem timeout maior com retry, e um retry da task não refaz os arquivos já processados.
* Conferência das linhas gravadas: antes do upload, a task compara as linhas passadas ao `to_csv` com as gravadas na pasta da tabela e falha se diferirem.
