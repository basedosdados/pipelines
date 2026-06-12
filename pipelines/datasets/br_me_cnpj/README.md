# Documentação ME/CNPJ

Documento de registro de informações importantes sobre a base de dados de CNPJ da Receita Federal e o estado de estruturação da pipeline, últimas mudanças e entendimento dos _flows_.

---

## Sobre a fonte

- [Fonte de Dados](https://arquivos.receitafederal.gov.br/index.php/s/gn672Ad4CF8N6TK?dir=/Dados/Cadastros/CNPJ)
- **Organização**: Ministério da Economia (ME) / Receita Federal
- **Atualização**: Mensal 
---

## Estrutura de Tabelas

### Tabelas de Dados Segmentadas (10 arquivos por tabela)

As tabelas abaixo são disponibilizadas em 10 arquivos compactados diferentes na fonte, para otimizar download e processamento:

- **Empresas**: Dados cadastrais de empresas (razão social, natureza jurídica, porte, capital social)
- **Estabelecimentos**: Dados cadastrais de cada estabelecimento vinculado a uma empresa (endereço, CNAE, situação)
- **Sócios**: Informações de sócios/proprietários (CPF, nome, qualificação, data de entrada)

### Tabelas de Dados Únicas

- **Simples**: Informações de inscrição em Simples Nacional e MEI

### Tabelas de Dicionário

As tabelas abaixo são baixadas e consolidadas em uma única tabela `br_me_cnpj__dicionario`:

- **Qualificações**: Mapeamento de código - descrição de qualificação de sócio/responsável
- **Naturezas**: Mapeamento de código - natureza jurídica (tipo legal da empresa)
- **Motivos**: Mapeamento de código - motivo de alteração de situação cadastral
- **Países**: Mapeamento de código - nome de país (para sócios e estabelecimentos no exterior)
- **CNAEs**: Mapeamento de código - descrição de classe CNAE (atividade econômica)

---

## Fluxo de Extração e Processamento

O pipeline Prefect (`br_me_cnpj`) executa as seguintes etapas:

### 1: Descoberta de Dados

**Task**: `get_data_source_max_date()`
- Consulta a url da Receita Federal para listar todas as pastas de data disponíveis
- Extrai data máxima de pasta (`YYYYMMDD`) para download

### 2: Download e Descompactação

**Task**: `download_unzip_csv()`
- Para cada arquivo .zip da fonte, faz download assíncrono
- Descompacta em pasta `input/{table_id}/`
- Mantém rastreamento de arquivos já baixados para evitar redownload

### 3: Processamento de Dados

**Task**: `main()` - Roteia para funções específicas por tipo de tabela
Em `constants.py` foi adicionado um 

#### **3a. Processamento de Tabelas de Dados Segmentadas**

Funções: `process_csv_empresas()`, `process_csv_estabelecimentos()`, `process_csv_socios()`

Para cada arquivo `i` (0–9):
   - Remove artefatos de conversão de tipo (`".0"`)
   - Normaliza datas (DDMMYYYY - YYYY-MM-DD)
   - Capitaliza strings (ex: `JOÃO DOS SANTOS` - `João dos Santos`)
   - Remove espaços extras
   - Verifica se CNPJ tem comprimento correto (14 dígitos)
Salva parquet particionado por `ano` (ano extraído da data da Receita Federal)
   - Caminho: `output/{table_id}/ano=YYYY/data.parquet`

#### **3b. Processamento de Tabelas de Dicionário**

Função: `process_csv_dicionario(input_path, output_path, table_name)`

**Normalização de Chaves**:
   - Remove artefatos de tipo (`".0"`)
   - Remove leading zeros com regex: `r"(^0+)(?:[^0]+|0{1})"` - `""`. Exemplo: `"007"` - `"7"`, `"0000"` - `"0"`
**Relacionamento com Dados**:
   - Para cada relacionamento em `TABLE_CONFIGS[table_name]["relationships"]`:
     - Extrai chaves únicas da coluna de dados correspondente (ex: `empresas.porte`)
     - Faz LEFT JOIN com dicionário (mantém chaves sem valor)
     - Permite identificar códigos faltantes em dicionário-fonte
**Tratamento de Países** (especial):
   - Chama `find_missing_countries()`: agrega países não presentes em CSV, usando dados da COMEX e Diretórios
   - Chama `format_country_name()`: que essencialmente reordena termos dos nomes de países, antes em ordem inversa e separados por vírgula (Exemplo: `'Pacífico, Ilhas do (Administ.dos Eua)'` para `'Ilhas do Pacífico (Administ. dos Eua)'`); coloca termos entre parênteses no final do nome e normaliza caracteres (palavras convertidas para `title`, com exceção de preposições e conectivos)
**Limpeza**:
   - Remove linhas onde ambos `chave` e `valor` são nulos
   - Deduplica por (`id_tabela`, `nome_coluna`, `chave`, `valor`)
   - Aplica `cobertura_temporal = "(1)"` (cobertura única)
**Saída**: CSV consolidado
   - Caminho: `output/dicionario/data.csv`
   - Colunas: `id_tabela`, `nome_coluna`, `chave`, `cobertura_temporal`, `valor`

#### **3c. Processamento de Simples Nacional**

Função: `process_csv_simples()`

- Leitura simples de CSV
- Normaliza datas e tipos
- Saída: Parquet único (não segmentado)

### 4: Upload para BigQuery (Dev)

**Task**: `create_table_dev_and_upload_to_gcs()`
- Sobe Parquets para GCS bucket de staging
- Cria tabela em `basedosdados-dev.br_me_cnpj_staging.<table_id>`

### 5: Materialização DBT

**Task**: `run_dbt(dbt_command="run/test")`
- Executa modelos SQL em `models/br_me_cnpj/`
- Cada tabela de dados recebe seu próprio modelo:
  - `br_me_cnpj__empresas.sql`
  - `br_me_cnpj__estabelecimentos.sql`
  - `br_me_cnpj__socios.sql`
  - `br_me_cnpj__simples.sql`
  - `br_me_cnpj__dicionario.sql` (**Novo**)

### 6: Publicação em Produção

**Task**: `create_table_prod_gcs_and_run_dbt()` (se `materialize_after_dump=True`)
- Replica dados para `basedosdados.br_me_cnpj.<table_id>`
- Executa modelos e testes em ambiente de produção
- Atualiza metadados no backend Django

---

## Observações Importantes

#### **Códigos Inválidos em Dicionário**
Alguns códigos presentes em dados não existem em dicionário-fonte (esperado e tratado):
- **qualificacao_responsavel "36" em `empresas`**: valores tratados via `dicionario_not_found()`
- **id_país "8", "9", "393","994" em `socios` e `estabelecimentos`**: valores tratados via `dicionario_not_found()`
- **motivo_situacao_cadastral "32" em `estabelecimentos`**: valores tratados via `dicionario_not_found()`
- **cnae_fiscal_secundaria "6202100", "4761000" em `estabelecimentos`**: valores tratados via `dicionario_not_found()`

####  **Encoding:**
A [Issue 1533](https://github.com/basedosdados/pipelines/issues/1533) documenta um erro de _encoding_ encontrado nas tabelas de estabelecimentos e empresas. 

Para investigar o problema, foram seguidos os seguintes passos:

##### Investigação do erro:
Para investigar o problema, foram seguidos os seguintes passos:
   
   1. Com a macro a seguir, verifiquei todas as colunas tipo `STRING` das tabelas, em busca do caractere inválido:

   ```sql
   {% set query_colunas %}
      SELECT DISTINCT table_name, column_name
      FROM `basedosdados`.`br_me_cnpj`.INFORMATION_SCHEMA.COLUMNS
      WHERE data_type = "STRING"
      AND table_name <> "dicionario"
   {% endset %}

   {% set resultados = run_query(query_colunas) %}

   {% if execute and resultados %}
      
      {% for row in resultados %}
         {# Armazena os valores de cada linha nas variáveis #}
         {% set tabela = row[0] %}
         {% set coluna = row[1] %}

         SELECT 
               data, 
               '{{ tabela }}' AS id_tabela, 
               '{{ coluna }}' AS nome_coluna, 
               COUNT(*) AS linhas_caracteres_invalidos
         FROM `basedosdados`.`br_me_cnpj`.`{{ tabela }}`
         WHERE `{{ coluna }}` LIKE "%�%"
         GROUP BY data

         {# Adiciona o UNION ALL entre as queries, exceto na última iteração #}
         {% if not loop.last %}
               UNION ALL
         {% endif %}

      {% endfor %}

   {% else %}
      {# Fallback caso o dbt execute em modo de parse rápido #}
      SELECT NULL AS data, NULL AS id_tabela, NULL AS nome_coluna, NULL AS linhas_caracteres_invalidos
   {% endif %}
   ```

   2. Como resultado, temos os dados [Nesta Planilha](https://docs.google.com/spreadsheets/d/1W_nui54rNA4dPmmNlAN-YNMp3bdribDukCJMe2LwVt4/edit?usp=sharing):

   **Data Range:** 2023-06-10 a 2026-05-10
   **Colunas afetadas**:

   `br_me_cnpj.estabelecimentos` : bairro, complemento, email, logradouro, nome_fantasia, numero, tipo_logradouro

   `br_me_cnpj.empresas`: razao_social

   `br_me_cnpj.socios`:  nome

   A maioria são casos de cedilha: mais de 93% (4087/4379)#### 

##### Correção

Correções aplicadas aos dados em DEV seguem a seguinte lógica:

   1. Macros:

      ```sql
      {% macro apply_regex_replacements(column_name, regex_replacements) %}
         {% set ns = namespace(expr=column_name) %}
         {% for regex, replacement in regex_replacements %}
            {% set ns.expr %}
                  REGEXP_REPLACE(
                     {{ ns.expr }},
                     r"{{ regex }}",
                     r"{{ replacement }}"
                  )
            {% endset %}
         {% endfor %}
         {{ ns.expr }}
      {% endmacro %}
      ```

      ```sql
      {% macro apply_regex_to_columns(columns, regex_replacements) %}
         {%- for column_name in columns %}
         `{{ column_name }}` =
            {{ apply_regex_replacements(column_name, regex_replacements) }} 
         {%- if not loop.last %},{% endif %}
         {%- endfor %}
      {% endmacro %}
      ```

      ```sql
      {% macro generate_regex_updates(
         project_id,
         dataset_id,
         table_columns,
         regex_replacements
      ) %}

      {% for table_name, columns in table_columns.items() %}

      UPDATE `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
      SET
      {{ apply_regex_to_columns(columns, regex_replacements) }}

      WHERE data >= '2023-06-01'
      AND (
      {%- for column_name in columns %}
         REGEXP_CONTAINS(
            `{{ column_name }}`,
            r"[\x{fff0}-\x{ffff}]"
         )
         {%- if not loop.last %}
            OR
         {% endif %}
      {%- endfor %}
      );
      {% endfor %}
      {% endmacro %}
      ```

   2. Definição de casos mais gerais e particulares para a lista de **regex** e seus respectivos **replacements*:

      a. Casos de cedilha com til:

         ```
            {% set regex_replacements = [
               ["[\\x{fff0}-\\x{ffff}](ão|õe)", "ç\\1"],
               ["[\\x{fff0}-\\x{ffff}](ÃO|ÕE)", "Ç\\1"],
               ["[\\x{fff0}-\\x{ffff}](ÂO)", "ÇÃO"],
               ["[\\x{fff0}-\\x{ffff}](âo)", "ção"],
               ["[\\x{fff0}-\\x{ffff}](ÀO|Ã0|ÃPO|ÕA)","ÇÃO"],
               ["[\\x{fff0}-\\x{ffff}](ÔE)", "ÇÕE"],
               ["[\\x{fff0}-\\x{ffff}](ôe)", "çõe"],
               ["[\\x{fff0}-\\x{ffff}](ÒE)", "ÇÕE"]
            ] %}

         ```

      #### b. Casos com 'd' e apóstrofe (d'água, D'ÁGUA):

         ```
         {% set regex_replacements = [
            ["(D)[\\x{fff0}-\\x{ffff}](Á)", "\\1'\\2"],
            ["(d)[\\x{fff0}-\\x{ffff}](á)", "\\1'\\2"],
            ["(D)[\x{fff0}-\x{ffff}](´ AGUA)", "\1'ÁGUA"]
         ] %}
         ```
      #### c. Casos de cedilha com acento (açá, açú):

         ```
         {% set regex_replacements = [
            ["(A)[\\x{fff0}-\\x{ffff}](Ú)", "\\1Ç\\2"],
            ["(a)[\\x{fff0}-\\x{ffff}](ú)", "\\1ç\\2"],
            ["([^D])[\\x{fff0}-\\x{ffff}](Á)", "\\1Ç\\2"],
            ["([^d])[\\x{fff0}-\\x{ffff}](á)", "\\1ç\\2"]
         ] %}
         ```

      #### d. Outros

         ```
         {% set regex_replacements = [
            ["(LEN)[\\x{fff0}-\\x{ffff}](ÓIS)", "\\1Ç\\2"],
            ["(S)[\\x{fff0}-\\x{ffff}](Õ)", "SÃO"],
            ["(SU)[\\x{fff0}-\\x{ffff}](Ç)", "\\1Í\\2"],
            ["(JOS)[\\x{fff0}-\\x{ffff}](É|´E)", "\\1É"],
            ["(S)[\\x{fff0}-\\x{ffff}](¨N)","S/N"]
         ] %}
         ```
      #### Aplicação da query de correção:**

         ```sql
         {% set table_columns = {
            "empresas": [
               "razao_social"
            ],
            "socios": [
               "nome"
            ],
            "estabelecimentos": [
               "bairro",
               "complemento",
               "email",
               "logradouro",
               "nome_fantasia",
               "numero",
               "tipo_logradouro"
            ]
         } %}

         {{ generate_regex_updates(
            "basedosdados-dev",
            "br_me_cnpj",
            table_columns,
            regex_replacements
         ) }}
         ```

      #### e.  Casos particulares
         As ocorrências únicas restantes foram avaliadas para definir um _replacement_:

         * Buscando o endereço (logradouro, bairro ou complemento) no Google, junto com o ID do município para encontrar algo mais próximo da realidade;
         * Buscando o CNPJ no Google;
         * Buscando outras ocorrências do CNPJ na tabela

         Seguem as queries:

         ```sql         
         UPDATE `basedosdados-dev.br_me_cnpj.estabelecimentos`
         SET
            `email` = REGEXP_REPLACE(email, r'^[\x{fff0}-\x{ffff}]+$', null)
         WHERE data >= "2023-06-01"
         AND REGEXP_CONTAINS(email, r'[\x{fff0}-\x{ffff}]');


         UPDATE `basedosdados-dev.br_me_cnpj.estabelecimentos`
         SET
         complemento =
         REGEXP_REPLACE(
         REGEXP_REPLACE(
         REGEXP_REPLACE(
         REGEXP_REPLACE(
         REGEXP_REPLACE(
         REGEXP_REPLACE(
               complemento,
               r'1[\x{fff0}-\x{ffff}]· ANDAR',
               '1º· ANDAR'
            ),
               r'[\x{fff0}-\x{ffff}]+',
               ''
            ),
               r'CONCEI[\x{fff0}-\x{ffff}]Ã',
               'CONCEIÇÃO'
            ),
               r'[\x{fff0}-\x{ffff}]ÇAÍ POINT',
               'AÇAÍ POINT'
            ),
               r' DENOMINA[\x{fff0}-\x{ffff}]Ã',
               ' DENOMINAÇÃO'
            ),
               r'ANEXO [\x{fff0}-\x{ffff}]´A´',
               'ANEXO ´A´'
            ),
         bairro =
         REGEXP_REPLACE(
         REGEXP_REPLACE(
         REGEXP_REPLACE(
         REGEXP_REPLACE(
         REGEXP_REPLACE(
         REGEXP_REPLACE(
         REGEXP_REPLACE(
         REGEXP_REPLACE(
         REGEXP_REPLACE(
         REGEXP_REPLACE(
         REGEXP_REPLACE(
         REGEXP_REPLACE(
         REGEXP_REPLACE(
         REGEXP_REPLACE(
         REGEXP_REPLACE(
         REGEXP_REPLACE(
               bairro,
               r'BALAN[\x{fff0}-\x{ffff}]É',
               'BALANÇÉ'
            ),
               r'MACAP[\x{fff0}-\x{ffff}]+.+II',
               'MACAPÁ II'
            ),
               r'FL[\x{fff0}-\x{ffff}]ÓRIDA',
               'FLÓRIDA'
            ),
               r'MA[\x{fff0}-\x{ffff}]ÔNICA',
               'MAÇÔNICA'
            ),
               r'ARA[\x{fff0}-\x{ffff}]À',
               'ARAÇÁ'
            ),
               r'JARDIM[\x{fff0}-\x{ffff}]+.+EUROPA',
               'JARDIM EUROPA'
            ),
               r'JARDIM DE[\x{fff0}-\x{ffff}]ÇA ROSAII',
               'JARDIM DELLA ROSA'
            ),
               r'INH[\x{fff0}-\x{ffff}]ÚMA',
               'INHAÚMA'
            ),
               r'S[\x{fff0}-\x{ffff}]Ó',
               'SÃO'
            ),
               r'GR[\x{fff0}-\x{ffff}]ÇAS',
               'GRAÇAS'
            ),
               r'NA[\x{fff0}-\x{ffff}]õES',
               'NAÇÕES'
            ),
               r'VILA RA[\x{fff0}-\x{ffff}]À',
               'VILA ARAÇÁ'
            ),
               r'MER[\x{fff0}-\x{ffff}]ÊS',
               'MERCÊS'
            ),
               r'ACLIMA[\x{fff0}-\x{ffff}]ãO',
               'ACLIMAÇÃO'
            ),
               r'LEN[\x{fff0}-\x{ffff}]ÕIS',
               'LENÇÓIS'
            ),
               r'AC[\x{fff0}-\x{ffff}]ÇIAS',
               'ACÁCIAS'
            ),
         logradouro =
         REGEXP_REPLACE(
         REGEXP_REPLACE(
         REGEXP_REPLACE(
         REGEXP_REPLACE(
         REGEXP_REPLACE(
         REGEXP_REPLACE(
         REGEXP_REPLACE(
         REGEXP_REPLACE(
         REGEXP_REPLACE(
         REGEXP_REPLACE(
               logradouro,
               r'[\x{fff0}-\x{ffff}]+.+ESQUERDA',
               'Á ESQUERDA'
            ),
               r'quadra ``e[\x{fff0}-\x{ffff}]´',
               'quadra `e`'),

               r'MA[\x{fff0}-\x{ffff}]Ã',
               'MAÇÃ'
            ),
               r'GRA[\x{fff0}-\x{ffff}]ÓPOLIS',
               'GRAÇÓPOLIS'
            ),
               r'Lad[\x{fff0}-\x{ffff}]´ario',
               'Ladário'
            ),
               r'CONCEI[\x{fff0}-\x{ffff}]¿O',
               'CONCEIÇÃO'
            ),
               r'C[\x{fff0}-\x{ffff}]ÓRREGO',
               'CÓRREGO'
            ),
               r'ANTONIO TOM[\x{fff0}-\x{ffff}]É',
               'ANTONIO TOMÉ'
            ),
               r'COR[\x{fff0}-\x{ffff}]ÃES',
               'CORÇÕES'
            ),
               r'A[\x{fff0}-\x{ffff}]úcar',
               'AÇÚCAR'
            )
         WHERE data >= '2023-06-01'
         AND (
         REGEXP_CONTAINS(bairro, r'[\x{fff0}-\x{ffff}]')
         OR REGEXP_CONTAINS(complemento, r'[\x{fff0}-\x{ffff}]')
         OR REGEXP_CONTAINS(logradouro, r'[\x{fff0}-\x{ffff}]')
         OR REGEXP_CONTAINS(nome_fantasia, r'[\x{fff0}-\x{ffff}]')
         OR REGEXP_CONTAINS(numero, r'[\x{fff0}-\x{ffff}]')
         OR REGEXP_CONTAINS(tipo_logradouro, r'[\x{fff0}-\x{ffff}]')
         );


         UPDATE `basedosdados-dev.br_me_cnpj.empresas` 
         SET
         razao_social =      
            REGEXP_REPLACE(
               razao_social, 'CAIXA ESCOLAR MUNICIPAL �´TEREZINHA MARQUES DE OLIVEIRA�´',
               'CAIXA ESCOLAR MUNICIPAL TEREZINHA MARQUES DE OLIVEIRA')
         WHERE data >= "2023-06-01";

         ```
---

## Mudanças

### Implementação de Dicionário

**Adições:**
- Novo modelo SQL: `br_me_cnpj__dicionario.sql`
- Função de processamento: `process_csv_dicionario()` em utils.py
- Configuração de dicionários: em `constants.py` foi adicionado a atributo `TABLE_CONFIGS`, com  propriedades das tabelas, tais como:

   * `"dicionario"`: indicam se a tabela é componente do dicionário ou não;
   * `"manual"`: indica se a tabela de dicionário é manual (precisa ter chaves e valores determinados manualmente) ou se é original da fonte na url. Nesse caso, haverá um campo:
   * `"chaves_valores"`: existente apenas em tabelas em que `"manual"` é `True`;
   * `"segmentada"`: indicando se a tabela é particionada (em 10) arquivos ou não;
   * `"relationships"`: indicando para as tabelas de dicionário, quais colunas de outras tabelas são cobertas;
- Uso de `dicionario_not_found()` para códigos faltantes

**Melhorias:**
- Consolidação de múltiplos dicionários em uma única tabela materializada
- Normalização automática de códigos (remoção de leading zeros)
- Tratamento especial para nomes de países

**Obs.:**
- Tabelas de dados (`empresas`, `estabelecimentos`, `socios`) sem mudanças estruturais
- Houve apenas mudanças na hierarquia das pastas temporárias de input e output
- Novo modelo `dicionario` é apenas aditivo — não quebra queries existentes

