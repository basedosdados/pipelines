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

####  **Nas tabelas de dados:**
- `custom_dictionary_coverage`: valida que **todas as colunas de código possuem entrada no dicionário**
  - Aplicado em: `empresas`, `socios`, `estabelecimentos`
  - Escopo temporal: apenas dados mais recentes (`__most_recent_date__`)

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

