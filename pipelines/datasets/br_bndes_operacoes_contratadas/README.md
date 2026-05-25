# Documentação BNDES/Operações Contratadas

Este documento registra informações importantes sobre a base de dados de Operações Contratadas do BNDES. O BNDES (Banco Nacional de Desenvolvimento Econômico e Social) disponibiliza, em sua Central de Downloads de Transparência, os dados de operações contratadas de financiamento direto e indireto não automático.

## Sobre a fonte

- [Fonte de Dados](https://www.bndes.gov.br/wps/portal/site/home/transparencia/centraldedownloads)
- **Organização**: Banco Nacional de Desenvolvimento Econômico e Social (BNDES)
- **Atualização**: Mensal, de acordo com cabeçalho do arquivo
- **Cobertura temporal**: Histórico de operações contratadas (2002-Presente)

## Tabelas

### 1. operacoes_contratadas_forma_direta_e_indireta_nao_automatica

Operações contratadas na forma direta e indireta não automática. Cada contrato pode ter um ou mais subcréditos. Cada linha da planilha corresponde a um subcrédito diferente. Por isso, há linhas supostamente duplicadas mas que segundo as informações acima, retiradas do arquivo, correspondem a subcréditos distintos.

**Fonte de Dados**: 
- [URL Arqvuivo](https://www.bndes.gov.br/arquivos/central-downloads/operacoes_financiamento/naoautomaticas/naoautomaticas.xlsx)
- O arquivo possui 3 abas:

  **a) SITE:** Aba com
    
    - **Cabeçalho**: 3 primeiras linhas, que possuem a data de apuração dos dados (coluna `data_apuracao`da tabela final) e a data de cobertura dos dados, na forma de um intervalo de datas do tipo `%s/%m/%Y`. Ambas as informações são extraídas por regex na função `get_xlsx_metadata` em `utils.py`;
    - **Dados**: dados das operações a serem extraídos;
  
  **b) DISCLAIMER:** informações e descrições sobre as colunas, valores esperados, esclarecimentos, cálculos aplicados, referências para mais informações. Essas informações foram usadas de maneira subjetiva para interpretar as colunas e preencher as arquiteturas e metadados no backend;

  **c) DE-PARA_CNAE:** tabela de equivalência entre diferentes níveis de código CNAE e agrupamentos de setor e subsetor elaborados pelo BNDES. Esta aba gá origem à tabela auxiliar `cnaes_agrupados_bndes`, abordada na próxima subseção.

#### Observações sobre Dados

**a) Registros Duplicados**:

Encontrados 689 registros duplicados com base no conjunto completo de colunas. As duplicatas identificadas incluem múltiplos registros do mesmo cliente, CNPJ, projeto e características contratuais. 
Na aba DISCLAIMER, há a informação de que "Cada contrato pode ter um ou mais subcréditos, sendo que cada subscrédito pode ser caracterizado por condições financeiras distintas. Cada linha da planilha corresponde a um subcrédito diferente." Por isso, mesmo havendo duplicatas (mesmo comparando combinações de **todas** as colunas da tabela, a interpretação é de que são subcréditos distintos com características iguais e, por isso, linhas com todas as colunas iguais.
 
**b) Distribuição de Valores Nulos por Coluna**:

- `id_municipio`: 7699 valores nulos (contratos vinculados, não a Unidades da Federação ou Municípios específicos, mas a outros níveis);
- `tipo_fonte_recursos`: 775 valores nulos;
- `nome_instituicao_financeira_credenciada`: 19.348 valores nulos;
- `cnpj_instituicao_financeira_credenciada`: 19.348 valores nulos;
- `tipo_excepcionalidade`: 23.202 valores nulos;
- `situacao_contrato`: 268 valores nulos;
- `grupo_cnae`: 255;
- `classe_cnae`: 497;
- `subclasse_cnae`: 5496.

Demais colunas não apresentam valores nulos, indicando que informações essenciais de identificação, datas, valores e classificações estão sempre preenchidas.
Essa distribuição foi importante para definir testes 

**c) CNAEs**:

A coluna "Subsetor CNAE - código" traz vinculação com CNAEs em diferentes níveis. Por mais que os valores sejam sempre códigos de 8 caracteres, nem sempre são subclasses CNAE válidas. Em muitos casos, há uma vínculação com a divisão ou o grupo e os demais caracteres que indicariam classes e subclasses são preenchidos com zeros ('0'), no que foi interpretado como indicativo de que todos os subníveis sob aquele grupo ou aquela divisão podem ser vinculados à linha em questão. 

Ex.: O valor "B0600000" não corresponde a nenhuma suclasse CNAE presente em  `basedosdados.br_bd_diretorios_brasil.cnae_2`, mas a classe "B06000" existe. Assim, a interpretação é de que o vínculo ocorre entre essa linha da tabela de operações e uma classe CNAE. 

A solução foi separar cada nível da hierarquia do CNAE presente nos valores da coluna "Subsetor CNAE - código" e cruzar com os diretórios, mantendo apenas o que existe.

**d) CNPJs**:

Valores atípicos foram identificados na fonte original: `77700001023743`, `77700001139504` e `77700001123974`. Estes CNPJs começam com o padrão `77700001%` que não corresponde a registros válidos no diretório de empresas (`br_bd_diretorios_brasil.empresa`). 

Este é um padrão recorrente na fonte BNDES para operações especiais ou sistêmicas. Por esse motivo, o teste de relacionamento (`relationships_where`) foi configurado com a condição `from_condition: cnpj_cliente not like '77700001%'` para permitir que essas operações passem na validação sem quebrar a integridade referencial do restante dos dados.

## Fluxo de Extração e Processamento

O pipeline executa as seguintes etapas em sequência:

### 1. **Extração dos dados da fonte (task: `get_source_last_date`)**
   - Faz download do arquivo Excel da URL oficial do BNDES
   - Extrai metadados do cabeçalho (datas de apuração e cobertura temporal)

### 2. **Processamento e Limpeza dos dados (task: `process_data`)**

#### a) Leitura e Validação Inicial
   - Leitura do Excel da aba "SITE" (skip das primeiras 3 linhas de cabeçalho)
   - Seleção de colunas específicas conforme mapeamento em `constants.py`
   - Aplicação de tipos de dados definidos em `DATA_DTYPES_MAPPING`
   - Verificação automática de duplicatas usando combinação de todas as colunas

#### b) Transformações de Identificadores

   **CNPJ do Cliente:**
   - Remove ocorrências de ".0" (artefato de conversão de tipos)
   - Normaliza para 14 dígitos com `zfill(14)`
   - Substitui `"00000000000000"` (CNPJ zerado) por `None`. Esses valores estão associados a razões sociais `"SEMM NOME"`
   - Nota: CNPJs começando com `77700001%` não são validados contra o diretório de empresas

   **ID Município:**
   - Substitui valores especiais `"9999999"` (não localizado) e `"0000000"` (sem informação) por `None`. Esses valores estão associados a combinações de `sigla_uf == "IE"` e `nome_municipio == "SEM MUNICÍPIO"`  ou a `nome_municipio == "DIVERSOS"`
   - Remove artefatos de conversão de tipo (".0")
   - Mantém outros valores como strings de 7 dígitos

   **CNAEs:**
   - Extrai hierarquia completa do código CNAE (seção, divisão, grupo, classe, subclasse)
   - Cruza com diretório (`br_bd_diretorios_brasil.cnae_2`) mantendo apenas códigos válidos

#### c) Limpeza de Valores Nulos em Colunas de String
   - Define placeholders de nulidade: `"None"`, `"NaN"`, `"Nan"`, `"----------"`, `"-"`, `"nan"`, `"null"`, `"none"`
   - Substitui todos os placeholders por `None` (NaN do pandas) em todas as colunas de tipo string/object

### 3. **Materialização em BigQuery (model dbt: `br_bndes_operacoes_contratadas__operacoes_nao_automaticas`)**
   - Aplica casts (`safe_cast`) para garantir tipos compatíveis
   - Normaliza strings: remove nulidades residuais e converte para UPPERCASE

### 4. **Validação em DBT (testes em `schema.yml`)**
   - `not_null` para chaves primárias: `id_contrato`, `data_contratacao`, `data_apuracao`, etc.
   - `relationships`: valida integridade referencial com diretórios (CNAE, UF, Município, Empresa)
   - `not_null_proportion`: permite até 1-10% de nulidade em colunas permissíveis

## Últimas alterações
* [PR data/br_bndes_operacoes_contratadas_forma_direta_e_indireta_nao_automatica #1540](https://github.com/basedosdados/pipelines/pull/1540)
