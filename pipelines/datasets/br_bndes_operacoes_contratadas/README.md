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
  
**URL de Processamento**:
```python
# constants.py
class constants(Enum):
  ...
  URL_OPERACOES_CONTRATADAS = "https://www.bndes.gov.br/arquivos/central-downloads/operacoes_financiamento/naoautomaticas/naoautomaticas.xlsx"
  ...
```

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

## Últimas alterações
* [PR data/br_bndes_operacoes_contratadas_forma_direta_e_indireta_nao_automatica #1540](https://github.com/basedosdados/pipelines/pull/1540)
