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


### 2. cnaes_agrupados_bndes

Tabela de correspondências entre as classificações de atividades econômicas (CNAE) utilizadas pelo BNDES e os códigos do IBGE. Esta tabela de referência mapeia setores e subsetores de acordo com a política de classificação do BNDES, facilitando análises setoriais.

**Conteúdo**:
- Mapeamento entre a classificação CNAE 2 (seção, divisão, grupo, classe, subclasse) e as classificações internas do BNDES (setor e subsetor)
- Associação com produtos financeiros
- Referência para classificação de projetos por atividade econômica
  

#### Observações sobre Dados

O processo de mapeamento dos CNAEs é complicado por causa da forma como a equivalência entre setores/subsetores do BNDES e CNAEs é estabelecida. 
- Há casos em que a equivalência ocorre entre um subsetor agrupado do BNDES e um **_range_** de divisões CNAE dentro de uma mesma seção, definidos pelo que convencionei como **limite_inferior** e um **limite_superior**: Ex.: Setor BNDES "Agropecuária" mapeado nas divisões CNAE "A01 a A03";
- Há casos em que a equivalência ocorre entre um subsetor agrupado do BNDES e uma divisão CNAE específica: Ex.: Subsetor BNDES "Alimento e bebida" associado a um agrupamento "Produtos Alimentícios" mapeado na divisão CNAE "C10", ou seja, seção "C" e divisão "10";
- Há casos em que a equivalência ocorre entre um subsetor agrupado do BNDES e um grupo CNAE específico: Ex.: Subsetor BNDES "Energia elétrica" associado a um agrupamento "Eletricidade e Gás" mapeado no grupo CNAE "D351", ou seja, seção "D" e divisão "35", grupo "351";
- Há casos em que a equivalência ocorre entre um subsetor agrupado do BNDES e uma **lista** de divisões CNAE em seções distintas: Ex.: Subsetor BNDES "Comércio e Serviços" associado a um agrupamento "Ativ imobil, profissional e adm" mapeado nas divisões "L68, M69, M70, M71, M72, M73, M74, M75, N77, N78, N79, N80, N81 e N82" ;
- Há listas de divisões dentro da mesma seção "K64, K65 e K66" ou "F41 e F43";
- Há listas que mesclam classe e subclasse, etc.

A solução foi dividida entre separar os casos em que se tem listas e os casos em que se tem _ranges_ para só então extrair níveis coerentes de CNAE. Cada um vira um novo dataframe em `get_cnaes_by_limits_and_lists()`. Em seguida, as funções `extract_cnaes_sections_by_lists()` e `extract_cnaes_sections_by_limits()`, extraem as seções com base nos códigos obtidos do passo anterior. Por fim, os demais níveis são extraídos por meio de regex.


## Últimas alterações
* [PR data/br_bndes_operacoes_contratadas_forma_direta_e_indireta_nao_automatica #1540](https://github.com/basedosdados/pipelines/pull/1540)
