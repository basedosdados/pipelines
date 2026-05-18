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
- [Central de Downloads BNDES](https://www.bndes.gov.br/arquivos/central-downloads/operacoes_financiamento/naoautomaticas/naoautomaticas.xlsx)

**URL de Processamento**:
```python
URL_OPERACOES_CONTRATADAS = "https://www.bndes.gov.br/arquivos/central-downloads/operacoes_financiamento/naoautomaticas/naoautomaticas.xlsx"
```

#### Observações sobre Dados

**Registros Duplicados**: Encontrados 689 registros duplicados com base no conjunto completo de colunas. As duplicatas identificadas incluem múltiplos registros do mesmo cliente, CNPJ, projeto e características contratuais.

**Distribuição de Valores Nulos por Coluna**:
- `tipo_fonte_recursos`: 775 valores nulos (representam contratos onde a fonte de recurso não foi registrada nos desembolsos)
- `nome_instituicao_financeira_credenciada`: 19.348 valores nulos (representam operações de forma direta, sem intermediação)
- `cnpj_instituicao_financeira_credenciada`: 19.348 valores nulos (correspondentes às operações diretas)
- `tipo_excepcionalidade`: 23.202 valores nulos (contratos sem classificação de excepcionalidade)
- `situacao_contrato`: 268 valores nulos

Demais colunas não apresentam valores nulos, indicando que informações essenciais de identificação, datas, valores e classificações estão sempre preenchidas.

### 2. cnaes_agrupados_bndes

Tabela de correspondências entre as classificações de atividades econômicas (CNAE) utilizadas pelo BNDES e os códigos do IBGE. Esta tabela de referência mapeia setores e subsetores de acordo com a política de classificação do BNDES, facilitando análises setoriais.

**Conteúdo**:
- Mapeamento entre a classificação CNAE 2 (seção, divisão, grupo, classe, subclasse) e as classificações internas do BNDES (setor e subsetor)
- Associação com produtos financeiros
- Referência para classificação de projetos por atividade econômica

## Últimas alterações
* [PR data/br_bndes_operacoes_contratadas_forma_direta_e_indireta_nao_automatica #1540](https://github.com/basedosdados/pipelines/pull/1540)
