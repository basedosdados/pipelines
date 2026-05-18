# Documentação BNDES/Operações Contratadas

Este documento registra informações importantes sobre a base de dados de Operações Contratadas do BNDES. O BNDES (Banco Nacional de Desenvolvimento Econômico e Social) disponibiliza, em sua Central de Downloads de Transparência, os dados de operações contratadas
---

## Sobre a fonte

- [Fonte de Dados](https://www.bndes.gov.br/wps/portal/site/home/transparencia/centraldedownloads)

---
## operacoes_contratadas_forma_direta_e_indireta_nao_automatica

- [Fonte de Dados](https://www.bndes.gov.br/arquivos/central-downloads/operacoes_financiamento/naoautomaticas/naoautomaticas.xlsx)
```python
URL_OPERACOES_CONTRATADAS = "https://www.bndes.gov.br/arquivos/central-downloads/operacoes_financiamento/naoautomaticas/naoautomaticas.xlsx"
```


#### Duplicatas
```bash
[2026-05-17 15:01:48-0300] INFO - prefect.process_data |                 ----
        Encontrados 689 registros duplicados com base nas colunas ['Cliente', 'CNPJ', 'Descrição do projeto', 'UF', 'Município', 'Município - código', 'Número do contrato', 'Data da contratação', 'Valor contratado  R$', 'Valor desembolsado R$', 'Fonte de recurso (desembolsos)', 'Custo financeiro', 'Juros', 'Prazo - carência (meses)', 'Prazo - amortização (meses)', 'Modalidade de apoio', 'Forma de apoio', 'Produto', 'Instrumento financeiro', 'Inovação', 'Área operacional', 'Setor CNAE', 'Subsetor CNAE agrupado', 'Subsetor CNAE - código', 'Subsetor CNAE - nome', 'Setor BNDES', 'Subsetor BNDES', 'Porte do cliente', 'Natureza do cliente', 'Instituição Financeira Credenciada', 'CNPJ da instituição financeira credenciada', 'Tipo de garantia', 'Tipo de excepcionalidade', 'Situação do contrato'].


[2026-05-17 15:01:48-0300] INFO - prefect.process_data |                 ----
        Exibindo os registros duplicados:


[2026-05-17 15:01:48-0300] INFO - prefect.process_data |                 ----
                                        Cliente            CNPJ  ... Tipo de excepcionalidade Situação do contrato
        124       CAMORIM SERVICOS MARITIMOS SA    649990000193  ...               ----------                ATIVO
        125       CAMORIM SERVICOS MARITIMOS SA    649990000193  ...               ----------                ATIVO
        126       CAMORIM SERVICOS MARITIMOS SA    649990000193  ...               ----------                ATIVO
        127       CAMORIM SERVICOS MARITIMOS SA    649990000193  ...               ----------                ATIVO
        128       CAMORIM SERVICOS MARITIMOS SA    649990000193  ...               ----------                ATIVO
        ...                                 ...             ...  ...                      ...                  ...
        23002  BARCAS S/A TRANSPORTES MARITIMOS  33644865000140  ...               ----------            LIQUIDADO
        23003  BARCAS S/A TRANSPORTES MARITIMOS  33644865000140  ...               ----------            LIQUIDADO
        23004  BARCAS S/A TRANSPORTES MARITIMOS  33644865000140  ...               ----------            LIQUIDADO
        23031  BARCAS S/A TRANSPORTES MARITIMOS  33644865000140  ...               ----------            LIQUIDADO
        23032  BARCAS S/A TRANSPORTES MARITIMOS  33644865000140  ...               ----------            LIQUIDADO
        
        [689 rows x 34 columns]
```


#### Valores Nulos
```bash
[2026-05-17 15:01:49-0300] INFO - prefect.process_data |                 ----
        Contagem de valores nulos por coluna:


[2026-05-17 15:01:49-0300] INFO - prefect.process_data |                 ----
        razao_social_cliente                           0
        cnpj_cliente                                   0
        descricao_projeto                              0
        sigla_uf                                       0
        nome_municipio                                 0
        id_municipio                                   0
        id_contrato                                    0
        data_contratacao                               0
        valor_contratado                               0
        valor_desembolsado                             0
        tipo_fonte_recursos                          775
        custo_financeiro                               0
        taxa_juros                                     0
        prazo_carencia                                 0
        prazo_amortizacao                              0
        modalidade_apoio                               0
        forma_apoio                                    0
        produto                                        0
        tipo_instrumento_financeiro                    0
        indicador_inovacao                             0
        area_operacional_bndes                         0
        setor_cnae_bndes                               0
        subsetor_agrupado_cnae_bndes                   0
        descricao_subclasse                            0
        setor_bndes                                    0
        subsetor_bndes                                 0
        porte_cliente                                  0
        natureza_cliente                               0
        nome_instituicao_financeira_credenciada    19348
        cnpj_instituicao_financeira_credenciada    19348
        tipo_garantia                                  0
        tipo_excepcionalidade                      23202
        situacao_contrato                            268
        secao_cnae                                     0
        divisao_cnae                                   0
        grupo_cnae                                     0
        classe_cnae                                    0
        subclasse_cnae                                 0
        data_apuracao                                  0
        dtype: int64
```

## Últimas alterações
* [Issue](https://github.com/basedosdados/pipelines/issues/)
* [PR](https://github.com/basedosdados/pipelines/pull/)
