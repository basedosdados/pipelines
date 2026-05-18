{{
    config(
        alias="operacoes_contratadas_forma_direta_e_indireta_nao_automatica",
        schema="br_bndes_operacoes_contratadas",
        materialized="table",
    )
}}
select
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(cnpj_cliente as string) cnpj_cliente,
    safe_cast(id_contrato as string) id_contrato,
    safe_cast(data_apuracao as date) data_apuracao,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(data_contratacao as date) data_contratacao,
    safe_cast(nome_municipio as string) nome_municipio,
    safe_cast(razao_social_cliente as string) razao_social_cliente,
    safe_cast(situacao_contrato as string) situacao_contrato,
    safe_cast(porte_cliente as string) porte_cliente,
    safe_cast(natureza_cliente as string) natureza_cliente,
    safe_cast(descricao_projeto as string) descricao_projeto,
    safe_cast(modalidade_apoio as string) modalidade_apoio,
    safe_cast(forma_apoio as string) forma_apoio,
    safe_cast(produto as string) produto,
    safe_cast(tipo_instrumento_financeiro as string) tipo_instrumento_financeiro,
    safe_cast(tipo_garantia as string) tipo_garantia,
    safe_cast(tipo_excepcionalidade as string) tipo_excepcionalidade,
    safe_cast(tipo_fonte_recursos as string) tipo_fonte_recursos,
    safe_cast(area_operacional_bndes as string) area_operacional_bndes,
    safe_cast(setor_cnae_bndes as string) setor_cnae_bndes,
    safe_cast(subsetor_agrupado_cnae_bndes as string) subsetor_agrupado_cnae_bndes,
    safe_cast(secao_cnae as string) secao_cnae,
    safe_cast(divisao_cnae as string) divisao_cnae,
    safe_cast(grupo_cnae as string) grupo_cnae,
    safe_cast(classe_cnae as string) classe_cnae,
    safe_cast(subclasse_cnae as string) subclasse_cnae,
    safe_cast(descricao_subclasse as string) descricao_subclasse,
    safe_cast(setor_bndes as string) setor_bndes,
    safe_cast(subsetor_bndes as string) subsetor_bndes,
    safe_cast(
        nome_instituicao_financeira_credenciada as string
    ) nome_instituicao_financeira_credenciada,
    safe_cast(
        cnpj_instituicao_financeira_credenciada as string
    ) cnpj_instituicao_financeira_credenciada,
    safe_cast(custo_financeiro as string) custo_financeiro,
    safe_cast(taxa_juros as float64) taxa_juros,
    safe_cast(valor_contratado as float64) valor_contratado,
    safe_cast(valor_desembolsado as float64) valor_desembolsado,
    safe_cast(prazo_carencia as int64) prazo_carencia,
    safe_cast(prazo_amortizacao as int64) prazo_amortizacao,
    safe_cast(indicador_inovacao as int64) indicador_inovacao,
from
    {{
        set_datalake_project(
            "br_bndes_operacoes_contratadas_staging.operacoes_contratadas_forma_direta_e_indireta_nao_automatica"
        )
    }}
    as t
