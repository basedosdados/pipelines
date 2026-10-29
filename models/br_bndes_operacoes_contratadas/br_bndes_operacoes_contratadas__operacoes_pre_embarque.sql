{{
    config(
        alias="operacoes_pre_embarque",
        schema="br_bndes_operacoes_contratadas",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2002, "end": 2031, "interval": 1},
        },
        labels={"project_id": "basedosdados"},
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(data_contratacao as date) data_contratacao,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(cnpj_cliente as string) cnpj_cliente,
    safe_cast(nome_cliente as string) nome_cliente,
    safe_cast(porte_cliente as string) porte_cliente,
    safe_cast(natureza_cliente as string) natureza_cliente,
    safe_cast(fonte_recurso as string) fonte_recurso,
    safe_cast(modalidade_apoio as string) modalidade_apoio,
    safe_cast(forma_apoio as string) forma_apoio,
    safe_cast(produto as string) produto,
    safe_cast(instrumento_financeiro as string) instrumento_financeiro,
    safe_cast(indicador_inovacao as string) indicador_inovacao,
    safe_cast(area_operacional as string) area_operacional,
    safe_cast(setor_cnae as string) setor_cnae,
    safe_cast(subsetor_cnae_agrupado as string) subsetor_cnae_agrupado,
    safe_cast(codigo_subsetor_cnae as string) codigo_subsetor_cnae,
    safe_cast(nome_subsetor_cnae as string) nome_subsetor_cnae,
    safe_cast(setor_bndes as string) setor_bndes,
    safe_cast(subsetor_bndes as string) subsetor_bndes,
    safe_cast(
        nome_instituicao_financeira_credenciada as string
    ) nome_instituicao_financeira_credenciada,
    safe_cast(
        cnpj_instituicao_financeira_credenciada as string
    ) cnpj_instituicao_financeira_credenciada,
    safe_cast(situacao_operacao as string) situacao_operacao,
    safe_cast(valor_operacao as float64) valor_operacao,
    safe_cast(valor_desembolsado as float64) valor_desembolsado
from
    {{
        set_datalake_project(
            "br_bndes_operacoes_contratadas_staging.operacoes_pre_embarque"
        )
    }} as t
