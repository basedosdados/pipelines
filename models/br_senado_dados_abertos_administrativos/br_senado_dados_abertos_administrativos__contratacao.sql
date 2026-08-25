{{
    config(
        schema="br_senado_dados_abertos_administrativos",
        alias="contratacao",
        materialized="table",
        partition_by={
            "field": "data_extracao",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}


select
    safe_cast(data_extracao as date) data_extracao,
    safe_cast(tipo_contratacao as string) tipo_contratacao,
    safe_cast(id_contratacao as string) id_contratacao,
    safe_cast(numero as string) numero,
    safe_cast(numero_formatado as string) numero_formatado,
    safe_cast(status as string) status,
    safe_cast(sigla_sub_especie as string) sigla_sub_especie,
    safe_cast(sub_especie as string) sub_especie,
    safe_cast(objeto as string) objeto,
    safe_cast(cpf_cnpj_empresa as string) cpf_cnpj_empresa,
    safe_cast(nome_empresa as string) nome_empresa,
    safe_cast(numero_licitacao as string) numero_licitacao,
    safe_cast(modalidade_licitacao as string) modalidade_licitacao,
    safe_cast(sigla_unidade_gestora as string) sigla_unidade_gestora,
    safe_cast(unidade_gestora as string) unidade_gestora,
    safe_cast(fundamentacao_legal as string) fundamentacao_legal,
    safe_cast(processo_principal as string) processo_principal,
    safe_cast(indicador_mao_de_obra as string) indicador_mao_de_obra,
    safe_cast(data_assinatura as date) data_assinatura,
    safe_cast(data_publicacao as date) data_publicacao,
    safe_cast(data_inicio_vigencia as date) data_inicio_vigencia,
    safe_cast(data_fim_vigencia as date) data_fim_vigencia,
    safe_cast(data_ultima_atualizacao as date) data_ultima_atualizacao
from
    {{
        set_datalake_project(
            "br_senado_dados_abertos_administrativos_staging.contratacao"
        )
    }} as t
