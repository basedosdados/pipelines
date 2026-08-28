{{
    config(
        schema="br_mgi_compras_publicas",
        alias="unidade_administrativa",
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
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(codigo_uasg as string) codigo_uasg,
    safe_cast(codigo_orgao as string) codigo_orgao,
    safe_cast(cnpj_orgao as string) cnpj_orgao,
    safe_cast(cnpj_orgao_vinculado as string) cnpj_orgao_vinculado,
    safe_cast(cnpj_orgao_superior as string) cnpj_orgao_superior,
    safe_cast(cnpj_uasg as string) cnpj_uasg,
    safe_cast(nome_uasg as string) nome_uasg,
    safe_cast(codigo_municipio_siasg as string) codigo_municipio_siasg,
    safe_cast(nome_municipio as string) nome_municipio,
    safe_cast(codigo_unidade_polo as string) codigo_unidade_polo,
    safe_cast(nome_unidade_polo as string) nome_unidade_polo,
    safe_cast(codigo_unidade_espelho as string) codigo_unidade_espelho,
    safe_cast(nome_unidade_espelho as string) nome_unidade_espelho,
    safe_cast(codigo_siorg as string) codigo_siorg,
    safe_cast(indicador_uso_sisg as boolean) indicador_uso_sisg,
    safe_cast(indicador_adesao_siasg as boolean) indicador_adesao_siasg,
    safe_cast(indicador_uasg_cadastradora as boolean) indicador_uasg_cadastradora,
    safe_cast(indicador_uasg_ativa as boolean) indicador_uasg_ativa,
    safe_cast(data_implantacao_sidec as datetime) data_implantacao_sidec,
    safe_cast(data_hora_movimento as datetime) data_hora_movimento
from
    {{ set_datalake_project("br_mgi_compras_publicas_staging.unidade_administrativa") }}
    as t
qualify
    row_number() over (
        partition by data_extracao, codigo_uasg order by data_hora_movimento desc
    )
    = 1
