{{
    config(
        schema="br_senado_dados_abertos_administrativos",
        alias="contratacao_orgao_gestor",
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
    safe_cast(id_orgao_gestor as string) id_orgao_gestor,
    safe_cast(sigla_orgao_gestor as string) sigla_orgao_gestor,
    safe_cast(orgao_gestor as string) orgao_gestor,
    safe_cast(tipo_gestao as string) tipo_gestao
from
    {{
        set_datalake_project(
            "br_senado_dados_abertos_administrativos_staging.contratacao_orgao_gestor"
        )
    }} as t
