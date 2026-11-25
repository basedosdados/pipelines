{{
    config(
        schema="br_senado_dados_abertos_administrativos",
        alias="contrato_aditivo",
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "data_extracao",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}


select
    safe_cast(data_extracao as date) data_extracao,
    safe_cast(id_contratacao as string) id_contratacao,
    safe_cast(id_aditivo as string) id_aditivo,
    safe_cast(numero as string) numero,
    safe_cast(valor as float64) valor,
    safe_cast(data_assinatura as date) data_assinatura,
    safe_cast(data_publicacao as date) data_publicacao,
    safe_cast(data_atualizacao as date) data_atualizacao
from
    {{
        set_datalake_project(
            "br_senado_dados_abertos_administrativos_staging.contrato_aditivo"
        )
    }} as t
