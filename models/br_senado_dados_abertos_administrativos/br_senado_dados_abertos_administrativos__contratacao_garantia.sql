{{
    config(
        schema="br_senado_dados_abertos_administrativos",
        alias="contratacao_garantia",
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
    safe_cast(tipo_contratacao as string) tipo_contratacao,
    safe_cast(id_contratacao as string) id_contratacao,
    safe_cast(id_garantia as string) id_garantia,
    safe_cast(tipo as string) tipo,
    safe_cast(valor as float64) valor,
    safe_cast(data_vencimento as date) data_vencimento
from
    {{
        set_datalake_project(
            "br_senado_dados_abertos_administrativos_staging.contratacao_garantia"
        )
    }} as t
