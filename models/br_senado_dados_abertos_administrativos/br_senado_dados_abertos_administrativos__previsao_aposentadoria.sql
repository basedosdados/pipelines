{{
    config(
        schema="br_senado_dados_abertos_administrativos",
        alias="previsao_aposentadoria",
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
    safe_cast(cargo as string) cargo,
    safe_cast(categoria as string) categoria,
    safe_cast(ano_direito as int64) ano_direito,
    safe_cast(mes_direito as int64) mes_direito,
    safe_cast(quantidade as int64) quantidade
from
    {{
        set_datalake_project(
            "br_senado_dados_abertos_administrativos_staging.previsao_aposentadoria"
        )
    }} as t
