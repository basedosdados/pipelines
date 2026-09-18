{{
    config(
        schema="br_senado_dados_abertos_administrativos",
        alias="servidor_exonerado",
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
    safe_cast(matricula as string) matricula,
    safe_cast(nome as string) nome,
    safe_cast(forma_vacancia as string) forma_vacancia,
    safe_cast(data_exercicio as date) data_exercicio,
    safe_cast(data_vacancia as date) data_vacancia
from
    {{
        set_datalake_project(
            "br_senado_dados_abertos_administrativos_staging.servidor_exonerado"
        )
    }} as t
