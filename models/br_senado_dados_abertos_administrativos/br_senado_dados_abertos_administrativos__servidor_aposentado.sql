{{
    config(
        schema="br_senado_dados_abertos_administrativos",
        alias="servidor_aposentado",
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
    safe_cast(tipo_quadro as string) tipo_quadro,
    safe_cast(nome as string) nome,
    safe_cast(categoria as string) categoria,
    safe_cast(cargo as string) cargo,
    safe_cast(data_aposentadoria as date) data_aposentadoria,
    safe_cast(tipo_aposentadoria as string) tipo_aposentadoria
from
    {{
        set_datalake_project(
            "br_senado_dados_abertos_administrativos_staging.servidor_aposentado"
        )
    }} as t
