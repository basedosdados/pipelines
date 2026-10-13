{{
    config(
        schema="br_senado_dados_abertos_administrativos",
        alias="ata_acionamento",
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
    safe_cast(id_acionamento as string) id_acionamento,
    safe_cast(numero as string) numero,
    safe_cast(objeto as string) objeto
from
    {{
        set_datalake_project(
            "br_senado_dados_abertos_administrativos_staging.ata_acionamento"
        )
    }} as t
