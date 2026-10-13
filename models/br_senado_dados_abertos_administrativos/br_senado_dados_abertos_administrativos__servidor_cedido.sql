{{
    config(
        schema="br_senado_dados_abertos_administrativos",
        alias="servidor_cedido",
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
    safe_cast(tipo_cessao as string) tipo_cessao,
    safe_cast(nome as string) nome,
    safe_cast(orgao_origem as string) orgao_origem,
    safe_cast(orgao_destino as string) orgao_destino,
    safe_cast(cargo as string) cargo,
    safe_cast(categoria as string) categoria,
    safe_cast(lotacao as string) lotacao,
    safe_cast(data_exercicio as date) data_exercicio
from
    {{
        set_datalake_project(
            "br_senado_dados_abertos_administrativos_staging.servidor_cedido"
        )
    }} as t
