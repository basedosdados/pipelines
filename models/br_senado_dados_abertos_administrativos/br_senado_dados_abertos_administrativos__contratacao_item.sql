{{
    config(
        schema="br_senado_dados_abertos_administrativos",
        alias="contratacao_item",
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
    safe_cast(id_item as string) id_item,
    safe_cast(numero_item as string) numero_item,
    safe_cast(descricao as string) descricao,
    safe_cast(quantidade as float64) quantidade,
    safe_cast(data_atualizacao as date) data_atualizacao
from
    {{
        set_datalake_project(
            "br_senado_dados_abertos_administrativos_staging.contratacao_item"
        )
    }} as t
