{{
    config(
        schema="br_senado_dados_abertos_administrativos",
        alias="licitacao_detalhamento",
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
    safe_cast(id_licitacao as string) id_licitacao,
    safe_cast(id_detalhamento as string) id_detalhamento,
    safe_cast(tipo as string) tipo,
    safe_cast(descricao as string) descricao,
    safe_cast(data_criacao as date) data_criacao,
    safe_cast(link as string) link
from
    {{
        set_datalake_project(
            "br_senado_dados_abertos_administrativos_staging.licitacao_detalhamento"
        )
    }} as t
