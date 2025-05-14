{{
    config(
        schema="br_rf_arrecadacao",
        alias="ir_ipi",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2019, "end": 2024, "interval": 1},
        },
        cluster_by=["mes"],
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(tributo as string) tributo,
    safe_cast(decendio as string) decendio,
    safe_cast(arrecadacao_bruta as float64) arrecadacao_bruta,
    safe_cast(retificacao as float64) retificacao,
    safe_cast(compensacao as float64) compensacao,
    safe_cast(restituicao as float64) restituicao,
    safe_cast(outros as float64) outros,
    safe_cast(arrecadacao_liquida as float64) arrecadacao_liquida,
from {{ set_datalake_project("br_rf_arrecadacao_staging.ir_ipi") }} as t
