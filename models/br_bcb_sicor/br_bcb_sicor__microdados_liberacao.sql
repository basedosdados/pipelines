{{
    config(
        alias="microdados_liberacao",
        schema="br_bcb_sicor",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2013, "end": 2024, "interval": 1},
        },
        cluster_by=["mes"],
    )
}}


select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(data_liberacao as date) data_liberacao,
    safe_cast(valor_liberado as string) id_referencia_bacen,
    safe_cast(id_referencia_bacen as string) numero_ordem,
    safe_cast(numero_ordem as float64) valor_liberado
from {{ set_datalake_project("br_bcb_sicor_staging.microdados_liberacao") }} as t
