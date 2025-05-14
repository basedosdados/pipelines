{{
    config(
        alias="microdados_saldo",
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
    safe_cast(id_referencia_bacen as string) id_referencia_bacen,
    safe_cast(numero_ordem as string) numero_ordem,
    safe_cast(id_situacao_operacao as string) id_situacao_operacao,
    safe_cast(valor_medio_diario as float64) valor_medio_diario,
    safe_cast(valor_medio_diario_vincendo as float64) valor_medio_diario_vincendo,
    safe_cast(valor_ultimo_dia as float64) valor_ultimo_dia
from {{ set_datalake_project("br_bcb_sicor_staging.microdados_saldo") }} as t
