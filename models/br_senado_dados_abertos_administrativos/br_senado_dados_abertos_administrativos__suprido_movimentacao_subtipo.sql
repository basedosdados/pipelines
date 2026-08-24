{{
    config(
        schema="br_senado_dados_abertos_administrativos",
        alias="suprido_movimentacao_subtipo",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2008, "end": 2031, "interval": 1},
        },
    )
}}


select
    safe_cast(ano as int64) ano,
    safe_cast(id_movimentacao as string) id_movimentacao,
    safe_cast(id_subtipo as string) id_subtipo,
    safe_cast(tipo_despesa as string) tipo_despesa,
    safe_cast(subtipo_despesa as string) subtipo_despesa,
    safe_cast(rubrica as string) rubrica,
    safe_cast(valor as float64) valor
from
    {{
        set_datalake_project(
            "br_senado_dados_abertos_administrativos_staging.suprido_movimentacao_subtipo"
        )
    }}
    as t
