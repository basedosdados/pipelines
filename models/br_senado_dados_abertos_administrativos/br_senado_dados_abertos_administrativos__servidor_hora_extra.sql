{{
    config(
        schema="br_senado_dados_abertos_administrativos",
        alias="servidor_hora_extra",
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2013, "end": 2031, "interval": 1},
        },
    )
}}


select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(id_hora_extra as string) id_hora_extra,
    safe_cast(nome as string) nome,
    safe_cast(mes_ano_prestacao as string) mes_ano_prestacao,
    safe_cast(mes_ano_pagamento as string) mes_ano_pagamento,
    safe_cast(valor_total as float64) valor_total
from
    {{
        set_datalake_project(
            "br_senado_dados_abertos_administrativos_staging.servidor_hora_extra"
        )
    }} as t
