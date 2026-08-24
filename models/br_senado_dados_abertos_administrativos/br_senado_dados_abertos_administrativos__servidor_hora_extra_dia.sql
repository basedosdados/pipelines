{{
    config(
        schema="br_senado_dados_abertos_administrativos",
        alias="servidor_hora_extra_dia",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2026, "end": 2031, "interval": 1},
        },
    )
}}


select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(id_hora_extra as string) id_hora_extra,
    safe_cast(data as date) data,
    safe_cast(quantidade_horas as float64) quantidade_horas,
    safe_cast(sigla_setor_prestacao as string) sigla_setor_prestacao,
    safe_cast(setor_prestacao as string) setor_prestacao
from
    {{
        set_datalake_project(
            "br_senado_dados_abertos_administrativos_staging.servidor_hora_extra_dia"
        )
    }} as t
