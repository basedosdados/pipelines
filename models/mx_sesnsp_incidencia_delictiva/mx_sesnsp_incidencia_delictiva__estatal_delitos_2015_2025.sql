{{
    config(
        schema="mx_sesnsp_incidencia_delictiva",
        alias="estatal_delitos_2015_2025",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2015, "end": 2031, "interval": 1},
        },
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(id_entidad as string) id_entidad,
    safe_cast(bien_juridico_afectado as string) bien_juridico_afectado,
    safe_cast(tipo_delito as string) tipo_delito,
    safe_cast(subtipo_delito as string) subtipo_delito,
    safe_cast(modalidad as string) modalidad,
    safe_cast(cantidad as int64) cantidad
from
    {{
        set_datalake_project(
            "mx_sesnsp_incidencia_delictiva_staging.estatal_delitos_2015_2025"
        )
    }} as t
