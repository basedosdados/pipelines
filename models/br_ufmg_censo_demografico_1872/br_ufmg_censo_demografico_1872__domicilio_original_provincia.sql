{{
    config(
        schema="br_ufmg_censo_demografico_1872",
        alias="domicilio_original_provincia",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 1872, "end": 1877, "interval": 1},
        },
    )
}}


select
    safe_cast(ano as int64) ano,
    safe_cast(id_provincia as string) id_provincia,
    safe_cast(casas_habitadas as int64) casas_habitadas,
    safe_cast(casas_desabitadas as int64) casas_desabitadas,
    safe_cast(fogos as int64) fogos
from
    {{
        set_datalake_project(
            "br_ufmg_censo_demografico_1872_staging.domicilio_original_provincia"
        )
    }} as t
