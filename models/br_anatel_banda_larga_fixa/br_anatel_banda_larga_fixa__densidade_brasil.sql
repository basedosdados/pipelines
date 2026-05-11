{{
    config(
        alias="densidade_brasil",
        schema="br_anatel_banda_larga_fixa",
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(densidade as float64) densidade
from
    {{ set_datalake_project("br_anatel_banda_larga_fixa_staging.densidade_brasil") }}
    as t
