{{
    config(
        alias="densidade_municipio",
        schema="br_anatel_telefonia_movel",
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(sigla_uf as string) sigla_uf,
    replace(cast(id_municipio as string), '.0', '') id_municipio,
    safe_cast(densidade as float64) densidade
from
    {{ set_datalake_project("br_anatel_telefonia_movel_staging.densidade_municipio") }}
    as t
