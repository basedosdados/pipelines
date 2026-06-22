{{
    config(
        alias="populacao_tea_indigena",
        schema="br_ibge_censo_2022",
        materialized="table",
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(populacao_tea as int64) populacao_tea,
from
    {{ set_datalake_project("br_ibge_censo_2022_staging.populacao_tea_indigena") }} as t
