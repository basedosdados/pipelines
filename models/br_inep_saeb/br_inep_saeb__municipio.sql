{{
    config(
        alias="municipio",
        schema="br_inep_saeb",
        materialized="table",
    )
}}


select
    safe_cast(ano as int64) ano,
    safe_cast(rede as string) rede,
    safe_cast(localizacao as string) localizacao,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(disciplina as string) disciplina,
    safe_cast(serie as int64) serie,
    round(safe_cast(media as float64), 2) media,
    round(safe_cast(nivel_0 as float64), 2) nivel_0,
    round(safe_cast(nivel_1 as float64), 2) nivel_1,
    round(safe_cast(nivel_2 as float64), 2) nivel_2,
    round(safe_cast(nivel_3 as float64), 2) nivel_3,
    round(safe_cast(nivel_4 as float64), 2) nivel_4,
    round(safe_cast(nivel_5 as float64), 2) nivel_5,
    round(safe_cast(nivel_6 as float64), 2) nivel_6,
    round(safe_cast(nivel_7 as float64), 2) nivel_7,
    round(safe_cast(nivel_8 as float64), 2) nivel_8,
    round(safe_cast(nivel_9 as float64), 2) nivel_9,
    round(safe_cast(nivel_10 as float64), 2) nivel_10
from {{ set_datalake_project("br_inep_saeb_staging.municipio") }} as t
