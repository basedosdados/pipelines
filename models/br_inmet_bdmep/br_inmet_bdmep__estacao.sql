{{
    config(
        alias="estacao",
        schema="br_inmet_bdmep",
        materialized="table",
    )
}}


with
    estacao as (
        select
            safe_cast(id_estacao as string) id_estacao,
            safe_cast(estacao as string) estacao,
            safe_cast(sigla_uf as string) sigla_uf,
            safe_cast(data_fundacao as date) data_fundacao,
            safe.st_geogpoint(
                safe_cast(longitude as float64), safe_cast(latitude as float64)
            ) geolocalizacao,
            safe_cast(altitude as float64) altitude
        from {{ set_datalake_project("br_inmet_bdmep_staging.estacao") }} as t
        qualify
            row_number() over (
                partition by id_estacao order by safe_cast(ano as int64) desc
            )
            = 1
    ),
    municipio as (
        select id_municipio, sigla_uf, geometria
        from `basedosdados.br_geobr_mapas.municipio`
    )
select
    m.id_municipio,
    e.id_estacao,
    e.estacao,
    e.data_fundacao,
    e.geolocalizacao,
    e.altitude
from estacao as e
left join municipio as m on e.sigla_uf = m.sigla_uf and e.geolocalizacao is not null
qualify
    row_number() over (
        partition by e.id_estacao
        order by st_distance(m.geometria, e.geolocalizacao), m.id_municipio
    )
    = 1
