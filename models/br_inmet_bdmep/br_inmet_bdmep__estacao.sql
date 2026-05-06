{{
    config(
        alias="estacao",
        schema="br_inmet_bdmep",
        materialized="table",
    )
}}


select
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(id_estacao as string) id_estacao,
    safe_cast(estacao as string) estacao,
    safe_cast(data_fundacao as date) data_fundacao,
    st_geopoint(
        safe_cast(latitude as float64), safe_cast(longitude as float64)
    ) as geolocalizacao,
    safe_cast(altitude as float64) altitude
from {{ set_datalake_project("br_inmet_bdmep_staging.estacao") }} as t
