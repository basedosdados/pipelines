{{
    config(
        schema="fr_meteofrance",
        alias="station_climatologique",
        materialized="table",
    )
}}


select
    safe_cast(numero_poste as string) numero_poste,
    safe_cast(nom_poste as string) nom_poste,
    safe_cast(id_departement as string) id_departement,
    safe_cast(latitude as float64) latitude,
    safe_cast(longitude as float64) longitude,
    safe_cast(altitude as float64) altitude,
    safe_cast(date_edition as date) date_edition,
    st_geogfromtext(
        safe_cast(geolocalisation as string), make_valid => true
    ) geolocalisation
from {{ set_datalake_project("fr_meteofrance_staging.station_climatologique") }} as t
