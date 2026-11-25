{{
    config(
        schema="fr_meteofrance",
        alias="station_synop",
        materialized="table",
    )
}}


select
    safe_cast(indicatif_omm as string) indicatif_omm,
    safe_cast(indicatif_wigos as string) indicatif_wigos,
    safe_cast(nom_station as string) nom_station,
    safe_cast(latitude as float64) latitude,
    safe_cast(longitude as float64) longitude,
    safe_cast(altitude as float64) altitude,
    safe_cast(date_ouverture as date) date_ouverture,
    safe_cast(annee_debut_observation as int64) annee_debut_observation,
    safe_cast(annee_fin_observation as int64) annee_fin_observation,
    st_geogfromtext(
        safe_cast(geolocalisation as string), make_valid => true
    ) geolocalisation
from {{ set_datalake_project("fr_meteofrance_staging.station_synop") }} as t
