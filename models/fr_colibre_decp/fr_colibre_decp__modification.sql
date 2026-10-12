{{
    config(
        alias="modification",
        schema="fr_colibre_decp",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2014, "end": 2031, "interval": 1},
        },
        cluster_by=["mes", "id_marche"],
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(id_marche as string) id_marche,
    safe_cast(id_modification as string) id_modification,
    safe_cast(date_notification as date) date_notification,
    safe_cast(date_publication_donnees as date) date_publication_donnees,
    safe_cast(duree_mois as int64) duree_mois,
    safe_cast(montant as float64) montant,
    safe_cast(montant_rationalise as float64) montant_rationalise,
    safe_cast(montant_anomalie as string) montant_anomalie,
    safe_cast(montant_anomalie_raisons as string) montant_anomalie_raisons,
    safe_cast(donnees_actuelles as string) donnees_actuelles
from {{ set_datalake_project("fr_colibre_decp_staging.modification") }} as t
