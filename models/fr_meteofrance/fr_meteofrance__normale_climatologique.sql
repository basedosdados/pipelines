{{
    config(
        schema="fr_meteofrance",
        alias="normale_climatologique",
        materialized="table",
    )
}}


select
    safe_cast(numero_poste as string) numero_poste,
    safe_cast(indicateur as string) indicateur,
    safe_cast(periode as string) periode,
    safe_cast(valeur as float64) valeur,
    safe_cast(unite as string) unite,
    safe_cast(libelle_indicateur as string) libelle_indicateur,
    safe_cast(annee_debut_reference as int64) annee_debut_reference,
    safe_cast(annee_fin_reference as int64) annee_fin_reference,
    safe_cast(date_debut_record as date) date_debut_record,
    safe_cast(date_fin_record as date) date_fin_record,
    safe_cast(jour_record as int64) jour_record,
    safe_cast(annee_record as int64) annee_record
from {{ set_datalake_project("fr_meteofrance_staging.normale_climatologique") }} as t
