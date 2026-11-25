{{
    config(
        schema="fr_insee_sirene",
        alias="etablissement_historique",
        materialized="table",
        partition_by={
            "field": "data",
            "data_type": "date",
        },
        cluster_by=["siret"],
    )
}}
select
    safe_cast(data as date) data,
    safe_cast(ano as int64) ano,
    safe_cast(siren as string) siren,
    safe_cast(nic as string) nic,
    safe_cast(siret as string) siret,
    safe_cast(date_fin as date) date_fin,
    safe_cast(date_debut as date) date_debut,
    safe_cast(etat_administratif as string) etat_administratif,
    safe_cast(changement_etat_administratif as string) changement_etat_administratif,
    safe_cast(enseigne_1 as string) enseigne_1,
    safe_cast(enseigne_2 as string) enseigne_2,
    safe_cast(enseigne_3 as string) enseigne_3,
    safe_cast(changement_enseigne as string) changement_enseigne,
    safe_cast(denomination_usuelle as string) denomination_usuelle,
    safe_cast(
        changement_denomination_usuelle as string
    ) changement_denomination_usuelle,
    safe_cast(activite_principale as string) activite_principale,
    safe_cast(
        nomenclature_activite_principale as string
    ) nomenclature_activite_principale,
    safe_cast(changement_activite_principale as string) changement_activite_principale,
    safe_cast(caractere_employeur as string) caractere_employeur,
    safe_cast(changement_caractere_employeur as string) changement_caractere_employeur
from {{ set_datalake_project("fr_insee_sirene_staging.etablissement_historique") }} as t
