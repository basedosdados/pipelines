{{
    config(
        schema="fr_insee_sirene",
        alias="unite_legale_historique",
        materialized="table",
        partition_by={
            "field": "data",
            "data_type": "date",
        },
        cluster_by=["siren"],
    )
}}
select
    safe_cast(data as date) data,
    safe_cast(ano as int64) ano,
    safe_cast(siren as string) siren,
    safe_cast(date_fin as date) date_fin,
    safe_cast(date_debut as date) date_debut,
    safe_cast(etat_administratif as string) etat_administratif,
    safe_cast(changement_etat_administratif as string) changement_etat_administratif,
    safe_cast(nom as string) nom,
    safe_cast(changement_nom as string) changement_nom,
    safe_cast(nom_usage as string) nom_usage,
    safe_cast(changement_nom_usage as string) changement_nom_usage,
    safe_cast(denomination as string) denomination,
    safe_cast(changement_denomination as string) changement_denomination,
    safe_cast(denomination_usuelle_1 as string) denomination_usuelle_1,
    safe_cast(denomination_usuelle_2 as string) denomination_usuelle_2,
    safe_cast(denomination_usuelle_3 as string) denomination_usuelle_3,
    safe_cast(
        changement_denomination_usuelle as string
    ) changement_denomination_usuelle,
    safe_cast(categorie_juridique as string) categorie_juridique,
    safe_cast(changement_categorie_juridique as string) changement_categorie_juridique,
    safe_cast(activite_principale as string) activite_principale,
    safe_cast(
        nomenclature_activite_principale as string
    ) nomenclature_activite_principale,
    safe_cast(changement_activite_principale as string) changement_activite_principale,
    safe_cast(nic_siege as string) nic_siege,
    safe_cast(changement_nic_siege as string) changement_nic_siege,
    safe_cast(economie_sociale_solidaire as string) economie_sociale_solidaire,
    safe_cast(
        changement_economie_sociale_solidaire as string
    ) changement_economie_sociale_solidaire,
    safe_cast(caractere_employeur as string) caractere_employeur,
    safe_cast(changement_caractere_employeur as string) changement_caractere_employeur,
    safe_cast(societe_mission as string) societe_mission,
    safe_cast(changement_societe_mission as string) changement_societe_mission
from {{ set_datalake_project("fr_insee_sirene_staging.unite_legale_historico") }} as t
