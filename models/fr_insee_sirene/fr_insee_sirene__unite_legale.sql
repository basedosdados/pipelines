{{
    config(
        schema="fr_insee_sirene",
        alias="unite_legale",
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
    safe_cast(statut_diffusion as string) statut_diffusion,
    safe_cast(unite_purgee as string) unite_purgee,
    safe_cast(date_creation as date) date_creation,
    safe_cast(sigle as string) sigle,
    safe_cast(sexe as string) sexe,
    safe_cast(prenom_1 as string) prenom_1,
    safe_cast(prenom_2 as string) prenom_2,
    safe_cast(prenom_3 as string) prenom_3,
    safe_cast(prenom_4 as string) prenom_4,
    safe_cast(prenom_usuel as string) prenom_usuel,
    safe_cast(pseudonyme as string) pseudonyme,
    safe_cast(identifiant_association as string) identifiant_association,
    safe_cast(tranche_effectifs as string) tranche_effectifs,
    safe_cast(annee_effectifs as int64) annee_effectifs,
    safe_cast(date_dernier_traitement as date) date_dernier_traitement,
    safe_cast(nombre_periodes as int64) nombre_periodes,
    safe_cast(categorie_entreprise as string) categorie_entreprise,
    safe_cast(annee_categorie_entreprise as int64) annee_categorie_entreprise,
    safe_cast(date_debut as date) date_debut,
    safe_cast(etat_administratif as string) etat_administratif,
    safe_cast(nom as string) nom,
    safe_cast(nom_usage as string) nom_usage,
    safe_cast(denomination as string) denomination,
    safe_cast(denomination_usuelle_1 as string) denomination_usuelle_1,
    safe_cast(denomination_usuelle_2 as string) denomination_usuelle_2,
    safe_cast(denomination_usuelle_3 as string) denomination_usuelle_3,
    safe_cast(categorie_juridique as string) categorie_juridique,
    safe_cast(activite_principale as string) activite_principale,
    safe_cast(
        nomenclature_activite_principale as string
    ) nomenclature_activite_principale,
    safe_cast(nic_siege as string) nic_siege,
    safe_cast(economie_sociale_solidaire as string) economie_sociale_solidaire,
    safe_cast(caractere_employeur as string) caractere_employeur,
    safe_cast(activite_principale_naf25 as string) activite_principale_naf25,
    safe_cast(societe_mission as string) societe_mission
from {{ set_datalake_project("fr_insee_sirene_staging.unite_legale") }} as t
