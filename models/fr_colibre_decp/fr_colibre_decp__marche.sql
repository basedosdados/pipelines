{{
    config(
        alias="marche",
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
    safe_cast(id_marche_acheteur as string) id_marche_acheteur,
    safe_cast(id_accord_cadre as string) id_accord_cadre,
    safe_cast(siret_acheteur as string) siret_acheteur,
    safe_cast(siren_acheteur as string) siren_acheteur,
    safe_cast(nom_acheteur as string) nom_acheteur,
    safe_cast(categorie_acheteur as string) categorie_acheteur,
    safe_cast(labels_acheteur as string) labels_acheteur,
    safe_cast(code_commune_acheteur as string) code_commune_acheteur,
    safe_cast(code_departement_acheteur as string) code_departement_acheteur,
    safe_cast(code_region_acheteur as string) code_region_acheteur,
    st_geogfromtext(
        safe_cast(geometria_acheteur as string), make_valid => true
    ) geometria_acheteur,
    safe_cast(nature as string) nature,
    safe_cast(objet as string) objet,
    safe_cast(type_marche as string) type_marche,
    safe_cast(code_cpv as string) code_cpv,
    safe_cast(procedure as string) procedure,
    safe_cast(techniques as string) techniques,
    safe_cast(modalites_execution as string) modalites_execution,
    safe_cast(date_notification as date) date_notification,
    safe_cast(date_publication_donnees as date) date_publication_donnees,
    safe_cast(duree_mois as int64) duree_mois,
    safe_cast(montant as float64) montant,
    safe_cast(montant_rationalise as float64) montant_rationalise,
    safe_cast(montant_anomalie as string) montant_anomalie,
    safe_cast(montant_anomalie_raisons as string) montant_anomalie_raisons,
    safe_cast(nombre_offres as int64) nombre_offres,
    safe_cast(forme_prix as string) forme_prix,
    safe_cast(types_prix as string) types_prix,
    safe_cast(attribution_avance as string) attribution_avance,
    safe_cast(taux_avance as float64) taux_avance,
    safe_cast(marche_innovant as string) marche_innovant,
    safe_cast(considerations_sociales as string) considerations_sociales,
    safe_cast(
        considerations_environnementales as string
    ) considerations_environnementales,
    safe_cast(ccag as string) ccag,
    safe_cast(sous_traitance_declaree as string) sous_traitance_declaree,
    safe_cast(type_groupement_operateurs as string) type_groupement_operateurs,
    safe_cast(origine_ue as float64) origine_ue,
    safe_cast(origine_france as float64) origine_france,
    safe_cast(code_lieu_execution as string) code_lieu_execution,
    safe_cast(type_code_lieu_execution as string) type_code_lieu_execution,
    safe_cast(source_donnees as string) source_donnees,
    safe_cast(fichier_source as string) fichier_source
from {{ set_datalake_project("fr_colibre_decp_staging.marche") }} as t
