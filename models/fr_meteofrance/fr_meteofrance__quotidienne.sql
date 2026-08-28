{{
    config(
        schema="fr_meteofrance",
        alias="quotidienne",
        materialized="table",
        partition_by={
            "field": "annee",
            "data_type": "int64",
            "range": {"start": 1688, "end": 2031, "interval": 1},
        },
        cluster_by=["numero_poste"],
    )
}}


select
    safe_cast(annee as int64) annee,
    safe_cast(mois as int64) mois,
    safe_cast(date as date) date,
    safe_cast(numero_poste as string) numero_poste,
    safe_cast(precipitation as float64) precipitation,
    safe_cast(qualite_precipitation as string) qualite_precipitation,
    safe_cast(temperature_minimale as float64) temperature_minimale,
    safe_cast(qualite_temperature_minimale as string) qualite_temperature_minimale,
    safe_cast(heure_temperature_minimale as string) heure_temperature_minimale,
    safe_cast(qualite_htn as string) qualite_htn,
    safe_cast(temperature_maximale as float64) temperature_maximale,
    safe_cast(qualite_temperature_maximale as string) qualite_temperature_maximale,
    safe_cast(heure_temperature_maximale as string) heure_temperature_maximale,
    safe_cast(qualite_htx as string) qualite_htx,
    safe_cast(temperature_moyenne as float64) temperature_moyenne,
    safe_cast(qualite_temperature_moyenne as string) qualite_temperature_moyenne,
    safe_cast(temperature_moyenne_min_max as float64) temperature_moyenne_min_max,
    safe_cast(
        qualite_temperature_moyenne_min_max as string
    ) qualite_temperature_moyenne_min_max,
    safe_cast(amplitude_thermique as float64) amplitude_thermique,
    safe_cast(qualite_amplitude_thermique as string) qualite_amplitude_thermique,
    safe_cast(temperature_minimale_sol_10cm as float64) temperature_minimale_sol_10cm,
    safe_cast(
        qualite_temperature_minimale_sol_10cm as string
    ) qualite_temperature_minimale_sol_10cm,
    safe_cast(temperature_minimale_sol_50cm as float64) temperature_minimale_sol_50cm,
    safe_cast(
        qualite_temperature_minimale_sol_50cm as string
    ) qualite_temperature_minimale_sol_50cm,
    safe_cast(duree_gel as float64) duree_gel,
    safe_cast(qualite_duree_gel as string) qualite_duree_gel,
    safe_cast(vitesse_vent_moyenne_10m as float64) vitesse_vent_moyenne_10m,
    safe_cast(
        qualite_vitesse_vent_moyenne_10m as string
    ) qualite_vitesse_vent_moyenne_10m,
    safe_cast(vitesse_vent_moyenne_2m as float64) vitesse_vent_moyenne_2m,
    safe_cast(
        qualite_vitesse_vent_moyenne_2m as string
    ) qualite_vitesse_vent_moyenne_2m,
    safe_cast(
        vitesse_vent_maximale_moyennee_10m as float64
    ) vitesse_vent_maximale_moyennee_10m,
    safe_cast(
        qualite_vitesse_vent_maximale_moyennee_10m as string
    ) qualite_vitesse_vent_maximale_moyennee_10m,
    safe_cast(
        direction_vent_maximal_moyenne_10m as int64
    ) direction_vent_maximal_moyenne_10m,
    safe_cast(
        qualite_direction_vent_maximal_moyenne_10m as string
    ) qualite_direction_vent_maximal_moyenne_10m,
    safe_cast(
        heure_vitesse_vent_maximale_moyennee_10m as string
    ) heure_vitesse_vent_maximale_moyennee_10m,
    safe_cast(qualite_hxy as string) qualite_hxy,
    safe_cast(rafale_maximale_10m as float64) rafale_maximale_10m,
    safe_cast(qualite_rafale_maximale_10m as string) qualite_rafale_maximale_10m,
    safe_cast(direction_rafale_maximale_10m as int64) direction_rafale_maximale_10m,
    safe_cast(
        qualite_direction_rafale_maximale_10m as string
    ) qualite_direction_rafale_maximale_10m,
    safe_cast(heure_rafale_maximale_10m as string) heure_rafale_maximale_10m,
    safe_cast(qualite_hxi as string) qualite_hxi,
    safe_cast(rafale_maximale_2m as float64) rafale_maximale_2m,
    safe_cast(qualite_rafale_maximale_2m as string) qualite_rafale_maximale_2m,
    safe_cast(direction_rafale_maximale_2m as int64) direction_rafale_maximale_2m,
    safe_cast(
        qualite_direction_rafale_maximale_2m as string
    ) qualite_direction_rafale_maximale_2m,
    safe_cast(heure_rafale_maximale_2m as string) heure_rafale_maximale_2m,
    safe_cast(qualite_hxi2 as string) qualite_hxi2,
    safe_cast(rafale_maximale_3s_10m as float64) rafale_maximale_3s_10m,
    safe_cast(qualite_rafale_maximale_3s_10m as string) qualite_rafale_maximale_3s_10m,
    safe_cast(
        direction_rafale_maximale_3s_10m as int64
    ) direction_rafale_maximale_3s_10m,
    safe_cast(
        qualite_direction_rafale_maximale_3s_10m as string
    ) qualite_direction_rafale_maximale_3s_10m,
    safe_cast(heure_rafale_maximale_3s_10m as string) heure_rafale_maximale_3s_10m,
    safe_cast(qualite_hxi3s as string) qualite_hxi3s,
    safe_cast(duree_precipitation as float64) duree_precipitation,
    safe_cast(qualite_duree_precipitation as string) qualite_duree_precipitation,
    safe_cast(mode_obtention_rafale_3s as string) mode_obtention_rafale_3s,
    safe_cast(
        mode_obtention_direction_rafale_3s as string
    ) mode_obtention_direction_rafale_3s,
    safe_cast(duree_humectation as float64) duree_humectation,
    safe_cast(qualite_duree_humectation as string) qualite_duree_humectation,
    safe_cast(pression_mer_moyenne as float64) pression_mer_moyenne,
    safe_cast(qualite_pression_mer_moyenne as string) qualite_pression_mer_moyenne,
    safe_cast(pression_mer_minimale as float64) pression_mer_minimale,
    safe_cast(qualite_pression_mer_minimale as string) qualite_pression_mer_minimale,
    safe_cast(duree_insolation as float64) duree_insolation,
    safe_cast(qualite_duree_insolation as string) qualite_duree_insolation,
    safe_cast(rayonnement_global as float64) rayonnement_global,
    safe_cast(qualite_rayonnement_global as string) qualite_rayonnement_global,
    safe_cast(rayonnement_diffus as float64) rayonnement_diffus,
    safe_cast(qualite_rayonnement_diffus as string) qualite_rayonnement_diffus,
    safe_cast(rayonnement_direct as float64) rayonnement_direct,
    safe_cast(qualite_rayonnement_direct as string) qualite_rayonnement_direct,
    safe_cast(rayonnement_infrarouge as float64) rayonnement_infrarouge,
    safe_cast(qualite_rayonnement_infrarouge as string) qualite_rayonnement_infrarouge,
    safe_cast(rayonnement_ultraviolet as float64) rayonnement_ultraviolet,
    safe_cast(
        qualite_rayonnement_ultraviolet as string
    ) qualite_rayonnement_ultraviolet,
    safe_cast(indice_uv_maximal as float64) indice_uv_maximal,
    safe_cast(qualite_indice_uv_maximal as string) qualite_indice_uv_maximal,
    safe_cast(fraction_insolation as float64) fraction_insolation,
    safe_cast(qualite_fraction_insolation as string) qualite_fraction_insolation,
    safe_cast(humidite_minimale as float64) humidite_minimale,
    safe_cast(qualite_humidite_minimale as string) qualite_humidite_minimale,
    safe_cast(heure_humidite_minimale as string) heure_humidite_minimale,
    safe_cast(qualite_hun as string) qualite_hun,
    safe_cast(humidite_maximale as float64) humidite_maximale,
    safe_cast(qualite_humidite_maximale as string) qualite_humidite_maximale,
    safe_cast(heure_humidite_maximale as string) heure_humidite_maximale,
    safe_cast(qualite_hux as string) qualite_hux,
    safe_cast(humidite_moyenne as float64) humidite_moyenne,
    safe_cast(qualite_humidite_moyenne as string) qualite_humidite_moyenne,
    safe_cast(duree_humidite_inf_40 as float64) duree_humidite_inf_40,
    safe_cast(qualite_duree_humidite_inf_40 as string) qualite_duree_humidite_inf_40,
    safe_cast(duree_humidite_sup_80 as float64) duree_humidite_sup_80,
    safe_cast(qualite_duree_humidite_sup_80 as string) qualite_duree_humidite_sup_80,
    safe_cast(tension_vapeur_moyenne as float64) tension_vapeur_moyenne,
    safe_cast(qualite_tension_vapeur_moyenne as string) qualite_tension_vapeur_moyenne,
    safe_cast(
        evapotranspiration_penman_monteith as float64
    ) evapotranspiration_penman_monteith,
    safe_cast(
        qualite_evapotranspiration_penman_monteith as string
    ) qualite_evapotranspiration_penman_monteith,
    safe_cast(
        evapotranspiration_point_grille as float64
    ) evapotranspiration_point_grille,
    safe_cast(
        qualite_evapotranspiration_point_grille as string
    ) qualite_evapotranspiration_point_grille,
    safe_cast(hauteur_neige_fraiche as float64) hauteur_neige_fraiche,
    safe_cast(qualite_hauteur_neige_fraiche as string) qualite_hauteur_neige_fraiche,
    safe_cast(hauteur_neige_maximale as float64) hauteur_neige_maximale,
    safe_cast(qualite_hauteur_neige_maximale as string) qualite_hauteur_neige_maximale,
    safe_cast(hauteur_neige_06h as float64) hauteur_neige_06h,
    safe_cast(qualite_hauteur_neige_06h as string) qualite_hauteur_neige_06h,
    safe_cast(occurrence_neige as string) occurrence_neige,
    safe_cast(qualite_occurrence_neige as string) qualite_occurrence_neige,
    safe_cast(occurrence_brouillard as string) occurrence_brouillard,
    safe_cast(qualite_occurrence_brouillard as string) qualite_occurrence_brouillard,
    safe_cast(occurrence_orage as string) occurrence_orage,
    safe_cast(qualite_occurrence_orage as string) qualite_occurrence_orage,
    safe_cast(occurrence_gresil as string) occurrence_gresil,
    safe_cast(qualite_occurrence_gresil as string) qualite_occurrence_gresil,
    safe_cast(occurrence_grele as string) occurrence_grele,
    safe_cast(qualite_occurrence_grele as string) qualite_occurrence_grele,
    safe_cast(occurrence_rosee as string) occurrence_rosee,
    safe_cast(qualite_occurrence_rosee as string) qualite_occurrence_rosee,
    safe_cast(occurrence_verglas as string) occurrence_verglas,
    safe_cast(qualite_occurrence_verglas as string) qualite_occurrence_verglas,
    safe_cast(occurrence_sol_enneige as string) occurrence_sol_enneige,
    safe_cast(qualite_occurrence_sol_enneige as string) qualite_occurrence_sol_enneige,
    safe_cast(occurrence_gelee_blanche as string) occurrence_gelee_blanche,
    safe_cast(
        qualite_occurrence_gelee_blanche as string
    ) qualite_occurrence_gelee_blanche,
    safe_cast(occurrence_fumee as string) occurrence_fumee,
    safe_cast(qualite_occurrence_fumee as string) qualite_occurrence_fumee,
    safe_cast(occurrence_brume as string) occurrence_brume,
    safe_cast(qualite_occurrence_brume as string) qualite_occurrence_brume,
    safe_cast(occurrence_eclair as string) occurrence_eclair,
    safe_cast(qualite_occurrence_eclair as string) qualite_occurrence_eclair,
    safe_cast(nebulosite_maximale_sous_300m as int64) nebulosite_maximale_sous_300m,
    safe_cast(
        qualite_nebulosite_maximale_sous_300m as string
    ) qualite_nebulosite_maximale_sous_300m,
    safe_cast(hauteur_base_sous_300m as float64) hauteur_base_sous_300m,
    safe_cast(qualite_hauteur_base_sous_300m as string) qualite_hauteur_base_sous_300m,
    safe_cast(temperature_mer_minimale as float64) temperature_mer_minimale,
    safe_cast(
        qualite_temperature_mer_minimale as string
    ) qualite_temperature_mer_minimale,
    safe_cast(temperature_mer_maximale as float64) temperature_mer_maximale,
    safe_cast(
        qualite_temperature_mer_maximale as string
    ) qualite_temperature_mer_maximale
from {{ set_datalake_project("fr_meteofrance_staging.quotidienne") }} as t
