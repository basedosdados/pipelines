{{
    config(
        schema="fr_meteofrance",
        alias="mensuelle",
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
    safe_cast(numero_poste as string) numero_poste,
    safe_cast(precipitation_totale as float64) precipitation_totale,
    safe_cast(qualite_precipitation_totale as string) qualite_precipitation_totale,
    safe_cast(nombre_valeurs_precipitation as int64) nombre_valeurs_precipitation,
    safe_cast(precipitation_totale_estimee as float64) precipitation_totale_estimee,
    safe_cast(
        precipitation_quotidienne_maximale as float64
    ) precipitation_quotidienne_maximale,
    safe_cast(
        qualite_precipitation_quotidienne_maximale as string
    ) qualite_precipitation_quotidienne_maximale,
    safe_cast(
        jour_precipitation_quotidienne_maximale as int64
    ) jour_precipitation_quotidienne_maximale,
    safe_cast(nombre_jours_rr1 as int64) nombre_jours_rr1,
    safe_cast(nombre_jours_rr5 as int64) nombre_jours_rr5,
    safe_cast(nombre_jours_rr10 as int64) nombre_jours_rr10,
    safe_cast(nombre_jours_rr30 as int64) nombre_jours_rr30,
    safe_cast(nombre_jours_rr50 as int64) nombre_jours_rr50,
    safe_cast(nombre_jours_rr100 as int64) nombre_jours_rr100,
    safe_cast(pression_mer_moyenne as float64) pression_mer_moyenne,
    safe_cast(qualite_pression_mer_moyenne as string) qualite_pression_mer_moyenne,
    safe_cast(
        nombre_valeurs_pression_mer_moyenne as int64
    ) nombre_valeurs_pression_mer_moyenne,
    safe_cast(pression_mer_minimale_absolue as float64) pression_mer_minimale_absolue,
    safe_cast(
        qualite_pression_mer_minimale_absolue as string
    ) qualite_pression_mer_minimale_absolue,
    safe_cast(
        jour_pression_mer_minimale_absolue as int64
    ) jour_pression_mer_minimale_absolue,
    safe_cast(temperature_maximale_moyenne as float64) temperature_maximale_moyenne,
    safe_cast(
        qualite_temperature_maximale_moyenne as string
    ) qualite_temperature_maximale_moyenne,
    safe_cast(
        nombre_valeurs_temperature_maximale as int64
    ) nombre_valeurs_temperature_maximale,
    safe_cast(
        temperature_maximale_moyenne_estimee as float64
    ) temperature_maximale_moyenne_estimee,
    safe_cast(temperature_maximale_absolue as float64) temperature_maximale_absolue,
    safe_cast(
        qualite_temperature_maximale_absolue as string
    ) qualite_temperature_maximale_absolue,
    safe_cast(
        jour_temperature_maximale_absolue as int64
    ) jour_temperature_maximale_absolue,
    safe_cast(
        temperature_maximale_la_plus_basse as float64
    ) temperature_maximale_la_plus_basse,
    safe_cast(
        qualite_temperature_maximale_la_plus_basse as string
    ) qualite_temperature_maximale_la_plus_basse,
    safe_cast(
        jour_temperature_maximale_la_plus_basse as int64
    ) jour_temperature_maximale_la_plus_basse,
    safe_cast(nombre_jours_tx0 as int64) nombre_jours_tx0,
    safe_cast(nombre_jours_tx25 as int64) nombre_jours_tx25,
    safe_cast(nombre_jours_tx30 as int64) nombre_jours_tx30,
    safe_cast(nombre_jours_tx35 as int64) nombre_jours_tx35,
    safe_cast(nombre_jours_txi20 as int64) nombre_jours_txi20,
    safe_cast(nombre_jours_txi27 as int64) nombre_jours_txi27,
    safe_cast(nombre_jours_txs32 as int64) nombre_jours_txs32,
    safe_cast(temperature_minimale_moyenne as float64) temperature_minimale_moyenne,
    safe_cast(
        qualite_temperature_minimale_moyenne as string
    ) qualite_temperature_minimale_moyenne,
    safe_cast(
        nombre_valeurs_temperature_minimale as int64
    ) nombre_valeurs_temperature_minimale,
    safe_cast(
        temperature_minimale_moyenne_estimee as float64
    ) temperature_minimale_moyenne_estimee,
    safe_cast(temperature_minimale_absolue as float64) temperature_minimale_absolue,
    safe_cast(
        qualite_temperature_minimale_absolue as string
    ) qualite_temperature_minimale_absolue,
    safe_cast(
        jour_temperature_minimale_absolue as int64
    ) jour_temperature_minimale_absolue,
    safe_cast(
        temperature_minimale_la_plus_haute as float64
    ) temperature_minimale_la_plus_haute,
    safe_cast(
        qualite_temperature_minimale_la_plus_haute as string
    ) qualite_temperature_minimale_la_plus_haute,
    safe_cast(
        jour_temperature_minimale_la_plus_haute as int64
    ) jour_temperature_minimale_la_plus_haute,
    safe_cast(nombre_jours_tn5 as int64) nombre_jours_tn5,
    safe_cast(nombre_jours_tn10 as int64) nombre_jours_tn10,
    safe_cast(nombre_jours_tni10 as int64) nombre_jours_tni10,
    safe_cast(nombre_jours_tni15 as int64) nombre_jours_tni15,
    safe_cast(nombre_jours_tni20 as int64) nombre_jours_tni20,
    safe_cast(nombre_jours_tns20 as int64) nombre_jours_tns20,
    safe_cast(nombre_jours_tns25 as int64) nombre_jours_tns25,
    safe_cast(nombre_jours_gelee as int64) nombre_jours_gelee,
    safe_cast(amplitude_thermique_moyenne as float64) amplitude_thermique_moyenne,
    safe_cast(
        qualite_amplitude_thermique_moyenne as string
    ) qualite_amplitude_thermique_moyenne,
    safe_cast(amplitude_thermique_absolue as float64) amplitude_thermique_absolue,
    safe_cast(
        qualite_amplitude_thermique_absolue as string
    ) qualite_amplitude_thermique_absolue,
    safe_cast(
        jour_amplitude_thermique_absolue as int64
    ) jour_amplitude_thermique_absolue,
    safe_cast(
        nombre_valeurs_amplitude_thermique as int64
    ) nombre_valeurs_amplitude_thermique,
    safe_cast(temperature_moyenne_min_max as float64) temperature_moyenne_min_max,
    safe_cast(
        qualite_temperature_moyenne_min_max as string
    ) qualite_temperature_moyenne_min_max,
    safe_cast(
        nombre_valeurs_temperature_moyenne_min_max as int64
    ) nombre_valeurs_temperature_moyenne_min_max,
    safe_cast(temperature_moyenne as float64) temperature_moyenne,
    safe_cast(qualite_temperature_moyenne as string) qualite_temperature_moyenne,
    safe_cast(
        nombre_valeurs_temperature_moyenne as int64
    ) nombre_valeurs_temperature_moyenne,
    safe_cast(nombre_jours_tms24 as int64) nombre_jours_tms24,
    safe_cast(
        temperature_moyenne_min_max_minimale as float64
    ) temperature_moyenne_min_max_minimale,
    safe_cast(
        qualite_temperature_moyenne_min_max_minimale as string
    ) qualite_temperature_moyenne_min_max_minimale,
    safe_cast(
        jour_temperature_moyenne_min_max_minimale as int64
    ) jour_temperature_moyenne_min_max_minimale,
    safe_cast(
        temperature_moyenne_min_max_maximale as float64
    ) temperature_moyenne_min_max_maximale,
    safe_cast(
        qualite_temperature_moyenne_min_max_maximale as string
    ) qualite_temperature_moyenne_min_max_maximale,
    safe_cast(
        jour_temperature_moyenne_min_max_maximale as int64
    ) jour_temperature_moyenne_min_max_maximale,
    safe_cast(humidite_minimale_absolue as float64) humidite_minimale_absolue,
    safe_cast(
        qualite_humidite_minimale_absolue as string
    ) qualite_humidite_minimale_absolue,
    safe_cast(jour_humidite_minimale_absolue as int64) jour_humidite_minimale_absolue,
    safe_cast(
        nombre_valeurs_humidite_minimale as int64
    ) nombre_valeurs_humidite_minimale,
    safe_cast(humidite_maximale_absolue as float64) humidite_maximale_absolue,
    safe_cast(
        qualite_humidite_maximale_absolue as string
    ) qualite_humidite_maximale_absolue,
    safe_cast(jour_humidite_maximale_absolue as int64) jour_humidite_maximale_absolue,
    safe_cast(
        nombre_valeurs_humidite_maximale as int64
    ) nombre_valeurs_humidite_maximale,
    safe_cast(humidite_moyenne as float64) humidite_moyenne,
    safe_cast(qualite_humidite_moyenne as string) qualite_humidite_moyenne,
    safe_cast(nombre_valeurs_humidite_moyenne as int64) nombre_valeurs_humidite_moyenne,
    safe_cast(tension_vapeur_moyenne as float64) tension_vapeur_moyenne,
    safe_cast(qualite_tension_vapeur_moyenne as string) qualite_tension_vapeur_moyenne,
    safe_cast(nombre_valeurs_tsv as int64) nombre_valeurs_tsv,
    safe_cast(evapotranspiration_totale as float64) evapotranspiration_totale,
    safe_cast(
        qualite_evapotranspiration_totale as string
    ) qualite_evapotranspiration_totale,
    safe_cast(rafale_maximale_absolue_10m as float64) rafale_maximale_absolue_10m,
    safe_cast(
        qualite_rafale_maximale_absolue_10m as string
    ) qualite_rafale_maximale_absolue_10m,
    safe_cast(
        direction_rafale_maximale_absolue_10m as int64
    ) direction_rafale_maximale_absolue_10m,
    safe_cast(
        qualite_direction_rafale_maximale_absolue_10m as string
    ) qualite_direction_rafale_maximale_absolue_10m,
    safe_cast(
        jour_rafale_maximale_absolue_10m as int64
    ) jour_rafale_maximale_absolue_10m,
    safe_cast(nombre_jours_ff10 as int64) nombre_jours_ff10,
    safe_cast(nombre_jours_ff16 as int64) nombre_jours_ff16,
    safe_cast(nombre_jours_ff28 as int64) nombre_jours_ff28,
    safe_cast(
        nombre_valeurs_rafale_maximale_10m as int64
    ) nombre_valeurs_rafale_maximale_10m,
    safe_cast(rafale_maximale_3s_absolue_10m as float64) rafale_maximale_3s_absolue_10m,
    safe_cast(
        qualite_rafale_maximale_3s_absolue_10m as string
    ) qualite_rafale_maximale_3s_absolue_10m,
    safe_cast(
        direction_rafale_maximale_3s_absolue_10m as int64
    ) direction_rafale_maximale_3s_absolue_10m,
    safe_cast(
        qualite_direction_rafale_maximale_3s_absolue_10m as string
    ) qualite_direction_rafale_maximale_3s_absolue_10m,
    safe_cast(
        jour_rafale_maximale_3s_absolue_10m as int64
    ) jour_rafale_maximale_3s_absolue_10m,
    safe_cast(nombre_jours_fxi3s10 as int64) nombre_jours_fxi3s10,
    safe_cast(nombre_jours_fxi3s16 as int64) nombre_jours_fxi3s16,
    safe_cast(nombre_jours_fxi3s28 as int64) nombre_jours_fxi3s28,
    safe_cast(
        nombre_valeurs_rafale_maximale_3s_10m as int64
    ) nombre_valeurs_rafale_maximale_3s_10m,
    safe_cast(
        vitesse_vent_maximale_moyennee_absolue_10m as float64
    ) vitesse_vent_maximale_moyennee_absolue_10m,
    safe_cast(
        qualite_vitesse_vent_maximale_moyennee_absolue_10m as string
    ) qualite_vitesse_vent_maximale_moyennee_absolue_10m,
    safe_cast(
        direction_vent_maximal_moyenne_absolue_10m as int64
    ) direction_vent_maximal_moyenne_absolue_10m,
    safe_cast(
        qualite_direction_vent_maximal_moyenne_absolue_10m as string
    ) qualite_direction_vent_maximal_moyenne_absolue_10m,
    safe_cast(
        jour_vitesse_vent_maximale_moyennee_absolue_10m as int64
    ) jour_vitesse_vent_maximale_moyennee_absolue_10m,
    safe_cast(nombre_jours_fxy8 as int64) nombre_jours_fxy8,
    safe_cast(nombre_jours_fxy10 as int64) nombre_jours_fxy10,
    safe_cast(nombre_jours_fxy15 as int64) nombre_jours_fxy15,
    safe_cast(
        nombre_valeurs_vitesse_vent_maximale_moyennee_10m as int64
    ) nombre_valeurs_vitesse_vent_maximale_moyennee_10m,
    safe_cast(vitesse_vent_moyenne_10m as float64) vitesse_vent_moyenne_10m,
    safe_cast(
        qualite_vitesse_vent_moyenne_10m as string
    ) qualite_vitesse_vent_moyenne_10m,
    safe_cast(
        nombre_valeurs_vitesse_vent_moyenne_10m as int64
    ) nombre_valeurs_vitesse_vent_moyenne_10m,
    safe_cast(duree_insolation_totale as float64) duree_insolation_totale,
    safe_cast(
        qualite_duree_insolation_totale as string
    ) qualite_duree_insolation_totale,
    safe_cast(nombre_valeurs_duree_insolation as int64) nombre_valeurs_duree_insolation,
    safe_cast(nombre_jours_sigma0 as int64) nombre_jours_sigma0,
    safe_cast(nombre_jours_sigma20 as int64) nombre_jours_sigma20,
    safe_cast(nombre_jours_sigma80 as int64) nombre_jours_sigma80,
    safe_cast(rayonnement_global_total as float64) rayonnement_global_total,
    safe_cast(
        qualite_rayonnement_global_total as string
    ) qualite_rayonnement_global_total,
    safe_cast(
        nombre_valeurs_rayonnement_global as int64
    ) nombre_valeurs_rayonnement_global,
    safe_cast(rayonnement_diffus_total as float64) rayonnement_diffus_total,
    safe_cast(
        qualite_rayonnement_diffus_total as string
    ) qualite_rayonnement_diffus_total,
    safe_cast(
        nombre_valeurs_rayonnement_diffus as int64
    ) nombre_valeurs_rayonnement_diffus,
    safe_cast(rayonnement_direct_total as float64) rayonnement_direct_total,
    safe_cast(
        qualite_rayonnement_direct_total as string
    ) qualite_rayonnement_direct_total,
    safe_cast(
        nombre_valeurs_rayonnement_direct as int64
    ) nombre_valeurs_rayonnement_direct,
    safe_cast(hauteur_neige_fraiche_totale as float64) hauteur_neige_fraiche_totale,
    safe_cast(
        qualite_hauteur_neige_fraiche_totale as string
    ) qualite_hauteur_neige_fraiche_totale,
    safe_cast(hauteur_neige_fraiche_maximale as float64) hauteur_neige_fraiche_maximale,
    safe_cast(
        qualite_hauteur_neige_fraiche_maximale as string
    ) qualite_hauteur_neige_fraiche_maximale,
    safe_cast(
        jour_hauteur_neige_fraiche_maximale as int64
    ) jour_hauteur_neige_fraiche_maximale,
    safe_cast(
        nombre_valeurs_hauteur_neige_fraiche as int64
    ) nombre_valeurs_hauteur_neige_fraiche,
    safe_cast(nombre_jours_neig as int64) nombre_jours_neig,
    safe_cast(nombre_jours_hneigef1 as int64) nombre_jours_hneigef1,
    safe_cast(nombre_jours_hneigef5 as int64) nombre_jours_hneigef5,
    safe_cast(nombre_jours_hneigef10 as int64) nombre_jours_hneigef10,
    safe_cast(nombre_jours_solng as int64) nombre_jours_solng,
    safe_cast(hauteur_neige_moyenne as float64) hauteur_neige_moyenne,
    safe_cast(qualite_hauteur_neige_moyenne as string) qualite_hauteur_neige_moyenne,
    safe_cast(hauteur_neige_maximale as float64) hauteur_neige_maximale,
    safe_cast(qualite_hauteur_neige_maximale as string) qualite_hauteur_neige_maximale,
    safe_cast(jour_hauteur_neige_maximale as int64) jour_hauteur_neige_maximale,
    safe_cast(nombre_jours_neigetot1 as int64) nombre_jours_neigetot1,
    safe_cast(nombre_jours_neigetot10 as int64) nombre_jours_neigetot10,
    safe_cast(nombre_jours_neigetot30 as int64) nombre_jours_neigetot30,
    safe_cast(nombre_jours_grel as int64) nombre_jours_grel,
    safe_cast(nombre_jours_orag as int64) nombre_jours_orag,
    safe_cast(nombre_jours_brou as int64) nombre_jours_brou
from {{ set_datalake_project("fr_meteofrance_staging.mensuelle") }} as t
