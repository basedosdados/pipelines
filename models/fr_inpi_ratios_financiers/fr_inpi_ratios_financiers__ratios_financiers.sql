{{
    config(
        schema="fr_inpi_ratios_financiers",
        alias="ratios_financiers",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 1919, "end": 2034, "interval": 1},
        },
        cluster_by=["siren"],
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(siren as string) siren,
    safe_cast(date_cloture_exercice as date) date_cloture_exercice,
    safe_cast(type_bilan as string) type_bilan,
    safe_cast(chiffre_d_affaires as int64) chiffre_d_affaires,
    safe_cast(marge_brute as int64) marge_brute,
    safe_cast(ebe as int64) ebe,
    safe_cast(ebit as int64) ebit,
    safe_cast(resultat_net as int64) resultat_net,
    safe_cast(taux_d_endettement as float64) taux_d_endettement,
    safe_cast(ratio_de_liquidite as float64) ratio_de_liquidite,
    safe_cast(ratio_de_vetuste as float64) ratio_de_vetuste,
    safe_cast(autonomie_financiere as float64) autonomie_financiere,
    safe_cast(poids_bfr_exploitation_sur_ca as float64) poids_bfr_exploitation_sur_ca,
    safe_cast(couverture_des_interets as float64) couverture_des_interets,
    safe_cast(caf_sur_ca as float64) caf_sur_ca,
    safe_cast(capacite_de_remboursement as float64) capacite_de_remboursement,
    safe_cast(marge_ebe as float64) marge_ebe,
    safe_cast(
        resultat_courant_avant_impots_sur_ca as float64
    ) resultat_courant_avant_impots_sur_ca,
    safe_cast(
        poids_bfr_exploitation_sur_ca_jours as float64
    ) poids_bfr_exploitation_sur_ca_jours,
    safe_cast(rotation_des_stocks_jours as float64) rotation_des_stocks_jours,
    safe_cast(credit_clients_jours as float64) credit_clients_jours,
    safe_cast(credit_fournisseurs_jours as float64) credit_fournisseurs_jours,
    safe_cast(confidentialite as string) confidentialite
from
    {{ set_datalake_project("fr_inpi_ratios_financiers_staging.ratios_financiers") }}
    as t
