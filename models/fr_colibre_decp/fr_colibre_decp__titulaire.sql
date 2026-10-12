{{
    config(
        alias="titulaire",
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
    safe_cast(id_titulaire as string) id_titulaire,
    safe_cast(type_identifiant_titulaire as string) type_identifiant_titulaire,
    safe_cast(siren_titulaire as string) siren_titulaire,
    safe_cast(nom_titulaire as string) nom_titulaire,
    safe_cast(categorie_titulaire as string) categorie_titulaire,
    safe_cast(labels_titulaire as string) labels_titulaire,
    safe_cast(code_activite_titulaire as string) code_activite_titulaire,
    safe_cast(code_commune_titulaire as string) code_commune_titulaire,
    safe_cast(code_departement_titulaire as string) code_departement_titulaire,
    safe_cast(code_region_titulaire as string) code_region_titulaire,
    st_geogfromtext(
        safe_cast(geometria_titulaire as string), make_valid => true
    ) geometria_titulaire,
    safe_cast(distance_acheteur as int64) distance_acheteur
from {{ set_datalake_project("fr_colibre_decp_staging.titulaire") }} as t
