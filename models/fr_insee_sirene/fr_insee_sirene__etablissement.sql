{{
    config(
        schema="fr_insee_sirene",
        alias="etablissement",
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
    safe_cast(statut_diffusion as string) statut_diffusion,
    safe_cast(date_creation as date) date_creation,
    safe_cast(tranche_effectifs as string) tranche_effectifs,
    safe_cast(annee_effectifs as int64) annee_effectifs,
    safe_cast(
        activite_principale_registre_metiers as string
    ) activite_principale_registre_metiers,
    safe_cast(date_dernier_traitement as date) date_dernier_traitement,
    safe_cast(etablissement_siege as string) etablissement_siege,
    safe_cast(nombre_periodes as int64) nombre_periodes,
    safe_cast(complement_adresse as string) complement_adresse,
    safe_cast(numero_voie as string) numero_voie,
    safe_cast(indice_repetition as string) indice_repetition,
    safe_cast(type_voie as string) type_voie,
    safe_cast(libelle_voie as string) libelle_voie,
    safe_cast(code_postal as string) code_postal,
    safe_cast(libelle_commune as string) libelle_commune,
    safe_cast(libelle_commune_etranger as string) libelle_commune_etranger,
    safe_cast(distribution_speciale as string) distribution_speciale,
    safe_cast(code_commune as string) code_commune,
    safe_cast(code_cedex as string) code_cedex,
    safe_cast(libelle_cedex as string) libelle_cedex,
    safe_cast(code_pays_etranger as string) code_pays_etranger,
    safe_cast(libelle_pays_etranger as string) libelle_pays_etranger,
    safe_cast(complement_adresse_2 as string) complement_adresse_2,
    safe_cast(numero_voie_2 as string) numero_voie_2,
    safe_cast(indice_repetition_2 as string) indice_repetition_2,
    safe_cast(type_voie_2 as string) type_voie_2,
    safe_cast(libelle_voie_2 as string) libelle_voie_2,
    safe_cast(code_postal_2 as string) code_postal_2,
    safe_cast(libelle_commune_2 as string) libelle_commune_2,
    safe_cast(libelle_commune_etranger_2 as string) libelle_commune_etranger_2,
    safe_cast(distribution_speciale_2 as string) distribution_speciale_2,
    safe_cast(code_commune_2 as string) code_commune_2,
    safe_cast(code_cedex_2 as string) code_cedex_2,
    safe_cast(libelle_cedex_2 as string) libelle_cedex_2,
    safe_cast(code_pays_etranger_2 as string) code_pays_etranger_2,
    safe_cast(libelle_pays_etranger_2 as string) libelle_pays_etranger_2,
    safe_cast(date_debut as date) date_debut,
    safe_cast(etat_administratif as string) etat_administratif,
    safe_cast(enseigne_1 as string) enseigne_1,
    safe_cast(enseigne_2 as string) enseigne_2,
    safe_cast(enseigne_3 as string) enseigne_3,
    safe_cast(denomination_usuelle as string) denomination_usuelle,
    safe_cast(activite_principale as string) activite_principale,
    safe_cast(
        nomenclature_activite_principale as string
    ) nomenclature_activite_principale,
    safe_cast(caractere_employeur as string) caractere_employeur,
    safe_cast(activite_principale_naf25 as string) activite_principale_naf25,
    safe_cast(x as float64) x,
    safe_cast(y as float64) y,
    safe_cast(qualite_geocodificacao as string) qualite_geocodificacao,
    safe_cast(epsg as string) epsg,
    safe_cast(code_commune_geolocalisation as string) code_commune_geolocalisation,
    safe_cast(qp24 as string) qp24,
    safe_cast(qp15 as string) qp15,
    safe_cast(iris as string) iris,
    safe_cast(zus as string) zus,
    safe_cast(qva as string) qva,
    safe_cast(distance_precision as float64) distance_precision,
    safe_cast(qualite_qp24 as string) qualite_qp24,
    safe_cast(qualite_qp15 as string) qualite_qp15,
    safe_cast(qualite_iris as string) qualite_iris,
    safe_cast(qualite_zus as string) qualite_zus,
    safe_cast(qualite_qva as string) qualite_qva,
    safe_cast(latitude as float64) latitude,
    safe_cast(longitude as float64) longitude,
    case
        when longitude is not null and latitude is not null
        then
            st_geogpoint(
                safe_cast(longitude as float64), safe_cast(latitude as float64)
            )
    end geometria,
    safe_cast(dernier_numero_voie as string) dernier_numero_voie,
    safe_cast(
        indice_repetition_dernier_numero_voie as string
    ) indice_repetition_dernier_numero_voie,
    safe_cast(identifiant_adresse as string) identifiant_adresse,
    safe_cast(coordonnee_lambert_abscisse as float64) coordonnee_lambert_abscisse,
    safe_cast(coordonnee_lambert_ordonnee as float64) coordonnee_lambert_ordonnee
from {{ set_datalake_project("fr_insee_sirene_staging.etablissement") }} as t
