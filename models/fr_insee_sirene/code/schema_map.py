"""Authoritative source->target->type mapping for fr_insee_sirene cleaning.

Each table: (target_name, source_name, target_type). Special source markers:
  "__DATA__" -> the snapshot DATE literal; "__ANO__" -> the snapshot year INT64.
Geoloc columns (etablissement) come from the joined geolocation file; their
`source` is the geoloc column name. `geometria` (GEOGRAPHY) is NOT built here —
the dbt model constructs it from longitude/latitude.

Types: STRING | DATE | INT64 | FLOAT64. The cleaner casts with TRY_CAST and
normalizes empty strings to NULL.
"""

SNAPSHOT_DATE = (
    "2026-08-01"  # base SIRENE stock snapshot (data.gouv last_modified)
)
SNAPSHOT_YEAR = 2026

INPUT = "~/Downloads/fr_insee_sirene_data/input"
OUTPUT = "~/Downloads/fr_insee_sirene_data/output"

UNITE_LEGALE = {
    "src_file": f"{INPUT}/StockUniteLegale.parquet",
    "columns": [
        ("data", "__DATA__", "DATE"),
        ("ano", "__ANO__", "INT64"),
        ("siren", "siren", "STRING"),
        ("statut_diffusion", "statutDiffusionUniteLegale", "STRING"),
        ("unite_purgee", "unitePurgeeUniteLegale", "STRING"),
        ("date_creation", "dateCreationUniteLegale", "DATE"),
        ("sigle", "sigleUniteLegale", "STRING"),
        ("sexe", "sexeUniteLegale", "STRING"),
        ("prenom_1", "prenom1UniteLegale", "STRING"),
        ("prenom_2", "prenom2UniteLegale", "STRING"),
        ("prenom_3", "prenom3UniteLegale", "STRING"),
        ("prenom_4", "prenom4UniteLegale", "STRING"),
        ("prenom_usuel", "prenomUsuelUniteLegale", "STRING"),
        ("pseudonyme", "pseudonymeUniteLegale", "STRING"),
        (
            "identifiant_association",
            "identifiantAssociationUniteLegale",
            "STRING",
        ),
        ("tranche_effectifs", "trancheEffectifsUniteLegale", "STRING"),
        ("annee_effectifs", "anneeEffectifsUniteLegale", "INT64"),
        (
            "date_dernier_traitement",
            "dateDernierTraitementUniteLegale",
            "DATE",
        ),
        ("nombre_periodes", "nombrePeriodesUniteLegale", "INT64"),
        ("categorie_entreprise", "categorieEntreprise", "STRING"),
        ("annee_categorie_entreprise", "anneeCategorieEntreprise", "INT64"),
        ("date_debut", "dateDebut", "DATE"),
        ("etat_administratif", "etatAdministratifUniteLegale", "STRING"),
        ("nom", "nomUniteLegale", "STRING"),
        ("nom_usage", "nomUsageUniteLegale", "STRING"),
        ("denomination", "denominationUniteLegale", "STRING"),
        (
            "denomination_usuelle_1",
            "denominationUsuelle1UniteLegale",
            "STRING",
        ),
        (
            "denomination_usuelle_2",
            "denominationUsuelle2UniteLegale",
            "STRING",
        ),
        (
            "denomination_usuelle_3",
            "denominationUsuelle3UniteLegale",
            "STRING",
        ),
        ("categorie_juridique", "categorieJuridiqueUniteLegale", "STRING"),
        ("activite_principale", "activitePrincipaleUniteLegale", "STRING"),
        (
            "nomenclature_activite_principale",
            "nomenclatureActivitePrincipaleUniteLegale",
            "STRING",
        ),
        ("nic_siege", "nicSiegeUniteLegale", "STRING"),
        (
            "economie_sociale_solidaire",
            "economieSocialeSolidaireUniteLegale",
            "STRING",
        ),
        ("caractere_employeur", "caractereEmployeurUniteLegale", "STRING"),
        (
            "activite_principale_naf25",
            "activitePrincipaleNAF25UniteLegale",
            "STRING",
        ),
        ("societe_mission", "societeMissionUniteLegale", "STRING"),
    ],
    "expected_rows": 29_922_486,
}

ETABLISSEMENT = {
    "src_file": f"{INPUT}/StockEtablissement.parquet",
    "geoloc_file": f"{INPUT}/GeolocalisationEtablissement.parquet",
    "join_key": "siret",
    "columns": [
        ("data", "__DATA__", "DATE"),
        ("ano", "__ANO__", "INT64"),
        ("siren", "s.siren", "STRING"),
        ("nic", "s.nic", "STRING"),
        ("siret", "s.siret", "STRING"),
        ("statut_diffusion", "s.statutDiffusionEtablissement", "STRING"),
        ("date_creation", "s.dateCreationEtablissement", "DATE"),
        ("tranche_effectifs", "s.trancheEffectifsEtablissement", "STRING"),
        ("annee_effectifs", "s.anneeEffectifsEtablissement", "INT64"),
        (
            "activite_principale_registre_metiers",
            "s.activitePrincipaleRegistreMetiersEtablissement",
            "STRING",
        ),
        (
            "date_dernier_traitement",
            "s.dateDernierTraitementEtablissement",
            "DATE",
        ),
        ("etablissement_siege", "s.etablissementSiege", "STRING"),
        ("nombre_periodes", "s.nombrePeriodesEtablissement", "INT64"),
        ("complement_adresse", "s.complementAdresseEtablissement", "STRING"),
        ("numero_voie", "s.numeroVoieEtablissement", "STRING"),
        ("indice_repetition", "s.indiceRepetitionEtablissement", "STRING"),
        ("type_voie", "s.typeVoieEtablissement", "STRING"),
        ("libelle_voie", "s.libelleVoieEtablissement", "STRING"),
        ("code_postal", "s.codePostalEtablissement", "STRING"),
        ("libelle_commune", "s.libelleCommuneEtablissement", "STRING"),
        (
            "libelle_commune_etranger",
            "s.libelleCommuneEtrangerEtablissement",
            "STRING",
        ),
        (
            "distribution_speciale",
            "s.distributionSpecialeEtablissement",
            "STRING",
        ),
        ("code_commune", "s.codeCommuneEtablissement", "STRING"),
        ("code_cedex", "s.codeCedexEtablissement", "STRING"),
        ("libelle_cedex", "s.libelleCedexEtablissement", "STRING"),
        ("code_pays_etranger", "s.codePaysEtrangerEtablissement", "STRING"),
        (
            "libelle_pays_etranger",
            "s.libellePaysEtrangerEtablissement",
            "STRING",
        ),
        (
            "complement_adresse_2",
            "s.complementAdresse2Etablissement",
            "STRING",
        ),
        ("numero_voie_2", "s.numeroVoie2Etablissement", "STRING"),
        ("indice_repetition_2", "s.indiceRepetition2Etablissement", "STRING"),
        ("type_voie_2", "s.typeVoie2Etablissement", "STRING"),
        ("libelle_voie_2", "s.libelleVoie2Etablissement", "STRING"),
        ("code_postal_2", "s.codePostal2Etablissement", "STRING"),
        ("libelle_commune_2", "s.libelleCommune2Etablissement", "STRING"),
        (
            "libelle_commune_etranger_2",
            "s.libelleCommuneEtranger2Etablissement",
            "STRING",
        ),
        (
            "distribution_speciale_2",
            "s.distributionSpeciale2Etablissement",
            "STRING",
        ),
        ("code_commune_2", "s.codeCommune2Etablissement", "STRING"),
        ("code_cedex_2", "s.codeCedex2Etablissement", "STRING"),
        ("libelle_cedex_2", "s.libelleCedex2Etablissement", "STRING"),
        ("code_pays_etranger_2", "s.codePaysEtranger2Etablissement", "STRING"),
        (
            "libelle_pays_etranger_2",
            "s.libellePaysEtranger2Etablissement",
            "STRING",
        ),
        ("date_debut", "s.dateDebut", "DATE"),
        ("etat_administratif", "s.etatAdministratifEtablissement", "STRING"),
        ("enseigne_1", "s.enseigne1Etablissement", "STRING"),
        ("enseigne_2", "s.enseigne2Etablissement", "STRING"),
        ("enseigne_3", "s.enseigne3Etablissement", "STRING"),
        (
            "denomination_usuelle",
            "s.denominationUsuelleEtablissement",
            "STRING",
        ),
        ("activite_principale", "s.activitePrincipaleEtablissement", "STRING"),
        (
            "nomenclature_activite_principale",
            "s.nomenclatureActivitePrincipaleEtablissement",
            "STRING",
        ),
        ("caractere_employeur", "s.caractereEmployeurEtablissement", "STRING"),
        (
            "activite_principale_naf25",
            "s.activitePrincipaleNAF25Etablissement",
            "STRING",
        ),
        # --- geolocation (joined on siret) ---
        ("x", "g.x", "FLOAT64"),
        ("y", "g.y", "FLOAT64"),
        ("qualite_geocodificacao", "g.qualite_xy", "STRING"),
        ("epsg", "g.epsg", "STRING"),
        ("code_commune_geolocalisation", "g.plg_code_commune", "STRING"),
        ("qp24", "g.plg_qp24", "STRING"),
        ("qp15", "g.plg_qp15", "STRING"),
        ("iris", "g.plg_iris", "STRING"),
        ("zus", "g.plg_zus", "STRING"),
        ("qva", "g.plg_qva", "STRING"),
        ("distance_precision", "g.distance_precision", "FLOAT64"),
        ("qualite_qp24", "g.qualite_qp24", "STRING"),
        ("qualite_qp15", "g.qualite_qp15", "STRING"),
        ("qualite_iris", "g.qualite_iris", "STRING"),
        ("qualite_zus", "g.qualite_zus", "STRING"),
        ("qualite_qva", "g.qualite_qva", "STRING"),
        ("latitude", "g.y_latitude", "FLOAT64"),
        ("longitude", "g.x_longitude", "FLOAT64"),
        # --- geometria built in dbt (NOT here) ---
        # --- newer stock columns (appended) ---
        ("dernier_numero_voie", "s.dernierNumeroVoieEtablissement", "STRING"),
        (
            "indice_repetition_dernier_numero_voie",
            "s.indiceRepetitionDernierNumeroVoieEtablissement",
            "STRING",
        ),
        ("identifiant_adresse", "s.identifiantAdresseEtablissement", "STRING"),
        (
            "coordonnee_lambert_abscisse",
            "s.coordonneeLambertAbscisseEtablissement",
            "FLOAT64",
        ),
        (
            "coordonnee_lambert_ordonnee",
            "s.coordonneeLambertOrdonneeEtablissement",
            "FLOAT64",
        ),
    ],
    "expected_rows": 43_896_818,
}

UNITE_LEGALE_HISTORICO = {
    "src_file": f"{INPUT}/StockUniteLegaleHistorique.parquet",
    "columns": [
        ("data", "__DATA__", "DATE"),
        ("ano", "__ANO__", "INT64"),
        ("siren", "siren", "STRING"),
        ("date_fin", "dateFin", "DATE"),
        ("date_debut", "dateDebut", "DATE"),
        ("etat_administratif", "etatAdministratifUniteLegale", "STRING"),
        (
            "changement_etat_administratif",
            "changementEtatAdministratifUniteLegale",
            "STRING",
        ),
        ("nom", "nomUniteLegale", "STRING"),
        ("changement_nom", "changementNomUniteLegale", "STRING"),
        ("nom_usage", "nomUsageUniteLegale", "STRING"),
        ("changement_nom_usage", "changementNomUsageUniteLegale", "STRING"),
        ("denomination", "denominationUniteLegale", "STRING"),
        (
            "changement_denomination",
            "changementDenominationUniteLegale",
            "STRING",
        ),
        (
            "denomination_usuelle_1",
            "denominationUsuelle1UniteLegale",
            "STRING",
        ),
        (
            "denomination_usuelle_2",
            "denominationUsuelle2UniteLegale",
            "STRING",
        ),
        (
            "denomination_usuelle_3",
            "denominationUsuelle3UniteLegale",
            "STRING",
        ),
        (
            "changement_denomination_usuelle",
            "changementDenominationUsuelleUniteLegale",
            "STRING",
        ),
        ("categorie_juridique", "categorieJuridiqueUniteLegale", "STRING"),
        (
            "changement_categorie_juridique",
            "changementCategorieJuridiqueUniteLegale",
            "STRING",
        ),
        ("activite_principale", "activitePrincipaleUniteLegale", "STRING"),
        (
            "nomenclature_activite_principale",
            "nomenclatureActivitePrincipaleUniteLegale",
            "STRING",
        ),
        (
            "changement_activite_principale",
            "changementActivitePrincipaleUniteLegale",
            "STRING",
        ),
        ("nic_siege", "nicSiegeUniteLegale", "STRING"),
        ("changement_nic_siege", "changementNicSiegeUniteLegale", "STRING"),
        (
            "economie_sociale_solidaire",
            "economieSocialeSolidaireUniteLegale",
            "STRING",
        ),
        (
            "changement_economie_sociale_solidaire",
            "changementEconomieSocialeSolidaireUniteLegale",
            "STRING",
        ),
        ("caractere_employeur", "caractereEmployeurUniteLegale", "STRING"),
        (
            "changement_caractere_employeur",
            "changementCaractereEmployeurUniteLegale",
            "STRING",
        ),
        ("societe_mission", "societeMissionUniteLegale", "STRING"),
        (
            "changement_societe_mission",
            "changementSocieteMissionUniteLegale",
            "STRING",
        ),
    ],
    "expected_rows": 71_355_318,
}

ETABLISSEMENT_HISTORICO = {
    "src_file": f"{INPUT}/StockEtablissementHistorique.parquet",
    "columns": [
        ("data", "__DATA__", "DATE"),
        ("ano", "__ANO__", "INT64"),
        ("siren", "siren", "STRING"),
        ("nic", "nic", "STRING"),
        ("siret", "siret", "STRING"),
        ("date_fin", "dateFin", "DATE"),
        ("date_debut", "dateDebut", "DATE"),
        ("etat_administratif", "etatAdministratifEtablissement", "STRING"),
        (
            "changement_etat_administratif",
            "changementEtatAdministratifEtablissement",
            "STRING",
        ),
        ("enseigne_1", "enseigne1Etablissement", "STRING"),
        ("enseigne_2", "enseigne2Etablissement", "STRING"),
        ("enseigne_3", "enseigne3Etablissement", "STRING"),
        ("changement_enseigne", "changementEnseigneEtablissement", "STRING"),
        ("denomination_usuelle", "denominationUsuelleEtablissement", "STRING"),
        (
            "changement_denomination_usuelle",
            "changementDenominationUsuelleEtablissement",
            "STRING",
        ),
        ("activite_principale", "activitePrincipaleEtablissement", "STRING"),
        (
            "nomenclature_activite_principale",
            "nomenclatureActivitePrincipaleEtablissement",
            "STRING",
        ),
        (
            "changement_activite_principale",
            "changementActivitePrincipaleEtablissement",
            "STRING",
        ),
        ("caractere_employeur", "caractereEmployeurEtablissement", "STRING"),
        (
            "changement_caractere_employeur",
            "changementCaractereEmployeurEtablissement",
            "STRING",
        ),
    ],
    "expected_rows": 95_865_102,
}

TABLES = {
    "unite_legale": UNITE_LEGALE,
    "etablissement": ETABLISSEMENT,
    "unite_legale_historique": UNITE_LEGALE_HISTORICO,
    "etablissement_historique": ETABLISSEMENT_HISTORICO,
}
