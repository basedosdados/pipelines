# Context Report — `fr_insee_sirene` (INSEE SIRENE)

## 0. Summary of key facts

- **Source:** INSEE SIRENE open data, hosted on data.gouv.fr at **stable `/api/1/datasets/r/<id>` resource URLs** (URL constant across monthly refreshes; the file behind it is swapped each month). Parquet native since June 2025.
- **Latest snapshot:** base SIRENE files = **01 August 2026**; geolocation file = **July 2026** (~21st of month). Source is current.
- **License:** **Licence Ouverte / Open Licence 2.0 (Etalab)** — open and redistributable, incl. commercially, with attribution to INSEE.
- **Files:** 4 base stock files + 1 geolocation file.

### Stable download URLs (snapshot 01 Aug 2026)

Base dataset — data.gouv id **`5b7ffc618b4c4169d30727e0`**. API: `https://www.data.gouv.fr/api/1/datasets/5b7ffc618b4c4169d30727e0/`

| File | Format | `/api/1/datasets/r/<id>` | Size |
|---|---|---|---|
| StockUniteLegale | parquet | `350182c9-148a-46e0-8389-76c2ec1374a3` | 705 MB |
| StockUniteLegale | csv.zip | `825f4199-cadd-486c-ac46-a65a8ea1a047` | 971 MB |
| StockEtablissement | parquet | `a29c1297-1f92-4e2a-8f6b-8c902ce96c5f` | 2.20 GB |
| StockEtablissement | csv.zip | `0651fb76-bcf3-4f6a-a38d-bc04fa708576` | 2.86 GB |
| StockUniteLegaleHistorique | parquet | `1b9290ed-d0bc-461f-ba31-0250a99cc140` | 856 MB |
| StockUniteLegaleHistorique | csv.zip | `0835cd60-2c2a-497b-bc64-404de704ce89` | 1.25 GB |
| StockEtablissementHistorique | parquet | `2b3a0c79-f97b-46b8-ac02-8be6c1f01a8c` | 870 MB |
| StockEtablissementHistorique | csv.zip | `88fbb6b4-0320-443e-b739-b4376a012c32` | 1.23 GB |

Geolocation dataset — data.gouv id **`61d5e2d372a52d9f9411ff88`**. API: `https://www.data.gouv.fr/api/1/datasets/61d5e2d372a52d9f9411ff88/`

| File | Format | `/api/1/datasets/r/<id>` | Size |
|---|---|---|---|
| GeolocalisationEtablissement | parquet | `672007af-0146-491f-835c-8314d63fa44e` | 765 MB |
| Geolocalisation | csv.zip | `ba6a4e4c-aac6-4764-bbd2-f80ae345afc5` | 1.1 GB |
| Geoloc documentation PDF (v8) | pdf | `9f6c2157-3c89-4e8d-8473-9894348c84cb` | 190 KB |

**Resolve resource ids via the API** (match `title` + `format`), do not trust the hand-copied ids: the data.gouv summariser twice swapped the establishment/historique ids. Size-based mapping above is the reconfirmed one. Each `/r/{id}` 302-redirects to stable object storage.

### Per-file column counts

| File | Columns | Grain / key |
|---|---|---|
| StockUniteLegale | 33 (34 w/ NAF25) | legal unit; PK `siren` |
| StockEtablissement | 48 (49 w/ NAF25) | establishment; PK `siret` |
| StockUniteLegaleHistorique | 26 (28 w/ societeMission) | SIREN × period; key `siren`+`dateDebut` |
| StockEtablissementHistorique | 18 | SIRET × period; key `siret`+`dateDebut` |
| Geolocalisation | 19 | establishment; PK `siret` |

## 1. What SIRENE is

France's national business register (Système Informatique pour le Répertoire des ENtreprises et des Etablissements), run by INSEE. Registers every legal unit (`unité légale`, 9-digit **SIREN**) and establishment (`établissement`, 14-digit **SIRET** = SIREN + 5-digit NIC). Direct analog of Brazil's CNPJ. Register from 1973; exhaustive for public bodies since 1983; private agricultural since 1993; historisation of variables since 2005; units ceased before 31/12/2002 purged (`unitePurgeeUniteLegale`).

## 2. Full variable dictionaries

### 2a. StockUniteLegale — 33 cols
siren; statutDiffusionUniteLegale; unitePurgeeUniteLegale; dateCreationUniteLegale; sigleUniteLegale; sexeUniteLegale; prenom1UniteLegale; prenom2UniteLegale; prenom3UniteLegale; prenom4UniteLegale; prenomUsuelUniteLegale; pseudonymeUniteLegale; identifiantAssociationUniteLegale; trancheEffectifsUniteLegale; anneeEffectifsUniteLegale; dateDernierTraitementUniteLegale; nombrePeriodesUniteLegale; categorieEntreprise; anneeCategorieEntreprise; dateDebut; etatAdministratifUniteLegale (A=active/C=cessée); nomUniteLegale; nomUsageUniteLegale; denominationUniteLegale; denominationUsuelle1UniteLegale; denominationUsuelle2UniteLegale; denominationUsuelle3UniteLegale; categorieJuridiqueUniteLegale (4-digit); activitePrincipaleUniteLegale (APE/NAFrév2); nomenclatureActivitePrincipaleUniteLegale; nicSiegeUniteLegale; economieSocialeSolidaireUniteLegale (O/N); caractereEmployeurUniteLegale (O/N). **+since Dec 2025** activitePrincipaleNAF25UniteLegale.

### 2b. StockEtablissement — 48 cols (file order)
siren; nic; siret; statutDiffusionEtablissement; dateCreationEtablissement; trancheEffectifsEtablissement; anneeEffectifsEtablissement; activitePrincipaleRegistreMetiersEtablissement; dateDernierTraitementEtablissement; etablissementSiege; nombrePeriodesEtablissement; complementAdresseEtablissement; numeroVoieEtablissement; indiceRepetitionEtablissement; typeVoieEtablissement; libelleVoieEtablissement; codePostalEtablissement; libelleCommuneEtablissement; libelleCommuneEtrangerEtablissement; distributionSpecialeEtablissement; codeCommuneEtablissement (INSEE COG); codeCedexEtablissement; libelleCedexEtablissement; codePaysEtrangerEtablissement; libellePaysEtrangerEtablissement; complementAdresse2Etablissement; numeroVoie2Etablissement; indiceRepetition2Etablissement; typeVoie2Etablissement; libelleVoie2Etablissement; codePostal2Etablissement; libelleCommune2Etablissement; libelleCommuneEtranger2Etablissement; distributionSpeciale2Etablissement; codeCommune2Etablissement; codeCedex2Etablissement; libelleCedex2Etablissement; codePaysEtranger2Etablissement; libellePaysEtranger2Etablissement; dateDebut; etatAdministratifEtablissement (A=actif/F=fermé); enseigne1Etablissement; enseigne2Etablissement; enseigne3Etablissement; denominationUsuelleEtablissement; activitePrincipaleEtablissement; nomenclatureActivitePrincipaleEtablissement; caractereEmployeurEtablissement. **+since Dec 2025** activitePrincipaleNAF25Etablissement.

### 2c. StockEtablissementHistorique — 18 cols
siren; nic; siret; dateFin; dateDebut; etatAdministratifEtablissement; changementEtatAdministratifEtablissement; enseigne1Etablissement; enseigne2Etablissement; enseigne3Etablissement; changementEnseigneEtablissement; denominationUsuelleEtablissement; changementDenominationUsuelleEtablissement; activitePrincipaleEtablissement; nomenclatureActivitePrincipaleEtablissement; changementActivitePrincipaleEtablissement; caractereEmployeurEtablissement; changementCaractereEmployeurEtablissement.

### 2d. StockUniteLegaleHistorique — 26 cols (28 w/ societeMission)
siren; dateFin; dateDebut; etatAdministratifUniteLegale; changementEtatAdministratifUniteLegale; nomUniteLegale; changementNomUniteLegale; nomUsageUniteLegale; changementNomUsageUniteLegale; denominationUniteLegale; changementDenominationUniteLegale; denominationUsuelle1UniteLegale; denominationUsuelle2UniteLegale; denominationUsuelle3UniteLegale; changementDenominationUsuelleUniteLegale; categorieJuridiqueUniteLegale; changementCategorieJuridiqueUniteLegale; activitePrincipaleUniteLegale; nomenclatureActivitePrincipaleUniteLegale; changementActivitePrincipaleUniteLegale; nicSiegeUniteLegale; changementNicSiegeUniteLegale; economieSocialeSolidaireUniteLegale; changementEconomieSocialeSolidaireUniteLegale; caractereEmployeurUniteLegale; changementCaractereEmployeurUniteLegale. **+recent** societeMissionUniteLegale + changementSocieteMissionUniteLegale. Verify header against parquet.

### 2e. Geolocalisation — 19 INSEE cols (exclude Koumoul artifacts `_geopoint`,`_id`,`_i`,`_rand`)
siret; x; y; qualite_xy; epsg; plg_code_commune; plg_qp24; plg_qp15; plg_iris; plg_zus (discontinued Mar 2024, kept empty); plg_qva; distance_precision; qualite_qp24; qualite_qp15; qualite_iris; qualite_zus; qualite_qva; y_latitude; x_longitude. Coords RGF93/Lambert-93 (epsg 2154) métropole. Zoning per 2021 geography.

## 3. Code lists (dicionario / directory FKs)

| Coded variable | Mapping source |
|---|---|
| NAF rév.2 / APE (6-char, e.g. 62.01Z) | INSEE `information/2120875` — ~732 codes |
| NAF 2025 (informatif since 16/12/2025, ref 01/01/2027) | INSEE `information/8181066` — ~688 codes; correspondence tables published |
| Catégorie juridique (4-digit niveau III) | INSEE `information/2028129`; machine-readable `xml.insee.fr/schema/cj.html` |
| Tranche d'effectifs (2-char) | file doc: NN=non employeur, 00=0, 01=1–2, 02=3–5, 03=6–9, 11=10–19, 12=20–49, 21=50–99, 22=100–199, 31=200–249, 32=250–499, 41=500–999, 42=1000–1999, 51=2000–4999, 52=5000–9999, 53=10000+ |
| État administratif | UL A=active/C=cessée; Étab A=actif/F=fermé |
| Caractère employeur | O/N/null |
| Sexe | M/F |
| Statut de diffusion | O=diffusible |
| Économie sociale et solidaire | O/N |
| Catégorie d'entreprise | PME/ETI/GE |
| Type de voie (4-char) | normalised list in file doc (ALL,AV,BD,CHE,IMP,PL,RTE,RUE,…) |
| Nomenclature activité | NAFRev2/NAFRev1/NAF1993/NAP |
| Code commune | INSEE COG `information/2028028` → **French commune directory FK** |
| Code pays étranger | INSEE COG country codes |

## 4. License

**Licence Ouverte / Open Licence 2.0 (Etalab)** — open, freely reusable & redistributable incl. commercial, attribution to INSEE. (data.gouv scrapers sometimes surface "ODbL"; authoritative license is Licence Ouverte 2.0. Confirm backend slug.)

**GDPR:** physical-person entrepreneurs included; those who objected are flagged non-diffusible (`statutDiffusion…` ≠ O) and their identifying fields masked/absent. Only diffusible records in the open files. Treat name/prénom/address columns as `has_sensitive_data=yes`.

## 5. Organization

- Slug (proposed): `insee` (verify on backend). GCP dataset id `fr_insee_sirene`.
- pt: Instituto Nacional de Estatística e Estudos Econômicos (França)
- en: National Institute of Statistics and Economic Studies (INSEE)
- es: Instituto Nacional de Estadística y Estudios Económicos (Francia)
- fr: Institut national de la statistique et des études économiques
- Website: https://www.insee.fr · Country: France

## 6. Coverage

- Geographic: France — métropole + DOM + collectivités (SPM, St-Barthélemy, St-Martin). **Geolocation file excludes Mayotte.**
- Temporal: stock = current snapshot (latest 2026-08-01); dateCreation back to 1973; historique from 2005 (earlier dateDebut may be 1900-01-01 = missing). Ongoing monthly-refreshed snapshot.

## 7. Themes / tags

- Themes: economia; emprego; geografia.
- Tags (attach existing, never empty): empresas/firms, business-registry, establishments, cnpj, france, insee, naf/ape, employment, geolocation.

## 8. Update cadence

- Base stock: monthly, ~1st of month (full file re-issued). Geoloc: monthly ~21st. Full universe each release → `dump_mode="overwrite"`.
- Recurring Prefect pipeline (monthly) after static onboard, using stable `/r/{id}` + data.gouv API to detect snapshot month. Consider BD Pro rolling-window tier per table.

## 9. Notes for downstream

1. Prefer parquet. Download one file at a time to `~/Downloads/fr_insee_sirene_data/`, delete after cleaning.
2. Resolve resource ids via API (title+format), not hand-copied ids.
3. **Column naming: data language is French → French snake_case names** (e.g. `activite_principale_etablissement`); keep INSEE camelCase as `original_name`. Descriptions still pt/en/es.
4. NAF 2025 columns from Dec 2025 snapshots — include.
5. `codeCommune…` → French commune directory FK (`br_bd_diretorios_fr`, create/verify), `covered_by_dictionary=no`. NAF / cat. juridique / tranche effectifs → `dicionario`, `covered_by_dictionary=yes`.
6. Types: all identifiers/codes/`changement…` booleans → STRING; only `nombrePeriodes…` → INT64; `numeroVoie…` → STRING; dates → DATE; x/y/lon/lat/distance_precision → FLOAT64 (unit metre/degree); consider GEOGRAPHY point from x_longitude/y_latitude.
7. Drive folder: `Base dos Dados - Geral/Dados/Conjuntos/fr_insee_sirene/`.

## Sources
- https://www.data.gouv.fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/
- https://www.data.gouv.fr/datasets/geolocalisation-des-etablissements-du-repertoire-sirene-pour-les-etudes-statistiques/
- INSEE variable descriptions (StockEtablissement / StockUniteLegale PDFs)
- https://www.insee.fr/fr/information/8181066 (NAF 2025)
- https://www.insee.fr/fr/information/2028129 (catégories juridiques)
