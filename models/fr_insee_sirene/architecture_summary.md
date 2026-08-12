# Architecture Summary — `fr_insee_sirene` (INSEE SIRENE)

France's national business register (SIREN/SIRET), the open analog of Brazil's CNPJ.
GCP dataset id: `fr_insee_sirene`. Data language: French → French snake_case column
names; descriptions trilingual (PT/EN/ES). Full-fidelity build: every source column.

Drive folder: `Base dos Dados - Geral/Dados/Conjuntos/fr_insee_sirene/architecture/`
(folder id `1TQAKQf9yb4KP8ZrSHVLbZMS3BnqtkKXt`).

## Tables and Drive URLs

| Table | Columns | Source file(s) | Sheet URL |
|-------|---------|----------------|-----------|
| unite_legale | 36 | StockUniteLegale (33 + NAF25) | https://docs.google.com/spreadsheets/d/1vtcum5h6b48EPdx313ADHNszqW5WlV4adRi7sWk2L8E |
| etablissement | 70 | StockEtablissement (48 + NAF25) + Geolocalisation (18) + geometria | https://docs.google.com/spreadsheets/d/1oRCrs8wlTTzS83ZzLS91AtcjWSk05ZGUkArTo4-Nm1Q |
| unite_legale_historico | 30 | StockUniteLegaleHistorique (26 + societeMission ×2) | https://docs.google.com/spreadsheets/d/1DfOjwu43yQ_OaIgseSDT1cnvX-VOydJIonZLCm96sDY |
| etablissement_historico | 20 | StockEtablissementHistorique (18) | https://docs.google.com/spreadsheets/d/1aL-G6TL2p68lyRTi0e4DtvcCwmyUgjQg4fOiRUoAz28 |
| dicionario | 5 (163 entries) | derived code lists | https://docs.google.com/spreadsheets/d/1IAMViObAqYeWFneMW5YgNO0Sv1hnuQ5sJk0Fr0P-3ck |

## Design decisions

- **Partition column:** `data` (DATE, monthly snapshot/extraction date). `ano` (INT64)
  is the derived snapshot year, front of every stock/historique table, FK to
  `br_bd_diretorios_data_tempo.ano:ano`.
- **`date_debut` / `date_fin`** are the source validity-period bounds (`dateDebut` /
  `dateFin`), distinct from the `data` partition column.
- **Column order:** `data`, `ano`, keys (`siren`/`nic`/`siret`), then source order.
- **Naming:** INSEE camelCase snake_cased, trailing entity suffix dropped
  (`activitePrincipaleUniteLegale` → `activite_principale`); secondary-address block
  keeps `_2`. Exact INSEE variable kept in `original_name`.
- **etablissement = StockEtablissement ⋈ Geolocalisation on siret.** Geoloc columns
  appended after SIRENE columns; `geometria` (GEOGRAPHY) built from longitude/latitude.
  Koumoul artifacts (`_geopoint/_id/_i/_rand`) excluded. Geolocation covers France
  excluding Mayotte.

## Typing (by arithmetic meaning)

- STRING: all identifiers/codes (`siren`, `nic`, `siret`, `code_*`, `nic_siege`,
  `numero_voie*`), all `changement_*` booleans, `unite_purgee`, `etablissement_siege`,
  and every coded/list field.
- INT64: `ano`, `annee_effectifs`, `annee_categorie_entreprise` (unit `ano`),
  `nombre_periodes` (unit `periodo`).
- DATE: `data`, `date_creation`, `date_debut`, `date_fin`, `date_dernier_traitement`.
- FLOAT64: `x`, `y`, `distance_precision` (unit `metro`); `latitude`, `longitude`
  (unit `grau`).
- GEOGRAPHY: `geometria`.

## Directory FKs

- `code_commune`, `code_commune_2`, `code_commune_geolocalisation` →
  `br_bd_diretorios_fr.comuna:id_comuna` (covered_by_dictionary=no)
- `activite_principale`, `activite_principale_registre_metiers` →
  `br_bd_diretorios_fr.naf_rev2:naf_rev2`
- `activite_principale_naf25` → `br_bd_diretorios_fr.naf_2025:naf_2025`
- `categorie_juridique` → `br_bd_diretorios_fr.categoria_juridica:categoria_juridica`
- `ano` → `br_bd_diretorios_data_tempo.ano:ano`

## Dictionary coverage (covered_by_dictionary=yes)

statut_diffusion (O/P/N), sexe (M/F), tranche_effectifs (16 bands), etat_administratif
(UL A/C, Étab A/F), caractere_employeur (O/N), categorie_entreprise (PME/ETI/GE),
economie_sociale_solidaire (O/N), nomenclature_activite_principale
(NAFRev2/NAFRev1/NAF1993/NAP), type_voie & type_voie_2 (42 codes each).

`code_pays_etranger` / `code_pays_etranger_2`: covered_by_dictionary=yes but values not
enumerated in v1 — INSEE COG country codes; a future FK to `br_bd_diretorios_mundo.pais`
is noted in `observations`.

## Sensitive fields (has_sensitive_data=yes)

`nom`, `nom_usage`, `prenom_1..4`, `prenom_usuel`, `pseudonyme`, `sexe`, and detailed
address fields (`complement_adresse`, `numero_voie`, `indice_repetition`,
`libelle_voie`, plus their `_2` variants).
