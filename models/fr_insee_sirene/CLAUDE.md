# fr_insee_sirene — INSEE SIRENE (France business registry)

France's national SIREN/SIRET business register (INSEE), the open analog of Brazil's CNPJ.
License **Licence Ouverte / Open Licence 2.0** (Etalab) — open, redistributable incl. commercial.
Data language French → **French column names and table slugs**; descriptions PT/EN/ES.

## Source (data.gouv.fr, stable `/api/1/datasets/r/<id>` parquet)
- Base SIRENE dataset id `5b7ffc618b4c4169d30727e0` — StockUniteLegale, StockEtablissement,
  StockUniteLegaleHistorique, StockEtablissementHistorique.
- Geolocation dataset id `61d5e2d372a52d9f9411ff88` — SIRET → x/y (Lambert-93) + lat/lon + IRIS/QPV
  zoning; covers France **excluding Mayotte**.
- Monthly snapshot, published ~1st of month (geoloc ~21st). Current load = **2026-08-01**.

## Tables (5, partition by `data` = snapshot date)
| table (output/prod slug) | cols | rows | grain |
|---|---|---|---|
| unite_legale | 37 | 29,922,486 | legal unit (SIREN) |
| etablissement | 75 | 43,896,818 | establishment (SIRET), geocoded, `geometria` |
| unite_legale_historique | 30 | 71,355,318 | SIREN × validity period |
| etablissement_historique | 20 | 95,865,102 | SIRET × validity period |
| dicionario | 5 | 163 | value→label for coded columns |

- `etablissement` = StockEtablissement ⋈ geolocation on `siret` (85.6% geocoded); `geometria`
  (GEOGRAPHY) is built in dbt from longitude/latitude.
- `dicionario` kept as the platform-reserved dictionary slug (not renamed to `dictionnaire`).

## Slug naming (important)
Output/prod table slugs are **French**. The `_staging` tables and the `~/Downloads` cleaned
parquet keep the earlier internal names (`unite_legale_historico`, `etablissement_historico`) —
the dbt models read those via `set_datalake_project(...)` and materialize the French-aliased
output. Do not "fix" that mismatch by re-uploading; it is intentional.

## Directory FKs → br_bd_diretorios_fr (must exist first)
`code_commune` / `code_commune_2` / `code_commune_geolocalisation` → `commune:id_comuna`;
`activite_principale` / `activite_principale_registre_metiers` → `naf_rev2:naf_rev2`;
`activite_principale_naf25` → `naf_2025:naf_2025`;
`categorie_juridique` → `categorie_juridique:categoria_juridica`; `ano` → data_tempo.
FK tests scope activity to `nomenclature_activite_principale = 'NAFRev2'`. Two documented
historical FK residuals via `ignore_values`: the `00.00Z` "activity not assigned" sentinel
(1.32% of unite_legale_historique) and 38 retired legal-form codes (0.36%).

## Full-fidelity note
The live Aug-2026 files carry columns newer than INSEE's published dictionaries, all kept:
`unite_legale.societe_mission`; `etablissement.dernier_numero_voie`,
`indice_repetition_dernier_numero_voie`, `identifiant_adresse`, `coordonnee_lambert_abscisse`,
`coordonnee_lambert_ordonnee`.

## Code (`code/`)
- `schema_map.py` — authoritative source→target→type mapping (single source of truth).
- `clean.py` — DuckDB out-of-core cleaning (30M–96M rows) → typed parquet.
- `build_dicionario.py` — builds the 163-row dicionario.
- `gen_dbt.py` — generates the 4 dbt models from schema_map (`OUT_ALIAS` maps historico→historique).
- `upload_gcs.py` — memory-safe upload for the large tables (stream to GCS + `load_table_from_uri`;
  `bd.Table.create` blows up RAM on 40M+ rows — see memory `reference_bd_table_create_ram_blowup`).

## Pipeline
Recurring monthly refresh is a **separate follow-up PR** (source republishes on the 1st;
`part_bdpro` rolling window given DB-Pro value).
