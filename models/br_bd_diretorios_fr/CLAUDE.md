# br_bd_diretorios_fr — French geography & classification directory

Directory dataset (analog of `br_bd_diretorios_brasil`) built as the prerequisite for
`fr_insee_sirene`. Open data (INSEE). **French table slugs and column values.**

## Tables (6, no partition — static reference tables)
| table (output slug) | rows | key | source |
|---|---|---|---|
| region | 18 | id_regiao | INSEE COG 2025 (v_region_2025.csv) |
| departement | 101 | id_departamento | INSEE COG 2025 (v_departement_2025.csv) |
| commune | 34,920 | id_comuna | INSEE COG 2025 (v_commune_2025.csv, TYPECOM ∈ COM/ARM) |
| naf_rev2 | 732 | naf_rev2 | INSEE NAF rév.2 (information/2120875), dotted `62.01Z` |
| naf_2025 | 747 | naf_2025 | INSEE NAF 2025 (information/8181066), dotted `81.21Y` |
| categorie_juridique | 260 | categoria_juridica | INSEE cat. juridiques (information/2028129), 4-digit |

Intra-directory FKs: `departement.id_regiao`→region; `commune.id_departamento`→departement,
`commune.id_regiao`→region; chef-lieu columns → commune.

## Slug naming note
Output/prod table slugs are **French** (region, departement, commune, categorie_juridique).
The `_staging` tables keep the earlier internal names (regiao, departamento, comuna,
categoria_juridica); dbt reads staging via `set_datalake_project(...)` and materializes the
French-aliased output. Key **column** names were left as-is (`id_comuna`, `id_regiao`,
`id_departamento`) — only table slugs were Frenchified.

## Code
`code/clean.py` — parses the INSEE COG CSVs + NAF/CJ Excel hierarchies into 6 typed all-STRING
parquet tables (`xlrd` for `.xls`, `openpyxl` for `.xlsx`; NAF 2025 section via NACE division
ranges). Raw refs downloaded to `~/Downloads/br_bd_diretorios_fr_data/` (deleted post-onboard).
