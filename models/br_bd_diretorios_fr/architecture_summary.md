# Architecture — br_bd_diretorios_fr

French geography + classification directory. Analog of `br_bd_diretorios_brasil`.
GCP dataset id: `br_bd_diretorios_fr`. Source: INSEE (Code Officiel Géographique,
NAF, catégories juridiques).

These are DIRECTORY tables: small, static reference tables. No partition column.
The key column of each table is a primary key (directory tables are the one place
`is_primary_key` applies). Column names in French/geographic snake_case; codes and
identifiers are STRING (never INT64 — no arithmetic meaning). Descriptions are
provided in PT (`description`), EN (`description_en`), ES (`description_es`).

Drive architecture folder:
`Base dos Dados - Geral/Dados/Conjuntos/br_bd_diretorios_fr/architecture/`

## Source verification note

Catégories juridiques (INSEE 2028129) confirmed: niveau I = 1 digit (9 positions),
niveau II = 2 digits (37 positions), niveau III = 4 digits (260 positions), e.g.
5710 = Société par actions simplifiée. NAF (INSEE 2120875) confirmed: five nested
levels (section letter A–U, division, groupe, classe, sous-classe), sous-classe in
dotted form e.g. 62.01Z, 732 sous-classes in rév. 2. The INSEE COG detail pages
returned 404 on fetch; the COG CSV schema (REG, DEP, COM, TYPECOM, CHEFLIEU, TNCC,
NCC, NCCENR, LIBELLE, CTCD, ARR, CAN, COMPARENT) is stable and applied from the
documented COG structure — confirm the exact header at download time.

## Tables (6)

### regiao — French régions (INSEE COG `region`)
Grain: one row per région. Key: `id_regiao`.
- id_regiao (STRING, PK) ← REG
- id_comuna_sede (STRING → comuna:id_comuna) ← CHEFLIEU
- nome_regiao (STRING) ← LIBELLE
- nome_regiao_maiusculo (STRING) ← NCCENR

### departamento — départements (INSEE COG `departement`)
Grain: one row per département. Key: `id_departamento`.
- id_departamento (STRING, PK) ← DEP (e.g. 01, 2A, 2B, 971)
- id_regiao (STRING → regiao:id_regiao) ← REG
- id_comuna_sede (STRING → comuna:id_comuna) ← CHEFLIEU
- nome_departamento (STRING) ← LIBELLE

### comuna — communes (INSEE COG `commune`, ~35k rows)
Grain: one row per commune. Key: `id_comuna`.
- id_comuna (STRING, PK) ← COM (5-char, e.g. 75056, 2A004)
- id_departamento (STRING → departamento:id_departamento) ← DEP
- id_regiao (STRING → regiao:id_regiao) ← REG
- nome_comuna (STRING) ← LIBELLE
- tipo_comuna (STRING, covered_by_dictionary=yes) ← TYPECOM
Grain choice: keep only real geographic communes — TYPECOM in COM (commune) and ARM
(arrondissement municipal: Paris, Lyon, Marseille). Exclude COMA (associée), COMD
(déléguée), COMP (provisoire).

### naf_rev2 — NAF rév. 2 / APE activity nomenclature (732 sous-classes)
Grain: one row per sous-classe with its parent labels. Key: `naf_rev2`.
Canonical code format: dotted, as used in SIRENE (e.g. 62.01Z).
- naf_rev2 (STRING, PK)
- descricao_naf_rev2 (STRING)
- id_classe (STRING, e.g. 62.01) + descricao_classe
- id_grupo (STRING, e.g. 62.0) + descricao_grupo
- id_divisao (STRING, e.g. 62) + descricao_divisao
- id_secao (STRING, e.g. J) + descricao_secao

### naf_2025 — NAF 2025 (~688 codes; reference nomenclature from 2027-01-01)
Same shape as naf_rev2. Key: `naf_2025`. Informative since Dec 2025; becomes the
reference nomenclature on 2027-01-01.
- naf_2025 (STRING, PK) + descricao_naf_2025
- id_classe + descricao_classe
- id_grupo + descricao_grupo
- id_divisao + descricao_divisao
- id_secao + descricao_secao

### categoria_juridica — catégories juridiques (260 niveau-III codes)
Grain: one row per niveau-III code with its parent labels. Key: `categoria_juridica`.
- categoria_juridica (STRING, PK, 4-digit, e.g. 5710) + descricao_categoria_juridica
- id_nivel_2 (STRING, 2-digit) + descricao_nivel_2
- id_nivel_1 (STRING, 1-digit) + descricao_nivel_1

## Directory FK map (within br_bd_diretorios_fr)
- regiao.id_comuna_sede      → comuna:id_comuna
- departamento.id_regiao     → regiao:id_regiao
- departamento.id_comuna_sede → comuna:id_comuna
- comuna.id_departamento     → departamento:id_departamento
- comuna.id_regiao           → regiao:id_regiao

## Drive URLs
- regiao:             https://docs.google.com/spreadsheets/d/11Xg11Dnpn51bqX1ReSi2PxzacCQvWMrfQVdaGNONONU
- departamento:       https://docs.google.com/spreadsheets/d/196lK65gd2ZpcBqArAudFPEhUC2zu0klC6Y8aEiQLoWI
- comuna:             https://docs.google.com/spreadsheets/d/1TiQPXuG8FxjKR3FKFtaUTQ_m0FJgqgbRjBsdnOCvzfY
- naf_rev2:           https://docs.google.com/spreadsheets/d/1iyvcl0TZYzKrPQ1DQunkttUK_CJRFzoMpXGzFEZfWz4
- naf_2025:           https://docs.google.com/spreadsheets/d/1WVWPY-0IEmuRonW2yUr2mr-lQzmMIwSZc3LR1cs4fL8
- categoria_juridica: https://docs.google.com/spreadsheets/d/1pVQrOXrUXNYGE6PGVzJH_W6WcS6bG15ecC9NMRcqfgk
