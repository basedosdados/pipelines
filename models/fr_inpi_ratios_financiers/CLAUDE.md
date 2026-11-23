# fr_inpi_ratios_financiers — Ratios financiers (BCE / INPI)

Financial indicators and ratios per French company (SIREN) per fiscal-year closing date.
Producer **INPI** (annual accounts filed with the RNCS, XML open data); processed by the
**DNUM** of the Ministère du Travail on the Base Commune Entreprise (BCE); published by the
**DGE** (Ministères économiques et financiers). License **Licence Ouverte v2.0** (Etalab).
Backend slug `ratios_financiers` (bare slug, like `sirene` for `fr_insee_sirene`).
Data language French → **French column/table names**; descriptions PT/EN/ES.

## Source
- Landing: https://www.data.gouv.fr/datasets/ratios-financiers-bce-inpi
- Data: https://data.economie.gouv.fr/explore/dataset/ratios_inpi_bce/ (Opendatasoft)
- Export used: `/api/explore/v2.1/catalog/datasets/ratios_inpi_bce/exports/parquet` (~690 MB).
  **Requires a browser User-Agent** (`-A "Mozilla/5.0"`), and the metadata endpoint is gzip
  (`curl --compressed`); without the UA the body is empty.
- Per-field formulas live in the metadata endpoint's field `description`; they are copied into
  each column's `observations` in the architecture CSV.
- Load of 2026-10-07 reflects source `modified` 2026-09-15: 6,813,145 rows, 1,615,938 SIRENs.

## Tables
| table | cols | rows | grain |
|---|---|---|---|
| ratios_financiers | 24 | 6,813,145 | siren × date_cloture_exercice × type_bilan |
| dicionario | 5 | 3 | `type_bilan` codes (C/K/S) |

- Partition `annee` = year of `date_cloture_exercice` (derived; the only non-source column).
- `(siren, date_cloture_exercice)` is **not** unique (32,025 pairs carry two balance-sheet
  types, e.g. C and K); the key includes `type_bilan`.
- `confidentiality` renamed `confidentialite`; values kept as readable source labels
  (no dictionary).
- `siren` has **no directory FK**: `fr_insee_sirene.unite_legale` is a snapshot, not a
  directory. Join on `siren` when needed.
- Amounts INT64 in `eur`; ratios FLOAT64 in `percent` (already ×100) except
  `capacite_de_remboursement` (debt / CAF, unit `year`) and the `*_jours` columns (`day`,
  360-day basis).

## Known data quirks (kept as published)
- Year tails: 1919 (1 row, likely a typo for 2019), 2002–2011 (91 rows), 2029 (1 row);
  2025 partial (405,531) and 2026 just starting (7,081).
- Extreme outliers in amounts and ratios (e.g. `chiffre_d_affaires` max 2.9e14 EUR; ratios in
  the ±1e9 range when denominators are near zero). Not filtered.
- `ratio_de_vetuste`: the published formula is net/gross ×100 (100 = new assets), the inverse
  of the usual "depreciated share" reading. Documented in observations.

## Code (`code/`)
- `architecture/*.csv` — source of truth (trilingual descriptions + observations).
- `clean.py` — pyarrow; validates the 23 source columns, derives `annee`, writes all-STRING
  hive-partitioned parquet + `dicionario` + `_manifest.json`.
- `upload.py` — `bd.Table.create` to `basedosdados-dev` staging, row-count check against the
  manifest. Needs `GOOGLE_APPLICATION_CREDENTIALS=~/.basedosdados/credentials/staging.json`
  for the count query.
- Scratch: `$FR_INPI_RATIOS_DATA_DIR` (default `~/bd_scratch/fr_inpi_ratios_financiers_data`;
  not `~/Downloads`, which is Dropbox-synced on the maintainer's machine).

## Pipeline
Static onboarding only. The source is updated irregularly (no declared frequency); a recurring
pipeline (AllFree — not monthly data) is a separate follow-up.
