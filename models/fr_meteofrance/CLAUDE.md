# fr_meteofrance — Météo-France (SYNOP observations + climate normals)

France's national weather service. Two products onboarded: the **SYNOP** 3-hourly surface
observation archive and the **fiches climatologiques** (1991–2020 normals + absolute records),
plus a station register for each.

License **Licence Ouverte / Open Licence 2.0** (Etalab, `lov2` on data.gouv.fr) — open,
redistributable including commercially. Registered as `cc_by`, the same mapping
`fr_insee_sirene` uses, because the platform vocabulary has no LO 2.0 entry and Etalab
declares LO 2.0 compatible with CC BY 4.0.

Data language French → **French column names and table slugs**, with the BD-standard temporal
scaffolding (`ano`, `mes`, `data`, `hora`) keeping its house name, exactly as in
`fr_insee_sirene`. Descriptions are PT/EN/ES.

## Sources

| Source | data.gouv id | Files | Cadence |
|---|---|---|---|
| Archive Synop OMM | `686f8595b351c06a3a790867` | `synop_<year>.csv.gz` 1996–2026 on `meteofrance.s3.sbg.io.cloud.ovh.net`, + `postes_synop.geojson`, + technical descriptor PDF | daily (current year rewritten) |
| Fiches climatologiques | `684c2d56f3861808c0a5d465` | `FICHECLIM_<num_poste>.data` × 1,576, + `liste_fiches_clim.geojson` | monthly |

The legacy `donneespubliques.meteofrance.fr/donnees_libres/Txt/Synop/...` URLs are **dead**
(404); everything now comes through data.gouv.fr / the OVH object store.

## Tables (5)

| table | rows | key | grain |
|---|---|---|---|
| `synop` | 5,367,183 | `data`, `hora`, `indicatif_omm` | station × 3-hourly observation, partitioned by `ano` (1996–2026), clustered by `indicatif_omm` |
| `station_synop` | 190 | `indicatif_omm` | SYNOP station |
| `normale_climatologique` | 402,831 | `numero_poste`, `indicateur`, `periode` | station × indicator × month-or-year, long format |
| `station_climatologique` | 1,576 | `numero_poste` | climatological station |
| `dicionario` | 312 | `id_tabela`, `nome_coluna`, `chave` | WMO code labels + normals indicator/period/unit labels |

## Things that will bite you

**Units are the source's own.** Temperature in **kelvin**, pressure in **pascal**, wind in m/s,
precipitation in mm, geopotential in m²/s². Nothing was converted; the unit is recorded in each
column's `measurement_unit`. Do not "fix" this by converting — the platform convention is
fidelity to the source.

**The SYNOP network tripled in 2025.** 59–60 stations from 1996 to 2024, then **189 in 2025 and
190 in 2026**. Row counts jump from ~165k/year to ~290k+. This is a real change in what
Météo-France publishes, not a cleaning bug.

**SYNOP messages are retransmitted.** The same `(station, validity_time)` appears up to three
times, differing in `insert_time` and sometimes in payload (corrections). `clean.py` keeps the
**latest `insert_time`**, which removed 12,176 of 5,379,359 raw rows. `reference_time` and
`insert_time` are null before 2025.

**The coded columns are BUFR, not the WMO tables the descriptor cites.** The technical PDF says
`ww` is WMO 4677, but the published values run past 99 (121, 141, 142) because the export is
BUFR 0 20 003, where 100+ is the automatic-station table 4680. Same story for `w1`/`w2`
(0 20 004/005, 10–19 automatic), the cloud columns (0 20 012 — CH 10–19, CM 20–29, CL 30–39,
genus 0–9, "invisible" 59–62) and `etat_sol` (0 20 062, 10–19 = ground with snow). The
`dicionario` is built against the **BUFR** ranges and covers every value that occurs in
1996–2026, verified by `validate.py`. Each affected column carries an `observations` note.

**`phenomene_special_1..4` are deliberately not dictionary-covered.** The descriptor cites WMO
3778, but the source publishes 435 distinct compound national codes up to four digits, outside
that table's domain, and no public value→label table was found. Left as STRING with a note.

**`cod_tend` publishes a value 10** that WMO 0200 does not define (109 of 4,750,246 filled
observations). It is in the dicionario as "valeur non définie dans la table de code" so
dictionary coverage stays complete rather than needing an exception.

**Some code columns arrive as reals.** `phenspe4` and `sw` come through as `"18.0"`, `"1.0"`.
`clean.py::normalise_code` strips the trailing `.0`; without it the dictionary join misses.

**Two indicator labels collide if you strip the minus sign.** `Tn <= 10°C` and `Tn <= -10°C`
both slugified to `nombre_jours_tn_inf_10c` and two stations (98404001, 98404002) publish both
rows. `parse_ficheclim.slugify` spells the sign out (`tn_inf_moins_10c`).

**`id_departement` is Météo-France's coding, not the INSEE COG.** Corsica is `20`, not `2A`/`2B`,
and `984`, `986`, `987`, `988` (TAAF, Wallis, Polynésie, Nouvelle-Calédonie) are absent from
`br_bd_diretorios_fr__departement`. The FK test uses `custom_relationships` with those five in
`ignore_values`.

**Nine `synop` columns are legitimately under 5% non-null** (`methode_mesure_tw` has 8 filled
values in 5.4M rows). They are listed in the `not_null_proportion_multiple_columns`
`ignore_values`.

## Code (`code/`)

- `schema_map.py` — the authoritative source→target→type→unit→description mapping, plus the
  `OBSERVATIONS` notes. Every other artifact is generated from it.
- `descriptions_i18n.py` — EN/ES translations keyed by column name.
- `parse_ficheclim.py` — parses the fixed-layout `FICHECLIM_*.data` sheets.
- `clean.py` — SYNOP + normals → partitioned all-STRING parquet under `$MF_OUTPUT`.
- `build_dicionario.py` — the BUFR/WMO code tables, in French.
- `gen_artifacts.py` — regenerates `architecture/*.csv`, the five dbt models and `columns.json`.
  **Run it after any schema change** so architecture, SQL and metadata cannot drift, then run
  `uv run pre-commit run --files models/fr_meteofrance/...` — it emits unformatted SQL that
  `sqlfmt` rewrites.
- `validate.py` — key uniqueness, coverage, sparsity, dictionary coverage. Run before uploading.
- `upload.py` — uploads to `basedosdados-dev.fr_meteofrance_staging`, verifying row counts
  against the parquet footers.
- `probe_codes.py` — reports the distinct values of every coded column, for dictionary work.

Scratch data lives in `~/Downloads/fr_meteofrance_data/{input,output}` (override with
`MF_INPUT` / `MF_OUTPUT`) and is deleted at the end of the onboarding.

## Auxiliary files

Two per-table bundles are published at
`gs://basedosdados/auxiliary_files/fr_meteofrance/{synop,normale_climatologique}/auxiliary_files.zip`
and recorded in `Table.auxiliaryFilesUrl`. Each holds a README with the citation, per-file
provenance and download dates, plus the caveats a reader needs (units, UTC, dedup, BUFR codes).

`synop` carries the technical parameter descriptor PDF, the shipped station list and
`postes_synop.geojson`; `normale_climatologique` carries `liste_fiches_clim.geojson` and one
example sheet. The 1,576 per-station PDF sheets are link-only.

**Both published URLs return HTTP 400 to an anonymous request**, verified with `curl -sI`. The
bucket is requester-pays, which is the documented platform-wide defect affecting all 84
production tables that use this field — not something specific to this dataset.

## Not onboarded (worth a follow-up)

Météo-France publishes 122 datasets on data.gouv.fr. The big omissions, all `lov2`:

- **Données climatologiques de base — quotidiennes** (`6569b51ae64326786e4e8e1a`): every station
  since opening, per département × period × (RR-T-Vent | autres-paramètres), 628 resources.
  The real climatological archive — hundreds of millions of rows back to the 1800s.
- **— mensuelles** (`6569b3d7d193b4daf2b43edc`) and **— horaires** (`6569b4473bedf2e7abad3b72`),
  same shape, smaller.
- **Informations sur les stations** (`656dab84db1bdf627a40eaae`): `fiches.json`, 199 MB, the full
  station metadata history (positions and measured parameters over time).
- **Longues Séries Homogénéisées**, **SQR**, **SIM**, and the DRIAS climate projections.

## Recurring pipeline

Not built yet. The source rewrites the current year's `synop_<year>.csv.gz` daily and reissues
the fiches monthly, so a daily flow re-downloading the current year and overwriting its
partition is the natural shape. See `.claude/rules/prefect-pipeline-conventions.md`.
