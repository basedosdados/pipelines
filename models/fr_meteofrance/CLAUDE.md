# fr_meteofrance — Météo-France (SYNOP observations + climate normals)

France's national weather service. Two products onboarded: the **SYNOP** 3-hourly surface
observation archive and the **fiches climatologiques** (1991–2020 normals + absolute records),
plus a station register for each.

License **Licence Ouverte / Open Licence 2.0** (Etalab, `lov2` on data.gouv.fr) — open,
redistributable including commercially. Registered as `cc_by`, the same mapping
`fr_insee_sirene` uses, because the platform vocabulary has no LO 2.0 entry and Etalab
declares LO 2.0 compatible with CC BY 4.0.

Data language French → **French column names and table slugs throughout**, including the
temporal scaffolding: `annee`, `mois`, `date`, `heure`. This **departs from
`fr_insee_sirene`**, which kept `ano`/`data` in Portuguese; the French spelling was chosen
deliberately for this dataset. Descriptions are PT/EN/ES.

The single exception is `dicionario`, whose columns (`id_tabela`, `nome_coluna`, `chave`,
`cobertura_temporal`, `valor`) are hard-coded in the generic `custom_dictionary_coverage`
test and cannot be renamed without breaking it.

`date` and `heure` are safe as bare BigQuery identifiers — `DATE` is a type name but not a
reserved keyword, and `safe_cast(date as date) date` parses (verified against BigQuery).

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
| `synop` | 5,367,183 | `date`, `heure`, `indicatif_omm` | station × 3-hourly observation, partitioned by `annee` (1996–2026), clustered by `indicatif_omm` |
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

## Données climatologiques de base (quotidienne / mensuelle / poste)

The second product in this dataset: Météo-France's full climatological archive,
per département, for every station since it opened. Separate from SYNOP, which is
only the 190 internationally-reported stations.

| table | rows | key | cols |
|---|---|---|---|
| `quotidienne` | ~230 M | `date`, `numero_poste` | 138 |
| `mensuelle` | 5,300,711 | `annee`, `mois`, `numero_poste` | 159 |
| `poste` | 14,746 | `numero_poste` | 7 |

Both fact tables are partitioned by `annee` over **1688–2031** and clustered by
`numero_poste`. 1688 is not a typo — the daily archive really does reach that far
back, and the range must cover it or BigQuery drops early rows into
`__UNPARTITIONED__`.

### Things that will bite you

**The daily series ships as two files that must be OUTER-joined.** `RR-T-Vent` and
`autres-parametres` share the `(NUM_POSTE, AAAAMMJJ)` key, but neither is a superset:
in département 01 alone, 6,833 keys exist only on the autres side. A left join
silently drops them.

**The `NB*` counts reference the DAILY parameter, not the monthly one.** `NBTX` is
"nombre de valeurs présentes de TX *quotidienne*" — the number of days with a maximum
temperature, not a count of monthly means. Naming it after the monthly `TX` (which is
the monthly *mean* of daily maxima) would be wrong. Likewise `TXDAT` is "jour du
TX**AB**", the day of the absolute extreme.

**Family membership is decided from the French descriptor, not the column name.**
`HXY` is "heure de FXY" — stripping the `H` gives `XY`, which is not a column.
`NBUM` references `UM`, which does not exist in the monthly file. `clim_schema.expand`
therefore reads Météo-France's own text. Two columns still need explicit overrides:
`NBTM` ("du couple (TN, TX)") and `NBRR` (prose rather than a parameter token).

**`ECOULEMENTM` is dropped.** The source labels it *champ inutilisé* and it is 100%
null in the sampled data.

**Occurrence flags and quality codes are STRING.** `NEIG`, `BROU`, `ORAG` and the rest
are 0/1 codes, and every measurement carries a `Q*` quality code (0/1/2/9) — booleans
and codes, not quantities, so STRING + dictionary-covered per the house rule.

**`poste.id_departement` misses 8 codes.** `20` (Corsica, COG uses 2A/2B), `99`
(outside France) and five overseas codes are absent from `br_bd_diretorios_fr` —
771 of 14,746 postes. One of them, `975` (Saint-Pierre-et-Miquelon), is a genuine
**gap in the directory** rather than a Météo-France invention and would be worth
adding there.

### Staging layout

One parquet per (département, period), **not** hive-partitioned by year. These series
span ~175 years, so partitioning staging by year emits tens of thousands of tiny files
— slow to write, slow for BigQuery, and pointless since the dbt model full-scans
staging anyway. Keeping the source's own dept × period unit also makes the incremental
refresh natural: Météo-France only rewrites the `latest-<years>` files.

### Code

`clim_schema.py` (99 hand-authored parameters + the family expander),
`clim_download.py`, `clim_clean.py`, `clim_gen_artifacts.py`, `clim_upload.py`.
Scratch data lives under `~/Downloads/fr_meteofrance_clim/` (`MFC_INPUT` / `MFC_OUTPUT`).

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

`pipelines/datasets/fr_meteofrance/` — **two** flows, because the source has two cadences:

| flow | cron (BRT) | what it does |
|---|---|---|
| `fr_meteofrance_synop_flow` | `23 7 * * *` (daily) | downloads only the current year's `synop_<year>.csv.gz` and replaces that one `annee=` partition |
| `fr_meteofrance_climatologie_flow` | `37 7 6,7,8,9 * *` (monthly) | re-downloads every sheet and the full SYNOP history; rebuilds the normals, both station registers, the dictionary and all of `synop` |

The monthly flow re-downloads all 31 SYNOP years because `station_synop` carries each station's
first and last observation year, which one year cannot give. That doubles as a monthly full
rebuild and repairs any partition a daily run left stale.

`dump_mode="append"` on both, never `"overwrite"` — overwrite drops the whole staging table, and
drops the prod table even from a dev run.

**The transform is not duplicated.** `pipelines/datasets/fr_meteofrance/utils.py` holds the only
copy; `models/fr_meteofrance/code/clean.py` is a thin CLI that imports it, and column order and
types are read from the architecture CSVs rather than a second schema. Verified byte-identical
against the bootstrap output: 0 differing cells on `synop` 2024 (170,871 rows),
`normale_climatologique` (402,831) and `station_climatologique` (1,576).

The `dicionario` is **not** rebuilt from the sheets by the pipeline. It materializes the
committed `code/dicionario.csv`, so a newly published indicator fails
`custom_dictionary_coverage` and a human writes the label rather than a script inventing one.

### Before arming: `synop` needs a BD Pro coverage

`synop` refreshes daily, so per the house rule it carries `PartBdpro(free_lag=6 months)` — the
most recent six months become BD Pro, everything older stays free. `normale_climatologique`
stays `AllFree` despite its monthly reissue: its content is a fixed 1991–2020 statistic that
does not advance, so a rolling window would paywall the tail of 2020 forever.

`assert_coverage_topology` **hard-fails** unless a pro Coverage (`is_closed=True`) plus its
`DateTimeRange` already exists on `synop`. Today the table has only the free Coverage, so this
must be created before the flow is armed:

```
create_update_coverage(table_id=<synop>, area_id=<fr>, is_closed=True, env="prod")
create_update_datetime_range(coverage_id=<new>, start_year=…, is_closed=True, env="prod")
```

A dev validation run (`materialize_to_prod=False`) does **not** need it — the metadata tasks are
skipped on that path.
