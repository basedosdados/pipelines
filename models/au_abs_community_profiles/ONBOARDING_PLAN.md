# au_abs_community_profiles — Onboarding Plan

ABS Census **Community Profiles** as ONE Data Basis dataset, **long format**. Org
`au_abs` (Australian Bureau of Statistics — does NOT exist yet, create it), license
**CC BY 4.0**. Ingestion from **DataPacks** (short-header CSV), not the per-area Excel
Community Profiles (same tables, machine-readable).

## Scope (user-approved 2026-08-04)
- **Profiles:** GCP (General Community Profile) + TSP (Time Series Profile). ATSIP/WPP/PEP deferred.
- **Years:** native GCP **2011 / 2016 / 2021** (per-census snapshots) + TSP.
- **Geo floor:** include **SA1** (GCP only; TSP floors at SA2). SA1 = the ~2–3B-row driver.
- 2026 Census data lands ~Jun 2027 (append later).

## Schema — LONG, one table per geography level
Geo tables: `national, state, sa1, sa2, sa3, sa4, gccsa, lga, suburb, postal_area,
commonwealth_electoral_division, state_electoral_division`.

Columns (each geo table):
| col | type | notes |
|---|---|---|
| `census_year` | INT64 | **partition** (2011/2016/2021) |
| `id_<level>` | STRING | FK → `br_bd_diretorios_au.<level>_<census_year>` (year-scoped) |
| `profile` | STRING | `GCP` / `TSP` |
| `table_code` | STRING | e.g. `G01` (2016/21), `B01` (2011), `T01` (TSP) |
| `cell_code` | STRING | cell short code from the DataPack header |
| `value` | FLOAT64 | mixed units (persons / AUD / years); unit lives per-cell in auxiliary_info |

Plus two support tables:
- **`auxiliary_info`** — cell catalogue: `profile, census_year, table_code, cell_code,
  cell_description, table_name, statistic_type (count/median/average), measurement_unit`.
  Built from the DataPack **cell-dictionary metadata file**. Absorbs the mixed-unit +
  human-readability concerns. English (source language); PT/EN/ES on the *column* metadata only.
- **`dicionario`** — standard BD role only (coded categorical column values → labels). Likely
  minimal here (`profile` is readable, geo/table/cell codes are catalogued in auxiliary_info,
  not the dicionario). Include only if a genuinely coded categorical column exists.

## FKs
- `id_<level>` → the matching-vintage directory table (`sa2_2011` / `sa2_2016` / `sa2_2021`).
  Because one column mixes vintages, use **year-scoped dbt relationships tests**
  (`config: {where: "census_year = 2011"}` → `*_2011`, etc.); backend `directory_column`
  points to the 2021 edition, vintage noted in observations.
- **TSP rows are all on 2021 boundaries** → their `id_<level>` always FKs to `*_2021`
  (scope those tests by `profile = 'TSP'`).
- `census_year` → `br_bd_diretorios_data_tempo.ano`.

## Key design decisions (locked)
1. Long over wide: wide would be ~200 BD tables / ~30k trilingual columns; long is ~14 tables.
2. `value` is one FLOAT64 column; **per-cell unit/statistic live in `auxiliary_info`**, NOT in
   the dicionario and NOT as a column-level measurement_unit.
3. `dicionario` keeps its standard coded-categorical→label role only.
4. 2011 uses a different table-code namespace (Basic Community Profile "B-series") — GCP is
   per-census snapshots; cross-year comparability is what TSP provides. Fine per user.
5. `profile` as a column (GCP vs TSP) within the same geo-level tables (revisit if the FK
   test logic gets cleaner with split tables).

## Steps
1. **Investigate DataPack structure** (in progress) — geo-code column names per year, cell-column
   format, A/B/C splits, the cell-dictionary metadata file, TSP year encoding, 2011 B-series,
   NULL/suppression sentinels.
2. Architecture CSVs + generators (`code/`), mirroring the directory's generator style.
3. Download DataPacks (short-header; GCP 2011/2016/2021 + TSP; all needed geo levels).
4. Clean → long partitioned parquet (partition by `census_year`; likely hive `census_year=/profile=`).
   Verify SA1 on a subset first (scale).
5. Upload to `basedosdados-dev` staging.
6. dbt: long models + `auxiliary_info` + `dicionario`; year-scoped FK tests; `dbt run`+`test` green.
7. Staging metadata + **publish on staging** (per amended lifecycle: publish dev/staging pre-PR).
   Create the `au_abs` org first.
8. **Combined PR** with the ASGS 2011 directory extension (user approves one PR for both).

## Resolved from investigation (2026-08-04)
- **Profile = a column** (GCP/TSP), one table per geo level (~14 tables). FK via profile+year-scoped dbt tests; backend `directory_column` → 2021 edition. SA1 has GCP rows only.
- **TSP: strip the year token** from cell_code (`Tot_persons_C11_M` → `Tot_persons_M`, census_year=2011); token from cell code, not filename (filename year is always 2021 for TSP). Known tokens only: `_C11/_C2011→2011`, `_C16/_C2016→2016`, `_C21/_C2021→2021`.
- **2011 = BCP product** (profile stored as `GCP`, table_code `B##`); DataPack profile token is `BCP`, REGION token `AUST`. Geo column is generic **`region_id`** → rename to `id_<level>` using the geo level from the **filename**.
- **value FLOAT64**; `..` (not-applicable, whole-column) → drop those rows (dense fact table); genuine `0` kept.
- **table_code** from filename: strip a trailing split letter (`G04A`→`G04`); validate vs dictionary `Profile table`.
- **auxiliary_info** = `profile, census_year, table_code, table_name, table_population, cell_code, long_description, heading, datapack_part, statistic_type, measurement_unit`. statistic: `count` unless table ∈ {G02,B02,T02} then Median_/Med_→median, Average_/Avg_→average. unit: table_population (Persons/Families/Dwellings) for counts; parsed from heading for medians/averages.
- **Download URL:** `.../datapacks/download/<YEAR>_<PROFILE>_<GEO>_for_<REGION>_short-header.zip` (browser UA). PROFILE∈{GCP,BCP,TSP}; REGION=AUS (2016/21) / AUST (2011). Short-header. Filename regex `(\d{4})Census_([A-Z]+\d+[A-Z]?)_(\w+)_([A-Z0-9]+?)(_short)?\.csv`.
- **Geo-code column:** 2016/21 = `<GEO>_CODE_<year>`; 2011 = `region_id`.

## Still open
- Partition/scale strategy for SA1 (billions of rows) — partition census_year; verify on subset first.
- Confirm which geo levels each profile offers (GCP → SA1..SED; TSP → SA2-floor, maybe not SAL/POA/CED/SED) — the download step reveals via 404s.
- dicionario likely minimal (profile/statistic readable) — include only if a coded categorical column exists.
