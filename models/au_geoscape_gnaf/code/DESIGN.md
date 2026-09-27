# au_geoscape_gnaf — Onboarding Design Spec

Geoscape **G-NAF** (Geocoded National Address File): authoritative geocoded index of
~15.9M Australian addresses. Own dataset (not a directory table). English column names.

> **AUTHORITATIVE SCHEMA = `code/architecture/*.csv`** (exported from the reviewed Google Sheets,
> user edits folded in). Downstream (clean, dbt, metadata) reads column names/types/FKs from those
> CSVs, not from the prose column lists below. User edits already applied:
> 1. **Coded columns dropped the `_code` suffix:** `flat_type`, `level_type`, `level_geocoded`,
>    `geocode_type` (address_detail); `street_type`, `street_suffix`, `street_class`,
>    `gnaf_reliability` (street_locality); `locality_class`, `gnaf_reliability` (locality).
>    Their `original_name` still points at the raw PSV `*_CODE` columns. The `dicionario`
>    `nome_coluna` values must use these NEW names.
> 2. **Temporal columns carry directory FKs:** `snapshot_date` + every `date_*` →
>    `br_bd_diretorios_data_tempo.data:data`; `year` → `br_bd_diretorios_data_tempo.ano:ano`.
>    → dbt schema.yml adds `relationships` tests for these (scoped `__most_recent_date__`).
> 3. locality `id_state` original_name = `STATE_PID` (raw column in LOCALITY.psv);
>    address_detail/street_locality derive `id_state` from the per-state file.

## Source & license
- Source: https://data.gov.au/data/dataset/geocoded-national-address-file-g-naf (CKAN UUID
  `19432f89-dc3a-4ef3-b943-5326ef1dbecc`). One **all-states** ZIP of pipe-separated `.psv`;
  inside, tables unpack **per state/territory** (ACT, NSW, NT, OT, QLD, SA, TAS, VIC, WA).
  Authority (`*_AUT`) tables are national/unsplit. Two datum variants offered (GDA94, GDA2020).
- **Datum choice: GDA2020** (modern Australian datum, standard since 2019). Note datum in obs.
- **Resolved current download (May 2026 release, GDA2020):**
  `https://data.gov.au/data/dataset/19432f89-dc3a-4ef3-b943-5326ef1dbecc/resource/f8666213-4079-44da-bede-ebda3a4363e0/download/g-naf_may26_allstates_gda2020_psv_1023.zip`
  — 1,706,838,674 bytes (~1.59 GB zip; ~5 GB unpacked per official page). resource-uuid
  `f8666213-4079-44da-bede-ebda3a4363e0` and the `may26`/`_1023` build tokens change each release;
  re-resolve off the landing page via browser each quarter (GDA94 variant resource
  `1d42210b-0760-4cad-96cf-45a3d66ac1cb`). snapshot_date for this release = `2026-05-01`.
- **Download transport:** `curl -L` works with the REAL network route only
  (`dangerouslyDisableSandbox=true` in Bash, or a browser). Sandboxed WebFetch/curl → HTTP 403.
- EULA PDF: `.../resource/09f74802-08b1-4214-a6ea-3591b2753d30/download/20160226-eula-open-g-naf.pdf`.
- Data model: https://docs.geoscape.com.au/projects/gnaf_desc/en/stable/ (Appendix B model,
  Appendix C dictionary). CRS GDA94 / GDA2020 (per-geocode datum).
- **License: Open G-NAF EULA = CC BY 4.0 + mail-out use-restriction. → publishable, free-tier
  `AllFree`, NOT BD-Pro.** Slug `cc_by_4_0` (verify). Verbatim terms (from data.gov.au landing
  page, confirmed 2026-08-14) to record in dataset `observations`:
  - Mail-out restriction: "The open G-NAF data must not be used for the generation of an address
    or the compilation of an address for the sending of mail unless the user has verified that each
    address to be used for the sending of mail is capable of receiving mail by reference to a
    secondary source of information."
  - Attribution (Licensed Material): "G-NAF © Geoscape Australia licensed by the Commonwealth of
    Australia under the Open Geo-coded National Address File (G-NAF) End User Licence Agreement."
  - Attribution (Adapted Material): "Incorporates or developed using G-NAF © Geoscape Australia
    licensed by the Commonwealth of Australia under the Open Geo-coded National Address File (G-NAF)
    End User Licence Agreement."
  - Privacy: "End users must only use the data in ways that are consistent with the Australian
    Privacy Principles issued under the Privacy Act 1988 (Cth)."
  These four are sufficient for the license record; the clause-numbered PDF adds nothing that
  changes the redistribution decision.

## History model (br_me_cnpj-style snapshot stacking)
Each quarterly release is a full snapshot. We **stack** releases, keeping full history:
- Every table carries `snapshot_date` (DATE) = the release date (first day of release month,
  e.g. May 2026 → `2026-05-01`).
- dbt: `materialized="incremental"`, `partition_by={field:"snapshot_date", data_type:"date"}`,
  `cluster_by=["year","id_state"]`, `pre_hook="DROP ALL ROW ACCESS POLICIES ON {{ this }}"`.
- Derived `year` (INT64) = year of snapshot_date.
- Tests scoped `where: __most_recent_date__`; `is_row_count_increasing` on `snapshot_date`.
- Pipeline `dump_mode="append"` (stack new snapshot); NOT overwrite.

## Column ordering (every table)
1. Temporal: `snapshot_date`, `year`
2. Geographic: `id_state`
3. Identifiers (`*_pid`)
4. Descriptive columns

## Type rules
- `*_pid`, `postcode`, all `*_code` fields, `confidence` (0/1/2), `alias_principal` (A/P),
  `primary_secondary` (P/S), reliability codes, number/lot/flat/level number & prefix/suffix →
  **STRING** (identifiers/codes; no arithmetic). Coded ones → `covered_by_dictionary=yes`.
- `longitude`, `latitude` → **FLOAT64**, measurement_unit `degree`.
- `year` → INT64 (measurement_unit `year`). Dates → DATE.
- Mesh-block codes → STRING, `covered_by_dictionary=no` (no mb directory yet; note intended ABS FK
  in observations).
- `id_state` → STRING, `directory_column = br_bd_diretorios_au.state:id_state`,
  `covered_by_dictionary=no`. **Domain = ABS state code 1–9** (matches the directory; G-NAF uses the
  same ASGS codes). Populate from the per-state file, not the abbreviation:
  `{NSW:1, VIC:2, QLD:3, SA:4, WA:5, TAS:6, NT:7, ACT:8, OT:9}`. (G-NAF STATE.STATE_PID already
  equals this code; for tables reached via LOCALITY, resolve via LOCALITY.STATE_PID which is 1–9.)

---

## Tables

Coded columns carry NO `_code` suffix (e.g. `flat_type`, `geocode_type`) — see the note at the top;
the architecture CSVs are authoritative and the cleaning/dbt models use these names. `original_name`
in the CSVs still points at the raw PSV `*_CODE` columns.

### 1. address_detail  (core, ~15.9M rows × snapshots)
Central table, one row per address. Fold in the **default geocode** (ADDRESS_DEFAULT_GEOCODE:
geocode_type, longitude, latitude) and the **ABS mesh-block** codes
(ADDRESS_MESH_BLOCK_2016→MB_2016.mb_2016_code, ADDRESS_MESH_BLOCK_2021→MB_2021.mb_2021_code).

Columns (order):
snapshot_date, year, id_state, address_detail_pid, date_created, date_last_modified, date_retired,
building_name, lot_number_prefix, lot_number, lot_number_suffix, flat_type, flat_number_prefix,
flat_number, flat_number_suffix, level_type, level_number_prefix, level_number,
level_number_suffix, number_first_prefix, number_first, number_first_suffix, number_last_prefix,
number_last, number_last_suffix, street_locality_pid, location_description, locality_pid,
alias_principal, postcode, private_street, legal_parcel_id, confidence, address_site_pid,
level_geocoded, property_pid, gnaf_property_pid, primary_secondary, geocode_type,
longitude, latitude, id_mb_2016, id_mb_2021

- PK (logical, dbt unique test): `snapshot_date + address_detail_pid`.
- FKs: street_locality_pid→street_locality, locality_pid→locality, id_state→directory.
- dict columns: flat_type, level_type, level_geocoded, alias_principal, confidence,
  primary_secondary, geocode_type.

### 2. street_locality  (~1.5M rows × snapshots)
Street reference; fold in STREET_LOCALITY_POINT lat/long. State known from per-state file.

Columns:
snapshot_date, year, id_state, street_locality_pid, date_created, date_retired, street_name,
street_type, street_suffix, street_class, locality_pid, gnaf_street_pid,
gnaf_reliability, longitude, latitude

- PK: `snapshot_date + street_locality_pid`.
- dict: street_type, street_suffix, street_class, gnaf_reliability.

### 3. locality  (~16k rows × snapshots)
Locality (suburb) reference; fold in LOCALITY_POINT lat/long. Resolve STATE_PID→id_state.

Columns:
snapshot_date, year, id_state, locality_pid, date_created, date_retired, locality_name,
primary_postcode, locality_class, gnaf_locality_pid, gnaf_reliability, longitude, latitude

- PK: `snapshot_date + locality_pid`.
- dict: locality_class, gnaf_reliability.

### 4. dicionario  (standard 5-col)
Covers every coded column above. Source = the G-NAF `*_AUT` tables (CODE/NAME/DESCRIPTION),
one dictionary entry set per column (`nome_coluna` uses the canonical, no-`_code` names):
- flat_type ← FLAT_TYPE_AUT
- level_type ← LEVEL_TYPE_AUT
- level_geocoded ← GEOCODED_LEVEL_TYPE_AUT
- geocode_type ← GEOCODE_TYPE_AUT
- street_type ← STREET_TYPE_AUT
- street_suffix ← STREET_SUFFIX_AUT
- street_class ← STREET_CLASS_AUT
- locality_class ← LOCALITY_CLASS_AUT
- gnaf_reliability ← GEOCODE_RELIABILITY_AUT (emitted for both street_locality and locality)
- alias_principal ← {A: Alias, P: Principal}
- primary_secondary ← {P: Endereço primário, S: Endereço secundário}
- confidence ← {-1, 0, 1, 2 confidence levels per product desc}

Columns: id_tabela, nome_coluna, chave, cobertura_temporal, valor (all STRING).

---

## Observation levels (metadata step)
- address_detail: address grain (may need new entity `address`) + geographic `state`.
- street_locality: street grain + state.
- locality: locality/suburb grain + state.
Link the identifying column of each OL (address_detail_pid, street_locality_pid, locality_pid,
id_state) per metadata-schema rules.

## Deferred (not in this phase)
Alias tables, primary_secondary link table, address_site, address_feature history, address_site_geocode,
extra geocode types beyond default. Revisit if a consumer needs them.
