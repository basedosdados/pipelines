# us_bea — Onboarding Plan

**Dataset:** `us_bea` (GCP dataset id + prod slug TBD at metadata stage; org **Bureau of Economic Analysis (BEA)**, US Dept of Commerce).
**Source:** BEA REST API — `https://apps.bea.gov/api/data/` (free `UserID` key; env var `BEA_API_KEY`).
**License:** US Government work → public domain (17 U.S.C. §105); `bea.R` DESCRIPTION states CC0. BEA ToS requires the attribution notice: *"This product uses the Bureau of Economic Analysis (BEA) Data API but is not endorsed or certified by BEA."* No share-alike. → **AllFree**.
**Cleaning reference:** `beaapi` (Python) row schemas + `bea.R`. See `scratchpad/bea_api_reference.md`.

## BEA universe enumerated (via GetParameterValues, key verified)
- **NIPA:** 253 standard tables; frequencies A/Q/M.
- **GDPbyIndustry:** 39 TableIDs; 104 industries; frequencies A/Q. (`Quarter==Year` for annual rows; quarters are Roman I–IV; field spelled `IndustrYDescription`.)
- **Regional:** 105 tables. Families:
  - County (annual): `CAGDP*` (5), `CAINC*` (8) = **13 CA tables**.
  - State annual: `SAGDP*`,`SAINC*`,`SAPCE*`,`SARPI/SARPP/SAIRPD/SASUMMARY`, arts/outdoor-rec satellite (`SAAC*`,`SAO*`).
  - State quarterly: `SQGDP*`,`SQINC*` = 18.
  - **Excluded (this pass):** metro/MSA (`MA*`,`PA*`), Puerto Rico (`PRGDP*`, 19), territories (`TASUMMARY`) — their geographies are not in `br_bd_diretorios_us` state/county, which would break the FK. Deferred to a future `regional_metro`.

## Tables (long format, one per BEA dataset)

| DB table | BEA pull | Grain |
|---|---|---|
| `nipa` | NIPA, each TableName, Freq A,Q,M, Year=ALL | table × line × freq × period |
| `gdp_by_industry` | GDPbyIndustry, each TableID, Freq A,Q, Industry=ALL | table × industry × freq × period |
| `regional_state` | Regional SA*/SQ*/**PRGDP*** tables, GeoFips=STATE, all LineCodes | table × line × state-entity × freq × period |
| `regional_county` | Regional CA* tables, GeoFips=COUNTY, all LineCodes | table × line × county × year |
| `regional_metro` | Regional MA*/PA* (MARPP, MAIRPD, PARPP, PAIRPD), GeoFips=MSA | table × line × CBSA × year |
| `dicionario` | code→label sets | — |

### Regional geography universe (enumerated via GetParameterValues GeoFips = 3,704 codes)
- US (00000, 1) + 8 BEA regions (91000–98000) → aggregate rows, `id_state` NULL, no FK.
- 56 state-level entities = 50 states + DC + PR(72000) + AS(60000)/GU(66000)/MP(69000)/VI(78000). FK → `state:id_state`.
- 3,639 counties (50 states + DC only; **no PR/territory counties**). FK → `county:id_county`.
- PRGDP* = PR territory-level only (72000) → folds into `regional_state`.
- Metro price-parity tables = ~387 MSAs (CBSA codes) → `regional_metro`, FK → `cbsa_2023:id_cbsa` (verify vintage).

### Directory work needed in `br_bd_diretorios_us` (prod slug `diretorios_us`) — pending BQ verify
- `state`: confirm it holds PR + AS/GU/MP/VI (type='territory'); append if missing.
- `county`: confirm ~3,639 rows (50 states + DC).
- `cbsa_2023`: confirm it covers BEA's ~387 MSA codes; reconcile vintage mismatch (tolerate unmatched in relationship test or append).

### Column design (English names)
Common: `year` INT64 (partition), `quarter` STRING (nullable; `frequency` STRING A/Q/M), `value` FLOAT64, `unit` STRING (CL_UNIT / METRIC_NAME), `unit_mult` STRING (UNIT_MULT base-10 exponent). Suppressed `(NA)/(NM)/(D)/''` → NULL. DataValue commas stripped.

- **nipa:** `year, quarter, frequency, table_name, line_number, series_code, line_description, metric_name, unit, unit_mult, value`. Codes STRING dict-covered.
- **gdp_by_industry:** `year, quarter, frequency, table_id, table_description, industry, industry_description, unit, value`. Codes STRING dict-covered.
- **regional_state:** `year, quarter, frequency, table_name, line_code, line_description, geo_fips, geo_name, id_state, unit, unit_mult, value`. `id_state` = 2-digit FIPS (from GeoFips `NN000`), FK → `br_bd_diretorios_us.state:id_state`; null for US/region aggregates.
- **regional_county:** `year, table_name, line_code, line_description, geo_fips, geo_name, id_county, id_state, unit, unit_mult, value`. `id_county`=5-digit FIPS FK → `br_bd_diretorios_us.county:id_county`; `id_state` FK → state. Combination-FIPS county rows that don't match the directory are tolerated in the relationship test.
- **dicionario:** standard `id_tabela, nome_coluna, chave, cobertura_temporal, valor`.

### Value/unit note
A long table mixes units within one `value` column (current $, chained $, index, percent). Per WDI precedent: `value` FLOAT64 with `measurement_unit` blank + observations note; the real per-row unit lives in the `unit`/`metric_name`/`unit_mult` STRING columns.

## Coverage & cadence
NIPA/GDPbyIndustry ~1929/1997→present; Regional state ~1963/1997→present (annual+quarterly); county ~1969→present (annual). Source republishes on GDP-release cadence (quarterly with monthly revisions) + annual regional. → recurring pipeline is a later step (12). BD Pro tier decided per table at pipeline stage (high-frequency national tables likely `PartBdpro`; annual county `AllFree`).

## Open/deferred
- GCP dataset id / prod slug at metadata stage (org `bea`; confirm org exists in backend).
- `br_bd_diretorios_us` GCP dataset id = `br_bd_diretorios_us`, prod slug `diretorios_us`.
- Pipeline runtime key → HashiCorp Vault (`get_vault_secret`), not GitHub repo secret.
