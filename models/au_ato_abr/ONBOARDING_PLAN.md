# au_ato_abr — Australian Business Register (ABN Bulk Extract)

Onboarding plan and design record. Analog of br_rf_cnpj (CNPJ), fr_insee_sirene
(SIRENE), mx_* (DENUE): a national business register.

## Source
- Publisher: Australian Taxation Office (ATO) / Australian Business Register (ABR)
- Landing: https://data.gov.au/data/dataset/abn-bulk-extract
- Data: two ZIPs (`public_split_1_10.zip`, `public_split_11_20.zip`), 20 XML files,
  one `<ABR>` record per business, ~20.4M records, ~12.5 GB uncompressed.
- Schema: `bulkextract.xsd` (kept in `code/bulkextract.xsd`).
- License: **CC BY 3.0 AU** (https://creativecommons.org/licenses/by/3.0/au/) —
  redistribution + commercial use permitted.
- Cadence: **weekly full snapshot** (each release replaces the whole register).
- Snapshot onboarded: **extraction_date = 2026-08-12** (from `<ExtractTime>`).
- Note: **no ANZSIC / industry code** exists in the extract.

## Data model (CNPJ-style stacked snapshots)
Each weekly extract is a full photograph; we **stack** extracts and keep an
`extraction_date` DATE partition column (mirrors CNPJ `data_referencia`). Tables
`materialized="incremental"`, partitioned by `extraction_date` (day granularity).

| Table | Grain | Notes |
|-------|-------|-------|
| `entity` | one row per ABN per snapshot | main register: status, entity type, name, ASIC, GST, state, postcode |
| `other_name` | one row per trading/business/other name | `<OtherEntity>` 0..* — name_type TRD/BN/OTN |
| `dgr` | one row per DGR endorsement | `<DGR>` 0..* — deductible gift recipient |
| `dicionario` | code → label | abn_status, entity_type, gst_status, state_code, asic_number_type, replaced, name_type |

- Observation level: `company` (all three data tables).
- Individuals (sole traders, `entity_type=IND`) are **included**; `entity_name`
  holds the assembled given+family name; `entity_name` flagged `has_sensitive_data`.
- Coded columns are STRING + dicionario. Geography (`state_code`, `postcode`) is
  STRING for now; link to `br_bd_diretorios_au` (state/territory) and a POA
  postcode directory when those land in prod.
- Date sentinels (`19000101`) mapped to NULL.

## Tier: PartBdpro
Weekly refresh ⇒ the recent snapshot window is paywalled to BD Pro; older
snapshots free. The rolling window and Row Access Policies are applied by the
recurring pipeline (`register_table_materialization_task`), not the static
onboard. At onboarding there is a single snapshot, so the free/pro split is
degenerate; the pro Coverage + policies go live with the pipeline (step 12).

## Files
- `code/clean_data.py` — streams `<ABR>` from the ZIPs (no full extraction),
  writes typed partitioned parquet for the 3 tables + a dicionario CSV.
- `code/columns_json/*.json` — trilingual column definitions (bulk_upsert source).
- `code/architecture/*.csv` — BD architecture source of truth (built from JSON).
- `au_ato_abr__*.sql`, `schema.yml` — dbt models + tests.

## Recurring pipeline (step 12)
`pipelines/datasets/au_ato_abr/` — weekly Prefect 3 flow `au_ato_abr_flow`.
- **Poll-first**: HEAD the ZIPs, compare `Last-Modified` against `Table.Update.latest`
  (`compare_against="table_update"`); download the ~1 GB payload only when the source
  republishes. `Table.Update.latest` was set to 2026-08-14 (after the onboarded
  snapshot), so the poll correctly skips until a newer snapshot lands.
- **Stacking**: staging upload `dump_mode="overwrite"` (current snapshot); the
  **incremental** dbt models append the new `extraction_date` partition to prod.
- **Shared transform**: `utils.py` holds the pure download+clean functions; the
  bootstrap `code/clean_data.py` imports them (DRY). Output is **all-STRING** parquet
  (upload_to_gcs staging requirement).
- **Tier**: `_COVERAGE` sets **PartBdpro** (`free_lag=6 months`) on entity/other_name/dgr.
  **Before arming**: (1) confirm the free_lag (weekly register → a shorter lag e.g.
  `weeks=4` narrows the initial free-tier lockout); (2) create the **pro Coverage**
  (`is_closed=True`) on each of the 3 tables in prod, or the first armed run hard-fails
  at `assert_coverage_topology`; (3) the first armed run applies the BigQuery Row Access
  Policies (paywall goes live). Deploy lands **paused**; arm via Django admin.
- **Deploy**: PR needs the **`deploy-flow`** label for the dev-pool registration, then a
  dev run with `{materialize_to_prod:False, update_metadata:False, force_run:True}` is
  the definition of done (local checks can't reach the upload/poll/dbt-prod paths).

## Workflow status
1. context ✓  2. architecture ✓  3. download ✓  4. clean (running)
5. upload dev — pending  6. dbt ✓ (written)  7. validate — pending
8. discover — pending  9. metadata dev — pending  9b. publish dev/staging — pending
[CHECKPOINT] 10. prod  11. PR  12. recurring pipeline (weekly, dump overwrite,
PartBdpro window)  13. publish prod  14. cleanup scratch.

Scratch data: `~/Downloads/au_ato_abr_data/` (deleted at step 14).
