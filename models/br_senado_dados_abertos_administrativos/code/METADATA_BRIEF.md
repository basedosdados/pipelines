# Metadata registration brief — br_senado_dados_abertos_administrativos

Backend: **staging** (`env="staging"`; dev backend was 503 during onboarding).
GCP project for cloud tables: **basedosdados-dev**. Dataset status:
**under_review**. Backend slug: **`dados_abertos_administrativos`** (mirrors the
sibling `dados_abertos_legislativos`; NOT the GCP dataset id, and NOT the linked
dataset `5c5506e8…`, which is the existing *legislative* dataset).

Registration is driven by a code script, not by hand or Google Sheets:
`senado_adm_metadata.py` (kept with the onboarding scratch, references
`~/Dropbox/BD/mcp/server.py` directly). It reads names/descriptions from
`architecture_spec.py` and columns from `columns_json/<slug>.json`
(regenerate with `gen_columns_json.py`). Idempotent — reuses ids from
`get_dataset`, so a partial run re-runs safely.

## Resolved IDs (staging)
- dataset `dados_abertos_administrativos`: `4ebbc1b5-83d0-4015-8898-ebcbf4b750cd`
- raw source (adm.senado.gov.br API): `90e7bd79-1f6a-4383-b137-540d3da58ced`
- organization `senado`: `197881e3-9074-4c39-a749-b150594697b4`
- theme `government`: `6dd730bb-89ab-4dba-a1bf-a25ca1c35003`
- status `under_review`: `47208305-…`, `published`: `e16221de-…`
- area `br`: `5503dd29-4d9b-483b-ae09-63dc8ed28875`
- license `unknown`: `77dfe32b-…`; availability `online`: `dd396d7d-…`
- account (published_by / data_cleaned_by): `57`
- entities: person `b4e76213-…`, year `e1bf146e-…`, company `b585c285-…`,
  contract `38e7435c-…`, procurement `4cce9a0f-…`, grant `ecfbd448-…`,
  office `4c68aa29-…`, agency `24326cfe-…`, day `81f0c890-…`

## Observation-level entity map (architecture `ol` slug → backend entity)
senador/servidor/pessoa → **person** · contratacao → **contract** ·
licitacao → **procurement** · empresa → **company** ·
ato_concessao → **grant** · quadro → **office** · setor → **agency**.
Every `ano`-partitioned table additionally gets a **year** OL on `ano`.

## Coverage (area br, is_closed=False — all FREE at onboarding)
- `ano` tables: annual `datetime_range` over the real min..max year
  (CEAPS 2008–2026; remuneração/horas-extras 2013–2026; supridos 2013–2026,
  but transações 2020–2026 and movimentações 2013–2019 by what the source
  exposes).
- `data_extracao` snapshots: `datetime_range` 2026–2026 (the onboarding snapshot;
  the stack grows as the pipeline runs).
- `dicionario`: area coverage only, no datetime range.

The BD Pro rolling window (a second, `is_closed=True` coverage) is **not** set at
onboarding. It is added later, before arming the recurring pipeline, only on the
ten `ano`-partitioned time series, per the standard 6-month paywall.

## Per-table sequence (what the driver does for each of the 39)
1. `create_update_table` (status published; published_by/data_cleaned_by = acct).
2. `create_update_observation_level` per OL entity.
3. `bulk_upsert_columns` with `columns_json/<slug>.json`.
4. `update_column` to set `is_partition=True` on the partition column and to link
   each OL to its grain column (re-passing `is_partition` when the grain column
   is the partition).
5. `create_update_cloud_table` (basedosdados-dev, gcp_dataset br_senado_dados_abertos_administrativos).
6. `create_update_coverage` (free) + `create_update_datetime_range` per above.
7. `create_update_update` (table-anchored; entity day; latest = onboarding date).
8. After all tables: `create_update_table` again with `raw_data_source_ids` set,
   then `reorder_tables` in architecture order.

## Status at last run
38 of 39 tables fully registered on staging; `dicionario` and the raw-source
linking + reorder were interrupted by a staging outage (504/timeout on heavy
`get_dataset`). Re-run `senado_adm_metadata.py tables` then `… link` once staging
recovers; the resume guard skips already-complete tables.
