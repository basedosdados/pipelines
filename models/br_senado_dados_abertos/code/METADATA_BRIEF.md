# Metadata registration brief — br_senado_dados_abertos (dev/staging)

Register all metadata in the **staging** backend (`env="staging"` — the dev backend is
down). GCP project for cloud tables: **basedosdados-dev**. Dataset status: **under_review**.
Follow `.claude/rules/metadata-schema.md`. Idempotent tools — pass existing ids to update.

## Resolved IDs (staging)
- organization `senado`: `197881e3-9074-4c39-a749-b150594697b4`
- theme `government`: `6dd730bb-89ab-4dba-a1bf-a25ca1c35003`; `politics`: `c4e75f8c-29de-4b76-9607-657d4ac7f490`
- status `under_review`: `47208305-325a-4da9-9222-ac6849405b78`; `published`: `e16221de-ac30-4926-83d3-de219998dab3`
- area `br`: resolve via `lookup_id(category="area", slug="br", env="staging")`
- entities: year `e1bf146e-b6bb-4b65-bee7-c800876e80a5`, vote `7bb94a71-12b8-42d2-b881-e750718a534f`,
  person `b4e76213-888b-40ea-b877-d82ce76d71a2`, party `fcee5475-ec7c-46c9-8000-b223e892932c`,
  bill `d7a07eb4-4861-4499-8cd0-e87eb88bf3d1`, committee `214ea681-46e5-43c2-bc07-5d3a20de5481`,
  caucus `af551e2e-c472-4a8e-b301-203d0c79216a`
- `get_authenticated_account(env="staging")` → use its id for `published_by_ids` and `data_cleaned_by_ids`

## Dataset
`create_update_dataset(slug="br_senado_dados_abertos", env="staging", status_id=under_review,
organization_ids=[senado], theme_ids=[government, politics], tag_ids=[],
name_pt="Senado Federal - Dados Abertos", name_en="Federal Senate - Open Data",
name_es="Senado Federal - Datos Abiertos", description_*=…)`.
Description (pt): "Dados abertos legislativos do Senado Federal e do Congresso Nacional — senadores,
votações nominais, processos legislativos, comissões, partidos, blocos, lideranças e Mesa Diretora —
extraídos do Serviço de Dados Abertos Legislativos (legis.senado.leg.br/dadosabertos)." Translate en/es.

## Raw data source (one, link to every table via deferred update)
`create_update_raw_data_source`: name "Serviço de Dados Abertos Legislativos do Senado Federal",
url `https://legis.senado.leg.br/dadosabertos`, availability=online, free (no API key), no IP/registration.

## Per-table sequence (all 10)
Names/descriptions (PT/EN/ES) are in `architecture_spec.py` (`TABLES[slug]` → name_pt/en/es, desc_pt/en/es).
Columns are in `code/columns_json/<slug>.json` (already include description_pt/en/es, bigquery_type,
covered_by_dictionary, directory_column, measurement_unit, has_sensitive_data).

For each table:
1. `create_update_table(slug, dataset_id, name_*, description_*, status_id=published,
   published_by_ids=[acct], data_cleaned_by_ids=[acct], env="staging")` — do NOT pass raw_data_source_ids yet.
2. `create_update_observation_level(table_id, entity_id)` for each OL entity (below). Keep the returned ids.
3. `bulk_upsert_columns(table_id, columns_json=<contents of columns_json/<slug>.json>, env="staging")`.
4. `update_column` for the partition column `ano` (is_partition=True) — tables votacao, votacao_parlamentar,
   votacao_orientacao_bancada, processo only.
5. `update_column` to LINK observation levels: set observation_level_id on each grain column (below).
   Re-pass is_partition=True when the grain column is also `ano`.
6. `create_update_cloud_table(table_id, gcp_project_id="basedosdados-dev",
   gcp_dataset_id="br_senado_dados_abertos", gcp_table_id=slug)`.
7. `create_update_coverage(table_id, area_id=br, is_closed=False)` → coverage_id.
   Then `create_update_datetime_range(coverage_id, start_year, end_year=2026, interval=1)` per the
   coverage table below (annual granularity; skip datetime_range for the snapshot dims marked "—").
8. `create_update_update(table_id, entity_id=<day 81f0c890-65a6-48a1-9523-af38d3f4af63>, frequency=1,
   latest="2026-08-06T00:00:00", env="staging")` — table-anchored; latest = today (onboarding date).
9. After all tables exist: `create_update_table(...)` again per table passing
   `raw_data_source_ids=[<the raw source id>]` (re-pass all required fields).
Finally `reorder_tables` in this order: senador, partido, bloco, lideranca, comissao, mesa,
votacao, votacao_parlamentar, votacao_orientacao_bancada, processo.

### OL entities + grain columns to link (step 2 + 5)
| table | OL entities | grain column(s) → OL |
|---|---|---|
| senador | person | id_senador → person |
| partido | party | id_partido → party |
| bloco | caucus | id_bloco → caucus |
| lideranca | person | id_senador → person |
| comissao | committee | id_comissao → committee |
| mesa | person | id_senador → person |
| votacao | year, vote | ano → year, id_votacao → vote |
| votacao_parlamentar | year, vote, person | ano → year, id_votacao → vote, id_senador → person |
| votacao_orientacao_bancada | year, vote, caucus | ano → year, id_votacao_sve → vote, bancada → caucus |
| processo | year, bill | ano → year, id_processo → bill |

### Coverage datetime_range (area br, is_closed=False, interval=1, end_year=2026)
| table | start_year |
|---|---|
| votacao | 1991 |
| votacao_parlamentar | 1991 |
| votacao_orientacao_bancada | 2018 |
| processo | 1946 |
| partido | 1935 |
| senador | 1959 |
| bloco, lideranca, comissao, mesa | — (area coverage only, no datetime_range) |

## Note
All coverages are FREE (is_closed=False) at onboarding. The BD Pro rolling window (pro Coverage,
is_closed=True) is added later, when the recurring pipeline is built (Phase 6), before arming —
only for the high-frequency tables (votacao*, processo).
