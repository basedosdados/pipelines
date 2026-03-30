---
description: Register or update all metadata in the Data Basis backend (dataset, tables, columns, OLs, coverage, updates)
argument-hint: <dataset_slug> [--env dev|prod] [--dry-run] [--tables table1,table2]
---

Register or update all metadata for a dataset in the Data Basis backend.

**Dataset:** $ARGUMENTS

Parse `--env` (default: dev), `--dry-run`, and `--tables` from arguments.

In dry-run mode: print all operations without executing them.

## Prerequisites

1. Run `databasis-discover` first (or have its output in context).
2. Have architecture table URLs in context (from `databasis-architecture`).
3. All IDs from the discover step must be available.

## Dataset-level step (once, before per-table loop)

### 0. Write dataset and table descriptions

Before registering tables, draft descriptions in Portuguese, English, and Spanish for:
1. The **dataset** itself
2. Each **table** being registered

Guidelines:
- Be cogent and technical: state what the data contains, the source system, the entity level, and the time period.
- Prefer direct copy-paste from raw data source documentation when it captures the content well — then translate.
- Format: 1–3 sentences per entity. No bullet lists.
- Pass descriptions to `create_update_dataset` and `create_update_table` via `description_pt/en/es`.

Example dataset description (PT): "Dados contábeis e fiscais do setor público brasileiro publicados pelo SICONFI (Sistema de Informações Contábeis e Fiscais do Setor Público Brasileiro) do Ministério da Fazenda. Abrange receitas, despesas, balanços patrimoniais e variações patrimoniais de municípios, estados e governo federal a partir de 1989."

---

## Per-table steps

For each table (in order):

### 1. Create/update table record

Call `create_update_table` with:
- `slug`: table slug
- `name_pt / name_en / name_es`: from architecture or context
- `description_pt / en / es`: from step 0
- `dataset_id`: from discover step (dataset must exist first — create if needed via `create_update_dataset`)
- `status_id`: use `status.published` from discovered IDs
- `published_by_ids`: skip — see TODO below
- `data_cleaned_by_ids`: skip — see TODO below
- `id`: existing table ID if updating

Do **not** pass `raw_data_source_ids` here — see step 8.

> **TODO (M2M fields broken in `CreateUpdateTable`):** `raw_data_source_ids`, `published_by_ids`, and `data_cleaned_by_ids` are Django `ManyToManyField`s on the `Table` model. The auto-generated `CreateUpdateMutation` in `backend/custom/graphql_auto.py` delegates to `DjangoModelFormMutation`, which calls `form.save()` (via `get_form_kwargs` and `mutate_and_get_payload`) but the `perform_mutate` classmethod does **not** call `form.save_m2m()` afterwards. M2M assignments are therefore silently dropped on every call. `raw_data_source_ids` in step 8 may appear to work if the upstream `graphene-django` version happens to call `_save_m2m` internally, but `published_by_ids` and `data_cleaned_by_ids` reliably fail. The backend fix is to override `perform_mutate` in `CreateUpdateMutation` (in `graphql_auto.py`) to call `form.save_m2m()` after `form.save(commit=True)`. Until then, skip these two fields.

**Name translation:** If only Portuguese names are available, translate to English and Spanish:
- Translate accurately using domain knowledge of Brazilian public finance / the relevant domain
- Use the same terminology conventions as existing tables in the dataset

### 2. Create/update observation levels

For each entity in the table's observation level list (from architecture design or context):
- Call `create_update_observation_level`
- Look up entity_id from discover IDs using the entity slug
- Pass existing OL id if updating

Track returned OL IDs — needed for column updates.

### 3. Upload columns from architecture table

Call `upload_columns_from_sheet` with:
- `table_id`: from step 1
- `architecture_url`: Google Sheets URL for this table's architecture
- `observation_levels`: JSON string mapping column name → bare OL ID from step 2
  (e.g. `{"ano": "<year_ol_id>", "sigla_uf": "<state_ol_id>", "estagio": "<financing_phase_ol_id>"}`)
- `env`: current env

This creates all columns with descriptions, BQ types, OL links, status, and directory primary keys in one call. Note: `directoryPrimaryKey` requires the referenced directory dataset to exist in the target environment — it will be silently skipped in dev if the directory dataset is absent.

### 4. Update individual columns

For each column in the architecture table, call `update_column` to set fields not handled by the sheet upload:

- `column_id`: from the `upload_columns_from_sheet` response
- `column_name`: column name
- `table_id`: table ID
- `is_partition`: True for partition columns (e.g. `ano`, `sigla_uf`)
- `is_primary_key`: True for primary key columns from architecture
- `description_en` / `description_es`: translate from Portuguese if only PT available

**Do not re-set** `observation_level_id`, `description_pt`, `measurement_unit`, `has_sensitive_data`, `covered_by_dictionary`, or `temporal_coverage` — those were already set in step 3.

**Translation rule:** Auto-translate Portuguese descriptions to English and Spanish using domain knowledge of Brazilian public finance. Apply consistent terminology.

### 5. Create/update cloud table

Call `create_update_cloud_table` with:
- `table_id`: table ID
- `gcp_project_id`: `basedosdados-dev` (dev) or `basedosdados` (prod)
- `gcp_dataset_id`: dataset slug (e.g. `br_me_siconfi`)
- `gcp_table_id`: table slug
- `id`: existing cloud table ID if updating

### 6. Create/update coverage and datetime range

Call `create_update_coverage` with table_id and area_id for "br".

Then call `create_update_datetime_range` with:
- `coverage_id`: from coverage step
- `start_year`: from context
- `end_year`: from context
- `interval`: 1 (annual) or appropriate value
- `is_closed`: False (unless the series has ended)
- `id`: existing DTR ID if updating

Wrap in try/except — log failures but continue to next step.

### 7. Create/update update record

Call `create_update_update` with:
- `table_id`: table ID
- `entity_id`: year entity ID
- `frequency`: 1
- `lag`: 1
- `latest`: current datetime as ISO string (use Python `datetime.now().isoformat()`)
- `id`: existing update ID if updating

Wrap in try/except — log failures but continue.

### 8. Link raw data sources (deferred update)

Call `create_update_table` again with the same `id` and `slug`, passing only:
- `raw_data_source_ids`: relevant sources from `get_raw_data_sources` (see step 1 for selection guidance)

All other fields must be re-passed as well (the API requires them). This deferred call ensures the table record is fully persisted before the relationship is written.

## Summary output

After processing all tables, output:

```
=== METADATA REGISTRATION COMPLETE (env=<env>) ===

Dataset: <slug> (id=<id>)

Tables:
  ✓ <table_slug> — table, <N> OLs, <N> columns, cloud table, coverage, update
  ✗ <table_slug> — FAILED: <error>

Next step: verify at https://development.basedosdados.org/dataset/<slug>
           then run `/databasis-metadata <slug> --env prod` to promote to prod.
```
