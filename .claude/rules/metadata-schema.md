# Metadata Schema

Reference for all agents that register or update metadata in the Data Basis backend.

## MCP server

All backend operations use the `mcp__databasis__*` MCP tools. Never write raw HTTP requests. Always call `mcp__databasis__auth` first (or rely on auto-auth) before any other MCP tool.

## Environment

| env | Backend URL | GCP project |
|-----|-------------|-------------|
| dev | `development.backend.basedosdados.org` | `basedosdados-dev` |
| prod | `backend.basedosdados.org` | `basedosdados` |

Default: `dev`. Only switch to `prod` after explicit user approval.

## ID resolution (always run first)

Before registering any metadata, resolve all reference IDs with:

```
mcp__databasis__discover_ids(env=<env>)
```

This returns IDs for: `status`, `bigquery_type`, `entity`, `license`, `availability`, `organization`, `theme`, `tag`, `entity_category`, `language`, `measurement_unit_category`.

**Never hardcode IDs or guess slugs.** IDs differ between dev and prod.

For individual lookups: `mcp__databasis__lookup_id(env=<env>, slug=<slug>, type=<type>)`

## Operation sequence

### Once per dataset

1. `get_dataset(slug, env)` — check if dataset exists; get existing IDs
2. `get_authenticated_account(env)` — get current user ID for `publishedBy`/`dataCleanedBy`
3. `get_raw_data_sources(dataset_slug, env)` — check for existing raw source IDs
4. `create_update_dataset(...)` — create or update dataset record
5. `create_update_raw_data_source(...)` — one call per raw source

### Per table (repeat for each)

6. `create_update_table(...)` — create table record (without `raw_data_source_ids`)
7. `create_update_observation_level(...)` — one call per entity level
8. `reorder_observation_levels(...)` — if human specifies order
9. `upload_columns_from_sheet(...)` — bulk upload columns from architecture Google Sheet
10. `update_column(...)` — per-column follow-up for `is_partition`, `is_primary_key`, EN/ES descriptions
11. `create_update_cloud_table(...)` — link to BigQuery table
12. `create_update_coverage(...)` — coverage for area "br"
13. `create_update_datetime_range(...)` — temporal range
14. `create_update_update(...)` — update frequency/lag record
15. `create_update_table(...)` again — link `raw_data_source_ids` (deferred update)

### After all tables

16. `reorder_tables(...)` — if human specifies order

## `create_update_dataset` fields

| Field | Type | Notes |
|-------|------|-------|
| `slug` | string | Bare slug (e.g. `cnuc`, NOT `br_mma_cnuc`) |
| `name_pt/en/es` | string | Dataset display names (all 3 languages) |
| `description_pt/en/es` | string | 1–3 sentences, technical, no bullet lists |
| `organization_ids` | list | From `discover_ids` using org slug(s) |
| `theme_ids` | list | From `discover_ids` |
| `tag_ids` | list | From `discover_ids`; empty list if none |
| `status_id` | string | Use `status.published` |
| `id` | string | Pass when updating an existing record |

## `create_update_table` fields

| Field | Type | Notes |
|-------|------|-------|
| `slug` | string | Table slug |
| `name_pt/en/es` | string | All 3 languages |
| `description_pt/en/es` | string | 1–3 sentences |
| `dataset_id` | string | From dataset step |
| `status_id` | string | `status.published` |
| `published_by_ids` | list | Authenticated account ID |
| `data_cleaned_by_ids` | list | Authenticated account ID |
| `id` | string | Pass when updating |

Do **not** pass `raw_data_source_ids` in the initial creation — link them in the deferred update (step 15).

## `upload_columns_from_sheet` fields

| Field | Notes |
|-------|-------|
| `table_id` | Table ID from step 6 |
| `architecture_url` | Google Sheets URL for this table's architecture |
| `observation_levels` | JSON string: `{"col_name": "<ol_id>", ...}` |
| `env` | Current env |

Note: `directoryPrimaryKey` is silently skipped in dev if the directory dataset is absent. This is expected behavior.

## `update_column` fields (per-column follow-up)

Only set these fields — do NOT re-set fields already handled by `upload_columns_from_sheet`:

| Field | Notes |
|-------|-------|
| `column_id` | From upload response |
| `column_name` | Column name |
| `table_id` | Table ID |
| `is_partition` | `True` for partition columns |
| `is_primary_key` | `True` for primary key columns |
| `description_en` | Translated from Portuguese |
| `description_es` | Translated from Portuguese |

## `create_update_cloud_table` fields

| Field | Notes |
|-------|-------|
| `table_id` | Table ID |
| `gcp_project_id` | `basedosdados-dev` (dev) or `basedosdados` (prod) |
| `gcp_dataset_id` | Full GCP dataset ID (e.g. `br_mma_cnuc`) |
| `gcp_table_id` | Table slug |
| `id` | Pass when updating |

## `create_update_datetime_range` fields

| Field | Notes |
|-------|-------|
| `coverage_id` | From coverage step |
| `start_year` | Integer |
| `end_year` | Integer or null if ongoing |
| `interval` | 1 for annual |
| `is_closed` | `False` unless series has ended |

## `create_update_update` — last_updated semantics

The `last_updated` field in the update record represents **when the table was last updated at Data Basis** — not the max date in the raw data. Always set it to **today's date** (the date the onboarding or update operation is being performed). Never derive it from the data's temporal coverage or the raw source's extraction date.

## Known issues

**M2M fields (organizations, themes, tags, raw_data_source_ids):** These are Django ManyToManyFields. Pass them and verify with `get_dataset` after saving. If they appear empty despite being passed, it is a backend deployment issue — note and continue; do not retry indefinitely.

**Deferred raw_data_source linking:** Always link raw data sources in a second `create_update_table` call (step 15), after all raw sources exist. Pass ALL required fields again — the API does not support partial updates.

## Description writing guidelines

- Be technical: state what the data contains, the source system, the entity level, and the time period.
- Prefer direct translation from raw data source documentation.
- Format: 1–3 sentences per entity. No bullet lists.
- Translate Portuguese descriptions to English and Spanish using domain knowledge of Brazilian public administration and statistics.

## Verification after dev registration

After completing all dev steps, output:

```text
=== METADATA REGISTRATION COMPLETE (env=dev) ===
Dataset: <slug> (id=<id>)
Tables: ✓ <table_slug> — N OLs, N columns, cloud table, coverage, update
Next: verify at https://development.basedosdados.org/dataset/<slug>
```
