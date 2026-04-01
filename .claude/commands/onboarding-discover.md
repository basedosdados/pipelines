---
description: Resolve all reference IDs from the Data Basis backend needed for metadata creation
argument-hint: <dataset_slug> [--env dev|prod]
---

Resolve all reference IDs from the Data Basis backend for a dataset.

**Dataset:** $ARGUMENTS

Parse `--env` (default: dev) from arguments.

## Step 1 — Fetch all reference IDs

Use the `discover_ids` MCP tool (env from argument):

```text
discover_ids(env=<env>)
```

This returns IDs for: status, bigquery_type, entity, license, availability, organization, theme.

**Never search the web, hardcode IDs, or guess slugs.** All reference IDs (themes,
organizations, licenses, tags, entities, statuses) must come from `discover_ids`
or `lookup_area`. IDs differ between dev and prod environments.

## Step 2 — Fetch dataset state

Use `get_dataset(slug=<dataset_slug>, env=<env>)` to get:
- Dataset ID (None if it doesn't exist yet)
- All existing tables and their IDs
- Existing columns, observation levels, cloud tables, coverages, updates per table

## Step 3 — Fetch authenticated account

Use `get_authenticated_account(env=<env>)` to get the current user's ID. This will be used as `dataCleanedBy` and `publishedBy`.

## Step 4 — Fetch raw data sources

Use `get_raw_data_sources(dataset_slug=<dataset_slug>, env=<env>)` to find any registered raw data source IDs.

## Step 5 — Output structured IDs block

Output a block in this format:

```text
=== DISCOVERED IDs (env=<env>) ===

Reference IDs:
  status.em_processamento: <id>
  status.editando:          <id>
  entity.year:              <id>
  entity.state:             <id>
  entity.municipality:      <id>
  area.br:                  <id>
  bigquery_type.INT64:       <id>
  availability.online:      <id>
  organization.<slug>:      <id>
  theme.<slug>:             <id>
  ...

Dataset:
  id:   <id or "NOT FOUND">
  slug: <slug>

Tables (existing):
  <table_slug>: <id>
    columns:           [<name>: <id>, ...]
    observation_levels: [<entity_slug>: <ol_id>, ...]
    cloud_tables:       [<ct_id>]
    coverages:          [<cov_id> (area=<slug>)]
    updates:            [<upd_id> (entity=<slug>)]

Account:
  id:    <id>
  email: <email>

Raw data sources:
  <name>: <id>
```

Keep this block in the conversation — `databasis-metadata` needs all these IDs.
