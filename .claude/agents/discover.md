---
name: discover
description: Resolves all reference IDs from the Data Basis backend needed for metadata creation — organizations, themes, tags, entities, statuses, and existing dataset/table IDs.
tools:
  - mcp__databasis__*
---

# Discover Agent

Resolve all reference IDs from the Data Basis backend for a dataset.

## Rules

Follow `metadata-schema` for the MCP tool sequence and ID resolution patterns.

## Input

Dataset slug, env (default: `dev`).

## Step 0 — Clarify dataset slug vs GCP ID

The **dataset slug** (e.g. `cnuc`) is the short identifier used in the backend.
The **GCP dataset ID** (e.g. `br_mma_cnuc`) is used for BigQuery and cloud table fields.

Always use the bare slug when calling backend MCP tools.

## Step 1 — Fetch all reference IDs

```
mcp__databasis__discover_ids(env=<env>)
```

**Never hardcode IDs or guess slugs.** IDs differ between dev and prod.

## Step 2 — Fetch dataset state

```
mcp__databasis__get_dataset(slug=<dataset_slug>, env=<env>)
```

Returns: dataset ID (or null), all existing tables and their IDs, columns, observation levels, cloud tables, coverages, and updates.

## Step 3 — Fetch authenticated account

```
mcp__databasis__get_authenticated_account(env=<env>)
```

This ID is used as `dataCleanedBy` and `publishedBy`.

## Step 4 — Fetch raw data sources

```
mcp__databasis__get_raw_data_sources(dataset_slug=<dataset_slug>, env=<env>)
```

## Step 5 — Output structured IDs block

```text
=== DISCOVERED IDs (env=<env>) ===

Reference IDs:
  status.em_processamento: <id>
  status.editando:          <id>
  status.published:         <id>
  entity.year:              <id>
  entity.state:             <id>
  entity.municipality:      <id>
  area.br:                  <id>
  bigquery_type.INT64:       <id>
  availability.online:      <id>
  organization.<slug>:      <id>
  theme.<slug>:             <id>
  tag.<slug>:               <id>
  language.<slug>:          <id>
  ...

Dataset:
  id:   <id or "NOT FOUND">
  slug: <slug>

Tables (existing):
  <table_slug>: <id>
    columns:            [<name>: <id>, ...]
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

Keep this block in the conversation — the `metadata` agent needs all these IDs.
