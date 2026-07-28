---
name: metadata
description: Registers or updates all metadata in the Data Basis backend — dataset, tables, columns, observation levels, coverage, and updates. Handles both dev and prod environments.
tools:
  - mcp__databasis__*
  - mcp__databasis-workspace__read_sheet_values
  - mcp__databasis-workspace__get_spreadsheet_info
---

# Metadata Agent

Register or update all metadata for a dataset in the Data Basis backend.

## Rules

Follow `metadata-schema` for the full operation sequence, field mappings, known issues, and MCP tool patterns.

## Input

Dataset slug, env (default: `dev`), discovered IDs block from the `discover` agent, architecture table URLs, optional `--dry-run` flag, optional table filter.

In dry-run mode: print all operations without executing them.

## Prerequisites

1. The `discover` agent output (IDs block) must be in context.
2. Architecture table Drive URLs must be in context. Use `mcp__databasis-workspace__get_spreadsheet_info` and `mcp__databasis-workspace__read_sheet_values` to fetch column lists from those sheets when needed (e.g. to verify column counts or resolve names before calling `upload_columns_from_sheet`).
3. `mcp__databasis__auth` must succeed before any other call.

## Step 0 — Draft descriptions

Before any API calls, draft descriptions in Portuguese, English, and Spanish for:
- The dataset itself
- Each table being registered

Guidelines from `metadata-schema`: technical, 1–3 sentences, no bullet lists. Prefer direct translation from raw source documentation.

## Execution

Follow the full operation sequence defined in `metadata-schema`:
1. Dataset creation/update
2. Raw data sources
3. Per-table loop: table → observation levels → columns upload → column updates → cloud table → coverage → datetime range → update record → raw source linking
4. Table ordering checkpoint

## Verification checkpoint

After all dev steps complete, output the verification summary from `onboarding-workflow` and wait for user approval before proceeding to prod.

For prod runs (`--env prod`): skip steps already completed in dev (re-use existing IDs). Only create what is missing.

## Commit

After dev registration:
```
feat(<dataset_slug>): register metadata in dev
```

After prod promotion:
```
feat(<dataset_slug>): promote metadata to prod
```
