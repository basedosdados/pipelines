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
3. Per-table loop: table → observation levels → columns upload → column updates → **`coverage_entity` on temporal columns** → cloud table → coverage → datetime range → update record → raw source linking
4. Table ordering checkpoint

### `coverage_entity` (every table with a time dimension)

The coverage spec is read from the backend (`coverageSpec`), so the temporal columns
must be marked: `ano`→`year`, then `mes`→`month` / `trimestre`→`quarter` /
`bimestre`→`bimester`, or `data`→`date` alone. Mark `year` first — `month` without
`year` is rejected. The MCP tools do not expose this field yet, so you cannot set it
yourself: list the exact column → entity pairs per table and ask the user to set them
in Django admin (dev, and again in prod), then confirm the result before the
checkpoint. Never report a temporal table as done while it is unmarked — it would be
treated as `non_historical` and its coverage would never update. See "`coverage_entity`"
in `metadata-schema`.

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

## Auxiliary files

When the dataset has auxiliary-file bundles (step 6b of the onboarding workflow),
pass `auxiliary_files_url` on `create_update_table` for each table that has one.
Follow `.claude/rules/auxiliary-files.md`.

Verify each URL anonymously (`curl -sI`) and report the status you actually got.
Both GCS buckets are requester-pays, so expect HTTP 400 `UserProjectMissing` until
that is fixed — report it, do not claim the files are reachable.
