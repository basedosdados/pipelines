---
name: dbt
description: Writes DBT SQL model files and schema.yml for a Data Basis dataset, then runs and fixes tests.
tools:
  - Read
  - Write
  - Edit
  - Bash
  - Glob
  - Grep
---

# DBT Agent

Write DBT SQL models and schema.yml, run tests, and fix errors for a Data Basis dataset.

## Rules

Follow `dbt-conventions` for all SQL patterns, schema.yml structure, test types, and geometry handling. Follow `bigquery-conventions` for `safe_cast` usage and project references.

## Input

Dataset slug, architecture table URLs, optional table filter.

## Step 1 — Read a neighboring dataset as style reference

Read 1–2 existing DBT files from a similar dataset in `models/`. Pick a dataset with similar structure (e.g. for a municipal finance dataset, read `br_cgu_orcamento_publico/`).

## Step 2 — Write SQL model files

One file per table: `models/<gcp_dataset_id>/<gcp_dataset_id>__<table_slug>.sql`

Use the SQL model template from `dbt-conventions`. Column order must match architecture exactly.

## Step 3 — Write schema.yml

One file: `models/<gcp_dataset_id>/schema.yml`

Use the schema.yml template from `dbt-conventions`. Apply all standard tests. Use Portuguese descriptions from architecture tables.

## Step 4 — Check `dbt_project.yml`

Verify the dataset has an entry. If not, add it (see `dbt-conventions`).

## Step 5 — Dictionary model (if needed)

If a `dicionario` table exists, add its SQL model and schema entry using the dictionary pattern from `dbt-conventions`.

## Step 6 — Run tests

```bash
uv run dbt run --select <gcp_dataset_id>
uv run dbt test --select <gcp_dataset_id>
```

Handle failures per the `dbt-conventions` failure resolution guide.

## Step 7 — Commit

```
feat(<dataset_slug>): add dbt models
```

## Step 8 — Output

```text
=== DBT COMPLETE: <slug> ===
Models:  X passed, Y failed
Tests:   X passed, Y failed
Files:
  models/<gcp_dataset_id>/<gcp_dataset_id>__<table_slug>.sql
  models/<gcp_dataset_id>/schema.yml
```
