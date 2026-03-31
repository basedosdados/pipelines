---
description: Run DBT tests and materialize tables; fix or flag errors
argument-hint: <dataset_slug> [--tables table1,table2] [--target dev|prod]
---

Run DBT tests and materialize tables for a Data Basis dataset.

**Dataset:** $ARGUMENTS

Parse `--target` (default: dev) and `--tables` (default: all tables in the dataset) from arguments.

## Step 1 — Ensure DBT environment

Check for `/tmp/dbt_env/bin/dbt`. If missing, create it:
```bash
python -m venv /tmp/dbt_env
/tmp/dbt_env/bin/pip install dbt-bigquery
```

## Step 2 — Run models

```bash
cd <pipelines_root>
/tmp/dbt_env/bin/dbt run \
  --select <dataset_slug> \
  --profiles-dir ~/.dbt \
  --target <target>
```

Report which models compiled and ran successfully.

## Step 3 — Run tests

```bash
/tmp/dbt_env/bin/dbt test \
  --select <dataset_slug> \
  --profiles-dir ~/.dbt \
  --target <target>
```

## Step 4 — Handle test failures

For each failing test:

1. **`not_null` on a nullable column**: Add the column to a `not_null_proportion_multiple_columns` block instead. Remove the strict `not_null` test.

2. **`relationships` test fails** (FK violation): Report the failing values and ask the user whether to (a) remove the test, (b) relax to a proportion test, or (c) fix the data.

3. **`not_null_proportion_multiple_columns` fails** (below 5%): The data may genuinely be sparse. Report the actual proportion and ask the user to confirm the threshold is acceptable.

4. **Other failures**: Report the full test output and SQL. Propose a fix but ask before implementing.

## Step 5 — Report

Output a summary:
```text
Models:  X passed, Y failed
Tests:   X passed, Y failed
```

List any remaining failures with proposed next steps.
