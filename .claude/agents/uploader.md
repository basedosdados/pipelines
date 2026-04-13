---
name: uploader
description: Uploads cleaned Parquet files to BigQuery via the basedosdados Python package. Handles dev and prod environments.
tools:
  - Bash
  - Read
  - Write
  - Glob
---

# Uploader Agent

Upload cleaned Parquet files to BigQuery for a Data Basis dataset.

## Rules

Follow `bigquery-conventions` for project mapping, upload sequence, and prerequisites.

## Input

Dataset slug, output path (`<dataset_root>/output/`), env (default: `dev`), optional table filter.

## Step 1 — Prerequisites check

1. Verify `~/.basedosdados/config.toml` exists. If missing, fail with:
   ```
   Missing ~/.basedosdados/config.toml — run `basedosdados config init` first.
   ```
2. Verify the output Parquet files exist at the expected path.
3. Verify `GOOGLE_APPLICATION_CREDENTIALS` is set.

## Step 2 — Write upload script

Write a Python script at `<dataset_root>/code/upload.py` (or reuse if it exists):

```python
import basedosdados as bd
import google.cloud.storage as gcs
from pathlib import Path
import argparse

BILLING_PROJECT = "basedosdados-dev"  # or "basedosdados" for prod

# Monkey-patch for requester-pays bucket
_orig_bucket = gcs.Client.bucket
def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)
gcs.Client.bucket = _patched_bucket
```

Key steps per table:
1. Instantiate `bd.Table(table_id=<table_slug>, dataset_id=<gcp_dataset_id>)`
2. Delete stale GCS staging prefix before upload
3. `table.create(if_exists="replace")`
4. `table.upload(path, if_exists="replace")`
5. Report success with row count

## Step 3 — Run and report

Run the upload script. Report success/failure per table. If any table fails, stop and report the error — do not continue to subsequent tables without user confirmation.

## Step 4 — Output

```text
=== UPLOAD COMPLETE: <slug> (env=<env>) ===
Tables uploaded:
  ✓ <table_slug>: <N> rows
  ✗ <table_slug>: FAILED — <error>
```
