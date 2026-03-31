---
description: Upload cleaned parquet files to BigQuery via the basedosdados package
argument-hint: <dataset_slug> [--tables table1,table2] [--env dev|prod]
---

Upload cleaned parquet files to BigQuery for a Data Basis dataset.

**Args:** $ARGUMENTS

Parse `--env` (default: dev) and `--tables` (default: all tables) from arguments.

## Prerequisites

1. Verify `~/.basedosdados/config.toml` exists. If missing, fail with:
   ```
   Missing ~/.basedosdados/config.toml — run `basedosdados config init` first.
   ```
2. Verify the output parquet files exist at the expected path.

## GCP project mapping

| env | GCP project |
|-----|-------------|
| dev | `basedosdados-dev` |
| prod | `basedosdados` |

## Upload script

Write a Python script at `<dataset_root>/code/upload_to_db.py`:

```python
import basedosdados as bd
import google.cloud.storage as gcs
from pathlib import Path
import argparse

# Monkey-patch for requester-pays bucket
_orig_bucket = gcs.Client.bucket
def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)
gcs.Client.bucket = _patched_bucket
```

Key steps per table:
1. Instantiate `bd.Table(table_id=<table_slug>, dataset_id=<dataset_slug>)`
2. Delete stale GCS staging prefix before upload (avoids BQ partition key conflicts)
3. Upload: `table.create(if_exists="replace")` then `table.upload(path, if_exists="replace")`
4. Report success with row count

## Run

Run the upload script and report success/failure per table. If any table fails, report the error and stop — do not continue to subsequent tables without user confirmation.
