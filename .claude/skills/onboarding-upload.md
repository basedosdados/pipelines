---
description: Upload cleaned parquet files to BigQuery via the basedosdados package
argument-hint: <dataset_slug> [--tables table1,table2] [--env dev|prod]
---

Spawn the `uploader` agent with: $ARGUMENTS

The agent will verify prerequisites, write an upload script, run it, and report success or failure per table.
