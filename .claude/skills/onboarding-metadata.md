---
description: Register or update all metadata in the Data Basis backend (dataset, tables, columns, OLs, coverage, updates)
argument-hint: <dataset_slug> [--env dev|prod] [--dry-run] [--tables table1,table2]
---

Spawn the `metadata` agent with: $ARGUMENTS

The agent will register the dataset, raw data sources, and all per-table metadata (observation levels, columns, cloud table, coverage, datetime range, update record) in the specified environment.
