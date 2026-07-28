---
description: Write and run data cleaning code to produce partitioned parquet output
argument-hint: <dataset_slug> <raw_data_path> [output_path]
---

Spawn the `cleaner` agent with: $ARGUMENTS

The agent will read architecture tables from Drive, inspect the raw files, write Python cleaning code, validate a subset, and scale to full data after user confirmation.
