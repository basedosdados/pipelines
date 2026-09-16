---
description: Download raw data files for a Data Basis dataset from the source URL(s)
argument-hint: <dataset_slug> [source_url] [output_path]
---

Download raw data files for a Data Basis dataset.

**Dataset / sources:** $ARGUMENTS

Spawn the `raw-data-downloader` agent with the following context:
- Dataset slug and source URL(s) from arguments or the context block in conversation
- Target directory: `<dataset_root>/input/`

The agent will inspect the source, assess volume, write a download script, run it, and verify all files downloaded successfully.
