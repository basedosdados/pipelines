---
description: Resolve all reference IDs from the Data Basis backend needed for metadata creation
argument-hint: <dataset_slug> [--env dev|prod]
---

Spawn the `discover` agent with: $ARGUMENTS

The agent will fetch all reference IDs, existing dataset/table state, the authenticated account ID, and raw data source IDs — then output a structured IDs block for the metadata agent.
