---
description: Run DBT tests and validate data quality; fix or flag errors
argument-hint: <dataset_slug> [--tables table1,table2] [--target dev|prod]
---

Spawn the `validator` agent with: $ARGUMENTS

The agent will run dbt models and tests, handle each failure category (null, referential integrity, uniqueness), run data quality spot-checks, and report results.
