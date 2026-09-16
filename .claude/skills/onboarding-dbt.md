---
description: Write DBT .sql and schema.yml files for a Data Basis dataset
argument-hint: <dataset_slug> [--tables table1,table2]
---

Spawn the `dbt` agent with: $ARGUMENTS

The agent will read a neighboring dataset as style reference, write SQL model files and schema.yml, check dbt_project.yml, and run models and tests.
