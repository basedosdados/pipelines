---
description: Build a recurring Prefect 3 pipeline for an already-onboarded Data Basis dataset
argument-hint: <gcp_dataset_id> [--cadence monthly|daily|annual]
---

Spawn the `pipeline` agent with: $ARGUMENTS

Use this **after** a dataset's static onboarding is verified in dev, when the
source publishes on a cadence and should refresh automatically. The agent reads
`pipelines/datasets/br_ibge_ipca` and `pipelines/datasets/us_bls_cpi` as
references, confirms the shared `pipelines/utils` signatures, and writes
`pipelines/datasets/<dataset_id>/{constants,utils,tasks,flows}.py` following
`prefect-pipeline-conventions`.

It shares the cleaning transform with `models/<dataset_id>/code/` (no
duplication), sets an inline schedule with a source-poll guard, and verifies
locally what can be verified (imports, transform parity, download, deploy
discovery). The upload/dbt/metadata halves run on the deployed Prefect worker
(dev pool on PR **only when the PR carries the `deploy-flow` label**; prod pool
on merge with schedule), so they are not exercised locally — the agent says so
explicitly.

It also sets the **BD Pro rolling window**: any table refreshing monthly or more
often gets `PartBdpro` (default `free_lag=6 months`) so its recent window is
pro-only and older data stays free; lower-frequency tables in the same dataset
stay `AllFree`. The window rolls itself — `register_table_materialization_task`
rewrites both coverage ranges and re-issues the BigQuery Row Access Policies on
every run, and the dbt model is untouched. The pro Coverage (`is_closed=True`)
must exist first or the run hard-fails at `assert_coverage_topology`.
