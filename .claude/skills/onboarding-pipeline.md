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
duplication) and sets an inline schedule with a source-poll guard.

**It is not done until the flow has actually run.** Local checks (imports,
transform parity, download, deploy discovery) fail fast but reach none of the
parts that break. The agent adds the **`deploy-flow`** label — without it the
staging deploy is `skipped` and nothing deploys, silently — and triggers a dev-pool
run with prod off (`materialize_to_prod=False, update_metadata=False,
force_run=True`; the defaults would write prod and apply the paywall). Done means
`dbt run OK` + `dbt test OK` per table, read from the logs — a green state alone
proves nothing, since the poll guard returns early and still reports `COMPLETED`.

The prod upload only runs once the schedule is armed (a manual tick in Django
admin — merging does **not** arm it), and the Row Access Policies only if a table
is `part_bdpro` — an all-free pipeline never issues them. The agent says plainly
which of those remain unexercised.

It also checks the **BD Pro rolling window**, which lives in the backend, not in
the pipeline code: any table refreshing monthly or more often has a free and a pro
Coverage (`is_closed=True`) and is read as `part_bdpro` (6-month lag), so its
recent window is pro-only and older data stays free; lower-frequency tables in the
same dataset have only the free Coverage (`all_free`). The window rolls itself —
`register_table_materialization_task` reads the spec with `coverage_from_backend`,
rewrites both coverage ranges and re-issues the BigQuery Row Access Policies on
every run, and the dbt model is untouched. The temporal columns must carry
`coverage_entity`, or the table is read as `non_historical` and nothing moves.
