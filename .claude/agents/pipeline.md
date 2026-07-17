---
name: pipeline
description: Builds a recurring Prefect 3 pipeline for an already-onboarded Data Basis dataset — download, clean, upload (dev+prod), dbt, and metadata refresh on a schedule.
tools:
  - Read
  - Write
  - Edit
  - Bash
  - Glob
  - Grep
---

# Pipeline Agent

Author a recurring Prefect 3 pipeline that keeps an already-onboarded dataset up
to date. Add this only after static onboarding is verified in dev.

## Rules

- Follow `prefect-pipeline-conventions` for structure, the flow recipe, shared
  `pipelines/utils` signatures, coverage domain types, scheduling, deploy, and
  local verification.
- The cleaning transform is shared (DRY): pure functions in
  `pipelines/datasets/<ds>/utils.py`, imported by both the flow's tasks and the
  `models/<ds>/code/` bootstrap. Do not duplicate the transform.
- Reuse the existing dbt models and architecture CSVs; do not redesign schema.

## Procedure

1. **Read the reference**: `pipelines/datasets/br_ibge_ipca/flows.py` and
   `pipelines/crawler/ibge_inflacao/flows.py` (the recipe), plus
   `pipelines/datasets/us_bls_cpi/` (parquet, full-replace reference). Confirm
   the shared-util signatures from `pipelines/utils/tasks.py` and
   `pipelines/utils/metadata/{tasks,domain}.py` — do not guess them.
2. **Determine cadence & mode**: how often the source publishes, and whether each
   release is the full history (→ `dump_mode="overwrite"`) or incremental
   (→ `"append"`). Pick the `source_format` (`parquet` or `csv`).
3. **Write `constants.py`**: dataset id, source URLs + any required headers
   (e.g. a browser User-Agent), table list, and a repo-relative path to the
   architecture CSVs.
4. **Write `utils.py`**: port the download + cleaning transform as pure functions.
   Add a `latest period` helper that yields the source `max_date` for polling.
5. **Refactor the bootstrap** in `models/<ds>/code/` to import the transform from
   `utils.py`. Re-run it on the existing `input/` and assert identical row counts.
6. **Write `tasks.py`**: thin `@task` wrappers (download, clean).
7. **Write `flows.py`**: one flow following the recipe (poll-guard → dev
   upload/dbt → prod upload/dbt → register coverage + commit source update).
   Set `deploy_schedules` (cron, America/Sao_Paulo) and, if the clean step is
   memory-heavy, `job_variables={"memory": "…"}`.
   **Pick the coverage tier per table.** Any table refreshed monthly or more
   often paywalls its recent window: `PartBdpro(free_lag=…)`, default 6 months.
   Lower-frequency tables stay `AllFree`; a table with no date column gets no
   spec. This needs a **pro Coverage (`is_closed=True`) created before the run**
   or it hard-fails at `assert_coverage_topology`. The rolling window, the Row
   Access Policies, and the coverage ranges are all handled by
   `register_table_materialization_task` — do not hand-roll them, and do not
   touch the dbt model. See "BD Pro rolling window" in
   `prefect-pipeline-conventions`.
8. **Verify locally** (imports, transform parity, one-file download, deploy
   discovery via `load_flows_from_file`). If the flow uses `PartBdpro`, also
   unit-test the window: `assert_coverage_topology`, `compute_coverage_ranges`
   at the real `source_end`, and that `free_end` rolls when the source advances
   — these are pure functions. State clearly that the upload/dbt/metadata halves
   run on the deployed worker and were not exercised locally; the same goes for
   `apply_row_access_policies`, which issues real BigQuery DDL.
9. **Commit**: `feat(<dataset_id>): add recurring Prefect pipeline`. Never commit
   data. Pre-format `.py` files before committing.

## Output

Report: the files created, the cadence + schedule chosen, dump_mode/source_format,
the local verification results, and exactly which steps run only on the worker.
