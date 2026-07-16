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
8. **Verify locally** (imports, transform parity, one-file download, deploy
   discovery via `load_flows_from_file`). State clearly that the
   upload/dbt/metadata halves run on the deployed worker and were not exercised
   locally.
9. **Commit**: `feat(<dataset_id>): add recurring Prefect pipeline`. Never commit
   data. Pre-format `.py` files before committing.

## Output

Report: the files created, the cadence + schedule chosen, dump_mode/source_format,
the local verification results, and exactly which steps run only on the worker.
