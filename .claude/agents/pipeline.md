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
   — these are pure functions.
9. **Then actually run it — this is the gate, not step 8.** Local checks pass
   while the interesting bugs sit in the parts they cannot reach; on
   `us_bls_cpi` three separate ones survived a clean local pass. Ask the user to
   add the **`deploy-flow`** label (no label → the staging deploy is `skipped`
   and nothing is deployed, silently), confirm `cd-prefect3 (staging)` logs
   `registrado`, then trigger the deployment with **prod off**:
   `{"materialize_to_prod": False, "update_metadata": False, "force_run": True}`.
   Defaults are `True/True/False`, so a run triggered with `{}` writes prod data
   and metadata and applies the paywall — the metadata tasks are pinned
   `env="prod"` even from the dev pool. Done = all tasks `COMPLETED` **and**
   `dbt run OK` + `dbt test OK` in the logs for every table. Inspect with
   `mcp__databasis__list_flow_runs` / `get_flow_run_logs`.
   **Green ≠ ingested**: the poll guard returns early and still reports
   `COMPLETED` — check the logs, not the state.
   Say plainly what remains unexercised: the prod upload and, for `part_bdpro`,
   `apply_row_access_policies` only run once armed.
10. **Fill in the Update/Poll records.** A recurring dataset needs all three, and
   the flow only writes them on a run with `update_metadata=True` — so create
   the missing ones by hand rather than assuming the first run will:
   - `Update` on the **table** — when we last refreshed (wall clock), plus
     `frequency`/`lag`.
   - `Update` on the **raw data source** — the source's **max coverage date**
     (`create_update_update(raw_data_source_id=…)`). Not today's date.
   - `Poll` on the raw data source — when we last looked (written by the poll
     task).
   Verify all three exist afterwards; see "Update and Poll records" in
   `prefect-pipeline-conventions`.
11. **Commit**: `feat(<dataset_id>): add recurring Prefect pipeline`. Never commit
   data. Pre-format `.py` files before committing. Keep framework changes
   (`pipelines/utils`, `AGENTS.md`) out of the dataset PR — they change behaviour
   for every pipeline and deserve their own review.

## Output

Report: the files created, the cadence + schedule chosen, dump_mode/source_format,
the local verification results, and exactly which steps run only on the worker.
