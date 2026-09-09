"""Flow for us_cms_hcris — Prefect 3.

CMS reissues **every** HCRIS fiscal-year archive each quarter, 1996's included:
all 33 carried the same ``Last-Modified`` when measured. Reports keep arriving,
being settled, reopened and amended for years after a fiscal year ends, so a
refresh is a full rebuild of the whole history, not an append of a new period.

That shapes three decisions:

* **The poll compares publication timestamps, not coverage dates.** HCRIS's
  maximum coverage date advances about once a year; polling on it would report
  "nothing new" for three quarters out of four while CMS was adding thousands
  of reports to years already published. See ``utils.source_last_modified``.
* **Staging is cleared before each upload**, rather than using
  ``dump_mode="overwrite"``. Overwrite calls ``tb.delete(mode="all")``, which
  drops the *production* table even from the dev half of a run.
* **Every model is built, then every model is tested** — never run/test per
  table. ``hospital_financial`` reads ``report_value`` and ``report``, and the
  dictionary coverage tests read ``dicionario``, so a per-table interleave
  would test a model before its sibling exists.

Deploy: ``.github/scripts/deploy_flows.py`` discovers ``us_cms_hcris_flow``; the
dev pool strips the schedule, the prod pool activates it (paused until armed).
"""

import shutil
import tempfile
from pathlib import Path

from prefect import flow

from pipelines.datasets.us_cms_hcris.constants import constants
from pipelines.datasets.us_cms_hcris.tasks import (
    clear_staging_task,
    download_and_clean_task,
    list_extracts_task,
    source_max_date_task,
)
from pipelines.utils.metadata.domain import AllFree, DateFormat, YearOnly
from pipelines.utils.metadata.tasks import (
    commit_source_update_task,
    poll_source_for_update_task,
    register_table_materialization_task,
)
from pipelines.utils.tasks import (
    rename_flow_run_dataset_table,
    run_dbt,
    upload_to_gcs,
)

DATASET_ID = constants.DATASET_ID.value
STAGED_TABLES = constants.STAGED_TABLES.value

# The table the poll and the source update are anchored to. One table, and one
# raw data source, because `client._raw_source_id` raises when a table has two
# or more sources -- the poll would fail before doing anything.
POLL_TABLE = "report"

# HCRIS refreshes quarterly, which is less often than monthly, so no table
# takes the BD Pro rolling window: everything is free. `dicionario` has no date
# column and takes no spec at all.
COVERAGE = {
    table: AllFree(
        date_column=YearOnly(col="year"), date_format=DateFormat.YEAR
    )
    for table in ("report", "report_value", "hospital_financial")
}


@flow(name="us_cms_hcris", log_prints=True)
def us_cms_hcris_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
    last_extract_year: int = 2035,
) -> None:
    """Refresh the HCRIS hospital cost reports end to end.

    Args:
        materialize_to_prod: Upload to the production bucket and build the
            production models. False leaves the run in dev.
        update_metadata: Write the coverage, table update and source update
            records. These tasks are pinned to the production backend, so a dev
            smoke run should pass False.
        force_run: Skip the source-freshness guard and rebuild regardless.
        last_extract_year: Highest federal fiscal year to probe for. CMS adds
            one each October; the probe stops at the first year that 404s.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id=POLL_TABLE
    )

    extracts = list_extracts_task(last_extract_year)
    source_max_date = source_max_date_task(extracts)

    has_new = poll_source_for_update_task(
        dataset_id=DATASET_ID,
        table_id=POLL_TABLE,
        source_max_date=source_max_date,
        env="prod",
        date_format="%Y-%m-%d",
    )
    if not has_new and not force_run:
        print("source has not been republished since the last refresh")
        return

    work_dir = tempfile.mkdtemp(prefix="us_cms_hcris_")
    try:
        input_dir = str(Path(work_dir) / "input")
        output_dir = str(Path(work_dir) / "output")
        download_and_clean_task(extracts, input_dir, output_dir)

        for bucket, target, enabled in (
            ("basedosdados-dev", "dev", True),
            ("basedosdados", "prod", materialize_to_prod),
        ):
            if not enabled:
                continue
            for table in STAGED_TABLES:
                clear_staging_task(bucket, table)
                upload_to_gcs(
                    data_path=str(Path(output_dir) / table),
                    dataset_id=DATASET_ID,
                    table_id=table,
                    bucket_name=bucket,
                    dump_mode="append",
                    source_format="parquet",
                )
            # One selector for the whole dataset: dbt orders the models by
            # their refs, so hospital_financial and dicionario are built after
            # the two they read.
            run_dbt(dataset_id=DATASET_ID, dbt_command="run", target=target)
            run_dbt(dataset_id=DATASET_ID, dbt_command="test", target=target)

        if not materialize_to_prod or not update_metadata:
            return

        for table, coverage in COVERAGE.items():
            register_table_materialization_task(
                dataset_id=DATASET_ID,
                table_id=table,
                coverage=coverage,
                env="prod",
                bq_project="basedosdados",
            )
        # Last, and only once production succeeded: this is what makes the next
        # run's poll a no-op.
        commit_source_update_task(
            dataset_id=DATASET_ID,
            table_id=POLL_TABLE,
            source_max_date=source_max_date,
            env="prod",
            date_format="%Y-%m-%d",
        )
    finally:
        # Covers the early returns and any exception. A k8s pod is fresh each
        # run, but a process worker reuses its filesystem and this is ~7 GB.
        shutil.rmtree(work_dir, ignore_errors=True)


# CMS republishes on no announced calendar -- the archives observed in 2026 were
# stamped 14 July. Poll four times a month at a minute nothing else uses; the
# freshness guard makes every poll a no-op until an archive is actually
# reissued, and a poll is 66 HEAD requests.
# pyrefly: ignore [missing-attribute]
us_cms_hcris_flow.deploy_schedules = [
    {"cron": "29 15 10,15,20,25 * *", "timezone": "America/Sao_Paulo"}
]
# `memory` alone is NOT a variable of this work pool's job template and is
# dropped silently, leaving the pod on the 4Gi default -- 43 flows in this repo
# are capped that way without knowing it. `memory_limit` is the one that
# applies. duckdb streams the clean, so the ceiling is headroom, not a measured
# peak.
# pyrefly: ignore [missing-attribute]
us_cms_hcris_flow.job_variables = {
    "memory_limit": "8Gi",
    "memory_request": "2Gi",
}
