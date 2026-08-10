"""Flows for au_abs_labour_force — Prefect 3.

Labour Force, Australia (ABS cat. 6202.0), monthly. Both sources ship the full
history every release — the SDMX ``all`` query returns every period, and each ABS
Excel spreadsheet carries the whole series — so every run is a **full replace**
(``dump_mode="overwrite"``), not an incremental append. A single flow downloads
once and rebuilds all four tables. The source poll short-circuits the run until
the ABS publishes a newer reference month, so a scheduled run is a cheap no-op
between releases.

Every table refreshes monthly, so each carries the BD Pro rolling window
(``PartBdpro``): the most recent six months are pro-only, older data is free, and
the window rolls forward on its own each run.

Deploy: ``.github/scripts/deploy_flows.py`` auto-discovers ``au_abs_labour_force_flow``;
the dev pool ignores the schedule, the prod pool activates it (paused until armed).
"""

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.au_abs_labour_force.constants import constants
from pipelines.datasets.au_abs_labour_force.tasks import (
    clean_and_write_task,
    download_excel_task,
    download_sdmx_task,
    latest_month_task,
)
from pipelines.utils.metadata.domain import (
    DateFormat,
    FreeLag,
    PartBdpro,
    YearMonth,
)
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
TABLES = constants.DATA_TABLES.value

# Every table is monthly, so every table gets the BD Pro rolling window: the most
# recent 6 months are pro-only, everything older is free. Each run recomputes
# free_end = source_end - 6 months, rewrites both DateTimeRanges, and re-issues the
# BigQuery Row Access Policies, so the window slides forward on its own.
#
# part_bdpro requires BOTH a free (is_closed=False) and a pro (is_closed=True)
# Coverage to already exist on the table, or assert_coverage_topology raises before
# anything is written. Onboarding created only the free Coverage, so the pro
# Coverage must be created by hand before the first armed (update_metadata) run.
_COVERAGE = {
    table: PartBdpro(
        date_column=YearMonth(year="year", month="month"),
        date_format=DateFormat.YEAR_MONTH,
        free_lag=FreeLag(unit="months", value=6),
    )
    for table in TABLES
}

# Each table is linked to exactly one raw source (the poll/commit client raises on
# a multi-source table). labour_force_status -> the SDMX API source, which drives
# the poll; hours_worked -> the Excel source. Committing the source update for one
# representative table per source writes both RawDataSource.Update records. Both
# sources release the same reference month together, so one poll gate suffices.
_POLL_TABLE = "labour_force_status"
_SOURCE_COMMIT_TABLES = ["labour_force_status", "hours_worked"]


@flow(name="au_abs_labour_force", log_prints=True)
def au_abs_labour_force_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Download the ABS labour-force sources, rebuild all four tables, materialize.

    Both sources ship the full history on every release, so each run is a full
    replace (``dump_mode="overwrite"``). The source poll no-ops the run when the
    ABS has not published a newer month, making a scheduled run cheap between
    releases.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half — required for a safe test run, since the
            default writes production.
        update_metadata: After a successful prod materialization, register table
            coverage (the rolling window) and commit the source update. Has no
            effect when ``materialize_to_prod`` is False.
        force_run: Materialize even when the source poll reports no new month.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id=_POLL_TABLE
    )

    work_dir = tempfile.mkdtemp(prefix="au_abs_labour_force_")
    try:
        # SDMX first (small, and it carries the latest reference month).
        input_dir = download_sdmx_task(work_dir=work_dir)
        max_ym = latest_month_task(input_dir=input_dir)

        # A scheduled run polls prod and no-ops until the ABS publishes a newer
        # month. force_run bypasses the poll entirely (not just its early return)
        # — the dev test sets it, and the poll is env="prod", so a dev-only test
        # must never reach it (prod metadata may not exist yet, and a test must
        # not write a prod Poll record).
        if not force_run:
            has_new_data = poll_source_for_update_task(
                dataset_id=DATASET_ID,
                table_id=_POLL_TABLE,
                source_max_date=max_ym,
                env="prod",
                date_format="%Y-%m",
                compare_against="coverage",
            )
            if not has_new_data:
                return

        # Only now fetch the heavy Excel spreadsheets and rebuild.
        download_excel_task(input_dir=input_dir, source_max_date=max_ym)
        result = clean_and_write_task(work_dir=work_dir, input_dir=input_dir)

        # Dev: upload staging + materialize/test.
        for table in TABLES:
            upload_to_gcs(
                data_path=result[table],
                dataset_id=DATASET_ID,
                table_id=table,
                bucket_name="basedosdados-dev",
                dump_mode="overwrite",
                source_format="parquet",
            )
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table,
                dbt_command="run/test",
                target="dev",
            )

        if not materialize_to_prod:
            return

        # Prod: upload staging + materialize/test.
        for table in TABLES:
            upload_to_gcs(
                data_path=result[table],
                dataset_id=DATASET_ID,
                table_id=table,
                bucket_name="basedosdados",
                dump_mode="overwrite",
                source_format="parquet",
            )
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table,
                dbt_command="run/test",
                target="prod",
            )

        if update_metadata:
            for table, coverage in _COVERAGE.items():
                register_table_materialization_task(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    coverage=coverage,
                    env="prod",
                    bq_project="basedosdados",
                )
            for table in _SOURCE_COMMIT_TABLES:
                commit_source_update_task(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    source_max_date=max_ym,
                    env="prod",
                    date_format="%Y-%m",
                )
    finally:
        # Covers both early returns (no new data, dev-only) and any exception.
        shutil.rmtree(work_dir, ignore_errors=True)


# ABS releases Labour Force monthly, on a Thursday roughly the 3rd-4th week, at
# 11:30 Canberra time. Poll daily across that window at 06:00 BRT (= evening AEST,
# after the morning release); the source-poll guard no-ops until a new month lands.
# pyrefly: ignore [missing-attribute]
au_abs_labour_force_flow.deploy_schedules = [
    {"cron": "0 6 14-27 * *", "timezone": "America/Sao_Paulo"}
]
# openpyxl reads the ~38 MB SEM1 pivot; give the worker headroom.
# pyrefly: ignore [missing-attribute]
au_abs_labour_force_flow.job_variables = {"memory": "6Gi"}
