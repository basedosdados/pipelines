"""Recurring refresh flows for us_ffiec_bank_reporting.

Two cadences, two flows:

* `us_ffiec_bank_reporting_quarterly_flow` -- Call Report and FR Y-9C, plus the
  MDRM dictionary they are read through. Polled on the CDR period dropdown.
* `us_ffiec_bank_reporting_cra_flow` -- the annual CRA disclosure.

**Re-materialise, do not append.** The FFIEC accepts amended Call Reports for
several quarters after the original filing, and an amendment restates values
that were already published. Appending would leave both the original and the
restated row behind the same (rssd_id, quarter, item_code) key. Each quarter is
therefore re-cleaned into its own `year=YYYY/data_q<N>.parquet`, so a rerun
replaces that object and `dump_mode="append"` stays idempotent.

**Run every table, then test every table.** The dictionary-coverage test on each
table `ref()`s the `dictionary` model, so interleaving run and test per table
would test a table before its sibling exists. That failure is invisible in a
re-run where a stale sibling survives and only bites in a clean environment.
"""

from __future__ import annotations

from datetime import datetime

from prefect import flow

# `log_prints=True` on both flows: the download and clean modules report
# progress with `print()`, which Prefect otherwise discards. Without it a run
# shows the poll and then nothing for the next hour, so a genuine hang is
# indistinguishable from a long dbt build. Matches au_abs_population and the
# other flows that wrap print-logging transforms.
from pipelines.datasets.us_ffiec_bank_reporting.constants import (
    constants as ffiec_constants,
)
from pipelines.datasets.us_ffiec_bank_reporting.tasks import (
    latest_source_quarter_task,
    quarter_to_coverage_date_task,
    refresh_cra_task,
    refresh_quarterly_task,
    table_path_task,
)
from pipelines.utils.metadata.domain import (
    AllFree,
    DateFormat,
    YearOnly,
    YearQuarter,
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

DATASET_ID = ffiec_constants.DATASET_ID.value

# Quarterly tables carry year+quarter; the CRA tables carry year only. mdrm_item
# and dictionary have no date column, so they get no coverage spec at all and
# are simply not registered for materialisation coverage.
QUARTERLY_COVERAGE = {
    t: AllFree(
        date_column=YearQuarter(year="year", quarter="quarter"),
        date_format=DateFormat.YEAR_MONTH,
    )
    for t in (
        "institution",
        "call_report_item",
        "holding_company",
        "holding_company_item",
    )
}
CRA_COVERAGE = {
    t: AllFree(date_column=YearOnly(col="year"), date_format=DateFormat.YEAR)
    for t in ("cra_respondent", "cra_lending", "cra_assessment_area_tract")
}


def _materialise(tables, output_root, bucket_name, target):
    """Upload and build every table, then test every table.

    The two loops are deliberate -- see the module docstring.
    """
    for table_id in tables:
        upload_to_gcs(
            data_path=table_path_task(output_root, table_id),
            dataset_id=DATASET_ID,
            table_id=table_id,
            bucket_name=bucket_name,
            dump_mode="append",
            source_format="parquet",
        )
        run_dbt(
            dataset_id=DATASET_ID,
            table_id=table_id,
            dbt_command="run",
            target=target,
        )
    for table_id in tables:
        run_dbt(
            dataset_id=DATASET_ID,
            table_id=table_id,
            dbt_command="test",
            target=target,
        )


@flow(name="us_ffiec_bank_reporting_quarterly", log_prints=True)
def us_ffiec_bank_reporting_quarterly_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
    trailing_quarters: int = ffiec_constants.TRAILING_QUARTERS.value,
):
    tables = ffiec_constants.QUARTERLY_TABLES.value
    poll_table = ffiec_constants.QUARTERLY_POLL_TABLE.value
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id=poll_table
    )

    latest = latest_source_quarter_task()
    source_date = quarter_to_coverage_date_task(latest)
    has_new = poll_source_for_update_task(
        dataset_id=DATASET_ID,
        table_id=poll_table,
        source_max_date=source_date,
        env="prod",
        date_format="%Y-%m",
    )
    if not has_new and not force_run:
        return

    # Written here rather than at the end of the flow: the source Update records
    # what the FFIEC *released*, which is true the moment the poll confirms it
    # and stays true even if a later step fails. Committing last would leave a
    # crashed run with no record that anything was published. Gated on
    # materialize_to_prod as well, because the metadata tasks are pinned
    # env="prod" whichever pool the run is on -- and skipped under force_run,
    # which re-materialises without the publisher having released anything.
    if update_metadata and materialize_to_prod and has_new:
        commit_source_update_task(
            dataset_id=DATASET_ID,
            table_id=poll_table,
            source_max_date=source_date,
            env="prod",
            date_format="%Y-%m",
        )

    output_root = refresh_quarterly_task(latest, trailing_quarters)

    _materialise(tables, output_root, "basedosdados-dev", "dev")
    if not materialize_to_prod:
        return
    _materialise(tables, output_root, "basedosdados", "prod")

    if update_metadata:
        for table_id, coverage in QUARTERLY_COVERAGE.items():
            register_table_materialization_task(
                dataset_id=DATASET_ID,
                table_id=table_id,
                coverage=coverage,
                env="prod",
                bq_project="basedosdados",
            )


@flow(name="us_ffiec_bank_reporting_cra", log_prints=True)
def us_ffiec_bank_reporting_cra_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
    last_year: int | None = None,
    trailing_years: int = ffiec_constants.TRAILING_CRA_YEARS.value,
):
    tables = ffiec_constants.CRA_TABLES.value
    poll_table = ffiec_constants.CRA_POLL_TABLE.value
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id=poll_table
    )

    # The CRA flat-files page offers no machine-readable index, so the newest
    # year is taken from the calendar: the disclosure for year Y is published
    # late in Y+1. The downloader treats a missing file as "not published yet"
    # and retries it on the next run rather than recording a hole.
    target_year = last_year or datetime.now().year - 1
    source_date = str(target_year)
    has_new = poll_source_for_update_task(
        dataset_id=DATASET_ID,
        table_id=poll_table,
        source_max_date=source_date,
        env="prod",
        date_format="%Y",
    )
    if not has_new and not force_run:
        return

    if update_metadata and materialize_to_prod and has_new:
        commit_source_update_task(
            dataset_id=DATASET_ID,
            table_id=poll_table,
            source_max_date=source_date,
            env="prod",
            date_format="%Y",
        )

    output_root = refresh_cra_task(target_year, trailing_years)

    _materialise(tables, output_root, "basedosdados-dev", "dev")
    if not materialize_to_prod:
        return
    _materialise(tables, output_root, "basedosdados", "prod")

    if update_metadata:
        for table_id, coverage in CRA_COVERAGE.items():
            register_table_materialization_task(
                dataset_id=DATASET_ID,
                table_id=table_id,
                coverage=coverage,
                env="prod",
                bq_project="basedosdados",
            )


# Call Reports land 30-45 days after quarter end, so the quarterly flow polls
# across the back half of the month following each quarter. The CRA disclosure
# lands late in the following year, so that flow polls monthly from October.
# Minutes are chosen off the crowded :00 slot -- twelve pipelines firing at the
# same instant compete for BigQuery slots and fail together if the daily quota
# trips.
# pyrefly: ignore [missing-attribute]
us_ffiec_bank_reporting_quarterly_flow.deploy_schedules = [
    {
        "cron": "47 14 4,11,18,25 1,2,4,5,7,8,10,11 *",
        "timezone": "America/Sao_Paulo",
    }
]
# pyrefly: ignore [missing-attribute]
us_ffiec_bank_reporting_cra_flow.deploy_schedules = [
    {"cron": "13 15 9 10,11,12,1,2 *", "timezone": "America/Sao_Paulo"}
]

# The clean step holds a quarter's melt in memory; the 2026Q2 Call Report is
# ~4.2 million item rows per quarter and the FR Y-9SP quarters are larger still.
# `memory` alone is silently dropped by the work pool template, which defaults
# to 4Gi -- memory_limit is the key that actually applies.
# Set on each flow explicitly rather than in a loop: a loop leaves its control
# variable bound to a Flow at module scope, and deploy_flows.py collects every
# module-level Flow object -- which would register a third, bogus deployment
# named after the loop variable.
_JOB_VARIABLES = {"memory_limit": "8Gi", "memory_request": "2Gi"}
# pyrefly: ignore [missing-attribute]
us_ffiec_bank_reporting_quarterly_flow.job_variables = dict(_JOB_VARIABLES)
# pyrefly: ignore [missing-attribute]
us_ffiec_bank_reporting_cra_flow.job_variables = dict(_JOB_VARIABLES)
