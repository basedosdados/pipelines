"""
Flows for us_fdic_bankfind — Prefect 3.

FDIC BankFind: the institution directory plus quarterly Call Report financials
for every FDIC-insured institution.

The FDIC publishes Call Report data about two months after each quarter end, so
this is an **incremental append**, not a full replace: a run rebuilds only the
trailing window of quarters (see `constants.TRAILING_QUARTERS`) and writes each
into its own hive partition, `year=YYYY/data_q<N>.parquet`. Re-running a quarter
replaces that object rather than adding a second copy, so the run is idempotent
on `dump_mode="append"` and a full 170-quarter re-download is never needed.

The window is deliberately wider than one quarter: institutions file amended
Call Reports for several quarters afterwards, and a one-quarter window would
leave those revisions behind.

All four tables are fully free. The BD Pro rolling window applies to tables that
refresh monthly or more often; this one is quarterly.

Deploy: `.github/scripts/deploy_flows.py` auto-discovers `us_fdic_bankfind_flow`;
the dev pool ignores the schedule, the prod pool activates it (paused).
"""

import shutil
import tempfile

import pandas as pd
from prefect import flow

from pipelines.datasets.us_fdic_bankfind.constants import constants
from pipelines.datasets.us_fdic_bankfind.tasks import (
    build_recent_quarters,
    build_static_tables,
    source_max_report_date,
)
from pipelines.utils.metadata.domain import (
    AllFree,
    DateFormat,
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

DATASET_ID = constants.DATASET_ID.value
POLL_TABLE = constants.POLL_TABLE.value

# Both quarterly tables are fully free: the BD Pro rolling window covers tables
# refreshed monthly or more often, and Call Reports are quarterly. `institution`
# and `indicator` carry no date column, so they take no coverage spec at all.
_COVERAGE = {
    "financials": AllFree(
        date_column=YearQuarter(year="year", quarter="quarter"),
        date_format=DateFormat.YEAR_MONTH,
    ),
    "financials_indicator": AllFree(
        date_column=YearQuarter(year="year", quarter="quarter"),
        date_format=DateFormat.YEAR_MONTH,
    ),
}


def _coverage_month(report_date: str) -> str:
    """Turn the FDIC's ``YYYYMMDD`` quarter end into the ``YYYY-MM`` the poll wants."""
    return pd.Timestamp(report_date).strftime("%Y-%m")


@flow(name="us_fdic_bankfind", log_prints=True)
def us_fdic_bankfind_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Refresh the FDIC tables for the newest quarters and materialize them.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half — required for a safe test run, since
            the default writes production.
        update_metadata: After a successful prod materialization, register table
            coverage and commit the source update. Has no effect when
            ``materialize_to_prod`` is False.
        force_run: Materialize even when the source poll reports no new quarter.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id="financials"
    )

    work_dir = tempfile.mkdtemp(prefix="us_fdic_bankfind_")
    try:
        max_report_date = source_max_report_date()
        max_month = _coverage_month(max_report_date)

        # A scheduled run is a cheap no-op until the FDIC publishes a quarter.
        has_new_data = poll_source_for_update_task(
            dataset_id=DATASET_ID,
            table_id=POLL_TABLE,
            source_max_date=max_month,
            env="prod",
            date_format="%Y-%m",
            compare_against="coverage",
        )
        if not has_new_data and not force_run:
            return

        # Committed before the download, as in us_bls_cpi: if the run dies part
        # way, the source metadata still records that a new quarter was
        # published, even though the table did not get it.
        commit_source_update_task(
            dataset_id=DATASET_ID,
            table_id=POLL_TABLE,
            source_max_date=max_month,
            env="prod",
            date_format="%Y-%m",
            update_metadata=update_metadata,
            materialize_after_dump=materialize_to_prod,
        )

        paths = build_static_tables(work_dir=work_dir)
        paths |= build_recent_quarters(work_dir=work_dir)
        tables = constants.ALL_TABLES.value

        # The dev pass is the pre-arm validation path, not part of a production
        # run: it rebuilds every table in basedosdados-dev, which nothing
        # downstream reads. Prod runs the same models and tests seconds later.
        bucket = "basedosdados" if materialize_to_prod else "basedosdados-dev"
        target = "prod" if materialize_to_prod else "dev"

        # Upload and run every table BEFORE testing any of them: the
        # relationships tests on financials_indicator read the institution and
        # indicator models, which must already exist.
        for table in tables:
            upload_to_gcs(
                data_path=paths[table],
                dataset_id=DATASET_ID,
                table_id=table,
                bucket_name=bucket,
                dump_mode="append",
                source_format="parquet",
            )
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table,
                dbt_command="run",
                target=target,
            )
        for table in tables:
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table,
                dbt_command="test",
                target=target,
            )

        if materialize_to_prod and update_metadata:
            for table, coverage in _COVERAGE.items():
                register_table_materialization_task(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    coverage=coverage,
                    env="prod",
                    bq_project="basedosdados",
                )
    finally:
        # Covers the early return, the dev-only path and any exception. A
        # process worker reuses its filesystem and the download is several GB.
        shutil.rmtree(work_dir, ignore_errors=True)


# The FDIC publishes Call Report data roughly two months after each quarter end,
# on no fixed day. Polling every five days year-round costs four real runs a
# year and a cheap no-op otherwise, and never waits a quarter for a slipped
# release. Minute 22 of hour 17 is unused elsewhere in the repo.
# pyrefly: ignore [missing-attribute]
us_fdic_bankfind_flow.deploy_schedules = [
    {"cron": "22 17 1,6,11,16,21,26 * *", "timezone": "America/Sao_Paulo"}
]
# The long table holds ~8M rows per quarter before it is written.
# pyrefly: ignore [missing-attribute]
us_fdic_bankfind_flow.job_variables = {"memory": "8Gi"}
