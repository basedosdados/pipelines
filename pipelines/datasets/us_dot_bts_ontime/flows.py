"""
Flows for us_dot_bts_ontime — Prefect 3.

BTS Reporting Carrier On-Time Performance. Each release is a **new month**, not a
restatement of the history, so a run appends one partition rather than rebuilding
the table: ``dump_mode="append"`` against ``flight/year=YYYY/``. The reference
tables are small and are rebuilt each run.

Deploy: `.github/scripts/deploy_flows.py` auto-discovers `us_dot_bts_ontime_flow`;
the dev pool ignores the schedule, the prod pool activates it.
"""

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.us_dot_bts_ontime.constants import constants
from pipelines.datasets.us_dot_bts_ontime.tasks import (
    discover_latest_month,
    download_and_clean_month,
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
TABLES = list(constants.TABLES.value)

# `flight` refreshes monthly, so it takes the house BD Pro rolling window: the
# most recent 6 months are pro-only, everything older is free. Each run
# recomputes free_end = source_end - free_lag, rewrites both DateTimeRanges and
# re-issues the BigQuery Row Access Policies, so the window slides on its own.
#
# part_bdpro requires BOTH a free (is_closed=False) and a pro (is_closed=True)
# Coverage to already exist on the table, or assert_coverage_topology raises
# before anything is written.
#
# `airport` and `dicionario` have no date column, so they take no coverage spec.
_COVERAGE = {
    "flight": PartBdpro(
        date_column=YearMonth(year="year", month="month"),
        date_format=DateFormat.YEAR_MONTH,
        free_lag=FreeLag(unit="months", value=6),
    ),
}


@flow(name="us_dot_bts_ontime", log_prints=True)
def us_dot_bts_ontime_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Append the newest published month of BTS on-time performance.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half — required for a safe test run, since
            the default writes production.
        update_metadata: After a successful prod materialization, register table
            coverage and commit the source update. Has no effect when
            ``materialize_to_prod`` is False.
        force_run: Materialize even when the source poll reports no new month.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id="flight"
    )

    work_dir = tempfile.mkdtemp(prefix="us_dot_bts_ontime_")
    try:
        latest = discover_latest_month()
        max_ym = latest["max_date"]

        has_new_data = poll_source_for_update_task(
            dataset_id=DATASET_ID,
            table_id="flight",
            source_max_date=max_ym,
            env="prod",
            date_format="%Y-%m",
            compare_against="coverage",
        )
        if not has_new_data and not force_run:
            return

        paths = download_and_clean_month(
            work_dir=work_dir, year=latest["year"], month=latest["month"]
        )

        # Every table is materialized before any table is tested. `flight`
        # carries a custom_dictionary_coverage test that reads
        # ref('us_dot_bts_ontime__dicionario'), so an interleaved run/test would
        # test flight before dicionario exists. That is invisible in a re-run
        # where a stale sibling survives, and only bites in a clean environment.
        #
        # Each release is a new month, so the history is appended, never
        # replaced. dump_mode="overwrite" would drop the prod table even when
        # triggered from a dev run.
        def materialize(bucket: str, target: str) -> None:
            for table in TABLES:
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
            for table in TABLES:
                run_dbt(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    dbt_command="test",
                    target=target,
                )

        # The dev materialization is the pre-arm validation path, not part of a
        # production run: it rebuilds and re-tests every table in
        # basedosdados-dev, which nothing downstream reads. Running it on an
        # armed run would double the BigQuery bytes billed for no signal — prod
        # runs the same models and the same tests seconds later.
        if not materialize_to_prod:
            materialize("basedosdados-dev", "dev")
            return

        materialize("basedosdados", "prod")

        if update_metadata:
            for table, coverage in _COVERAGE.items():
                register_table_materialization_task(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    coverage=coverage,
                    env="prod",
                    bq_project="basedosdados",
                )
            # Last, and only after prod succeeded.
            commit_source_update_task(
                dataset_id=DATASET_ID,
                table_id="flight",
                source_max_date=max_ym,
                env="prod",
                date_format="%Y-%m",
                update_metadata=update_metadata,
                materialize_after_dump=materialize_to_prod,
            )
    finally:
        # Covers both early returns and any exception. A process worker reuses
        # its filesystem and one month is several hundred MB unpacked.
        shutil.rmtree(work_dir, ignore_errors=True)


# BTS publishes a month roughly two months later, mid-month (2026-06 landed
# 2026-08-12). Poll across a few mid-month days; the source-poll guard no-ops
# until a new month actually appears. Minute 28 is unused elsewhere in the repo,
# so scheduled runs do not pile onto another pipeline's instant.
# pyrefly: ignore [missing-attribute]
us_dot_bts_ontime_flow.deploy_schedules = [
    {"cron": "28 16 12,14,16,18,20 * *", "timezone": "America/Sao_Paulo"}
]
# One month is ~600k rows across 114 columns held in arrow during the clean.
# pyrefly: ignore [missing-attribute]
us_dot_bts_ontime_flow.job_variables = {"memory": "8Gi"}
