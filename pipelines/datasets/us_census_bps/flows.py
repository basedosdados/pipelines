"""Flows for us_census_bps — Prefect 3.

Census Building Permits Survey. Each run rebuilds the whole series rather than
appending the newest month, for two reasons. The survey revises prior periods,
and a revision can land in any earlier month, not only the trailing few, so a
window-based refresh would leave stale figures behind wherever a correction
fell outside the window. And a full rebuild reuses the onboarding path exactly,
which avoids the class of bug where a trailing window and an upload mode
disagree and history is quietly erased.

The cost is bounded: about 700 MB of downloads and 25.7 million rows once a
month.

Deploy: `.github/scripts/deploy_flows.py` auto-discovers `us_census_bps_flow`;
the dev pool strips the schedule, the prod pool activates it.
"""

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.us_census_bps.constants import constants
from pipelines.datasets.us_census_bps.tasks import (
    clean_bps,
    clear_staging_prefix,
    download_bps,
)
from pipelines.utils.metadata.domain import (
    AllFree,
    DateFormat,
    FreeLag,
    PartBdpro,
    YearMonth,
    YearOnly,
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

# The table whose coverage the source poll compares against.
POLL_TABLE = "permit_state_monthly"

# Coverage spec per table.
#
# Data Basis paywalls the trailing window of any table that refreshes monthly
# or more often, so the four live monthly tables are part_bdpro with the
# standard six-month free lag: each run recomputes free_end = source_end -
# free_lag, rewrites both DateTimeRanges and re-issues the BigQuery row access
# policies, so the window slides on its own.
#
# part_bdpro requires a free (is_closed=False) *and* a pro (is_closed=True)
# Coverage to already exist on the table, or assert_coverage_topology raises
# before anything is written. Both were registered at onboarding.
#
# The MSA tables ended in 2003 and have nothing recent to gate, so they stay
# free even though one of them is monthly. The annual tables are
# lower-frequency and stay free. `dicionario` has no date column and takes no
# spec at all.
_MONTHLY = YearMonth(year="year", month="month")
_ANNUAL = YearOnly(col="year")
_COVERAGE = {
    "permit_place_monthly": PartBdpro(
        date_column=_MONTHLY,
        date_format=DateFormat.YEAR_MONTH,
        free_lag=FreeLag(unit="months", value=6),
    ),
    "permit_county_monthly": PartBdpro(
        date_column=_MONTHLY,
        date_format=DateFormat.YEAR_MONTH,
        free_lag=FreeLag(unit="months", value=6),
    ),
    "permit_cbsa_monthly": PartBdpro(
        date_column=_MONTHLY,
        date_format=DateFormat.YEAR_MONTH,
        free_lag=FreeLag(unit="months", value=6),
    ),
    "permit_state_monthly": PartBdpro(
        date_column=_MONTHLY,
        date_format=DateFormat.YEAR_MONTH,
        free_lag=FreeLag(unit="months", value=6),
    ),
    "permit_msa_monthly": AllFree(
        date_column=_MONTHLY, date_format=DateFormat.YEAR_MONTH
    ),
    "permit_place_annual": AllFree(
        date_column=_ANNUAL, date_format=DateFormat.YEAR
    ),
    "permit_county_annual": AllFree(
        date_column=_ANNUAL, date_format=DateFormat.YEAR
    ),
    "permit_cbsa_annual": AllFree(
        date_column=_ANNUAL, date_format=DateFormat.YEAR
    ),
    "permit_msa_annual": AllFree(
        date_column=_ANNUAL, date_format=DateFormat.YEAR
    ),
    "permit_state_annual": AllFree(
        date_column=_ANNUAL, date_format=DateFormat.YEAR
    ),
}


def _materialize(result: dict, bucket: str, target: str) -> None:
    """Upload every table's Parquet and rebuild it, then test the whole set.

    Every table is built before any test runs. The dictionary-coverage and
    relationship tests read sibling models, so interleaving run and test per
    table fails the first table's tests against a sibling that does not exist
    yet — invisible in a re-run where a stale sibling survives, and fatal in a
    clean environment.

    Args:
        result: Output of :func:`clean_bps`, mapping table slug to its path.
        bucket: GCS bucket to stage into.
        target: dbt target.
    """
    tables = constants.TABLES.value
    for table in tables:
        # append, never overwrite: overwrite calls tb.delete(mode="all"),
        # which drops the materialized production table even from a dev run.
        # The prefix clear does the part of overwrite that is actually wanted.
        clear_staging_prefix(table_id=table, bucket_name=bucket)
        upload_to_gcs(
            data_path=result[table],
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


@flow(name="us_census_bps", log_prints=True)
def us_census_bps_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Rebuild the Building Permits Survey tables from the published files.

    The source poll short-circuits the run when the Census Bureau has not
    published a new month, which makes a scheduled run a cheap no-op between
    releases. A completed run is therefore not evidence that anything was
    ingested — read the logs or check whether coverage moved.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set
            False to exercise only the dev half; the default writes production.
        update_metadata: After a successful prod materialization, register
            table coverage and commit the source update. No effect when
            ``materialize_to_prod`` is False.
        force_run: Materialize even when the poll reports no new month.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id="bps"
    )

    work_dir = tempfile.mkdtemp(prefix="us_census_bps_")
    try:
        input_dir = download_bps(work_dir=work_dir)
        result = clean_bps(work_dir=work_dir, input_dir=input_dir)
        max_year_month = result["max_year_month"]
        print(
            f"source max period {max_year_month}; rows {result['row_counts']}"
        )

        has_new_data = poll_source_for_update_task(
            dataset_id=DATASET_ID,
            table_id=POLL_TABLE,
            source_max_date=max_year_month,
            env="prod",
            date_format="%Y-%m",
            compare_against="coverage",
        )
        if not has_new_data and not force_run:
            print("no new period published; nothing to do")
            return

        if not materialize_to_prod:
            _materialize(result, "basedosdados-dev", "dev")
            return

        commit_source_update_task(
            dataset_id=DATASET_ID,
            table_id=POLL_TABLE,
            source_max_date=max_year_month,
            env="prod",
            date_format="%Y-%m",
            update_metadata=update_metadata,
            materialize_after_dump=materialize_to_prod,
        )

        _materialize(result, "basedosdados", "prod")

        if update_metadata:
            for table, coverage in _COVERAGE.items():
                register_table_materialization_task(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    coverage=coverage,
                    env="prod",
                    bq_project="basedosdados",
                )
    finally:
        # Covers both early returns and any exception. The download is about
        # 700 MB; a process worker reuses its filesystem between runs.
        shutil.rmtree(work_dir, ignore_errors=True)


# The Census Bureau publishes a reference month about four weeks later, in the
# New Residential Construction release around the 17th. Poll across a few days
# of that window; the source-poll guard no-ops until a new period lands.
# 15:25 BRT is an unused slot — piling flows onto the same instant makes them
# compete for BigQuery slots and fail together.
# pyrefly: ignore [missing-attribute]
us_census_bps_flow.deploy_schedules = [
    {"cron": "25 15 17,18,19,20,21,22 * *", "timezone": "America/Sao_Paulo"}
]
# `memory` alone is silently dropped by the work pool's job template, which
# defaults to 4Gi; `memory_limit` is the key the pod actually gets. The clean
# step holds one 400k-row buffer at a time, but the downloaded files are about
# 700 MB on disk.
# pyrefly: ignore [missing-attribute]
us_census_bps_flow.job_variables = {
    "memory": "8Gi",
    "memory_limit": "8Gi",
    "memory_request": "2Gi",
}
