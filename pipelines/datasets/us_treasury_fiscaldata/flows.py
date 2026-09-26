"""Flows for us_treasury_fiscaldata — Prefect 3.

U.S. Treasury FiscalData API. Each endpoint serves its full history cheaply, so
every table is a **full replace** (``dump_mode="overwrite"``), not an incremental
append — this sidesteps the append traps
([[reference_append_upload_overwrites_same_named_parts]]). The four tables update
on different cadences (debt daily, MTS and interest monthly, exchange quarterly),
so each is **polled independently**: a daily run refreshes only ``debt_outstanding``
and leaves the monthly MTS tables untouched until a new month appears.

Deploy: `.github/scripts/deploy_flows.py` auto-discovers `us_treasury_fiscaldata_flow`;
the dev pool ignores the schedule, the prod pool activates it.
"""

import shutil
import tempfile

from prefect import flow
from prefect.utilities.asyncutils import run_coro_as_sync

from pipelines.datasets.us_treasury_fiscaldata.constants import constants
from pipelines.datasets.us_treasury_fiscaldata.tasks import (
    clean_fiscaldata,
    download_fiscaldata,
)
from pipelines.utils.metadata.domain import (
    AllFree,
    DateFormat,
    DateOnly,
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

# Coverage spec per table. Tables refreshed monthly or more often paywall their
# most recent window to BD Pro (rolling free_lag of 6 months); the quarterly
# exchange_rate stays fully free. part_bdpro requires BOTH a free
# (is_closed=False) and a pro (is_closed=True) Coverage to already exist, or
# assert_coverage_topology raises before anything is written — these are created
# at metadata registration.
_COVERAGE = {
    "debt_outstanding": PartBdpro(
        date_column=DateOnly(col="record_date"),
        date_format=DateFormat.YEAR_MD,
        free_lag=FreeLag(unit="months", value=6),
    ),
    "historical_debt_outstanding": AllFree(
        date_column=YearOnly(col="year"),
        date_format=DateFormat.YEAR,
    ),
    "mts_summary": PartBdpro(
        date_column=YearMonth(year="year", month="month"),
        date_format=DateFormat.YEAR_MONTH,
        free_lag=FreeLag(unit="months", value=6),
    ),
    "mts_receipts": PartBdpro(
        date_column=YearMonth(year="year", month="month"),
        date_format=DateFormat.YEAR_MONTH,
        free_lag=FreeLag(unit="months", value=6),
    ),
    "mts_outlays": PartBdpro(
        date_column=YearMonth(year="year", month="month"),
        date_format=DateFormat.YEAR_MONTH,
        free_lag=FreeLag(unit="months", value=6),
    ),
    "mts_means_of_financing": PartBdpro(
        date_column=YearMonth(year="year", month="month"),
        date_format=DateFormat.YEAR_MONTH,
        free_lag=FreeLag(unit="months", value=6),
    ),
    "average_interest_rate": PartBdpro(
        date_column=YearMonth(year="year", month="month"),
        date_format=DateFormat.YEAR_MONTH,
        free_lag=FreeLag(unit="months", value=6),
    ),
    "exchange_rate": AllFree(
        date_column=DateOnly(col="record_date"),
        date_format=DateFormat.YEAR_MD,
    ),
}


@flow(name="us_treasury_fiscaldata", log_prints=True)
def us_treasury_fiscaldata_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Refresh each FiscalData table that has new data, then materialize it.

    Every table is polled independently and rebuilt as a full replace when its
    source has advanced.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half — required for a safe test run, since
            the default writes production.
        update_metadata: After a successful prod materialization, register table
            coverage and commit the source update. No effect when
            ``materialize_to_prod`` is False.
        force_run: Materialize even when a table's source poll reports no new data.
    """
    run_coro_as_sync(
        rename_flow_run_dataset_table(
            prefix="Dump: ", dataset_id=DATASET_ID, table_id="fiscaldata"
        )
    )

    work_dir = tempfile.mkdtemp(prefix="us_treasury_fiscaldata_")
    bucket = "basedosdados" if materialize_to_prod else "basedosdados-dev"
    target = "prod" if materialize_to_prod else "dev"
    try:
        for table in constants.DATA_TABLES.value:
            input_dir = download_fiscaldata(work_dir=work_dir, table=table)
            res = clean_fiscaldata(
                work_dir=work_dir, input_dir=input_dir, table=table
            )
            max_date = res["max_date"]

            has_new = poll_source_for_update_task(
                dataset_id=DATASET_ID,
                table_id=table,
                source_max_date=max_date,
                env="prod",
                date_format=constants.DATE_FORMAT.value[table],
                compare_against="coverage",
            )
            if not has_new and not force_run:
                continue

            commit_source_update_task(
                dataset_id=DATASET_ID,
                table_id=table,
                source_max_date=max_date,
                env="prod",
                date_format=constants.DATE_FORMAT.value[table],
                update_metadata=update_metadata,
                materialize_after_dump=materialize_to_prod,
            )

            upload_to_gcs(
                data_path=res["path"],
                dataset_id=DATASET_ID,
                table_id=table,
                bucket_name=bucket,
                dump_mode="overwrite",
                source_format="parquet",
            )
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table,
                dbt_command="run/test",
                target=target,
            )

            if materialize_to_prod and update_metadata and table in _COVERAGE:
                register_table_materialization_task(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    coverage=_COVERAGE[table],
                    env="prod",
                    bq_project="basedosdados",
                )
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


# FiscalData publishes debt daily (business days, ~mid-afternoon US Eastern), the
# MTS ~8 business days after month end, interest rates monthly, exchange rates
# quarterly. Poll daily in the evening BRT (after the US release window) on a
# unique minute; each table's source-poll guard no-ops until its own new period
# lands, so the one daily schedule serves all four cadences.
# pyrefly: ignore [missing-attribute]
us_treasury_fiscaldata_flow.deploy_schedules = [
    {"cron": "50 18 * * *", "timezone": "America/Sao_Paulo"}
]
# The MTS clean holds ~0.9M melted rows plus the 139MB raw JSON in memory; give
# the worker headroom. `memory` alone is silently ignored (capped at 4Gi) —
# memory_limit is the one the pod actually gets.
# pyrefly: ignore [missing-attribute]
us_treasury_fiscaldata_flow.job_variables = {
    "memory_limit": "6Gi",
    "memory_request": "2Gi",
}
