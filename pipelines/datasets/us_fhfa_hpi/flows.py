"""
Flows for us_fhfa_hpi — Prefect 3.

FHFA House Price Index®. Two flows, because the source publishes on two
cadences and the products are disjoint:

- ``us_fhfa_hpi_master_flow`` — monthly. ``hpi_master.csv`` is republished with
  every monthly HPI release and carries the full history, so each run is a full
  replace (``dump_mode="overwrite"``) of the four master tables plus the dictionary.
- ``us_fhfa_hpi_annual_flow`` — yearly, late March. The annual developmental
  indexes down to census tract are released once a year, and rebuilding 3M rows
  every month would be pure waste.

Deploy: `.github/scripts/deploy_flows.py` auto-discovers both flows; the dev pool
ignores the schedules, the prod pool activates them.
"""

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.us_fhfa_hpi.constants import constants
from pipelines.datasets.us_fhfa_hpi.tasks import (
    clean_annual_task,
    clean_master_task,
    download_annual_task,
    download_master_task,
)
from pipelines.utils.metadata.domain import (
    AllFree,
    DateFormat,
    FreeLag,
    PartBdpro,
    YearMonth,
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

DATASET_ID = constants.DATASET_ID.value
MASTER_TABLES = ["dicionario", *constants.MASTER_TABLES.value]
ANNUAL_TABLES = list(constants.ANNUAL_FILES.value)

# Coverage spec per table.
#
# `monthly_national` is the only table refreshed monthly, so it is the only one
# carrying the BD Pro rolling window: the most recent 6 months are pro-only,
# everything older is free. Each run recomputes free_end = source_end - free_lag,
# rewrites both DateTimeRanges and re-issues the BigQuery Row Access Policies, so
# the window slides forward on its own.
#
# part_bdpro requires BOTH a free (is_closed=False) and a pro (is_closed=True)
# Coverage to already exist on the table, or assert_coverage_topology raises
# before anything is written.
#
# The quarterly and annual tables are lower-frequency and stay fully free.
# `dicionario` has no date column, so it takes no coverage spec at all.
_MASTER_COVERAGE = {
    "monthly_national": PartBdpro(
        date_column=YearMonth(year="year", month="month"),
        date_format=DateFormat.YEAR_MONTH,
        free_lag=FreeLag(unit="months", value=6),
    ),
    "quarterly_national": AllFree(
        date_column=YearQuarter(year="year", quarter="quarter"),
        date_format=DateFormat.YEAR_MONTH,
    ),
    "quarterly_state": AllFree(
        date_column=YearQuarter(year="year", quarter="quarter"),
        date_format=DateFormat.YEAR_MONTH,
    ),
    "quarterly_metro": AllFree(
        date_column=YearQuarter(year="year", quarter="quarter"),
        date_format=DateFormat.YEAR_MONTH,
    ),
}

_ANNUAL_COVERAGE = {
    table: AllFree(date_column=YearOnly(col="year"), date_format=DateFormat.YEAR)
    for table in ANNUAL_TABLES
}


def _materialize(tables: list[str], paths: dict, target: str, bucket: str) -> None:
    """Upload every table, run every model, then test every model.

    Run and test are two separate loops on purpose. The master tables carry a
    ``custom_dictionary_coverage`` test that reads ``ref(..._dicionario)``, a
    sibling model — interleaved, that test would run before the dictionary exists
    and fail in a clean environment.
    """
    for table in tables:
        upload_to_gcs(
            data_path=paths[table],
            dataset_id=DATASET_ID,
            table_id=table,
            bucket_name=bucket,
            dump_mode="overwrite",
            source_format="parquet",
        )
        run_dbt(dataset_id=DATASET_ID, table_id=table, dbt_command="run", target=target)
    for table in tables:
        run_dbt(dataset_id=DATASET_ID, table_id=table, dbt_command="test", target=target)


@flow(name="us_fhfa_hpi_master", log_prints=True)
def us_fhfa_hpi_master_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Rebuild the monthly and quarterly HPI tables from ``hpi_master.csv``.

    FHFA republishes the whole history on every release, so each run is a full
    replace rather than an incremental append. The source poll short-circuits the
    run when no new month has been published, which makes a scheduled run a cheap
    no-op between releases.

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
        prefix="Dump: ", dataset_id=DATASET_ID, table_id="master"
    )

    work_dir = tempfile.mkdtemp(prefix="us_fhfa_hpi_master_")
    try:
        input_dir = download_master_task(work_dir=work_dir)
        result = clean_master_task(work_dir=work_dir, input_dir=input_dir)
        max_ym = result["max_year_month"]
        paths = result["paths"]

        has_new_data = poll_source_for_update_task(
            dataset_id=DATASET_ID,
            table_id="monthly_national",
            source_max_date=max_ym,
            env="prod",
            date_format="%Y-%m",
            compare_against="coverage",
        )
        if not has_new_data and not force_run:
            return

        commit_source_update_task(
            dataset_id=DATASET_ID,
            table_id="monthly_national",
            source_max_date=max_ym,
            env="prod",
            date_format="%Y-%m",
            update_metadata=update_metadata,
            materialize_after_dump=materialize_to_prod,
        )

        if not materialize_to_prod:
            _materialize(MASTER_TABLES, paths, target="dev", bucket="basedosdados-dev")
            return

        _materialize(MASTER_TABLES, paths, target="prod", bucket="basedosdados")

        if update_metadata:
            for table, coverage in _MASTER_COVERAGE.items():
                register_table_materialization_task(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    coverage=coverage,
                    env="prod",
                    bq_project="basedosdados",
                )
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


@flow(name="us_fhfa_hpi_annual", log_prints=True)
def us_fhfa_hpi_annual_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Rebuild the annual developmental index tables, down to census tract.

    FHFA releases these once a year, in late March. Args are as for
    :func:`us_fhfa_hpi_master_flow`.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id="annual"
    )

    work_dir = tempfile.mkdtemp(prefix="us_fhfa_hpi_annual_")
    try:
        input_dir = download_annual_task(work_dir=work_dir)
        result = clean_annual_task(work_dir=work_dir, input_dir=input_dir)
        max_year = result["max_year"]
        paths = result["paths"]

        has_new_data = poll_source_for_update_task(
            dataset_id=DATASET_ID,
            table_id="annual_national",
            source_max_date=max_year,
            env="prod",
            date_format="%Y",
            compare_against="coverage",
        )
        if not has_new_data and not force_run:
            return

        commit_source_update_task(
            dataset_id=DATASET_ID,
            table_id="annual_national",
            source_max_date=max_year,
            env="prod",
            date_format="%Y",
            update_metadata=update_metadata,
            materialize_after_dump=materialize_to_prod,
        )

        if not materialize_to_prod:
            _materialize(ANNUAL_TABLES, paths, target="dev", bucket="basedosdados-dev")
            return

        _materialize(ANNUAL_TABLES, paths, target="prod", bucket="basedosdados")

        if update_metadata:
            for table, coverage in _ANNUAL_COVERAGE.items():
                register_table_materialization_task(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    coverage=coverage,
                    env="prod",
                    bq_project="basedosdados",
                )
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


# FHFA releases the monthly HPI around the 25th and the quarterly HPI in the last
# week of February, May, August and November — the master file carries both. Poll
# across the release window at 16:23 BRT; the source-poll guard no-ops until a new
# month actually appears.
# pyrefly: ignore [missing-attribute]
us_fhfa_hpi_master_flow.deploy_schedules = [
    {"cron": "23 16 25,26,27,28 * *", "timezone": "America/Sao_Paulo"}
]
# The clean step holds the whole master file in pandas.
# pyrefly: ignore [missing-attribute]
us_fhfa_hpi_master_flow.job_variables = {"memory": "4Gi"}

# The annual indexes land in late March (2026 vintage: 31 March).
# pyrefly: ignore [missing-attribute]
us_fhfa_hpi_annual_flow.deploy_schedules = [
    {"cron": "43 16 26,27,28,29,30,31 3,4 *", "timezone": "America/Sao_Paulo"}
]
# The census tract file alone is 2.2M rows.
# pyrefly: ignore [missing-attribute]
us_fhfa_hpi_annual_flow.job_variables = {"memory": "8Gi"}
