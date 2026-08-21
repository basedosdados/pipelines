"""Flows for us_cfpb_hmda - Prefect 3.

Home Mortgage Disclosure Act LAR (CFPB/FFIEC). Only the modern table
`loan_application_register` (2018+) refreshes; CFPB publishes one new year's
Snapshot National Loan-Level Dataset annually (~mid-year). The legacy table
(2007-2017) and the dicionario are frozen and are not materialized here.

The modern Snapshot files are immutable per year, so each run is a full replace
(dump_mode="overwrite") that re-cleans every modern year 2018..N into all-STRING
partitioned parquet - this keeps the staging schema consistently all-STRING and
mirrors us_bls_cpi. The source poll makes a scheduled run a no-op until CFPB
publishes a newer year. (Future optimization: append only the new year, which
would first require migrating the existing typed staging to all-STRING.)

Deploy: `.github/scripts/deploy_flows.py` auto-discovers `us_cfpb_hmda_flow`; the
dev pool ignores the schedule, the prod pool activates it (deployed paused).
"""

import shutil
import tempfile
from datetime import UTC, datetime

from prefect import flow

from pipelines.datasets.us_cfpb_hmda.constants import constants
from pipelines.datasets.us_cfpb_hmda.tasks import build_tables, resolve_years
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
TABLE_ID = constants.TABLE_ID.value

# Annual, fully public -> free coverage keyed on the year column.
_COVERAGE = AllFree(
    date_column=YearOnly(col="year"), date_format=DateFormat.YEAR
)


@flow(name="us_cfpb_hmda", log_prints=True)
def us_cfpb_hmda_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Refresh `loan_application_register` when CFPB publishes a new modern year.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against target="prod". Set False to
            exercise only the dev half - required for a safe test run.
        update_metadata: After a successful prod materialization, register table
            coverage and commit the source update. No effect when
            materialize_to_prod is False.
        force_run: Materialize even when the source poll reports no new year.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id=TABLE_ID
    )

    this_year = datetime.now(UTC).year
    resolved = resolve_years(this_year)
    max_year = resolved["max_year"]

    # Skip unless CFPB has published a year beyond current coverage (unless forced).
    has_new_data = poll_source_for_update_task(
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        source_max_date=str(max_year),
        env="prod",
        date_format="%Y",
        compare_against="coverage",
    )
    if not has_new_data and not force_run:
        return

    # Commit the source Update up front: if the run fails mid-way, source
    # metadata still reflects that a new year was published.
    commit_source_update_task(
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        source_max_date=str(max_year),
        env="prod",
        date_format="%Y",
        update_metadata=update_metadata,
        materialize_after_dump=materialize_to_prod,
    )

    work_dir = tempfile.mkdtemp(prefix="us_cfpb_hmda_")
    try:
        result = build_tables(work_dir=work_dir, years=resolved["years"])
        data_path = result[TABLE_ID]

        # The dev materialization is the pre-arm validation path, not part of a
        # production run: it rebuilds and re-tests every table in
        # basedosdados-dev, which nothing downstream reads. Running it on an
        # armed run doubled the BigQuery bytes billed for no signal — prod
        # runs the same models and the same tests seconds later.
        if not materialize_to_prod:
            # Dev: upload staging (overwrite -> clean all-STRING schema) + materialize.
            upload_to_gcs(
                data_path=data_path,
                dataset_id=DATASET_ID,
                table_id=TABLE_ID,
                bucket_name="basedosdados-dev",
                dump_mode="overwrite",
                source_format="parquet",
            )
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=TABLE_ID,
                dbt_command="run/test",
                target="dev",
            )
            return

        # Prod: upload staging + materialize/test.
        upload_to_gcs(
            data_path=data_path,
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            bucket_name="basedosdados",
            dump_mode="overwrite",
            source_format="parquet",
        )
        run_dbt(
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            dbt_command="run/test",
            target="prod",
        )

        if update_metadata:
            register_table_materialization_task(
                dataset_id=DATASET_ID,
                table_id=TABLE_ID,
                coverage=_COVERAGE,
                env="prod",
                bq_project="basedosdados",
            )
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


# CFPB releases the Snapshot National Loan-Level Dataset annually, ~mid-year
# (spring-summer). Poll a few days per month across Mar-Aug at 16:00 BRT; the
# source-poll guard no-ops until a new year actually appears.
# pyrefly: ignore [missing-attribute]
us_cfpb_hmda_flow.deploy_schedules = [
    {"cron": "0 16 8,9,10 3,4,5,6,7,8 *", "timezone": "America/Sao_Paulo"}
]
# Clean is out-of-core (~0.8 GB), but the download is several GB per year; give
# the worker headroom. Peak disk ~ one raw CSV (~5 GB) + all-year parquet (~6 GB).
# pyrefly: ignore [missing-attribute]
us_cfpb_hmda_flow.job_variables = {"memory": "8Gi"}
