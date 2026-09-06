"""Recurring refresh for us_nchs_vital_statistics.

NCHS publishes one final annual file per product, roughly 11-18 months after the
close of the data year. The flow polls for a data year newer than the one already
registered and, when it appears, ingests that single year as a new partition.

Provisional files are deliberately NOT ingested: they are revised in place and
would silently change published counts. Only the final annual file is loaded.
"""

import os
from typing import Any

from prefect import flow, get_run_logger

from pipelines.datasets.us_nchs_vital_statistics.constants import constants
from pipelines.datasets.us_nchs_vital_statistics.tasks import (
    download_and_clean_task,
    latest_source_year_task,
    write_dicionario_task,
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
DATA_DIR = os.environ.get(
    "NCHS_DATA_DIR",
    os.path.expanduser("~/Downloads/us_nchs_vital_statistics_data"),
)
COVERAGE = {
    "birth": AllFree(
        date_column=YearOnly(col="year"), date_format=DateFormat.YEAR
    ),
    "death": AllFree(
        date_column=YearOnly(col="year"), date_format=DateFormat.YEAR
    ),
}


@flow(name="us_nchs_vital_statistics", log_prints=True)
def us_nchs_vital_statistics_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
    year: int | None = None,
):
    logger = get_run_logger()
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id="birth"
    )

    products = ["birth", "death"]
    to_ingest: dict[str, int] = {}
    for product in products:
        # Prefect's stubs type a task call as Task rather than its return value,
        # so the year is taken as Any here and validated at runtime.
        found: Any = year or latest_source_year_task(product)
        source_year = found
        if source_year is None:
            logger.warning(
                "%s: could not determine the latest source year, skipping",
                product,
            )
            continue
        max_date = f"{source_year}-01-01"
        has_new = poll_source_for_update_task(
            dataset_id=DATASET_ID,
            table_id=product,
            source_max_date=max_date,
            env="prod",
            date_format="%Y-%m-%d",
        )
        if not has_new and not force_run:
            logger.info(
                "%s: no new final data year at NCHS (latest is %s)",
                product,
                source_year,
            )
            continue
        to_ingest[product] = source_year

    if not to_ingest:
        logger.info("nothing to ingest")
        return

    for product, source_year in to_ingest.items():
        rows = download_and_clean_task(product, source_year, DATA_DIR)
        logger.info("%s %s cleaned: %s rows", product, source_year, rows)
    write_dicionario_task(DATA_DIR)

    tables = [*to_ingest, "dicionario"]

    # dev: upload and RUN every table first, then TEST -- cross-table tests read
    # sibling models, so interleaving run/test fails on a clean environment.
    for table in tables:
        path = f"{DATA_DIR}/output/{table}"
        upload_to_gcs(
            data_path=path,
            dataset_id=DATASET_ID,
            table_id=table,
            bucket_name="basedosdados-dev",
            dump_mode="append",
            source_format="parquet",
        )
        run_dbt(
            dataset_id=DATASET_ID,
            table_id=table,
            dbt_command="run",
            target="dev",
        )
    for table in tables:
        run_dbt(
            dataset_id=DATASET_ID,
            table_id=table,
            dbt_command="test",
            target="dev",
        )

    if not materialize_to_prod:
        logger.info("materialize_to_prod is False, stopping after dev")
        return

    for table in tables:
        path = f"{DATA_DIR}/output/{table}"
        upload_to_gcs(
            data_path=path,
            dataset_id=DATASET_ID,
            table_id=table,
            bucket_name="basedosdados",
            dump_mode="append",
            source_format="parquet",
        )
        run_dbt(
            dataset_id=DATASET_ID,
            table_id=table,
            dbt_command="run",
            target="prod",
        )
    for table in tables:
        run_dbt(
            dataset_id=DATASET_ID,
            table_id=table,
            dbt_command="test",
            target="prod",
        )

    if not update_metadata:
        return

    for product, source_year in to_ingest.items():
        register_table_materialization_task(
            dataset_id=DATASET_ID,
            table_id=product,
            coverage=COVERAGE[product],
            env="prod",
            bq_project="basedosdados",
        )
        commit_source_update_task(
            dataset_id=DATASET_ID,
            table_id=product,
            source_max_date=f"{source_year}-01-01",
            env="prod",
            date_format="%Y-%m-%d",
        )


# NCHS releases the final annual files without a fixed date, usually between the
# following autumn and the second spring. Poll on a few days each month; the
# source-poll guard makes a run a no-op until a new data year actually appears.
us_nchs_vital_statistics_flow.deploy_schedules = [
    {"cron": "38 7 12,19,26 * *", "timezone": "America/Sao_Paulo"}
]
us_nchs_vital_statistics_flow.job_variables = {"memory": "8Gi"}
