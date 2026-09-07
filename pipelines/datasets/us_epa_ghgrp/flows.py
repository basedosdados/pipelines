"""Flow for us_epa_ghgrp — Prefect 3.

EPA Greenhouse Gas Reporting Program (GHGRP). EPA publishes each reporting year
in the autumn of the following year and occasionally revises prior years, so
every run rebuilds the whole history from the Envirofacts API and replaces the
tables (``dump_mode="overwrite"``) — a revised year is re-materialized rather
than appended twice.

Deploy: `.github/scripts/deploy_flows.py` auto-discovers the flow; the dev pool
ignores the schedule, the prod pool activates it.
"""

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.us_epa_ghgrp.constants import constants
from pipelines.datasets.us_epa_ghgrp.tasks import (
    clean_task,
    download_task,
    source_max_year_task,
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
TABLES = constants.TABLES.value

# The table whose coverage the source poll is compared against, and whose raw
# data source carries the Update/Poll records.
POLL_TABLE = "emission_subpart"

# Annual data, fully public: every dated table is AllFree on `year`.
# `dicionario` has no date column, so it takes no coverage spec at all.
_COVERAGE = {
    table: AllFree(
        date_column=YearOnly(col="year"), date_format=DateFormat.YEAR
    )
    for table in TABLES
    if table != "dicionario"
}


def _materialize(paths: dict, target: str, bucket: str) -> None:
    """Upload every table, run every model, then test every model.

    Run and test are two separate loops on purpose: the data tables carry a
    ``custom_dictionary_coverage`` test that reads ``ref(..._dicionario)``, a
    sibling model — interleaved, that test would run before the dictionary
    exists and fail in a clean environment.
    """
    for table in TABLES:
        upload_to_gcs(
            data_path=paths[table],
            dataset_id=DATASET_ID,
            table_id=table,
            bucket_name=bucket,
            dump_mode="overwrite",
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


@flow(name="us_epa_ghgrp", log_prints=True)
def us_epa_ghgrp_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Rebuild the GHGRP tables from the Envirofacts API.

    The source poll runs first, on a few count requests, so a scheduled run is a
    cheap no-op until EPA publishes a new reporting year.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half — required for a safe test run, since
            the default writes production.
        update_metadata: After a successful prod materialization, register table
            coverage and commit the source update. Has no effect when
            ``materialize_to_prod`` is False.
        force_run: Materialize even when the source poll reports no new year.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id=POLL_TABLE
    )

    max_year = source_max_year_task()
    has_new_data = poll_source_for_update_task(
        dataset_id=DATASET_ID,
        table_id=POLL_TABLE,
        source_max_date=max_year,
        env="prod",
        date_format="%Y",
        compare_against="coverage",
    )
    if not has_new_data and not force_run:
        return

    work_dir = tempfile.mkdtemp(prefix="us_epa_ghgrp_")
    try:
        input_dir = download_task(work_dir=work_dir)
        result = clean_task(work_dir=work_dir, input_dir=input_dir)
        paths = result["paths"]

        commit_source_update_task(
            dataset_id=DATASET_ID,
            table_id=POLL_TABLE,
            source_max_date=result["max_year"],
            env="prod",
            date_format="%Y",
            update_metadata=update_metadata,
            materialize_after_dump=materialize_to_prod,
        )

        if not materialize_to_prod:
            _materialize(paths, target="dev", bucket="basedosdados-dev")
            return

        _materialize(paths, target="prod", bucket="basedosdados")

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
        shutil.rmtree(work_dir, ignore_errors=True)


# EPA releases a reporting year in early-to-mid October of the following year
# (2023 data: October 2024) and sometimes later. Poll weekly from October to
# January at 15:33 BRT; the source-poll guard no-ops until the new year lands.
# pyrefly: ignore [missing-attribute]
us_epa_ghgrp_flow.deploy_schedules = [
    {
        "cron": "33 15 3,10,17,24,31 10,11,12,1 *",
        "timezone": "America/Sao_Paulo",
    }
]
# ~900k rows across the three fact tables, held in pandas at once.
# pyrefly: ignore [missing-attribute]
us_epa_ghgrp_flow.job_variables = {"memory": "2Gi"}
