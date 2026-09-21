"""Flows for au_dcceew_greenhouse — Prefect 3.

Australia's National Greenhouse Gas Inventory (DCCEEW ANGA OData). Each release
restates the full history, so every run is a **full replace**
(``dump_mode="overwrite"``), not an append. A single flow downloads all 30
entity sets once and rebuilds all three tables.

The ingested feed is the annual UNFCCC inventory (annual ``InventoryYear``),
which advances once a year. The flow is checked on a quarterly cadence but the
source poll no-ops until a new annual year appears, so most scheduled runs do
nothing — cheap. All three tables are fully free (annual data, no BD Pro
rolling window).

Deploy: `.github/scripts/deploy_flows.py` auto-discovers ``au_dcceew_greenhouse_flow``;
the dev pool ignores the schedule, the prod pool activates it.
"""

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.au_dcceew_greenhouse.constants import constants
from pipelines.datasets.au_dcceew_greenhouse.tasks import (
    clean_inventory,
    download_inventory,
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
DATA_TABLES = constants.DATA_TABLES.value

# Every table is annual and fully free (annual data refreshes at most yearly, so
# no monthly-or-more-often BD Pro rolling window applies).
_COVERAGE = {
    table: AllFree(
        date_column=YearOnly(col="year"), date_format=DateFormat.YEAR
    )
    for table in DATA_TABLES
}


@flow(name="au_dcceew_greenhouse", log_prints=True)
def au_dcceew_greenhouse_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Download the ANGA OData inventory, rebuild all three tables, materialize them.

    Each release restates the full history, so each run is a full replace
    (``dump_mode="overwrite"``). The source poll short-circuits the run until a
    new annual ``InventoryYear`` appears, making a scheduled run a cheap no-op
    between annual updates.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half — required for a safe test run, since
            the default writes production.
        update_metadata: After a successful prod materialization, register table
            coverage and commit the source update. No effect when
            ``materialize_to_prod`` is False.
        force_run: Materialize even when the source poll reports no new year.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id="inventory_unfccc"
    )

    work_dir = tempfile.mkdtemp(prefix="au_dcceew_greenhouse_")
    try:
        input_dir = download_inventory(work_dir=work_dir)
        result = clean_inventory(work_dir=work_dir, input_dir=input_dir)
        max_year = str(result["max_year"])

        has_new_data = poll_source_for_update_task(
            dataset_id=DATASET_ID,
            table_id="inventory_unfccc",
            source_max_date=max_year,
            env="prod",
            date_format="%Y",
            compare_against="coverage",
        )
        if not has_new_data and not force_run:
            return

        commit_source_update_task(
            dataset_id=DATASET_ID,
            table_id="inventory_unfccc",
            source_max_date=max_year,
            env="prod",
            date_format="%Y",
            update_metadata=update_metadata,
            materialize_after_dump=materialize_to_prod,
        )

        if not materialize_to_prod:
            for table in DATA_TABLES:
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
            return

        for table in DATA_TABLES:
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
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


# DCCEEW publishes the annual National Inventory Report ~April-May; the ANGA
# OData reflects it some weeks later. Check quarterly (Feb/May/Aug/Nov) across a
# few days; the source-poll guard no-ops until a new annual year lands.
# pyrefly: ignore [missing-attribute]
au_dcceew_greenhouse_flow.deploy_schedules = [
    {"cron": "23 15 18,19,20 2,5,8,11 *", "timezone": "America/Sao_Paulo"}
]
