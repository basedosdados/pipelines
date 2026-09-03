"""
Flow for us_state_foreign_assistance — Prefect 3.

ForeignAssistance.gov (U.S. Department of State + USAID) republishes its bulk
files roughly quarterly, and every release restates the full history (agencies
revise prior fiscal years for up to two years). Each run is therefore a full
replace of all three tables, guarded by a cheap poll: a HEAD request on the
complete CSV's S3 ``Last-Modified`` header, compared against the table's last
materialization. Nothing is downloaded until the source is newer.

Deploy: `.github/scripts/deploy_flows.py` auto-discovers
`us_state_foreign_assistance_flow`; the dev pool ignores the schedule, the prod
pool activates it (paused until armed in Django admin).
"""

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.us_state_foreign_assistance.constants import constants
from pipelines.datasets.us_state_foreign_assistance.tasks import (
    check_source_release,
    clean_source,
    clear_staging_blobs,
    download_source,
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
TABLES = constants.TABLES.value  # transaction, budget, dicionario
POLL_TABLE = "transaction"

# Both fact tables are annual (fiscal year) and refreshed quarterly, below the
# monthly threshold for a BD Pro window, so they stay fully free. `dicionario`
# has no date column and takes no coverage spec.
_COVERAGE = {
    "transaction": AllFree(
        date_column=YearOnly(col="year"), date_format=DateFormat.YEAR
    ),
    "budget": AllFree(
        date_column=YearOnly(col="year"), date_format=DateFormat.YEAR
    ),
}


def _upload_and_run(result: dict, bucket_name: str, target: str) -> None:
    """Full-replace upload of every table, then dbt run, then dbt test.

    Run every model before testing any: `custom_dictionary_coverage` on the
    fact tables reads the `dicionario` sibling, which must already exist.
    """
    for table in TABLES:
        clear_staging_blobs(
            dataset_id=DATASET_ID, table_id=table, bucket_name=bucket_name
        )
        upload_to_gcs(
            data_path=result[table],
            dataset_id=DATASET_ID,
            table_id=table,
            bucket_name=bucket_name,
            dump_mode="append",
            source_format="parquet",
        )
    for table in TABLES:
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


@flow(name="us_state_foreign_assistance", log_prints=True)
def us_state_foreign_assistance_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Poll ForeignAssistance.gov, and on a new release rebuild the three tables.

    Args:
        materialize_to_prod: Write the prod staging bucket and run dbt against
            ``target="prod"``. Set False to exercise only the dev half — the
            required setting for a test run, since the default writes production
            and the metadata tasks are pinned to the prod backend.
        update_metadata: After a successful prod materialization, refresh the
            coverage ranges and table Updates and commit the source Update. No
            effect when ``materialize_to_prod`` is False.
        force_run: Download and materialize even when the source poll reports
            no newer release.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id=POLL_TABLE
    )

    # The release date is a publication timestamp, so it is compared against
    # Table.Update.latest (when we last materialized), not against the
    # fiscal-year coverage, which only moves once a year.
    release_date = check_source_release()
    has_new_data = poll_source_for_update_task(
        dataset_id=DATASET_ID,
        table_id=POLL_TABLE,
        source_max_date=release_date,
        env="prod",
        date_format="%Y-%m-%d",
        compare_against="table_update",
    )
    if not has_new_data and not force_run:
        print(f"source release {release_date} is not newer; nothing to do")
        return

    commit_source_update_task(
        dataset_id=DATASET_ID,
        table_id=POLL_TABLE,
        source_max_date=release_date,
        env="prod",
        date_format="%Y-%m-%d",
        update_metadata=update_metadata,
        materialize_after_dump=materialize_to_prod,
    )

    work_dir = tempfile.mkdtemp(prefix="us_state_foreign_assistance_")
    try:
        input_dir = download_source(work_dir=work_dir)
        result = clean_source(work_dir=work_dir, input_dir=input_dir)
        print(f"rows written: {result['rows']}")

        # Dev-only validation path: rebuild and test in basedosdados-dev, then
        # stop. An armed run skips it — prod runs the same models seconds later.
        if not materialize_to_prod:
            _upload_and_run(
                result, bucket_name="basedosdados-dev", target="dev"
            )
            return

        _upload_and_run(result, bucket_name="basedosdados", target="prod")

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
        # The download is 3.75 GB; never leave it behind on a reused worker.
        shutil.rmtree(work_dir, ignore_errors=True)


# The publisher uploads "quarterly" with no fixed calendar (observed releases
# 2026-07-08 and 2026-09-02). Poll weekly at a free minute; the HEAD-based
# guard makes a run without a new release a no-op that downloads nothing.
# pyrefly: ignore [missing-attribute]
us_state_foreign_assistance_flow.deploy_schedules = [
    {"cron": "40 5 5,12,19,26 * *", "timezone": "America/Sao_Paulo"}
]
# DuckDB reads the 3.75 GB CSV with a 10 GB memory limit; give the pod headroom.
# pyrefly: ignore [missing-attribute]
us_state_foreign_assistance_flow.job_variables = {"memory": "16Gi"}
