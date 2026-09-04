"""
Flow for au_doe_higher_education — Prefect 3.

The Australian Department of Education republishes its higher education
collections once a year: student data around mid-year, staff data a little
later, undergraduate applications early in the year. Each run rediscovers the
current release, because every download URL carries an opaque node id that
changes with the release; only the resource slug is stable, and it carries the
year.

The refresh is deliberately partition-scoped rather than a full rebuild. The
pivot-table releases carry a rolling five-to-seven year window and the
department delists the older ones, so the 2016-2019 partitions exist only
because the onboarding stacked vintages that can no longer be downloaded.
Rebuilding the tables from a single release would silently drop them. Instead
the run replaces exactly the partitions it rebuilt and leaves the rest alone,
and the institution directory is merged rather than replaced for the same
reason.

Deploy: `.github/scripts/deploy_flows.py` auto-discovers
`au_doe_higher_education_flow`; the dev pool ignores the schedule, the prod
pool activates it (paused until armed in Django admin).
"""

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.au_doe_higher_education.constants import constants
from pipelines.datasets.au_doe_higher_education.tasks import (
    clean_task,
    delete_staging_partitions_task,
    discover_sources_task,
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
DIRECTORY_DATASET_ID = constants.DIRECTORY_DATASET_ID.value
DIRECTORY_TABLE_ID = constants.DIRECTORY_TABLE_ID.value

#: The table the source poll is anchored on. Student enrolments is the
#: headline release and the one whose year moves first.
POLL_TABLE = "student_enrolment"

#: Every table is annual, far below the monthly threshold that would put a
#: window behind BD Pro, so all of them stay fully free. `student_completion_rate`
#: is keyed on the cohort's start year rather than a reference year.
COVERAGE = {
    table: AllFree(
        date_column=YearOnly(
            col=constants.PARTITION_OVERRIDE.value.get(table, "year")
        ),
        date_format=DateFormat.YEAR,
    )
    for table in TABLES
}


@flow(name="au_doe_higher_education")
def au_doe_higher_education_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
):
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id=POLL_TABLE
    )

    sources = discover_sources_task()
    max_year = source_max_year_task(sources)

    has_new = poll_source_for_update_task(
        dataset_id=DATASET_ID,
        table_id=POLL_TABLE,
        source_max_date=f"{max_year}-01-01",
        env="prod",
        date_format="%Y-%m-%d",
    )
    if not has_new and not force_run:
        return

    workdir = tempfile.mkdtemp(prefix="au_doe_higher_education_")
    try:
        input_dir = download_task(f"{workdir}/input", sources)
        output_dir = f"{workdir}/output"
        covered = clean_task(input_dir, output_dir)

        for bucket_name, target in (
            ("basedosdados-dev", "dev"),
            ("basedosdados", "prod"),
        ):
            if target == "prod" and not materialize_to_prod:
                break

            # The directory is a dependency of every model's relationship
            # test, so it is built before the dataset's own tables.
            upload_to_gcs(
                data_path=f"{output_dir}/higher_education_institution.parquet",
                dataset_id=DIRECTORY_DATASET_ID,
                table_id=DIRECTORY_TABLE_ID,
                bucket_name=bucket_name,
                dump_mode="overwrite",
                source_format="parquet",
            )
            run_dbt(
                dataset_id=DIRECTORY_DATASET_ID,
                table_id=DIRECTORY_TABLE_ID,
                dbt_command="run",
                target=target,
            )

            delete_staging_partitions_task(covered, bucket_name)
            for table in TABLES:
                upload_to_gcs(
                    data_path=f"{output_dir}/{table}",
                    dataset_id=DATASET_ID,
                    table_id=table,
                    bucket_name=bucket_name,
                    dump_mode="append",
                    source_format="parquet",
                )
                run_dbt(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    dbt_command="run",
                    target=target,
                )

            # Every table is built before any is tested: the relationship
            # tests read the directory and would fail against a sibling that
            # does not exist yet in a clean environment.
            for table in TABLES:
                run_dbt(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    dbt_command="test",
                    target=target,
                )
    finally:
        shutil.rmtree(workdir, ignore_errors=True)

    if not materialize_to_prod or not update_metadata:
        return

    for table in TABLES:
        register_table_materialization_task(
            dataset_id=DATASET_ID,
            table_id=table,
            coverage=COVERAGE[table],
            env="prod",
            bq_project="basedosdados",
        )
    commit_source_update_task(
        dataset_id=DATASET_ID,
        table_id=POLL_TABLE,
        source_max_date=f"{max_year}-01-01",
        env="prod",
        date_format="%Y-%m-%d",
    )


# Student data lands around mid-year, staff data later, undergraduate
# applications early in the year, and none of them on a fixed day. Polling on
# the 9th of each month is cheap: the guard returns before anything is
# downloaded unless the source year actually moved.
# pyrefly: ignore [missing-attribute]
au_doe_higher_education_flow.deploy_schedules = [
    {"cron": "35 17 9 * *", "timezone": "America/Sao_Paulo"}
]
# pyrefly: ignore [missing-attribute]
au_doe_higher_education_flow.job_variables = {"memory": "12Gi"}
