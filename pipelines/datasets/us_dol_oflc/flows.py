"""
Flows for us_dol_oflc — Prefect 3.

U.S. Department of Labor, Office of Foreign Labor Certification case disclosure
data: LCA (H-1B, H-1B1, E-3), PERM, H-2A and H-2B.

The source publishes on a quarterly cadence and **restates the open fiscal
year**: a new quarterly LCA file adds a quarter's decisions, and a program's
annual file supersedes what came before it. So a run does not append — it
re-materialises the open fiscal year and the one before it from scratch, and
leaves every closed year alone.

Deploy: `.github/scripts/deploy_flows.py` auto-discovers `us_dol_oflc_flow`; the
dev pool ignores the schedule, the prod pool activates it (paused).
"""

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.us_dol_oflc.constants import constants
from pipelines.datasets.us_dol_oflc.tasks import (
    clean_program,
    download_program,
    fiscal_years_to_refresh,
)
from pipelines.utils.metadata.domain import AllFree, DateFormat, DateOnly
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
PROGRAMS = constants.PROGRAMS.value

# Every table refreshes quarterly — less often than monthly — so none of them
# carries a BD Pro rolling window and all coverage is free. `dictionary` has no
# date column and takes no coverage spec.
_COVERAGE = {
    program: AllFree(
        date_column=DateOnly(col="decision_date"),
        date_format=DateFormat.YEAR_MD,
    )
    for program in PROGRAMS
}

# The poll is anchored on the LCA table, the highest-volume program and the only
# one published quarterly; the other three are re-read in the same run.
POLL_TABLE = "lca"


@flow(name="us_dol_oflc", log_prints=True)
def us_dol_oflc_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Refresh the open fiscal year of every OFLC program table.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half — required for a safe test run, since
            the default writes production.
        update_metadata: After a successful prod materialization, register table
            coverage and commit the source update. Has no effect when
            ``materialize_to_prod`` is False.
        force_run: Materialize even when the source poll reports nothing new.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id=POLL_TABLE
    )

    work_dir = tempfile.mkdtemp(prefix="us_dol_oflc_")
    try:
        years = fiscal_years_to_refresh()

        # Download and rebuild the poll table first: its latest decision date is
        # what tells us whether the source has published anything new.
        results = {}
        input_dir = download_program(
            program=POLL_TABLE, years=years, work_dir=work_dir
        )
        results[POLL_TABLE] = clean_program(
            program=POLL_TABLE,
            years=years,
            work_dir=work_dir,
            input_dir=input_dir,
        )
        max_date = results[POLL_TABLE]["max_decision_date"]

        has_new_data = poll_source_for_update_task(
            dataset_id=DATASET_ID,
            table_id=POLL_TABLE,
            source_max_date=max_date,
            env="prod",
            date_format="%Y-%m-%d",
            compare_against="coverage",
        )
        if not has_new_data and not force_run:
            return

        for program in PROGRAMS:
            if program == POLL_TABLE:
                continue
            program_input = download_program(
                program=program, years=years, work_dir=work_dir
            )
            results[program] = clean_program(
                program=program,
                years=years,
                work_dir=work_dir,
                input_dir=program_input,
            )

        # `dictionary` is rebuilt from the full history, not from a two-year
        # refresh, so the pipeline never uploads it — but dbt still runs the
        # model so the dictionary-coverage tests read a current table.
        bucket = (
            "basedosdados-dev" if not materialize_to_prod else "basedosdados"
        )
        target = "dev" if not materialize_to_prod else "prod"

        # Upload the table root, never an individual `year=<FY>` directory.
        # `Storage.upload` derives each object's hive partition from the file's
        # path *relative to the path it is given*, so handing it the partition
        # directory leaves that relative path empty and the object lands at the
        # prefix root as `staging/<dataset>/<table>/data.parquet`. BigQuery then
        # reads one object with no partition key and the whole external table
        # fails: "Incompatible partition schemas. Expected schema
        # ([year:TYPE_STRING]) has 1 columns. Observed schema ([]) has 0
        # columns."
        #
        # Uploading the root does not widen what the run touches: `build` writes
        # only the refreshed fiscal years into this run's own temp directory, so
        # the root holds exactly those partitions. Combined with
        # dump_mode="append" — "overwrite" would delete the whole prefix, and the
        # prod table with it — the closed years already in the staging prefix are
        # left alone.
        for program in PROGRAMS:
            upload_to_gcs(
                data_path=results[program]["path"],
                dataset_id=DATASET_ID,
                table_id=program,
                bucket_name=bucket,
                dump_mode="append",
                source_format="parquet",
            )

        # Build every table before testing any of them: the dictionary-coverage
        # tests read a sibling model, and interleaving run/test per table fails
        # in a clean environment where that sibling does not exist yet.
        for table in constants.TABLES.value:
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table,
                dbt_command="run",
                target=target,
            )
        for table in constants.TABLES.value:
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table,
                dbt_command="test",
                target=target,
            )

        if not materialize_to_prod:
            return

        if update_metadata:
            for program, coverage in _COVERAGE.items():
                register_table_materialization_task(
                    dataset_id=DATASET_ID,
                    table_id=program,
                    coverage=coverage,
                    env="prod",
                    bq_project="basedosdados",
                )
            commit_source_update_task(
                dataset_id=DATASET_ID,
                table_id=POLL_TABLE,
                source_max_date=max_date,
                env="prod",
                date_format="%Y-%m-%d",
                update_metadata=update_metadata,
                materialize_after_dump=materialize_to_prod,
            )
    finally:
        # Covers both early returns and any exception. A process worker reuses
        # its filesystem and the download is several hundred megabytes.
        shutil.rmtree(work_dir, ignore_errors=True)


# OFLC publishes each fiscal quarter roughly a month after it ends, so the files
# appear in February, May, August and November. Poll across a few days in those
# months; the source-poll guard no-ops until a newer decision date appears.
# pyrefly: ignore [missing-attribute]
us_dol_oflc_flow.deploy_schedules = [
    {"cron": "23 14 5,12,19,26 2,5,8,11 *", "timezone": "America/Sao_Paulo"}
]
# The clean step holds one fiscal year of LCA (~700k rows x 57 columns) in
# pandas while it is written, and calamine builds a Python object per cell of
# the workbook it is reading.
#
# The keys are memory_limit / memory_request: this work pool's job template
# defines no "memory" variable, so the {"memory": "8Gi"} spelling used by most
# datasets in this repo is silently discarded and the pod runs at the pool
# default of 4Gi. That is what OOM-killed the first full run here.
# pyrefly: ignore [missing-attribute]
us_dol_oflc_flow.job_variables = {
    "memory_limit": "12Gi",
    "memory_request": "4Gi",
}
