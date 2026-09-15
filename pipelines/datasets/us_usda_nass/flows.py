"""Flows for us_usda_nass — Prefect 3.

USDA NASS QuickStats survey + Census of Agriculture (curated seed). The 5 bulk
sector files carry the full history on every release, so each run is a **full
replace** (``dump_mode="overwrite"``), not an incremental append. A single flow
downloads once, rebuilds the seven per-grain fact tables plus the dicionario, and
materializes them.

The source poll is year-granularity: it re-materializes when NASS first exposes a
new ``year`` in the bulk files (which happens as the new survey year opens and as
prior-year finals land), and no-ops otherwise. Intra-year revisions to
already-present years are picked up at the next new-year trigger; use
``force_run=True`` for an on-demand full refresh. (Refining this to a
load-time-based poll for true continuous refresh is a documented follow-up.)

Deploy: ``.github/scripts/deploy_flows.py`` auto-discovers ``us_usda_nass_flow``;
the dev pool ignores the schedule, the prod pool activates it.
"""

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.us_usda_nass.constants import constants
from pipelines.datasets.us_usda_nass.tasks import clean_nass, download_nass
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

# Every fact table is annual and fully free (AllFree). The dicionario has no
# date column and takes no coverage spec.
_COVERAGE = {
    t: AllFree(date_column=YearOnly(col="year"), date_format=DateFormat.YEAR)
    for t in (
        "survey_national",
        "survey_state",
        "survey_agricultural_district",
        "survey_county",
        "census_of_agriculture_national",
        "census_of_agriculture_state",
        "census_of_agriculture_county",
    )
}

# Representative table for the source-update poll (national is published earliest
# each year and carries every year).
_POLL_TABLE = "survey_national"


@flow(name="us_usda_nass", log_prints=True)
def us_usda_nass_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Download the NASS bulk files, rebuild all tables, materialize them.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half — required for a safe test run.
        update_metadata: After a successful prod materialization, register table
            coverage and commit the source update. No effect when
            ``materialize_to_prod`` is False.
        force_run: Materialize even when the source poll reports no new year.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id=_POLL_TABLE
    )

    work_dir = tempfile.mkdtemp(prefix="us_usda_nass_")
    try:
        input_dir = download_nass(work_dir=work_dir)
        result = clean_nass(work_dir=work_dir, input_dir=input_dir)
        max_year = str(result["max_year"])
        tables = constants.ALL_TABLES.value

        # Skip the run unless the source exposes a newer year (unless forced).
        has_new_data = poll_source_for_update_task(
            dataset_id=DATASET_ID,
            table_id=_POLL_TABLE,
            source_max_date=max_year,
            env="prod",
            date_format="%Y",
            compare_against="coverage",
        )
        if not has_new_data and not force_run:
            return

        commit_source_update_task(
            dataset_id=DATASET_ID,
            table_id=_POLL_TABLE,
            source_max_date=max_year,
            env="prod",
            date_format="%Y",
            update_metadata=update_metadata,
            materialize_after_dump=materialize_to_prod,
        )

        if not materialize_to_prod:
            # Dev: upload staging, then run ALL tables, then test ALL tables.
            for table in tables:
                upload_to_gcs(
                    data_path=result[table],
                    dataset_id=DATASET_ID,
                    table_id=table,
                    bucket_name="basedosdados-dev",
                    dump_mode="overwrite",
                    source_format="parquet",
                )
            for table in tables:
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
            return

        # Prod: upload staging, then run ALL tables, then test ALL tables. The
        # dicionario is referenced by the fact tables' custom_dictionary_coverage
        # test, so every table must be built before any test runs.
        for table in tables:
            upload_to_gcs(
                data_path=result[table],
                dataset_id=DATASET_ID,
                table_id=table,
                bucket_name="basedosdados",
                dump_mode="overwrite",
                source_format="parquet",
            )
        for table in tables:
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


# NASS regenerates the bulk files continuously; a new `year` appears a couple of
# times a year. Poll monthly at a free minute; the source-poll guard no-ops until
# a new year lands. Minute 23 chosen to avoid the crowded top-of-hour slots.
# pyrefly: ignore [missing-attribute]
us_usda_nass_flow.deploy_schedules = [
    {"cron": "23 15 12 * *", "timezone": "America/Sao_Paulo"}
]
# The clean streams to disk in bounded flush windows; the upload globs parquet.
# Give the worker headroom. memory_limit is the key the pool honors — bare
# `memory` is silently ignored (capped at 4Gi).
# pyrefly: ignore [missing-attribute]
us_usda_nass_flow.job_variables = {
    "memory_limit": "8Gi",
    "memory_request": "2Gi",
}
