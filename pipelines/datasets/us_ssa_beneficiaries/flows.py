"""Flows for us_ssa_beneficiaries — Prefect 3.

SSA republishes its *flattened time series* once a year, regenerated from the
full archive of annual editions including any post-release corrections.  Every
file therefore carries the whole history on each release, so a run is a full
replace (``dump_mode="overwrite"``), never an incremental append.

The poll is year-granularity, matching the tables' YEAR coverage: the flow
no-ops until SSA exposes a new December edition.  Because the files are
regenerated wholesale, a corrected earlier year is picked up by the next
new-year run; use ``force_run=True`` to refresh on demand.

Accepting a year is gated inside ``clean_ssa``: county sums are reconciled
against SSA's published state totals and state sums against its national total,
and a year that fails raises rather than uploading.

Deploy: ``.github/scripts/deploy_flows.py`` auto-discovers
``us_ssa_beneficiaries_flow``; the dev pool ignores the schedule, the prod pool
activates it.
"""

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.us_ssa_beneficiaries.constants import constants
from pipelines.datasets.us_ssa_beneficiaries.tasks import (
    clean_ssa,
    download_ssa,
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

# Annual release, so every table stays fully free: the BD Pro rolling window
# applies to tables refreshed monthly or more often. The dicionario has no date
# column and takes no coverage spec.
_COVERAGE = {
    table: AllFree(
        date_column=YearOnly(col="year"), date_format=DateFormat.YEAR
    )
    for table in constants.PARTITIONED_TABLES.value
}

# Representative table for the source-update poll: the OASDI county series is
# the largest and is published together with the rest of the edition.
_POLL_TABLE = "oasdi_county"


@flow(name="us_ssa_beneficiaries", log_prints=True)
def us_ssa_beneficiaries_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Download SSA's flat series, rebuild every table, materialize them.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half — required for a safe test run.
        update_metadata: After a successful prod materialization, register table
            coverage. No effect when ``materialize_to_prod`` is False.
        force_run: Materialize even when the source poll reports no new year.
    """
    # rename_flow_run_dataset_table is an async task that every flow in the
    # repo calls without awaiting, so it silently does nothing. That is tracked
    # repo-wide in #2097 (issue #1940); fixing it here alone would duplicate
    # that PR and leave this flow inconsistent with the other 99.
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id=_POLL_TABLE
    )

    work_dir = tempfile.mkdtemp(prefix="us_ssa_beneficiaries_")
    try:
        input_dir = download_ssa(work_dir=work_dir)
        result = clean_ssa(work_dir=work_dir, input_dir=input_dir)
        max_year = result["max_year"]
        tables = constants.TABLES.value

        # Skip the run unless SSA has published a newer edition.
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

        bucket = "basedosdados" if materialize_to_prod else "basedosdados-dev"
        target = "prod" if materialize_to_prod else "dev"

        # Upload every table, then run every model, then test every model. The
        # fact tables' dictionary-coverage test reads the dicionario sibling, so
        # interleaving run and test per table fails in a clean environment.
        for table in tables:
            upload_to_gcs(
                data_path=result[table],
                dataset_id=DATASET_ID,
                table_id=table,
                bucket_name=bucket,
                dump_mode="overwrite",
                source_format="parquet",
            )
        for table in tables:
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table,
                dbt_command="run",
                target=target,
            )
        for table in tables:
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table,
                dbt_command="test",
                target=target,
            )

        if materialize_to_prod and update_metadata:
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


# SSA publishes the new edition in the northern summer (the 2025 edition was
# last updated 2026-08-02). Poll four days a month across August to October; the
# source-poll guard no-ops until the new year actually lands. Minute 41 chosen
# to avoid the crowded top-of-hour slots.
# pyrefly: ignore [missing-attribute]
us_ssa_beneficiaries_flow.deploy_schedules = [
    {"cron": "41 13 5,12,19,26 8,9,10 *", "timezone": "America/Sao_Paulo"}
]
# The eight source files total ~145 MB of JSON and the melt holds ~1.6M rows in
# memory. memory_limit is the key the pool honors — bare `memory` is silently
# ignored and capped at 4Gi.
# pyrefly: ignore [missing-attribute]
us_ssa_beneficiaries_flow.job_variables = {
    "memory_limit": "8Gi",
    "memory_request": "2Gi",
}
