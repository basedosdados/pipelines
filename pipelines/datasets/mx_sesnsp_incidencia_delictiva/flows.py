"""
Flows for mx_sesnsp_incidencia_delictiva — Prefect 3.

Mexican monthly crime counts from SESNSP. Only the four new-methodology tables
that gain a month on every release are refreshed here — the three
``*_2015_2025`` legacy tables are frozen. Each SESNSP release ships the full
current-year history, so every run is a full replace (dump_mode="overwrite"),
not an incremental append. A single flow scrapes the rotating SharePoint tokens,
downloads and cleans the four tables, and materializes them. Schedule targets the
monthly SESNSP release window (~20th).

Deploy: `.github/scripts/deploy_flows.py` auto-discovers
`mx_sesnsp_incidencia_delictiva_flow`; the dev pool ignores the schedule, the
prod pool activates it.
"""

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.mx_sesnsp_incidencia_delictiva.constants import (
    constants,
)
from pipelines.datasets.mx_sesnsp_incidencia_delictiva.tasks import (
    clean_sesnsp,
    download_sesnsp,
)
from pipelines.utils.metadata.domain import (
    AllBdpro,
    DateFormat,
    YearMonth,
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

# Coverage spec per table.
#
# All four tables carry the whole new-methodology series as BD Pro (all_bdpro):
# every row is pro-gated. The 6-month rolling window (part_bdpro) is not usable
# yet — with only ~6 months of data the free window (source_end - 6 months)
# falls before the data starts, producing an inverted DateTimeRange the backend
# rejects. Revisit part_bdpro once the tables hold more than 6 months of data.
#
# all_bdpro requires a single pro (is_closed=True) Coverage and NO free Coverage
# on each table, or assert_coverage_topology raises before anything is written.
# The frozen *_2015_2025 tables are not in this pipeline and stay AllFree.
_COVERAGE = {
    table: AllBdpro(
        date_column=YearMonth(year="ano", month="mes"),
        date_format=DateFormat.YEAR_MONTH,
    )
    for table in constants.ALL_TABLES.value
}


@flow(name="mx_sesnsp_incidencia_delictiva", log_prints=True)
def mx_sesnsp_incidencia_delictiva_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Download the SESNSP files, rebuild the four ongoing tables, materialize them.

    Each SESNSP release ships the full current-year history, so every run is a
    full replace (``dump_mode="overwrite"``) rather than an incremental append.
    The source poll short-circuits the run when SESNSP has not published a new
    month, which makes a scheduled run a cheap no-op between releases.

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
        prefix="Dump: ", dataset_id=DATASET_ID, table_id="incidencia_delictiva"
    )

    work_dir = tempfile.mkdtemp(prefix="mx_sesnsp_")
    try:
        input_dir = download_sesnsp(work_dir=work_dir)
        result = clean_sesnsp(work_dir=work_dir, input_dir=input_dir)
        max_ym = result["max_year_month"]

        tables = constants.ALL_TABLES.value

        # Skip the run when SESNSP has not published a newer month (unless
        # forced). All four tables share the same release month; poll on one.
        has_new_data = poll_source_for_update_task(
            dataset_id=DATASET_ID,
            table_id="municipio_delitos",
            source_max_date=max_ym,
            env="prod",
            date_format="%Y-%m",
        )
        if not has_new_data and not force_run:
            return

        # Dev: upload staging + materialize/test.
        for table in tables:
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

        if not materialize_to_prod:
            return

        # Prod: upload staging + materialize/test.
        for table in tables:
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
            # All four tables share one raw source and one release month; commit
            # the source update once, last, after prod succeeded.
            commit_source_update_task(
                dataset_id=DATASET_ID,
                table_id="municipio_delitos",
                source_max_date=max_ym,
                env="prod",
                date_format="%Y-%m",
            )
    finally:
        # Covers both early returns (no new data, dev-only) and any exception.
        # The k8s work pool gives each run a fresh pod, but a process/local
        # worker reuses its filesystem — the download is several hundred MB.
        shutil.rmtree(work_dir, ignore_errors=True)


# SESNSP releases monthly, ~20th of the month, Mexico City time. Poll across a
# few days; the source-poll guard no-ops until a new month actually appears.
# pyrefly: ignore [missing-attribute]
mx_sesnsp_incidencia_delictiva_flow.deploy_schedules = [
    {"cron": "0 9 20,21,22,23,24 * *", "timezone": "America/Mexico_City"}
]
# The municipal melt holds ~1.7M rows in pandas; give the worker headroom.
# pyrefly: ignore [missing-attribute]
mx_sesnsp_incidencia_delictiva_flow.job_variables = {"memory": "8Gi"}
