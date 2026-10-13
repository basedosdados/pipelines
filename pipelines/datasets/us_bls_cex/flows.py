"""
Flows for us_bls_cex — Prefect 3.

Refreshes the LABSTAT published tables (``series``, ``annual``) of the BLS
Consumer Expenditure Surveys. BLS publishes one new year per release, in
September-December of the following year, and the flat files carry the full
history, so each run is a **full replace** (``dump_mode="overwrite"``).

Out of scope: the PUMD microdata, ``ucc`` and ``dicionario`` are loaded once by
``models/us_bls_cex/code/``. Variables change between microdata releases, so
those are re-run by hand; ``dicionario`` also carries the microdata labels, so
the pipeline must never overwrite it.

The release check asks the BLS API for the latest published year before
downloading anything, so the twice-weekly runs in the release window are a few
hundred bytes each until a new year lands.

Deploy: `.github/workflows/scripts/deploy_flows.py` auto-discovers
`us_bls_cex_flow`; the dev pool ignores the schedule, the prod pool deploys it
paused.
"""

import shutil
import tempfile

from prefect.schedules import Cron

from pipelines.datasets.us_bls_cex.constants import constants
from pipelines.datasets.us_bls_cex.tasks import (
    clean_cex,
    download_cex,
    get_latest_year,
)
from pipelines.utils.flow import flow
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
TABLES = constants.LABSTAT_TABLES.value

# Annual data: below the monthly-or-faster threshold for the BD Pro window, so
# fully free. `series` has no date column and takes no coverage spec.
_COVERAGE = {
    "annual": AllFree(
        date_column=YearOnly(col="year"), date_format=DateFormat.YEAR
    ),
}


def _materialize(tables: list[str], result: dict, bucket: str, target: str):
    """Upload and run every table, then test every table.

    Run-all-then-test-all, not per table: ``annual.series_id`` has a
    relationships test against ``series``, which must exist before it runs.
    """
    for table in tables:
        upload_to_gcs(
            data_path=result[table],
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
    for table in tables:
        run_dbt(
            dataset_id=DATASET_ID,
            table_id=table,
            dbt_command="test",
            target=target,
        )


@flow(name="us_bls_cex", log_prints=True)
def us_bls_cex_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Refresh the CE published tables when BLS releases a new year.

    Args:
        materialize_to_prod: Write the prod staging bucket and run dbt against
            ``target="prod"``. Set False to exercise only the dev half — required
            for a safe test run, since the default writes production.
        update_metadata: After a successful prod materialization, register table
            coverage and commit the source update.
        force_run: Materialize even when the source poll reports no new year.
    """
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id="annual"
    )

    latest_year = get_latest_year()
    has_new_data = poll_source_for_update_task(
        dataset_id=DATASET_ID,
        table_id="annual",
        source_max_date=latest_year,
        env="prod",
        date_format="%Y",
        compare_against="coverage",
    )
    if not has_new_data and not force_run:
        print(f"No new CE release: latest published year is {latest_year}")
        return

    work_dir = tempfile.mkdtemp(prefix="us_bls_cex_")
    try:
        input_dir = download_cex(work_dir=work_dir)
        result = clean_cex(work_dir=work_dir, input_dir=input_dir)
        max_year = str(result["max_year"])
        if max_year != latest_year:
            # The flat files and the API disagree on the latest year, e.g. the
            # API updated before download.bls.gov. Do not commit a coverage
            # date the data does not have; the next run will retry.
            raise ValueError(
                f"BLS API reports {latest_year} but the flat files end in {max_year}"
            )

        if not materialize_to_prod:
            # Dev validation path only; nothing downstream reads basedosdados-dev.
            _materialize(TABLES, result, "basedosdados-dev", "dev")
            return

        _materialize(TABLES, result, "basedosdados", "prod")

        if update_metadata:
            for table, coverage in _COVERAGE.items():
                register_table_materialization_task(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    coverage=coverage,
                    env="prod",
                    bq_project="basedosdados",
                )
            # Last, and only after prod succeeded: the source's max coverage.
            commit_source_update_task(
                dataset_id=DATASET_ID,
                table_id="annual",
                source_max_date=max_year,
                env="prod",
                date_format="%Y",
                update_metadata=update_metadata,
                materialize_after_dump=materialize_to_prod,
            )
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


# BLS publishes year Y between September and December of Y+1 (2024 data landed
# 2025-12-19; 2025 data is scheduled for 2026-10-29). Poll Mondays and Thursdays
# in that window at 16:13 BRT; the API check makes a no-news run nearly free.
us_bls_cex_flow.deploy_schedules = [
    Cron("13 16 * 9-12 1,4", timezone="America/Sao_Paulo")
]
# cx.aspect is 13M rows pivoted in arrow; give the pod headroom. `memory` alone
# is ignored by the work pool, so set the limit and request explicitly.
us_bls_cex_flow.job_variables = {
    "memory": "8Gi",
    "memory_limit": "8Gi",
    "memory_request": "2Gi",
}
