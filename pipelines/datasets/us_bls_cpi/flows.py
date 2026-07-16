"""
Flows for us_bls_cpi — Prefect 3.

US Consumer Price Index (BLS), CPI-U + CPI-W. The BLS flat files carry the full
history every month, so each run is a **full replace** (dump_mode="overwrite"),
not an incremental append. A single flow downloads once and rebuilds all four
tables. Schedule targets the BLS monthly release window (~2nd week).

Deploy: `.github/scripts/deploy_flows.py` auto-discovers `us_bls_cpi_flow`; the
dev pool ignores the schedule, the prod pool activates it.
"""

import tempfile

from prefect import flow

from pipelines.datasets.us_bls_cpi.constants import constants
from pipelines.datasets.us_bls_cpi.tasks import clean_cpi, download_cpi
from pipelines.utils.metadata.domain import (
    AllFree,
    DateFormat,
    YearMonth,
    YearOnly,
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

# Coverage spec per table (all public → all_free tier).
_COVERAGE = {
    "monthly": AllFree(
        date_column=YearMonth(year="year", month="month"),
        date_format=DateFormat.YEAR_MONTH,
    ),
    "annual": AllFree(
        date_column=YearOnly(col="year"), date_format=DateFormat.YEAR
    ),
    "semiannual": AllFree(
        date_column=YearOnly(col="year"), date_format=DateFormat.YEAR
    ),
}


@flow(name="us_bls_cpi", log_prints=True)
def us_bls_cpi_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id="cpi"
    )

    work_dir = tempfile.mkdtemp(prefix="us_bls_cpi_")
    input_dir = download_cpi(work_dir=work_dir)
    result = clean_cpi(work_dir=work_dir, input_dir=input_dir)
    max_ym = result["max_year_month"]

    # Skip the run when BLS has not published a newer month (unless forced).
    has_new_data = poll_source_for_update_task(
        dataset_id=DATASET_ID,
        table_id="monthly",
        source_max_date=max_ym,
        env="prod",
        date_format="%Y-%m",
    )
    if not has_new_data and not force_run:
        return

    tables = constants.ALL_TABLES.value

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
        commit_source_update_task(
            dataset_id=DATASET_ID,
            table_id="monthly",
            source_max_date=max_ym,
            env="prod",
            date_format="%Y-%m",
        )


# BLS releases CPI monthly, ~2nd week, on a US business day. Poll across a few
# mid-month days at 16:00 BRT; the source-poll guard no-ops until a new month
# actually appears.
us_bls_cpi_flow.deploy_schedules = [
    {"cron": "0 16 10,11,12,13,14,15 * *", "timezone": "America/Sao_Paulo"}
]
# The clean step holds ~4M rows in pandas; give the worker headroom.
us_bls_cpi_flow.job_variables = {"memory": "8Gi"}
