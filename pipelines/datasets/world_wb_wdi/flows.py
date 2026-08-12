"""
Flows for world_wb_wdi — Prefect 3.

World Bank World Development Indicators (WDI). The bulk WDI_CSV.zip carries the
full history on every release, so each run is a **full replace**
(dump_mode="overwrite"), not an incremental append. A single flow downloads once
and rebuilds all six tables.

The data is annual: the source poll compares the latest year in the source
against what is registered and short-circuits the run when the World Bank has not
published a new year, which makes a scheduled run a cheap no-op between the yearly
updates. WDI is CC BY 4.0 and fully open, so every table is AllFree — no BD Pro
paywall (the rolling-window paywall applies only to monthly-or-faster tables).

Deploy: `.github/scripts/deploy_flows.py` auto-discovers `world_wb_wdi_flow`; the
dev pool ignores the schedule, the prod pool activates it.
"""

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.world_wb_wdi.constants import constants
from pipelines.datasets.world_wb_wdi.tasks import clean_wdi, download_wdi
from pipelines.utils.metadata.domain import (
    AllFree,
    DateFormat,
    NonHistorical,
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

# Coverage spec per table. WDI is fully open -> every dated table is AllFree.
# The three dateless reference tables (indicators, country_indicator,
# dicionario) take NonHistorical (a single last-modified coverage).
_YEAR = AllFree(date_column=YearOnly(col="year"), date_format=DateFormat.YEAR)
_COVERAGE = {
    "data": _YEAR,
    "footnote": _YEAR,
    "indicator_time": _YEAR,
    "indicators": NonHistorical(),
    "country_indicator": NonHistorical(),
    "dicionario": NonHistorical(),
}


@flow(name="world_wb_wdi", log_prints=True)
def world_wb_wdi_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Download the World Bank WDI archive, rebuild all six tables, materialize them.

    WDI ships the full history on every release, so each run is a full replace
    (``dump_mode="overwrite"``) rather than an incremental append. The source poll
    short-circuits the run when the World Bank has not published a new year, which
    makes a scheduled run a cheap no-op between the annual updates.

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
        prefix="Dump: ", dataset_id=DATASET_ID, table_id="data"
    )

    work_dir = tempfile.mkdtemp(prefix="world_wb_wdi_")
    try:
        input_dir = download_wdi(work_dir=work_dir)
        result = clean_wdi(work_dir=work_dir, input_dir=input_dir)
        max_year = str(result["max_year"])

        # Skip the run when the World Bank has not published a newer year.
        # force_run bypasses the poll entirely: the poll resolves the table via
        # its prod-backend cloud table, which does not exist until the metadata
        # migration repoints it to world_wb_wdi, so a forced dev test run must
        # not call it.
        if not force_run:
            has_new_data = poll_source_for_update_task(
                dataset_id=DATASET_ID,
                table_id="data",
                source_max_date=max_year,
                env="prod",
                date_format="%Y",
            )
            if not has_new_data:
                return

        # Comita o Update da fonte já aqui, antes de baixar/materializar: se o
        # flow falhar no meio, o metadado da fonte ainda reflete que havia dado
        # novo publicado, mesmo que a tabela não tenha sido atualizada.
        commit_source_update_task(
            dataset_id=DATASET_ID,
            table_id="data",
            source_max_date=max_year,
            env="prod",
            date_format="%Y",
            update_metadata=update_metadata,
            materialize_after_dump=materialize_to_prod,
        )

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
    finally:
        # Covers both early returns (no new data, dev-only) and any exception.
        # The k8s work pool gives each run a fresh pod, but a process/local
        # worker reuses its filesystem — the download is ~270MB and the melt
        # holds tens of millions of rows.
        shutil.rmtree(work_dir, ignore_errors=True)


# WDI data is annual; the World Bank refreshes the bulk archive roughly
# quarterly. Poll on the 15th of March, June, September and December at 16:00 BRT
# — the source-poll guard no-ops until a new year of data actually appears, so
# this ingests the annual update whenever the World Bank publishes it.
# pyrefly: ignore [missing-attribute]
world_wb_wdi_flow.deploy_schedules = [
    {"cron": "0 16 15 3,6,9,12 *", "timezone": "America/Sao_Paulo"}
]
# The clean step melts the wide file into ~26M rows in pandas; give the worker
# headroom.
# pyrefly: ignore [missing-attribute]
world_wb_wdi_flow.job_variables = {"memory": "16Gi"}
