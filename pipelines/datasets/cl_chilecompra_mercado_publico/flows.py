"""Recurring pipeline for cl_chilecompra_mercado_publico (ChileCompra / Mercado Público).

ChileCompra rebuilds its bulk monthly files every day between 12:00 and 14:00 Chile
time, and it rewrites *old* months in place rather than only the current one. The flow
therefore does not ask "is there a new period?" -- it asks "which months did the
publisher touch?", by HEADing every monthly blob and keeping those whose Last-Modified
falls inside the lookback window. Each selected month is re-cleaned and its parquet
partition overwritten in place, so a revision replaces the old rows instead of doubling
them.
"""

from __future__ import annotations

import shutil
import tempfile

from prefect import flow, get_run_logger

from pipelines.datasets.cl_chilecompra_mercado_publico.constants import (
    constants,
)
from pipelines.datasets.cl_chilecompra_mercado_publico.tasks import (
    download_and_clean_task,
    select_stale_months_task,
    source_max_date_task,
    survey_source_task,
)
from pipelines.utils.metadata.domain import (
    DateFormat,
    FreeLag,
    PartBdpro,
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
ALL_TABLES = constants.ALL_TABLES.value

# Every table refreshes weekly, so the house rule paywalls the most recent window of
# each one and leaves everything older free.
_PART_BDPRO = dict(
    date_column=YearMonth(year="ano", month="mes"),
    date_format=DateFormat.YEAR_MONTH,
    free_lag=FreeLag(unit="months", value=6),
)
COVERAGE = {
    "orden_compra_item": PartBdpro(**_PART_BDPRO),
    "licitacion_item": PartBdpro(**_PART_BDPRO),
    "licitacion_oferta": PartBdpro(**_PART_BDPRO),
}


@flow(name="cl_chilecompra_mercado_publico")
def cl_chilecompra_mercado_publico_flow(
    lookback_days: int = constants.DEFAULT_LOOKBACK_DAYS.value,
    force_all: bool = False,
    force_run: bool = False,
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
):
    """Refresh the months ChileCompra revised since the last run.

    Args:
        lookback_days: treat a month as stale when its blob was modified this recently.
        force_all: re-ingest the entire 2007-present history (~27 GB download).
        force_run: continue even when no month looks stale.
        materialize_to_prod: also upload and materialize in the production project.
        update_metadata: write coverage, table Update and raw-source Update records.
    """
    logger = get_run_logger()
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id="mercado_publico"
    )

    scratch_root = tempfile.mkdtemp(prefix="cl_chilecompra_")
    try:
        manifest = survey_source_task()
        max_date = source_max_date_task(manifest)
        logger.info(
            "source exposes %d monthly files, latest coverage %s",
            len(manifest),
            max_date,
        )

        # Recorded for metadata hygiene (it writes the Poll), but deliberately NOT the gate:
        # a retroactive rewrite of a closed month leaves the source's max coverage date
        # unchanged, so gating on it would make the pipeline ignore real revisions.
        poll_source_for_update_task(
            dataset_id=DATASET_ID,
            table_id="orden_compra_item",
            source_max_date=max_date,
            env="prod",
            date_format="%Y-%m-%d",
        )

        stale = select_stale_months_task(manifest, lookback_days, force_all)
        logger.info(
            "%d month-files modified within %d days: %s",
            len(stale),
            lookback_days,
            [f"{e['kind']} {e['year']}-{e['month']:02d}" for e in stale],
        )
        if not stale and not force_run:
            logger.info("Não há novas atualizações na fonte original")
            return

        ingested = []
        for entry in stale:
            ingested.append(download_and_clean_task(entry, scratch_root))
        logger.info("ingested %d month-files: %s", len(ingested), ingested)

        touched = sorted(
            {t for e in stale for t in constants.TABLES.value[e["kind"]]}
        )
        output_root = f"{scratch_root}/output"

        # dev: upload and run every table first, then test. Interleaving run and test per
        # table fails cross-table tests whose sibling model has not been built yet.
        for table in touched:
            upload_to_gcs(
                data_path=f"{output_root}/{table}",
                dataset_id=DATASET_ID,
                table_id=table,
                bucket_name="basedosdados-dev",
                # append, never overwrite: overwrite drops the staging *and* production
                # table. Partition paths are deterministic, so re-uploading a revised month
                # replaces exactly that partition's file.
                dump_mode="append",
                source_format="parquet",
            )
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table,
                dbt_command="run",
                target="dev",
            )
        for table in touched:
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table,
                dbt_command="test",
                target="dev",
            )

        if not materialize_to_prod:
            return

        for table in touched:
            upload_to_gcs(
                data_path=f"{output_root}/{table}",
                dataset_id=DATASET_ID,
                table_id=table,
                bucket_name="basedosdados",
                dump_mode="append",
                source_format="parquet",
            )
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table,
                dbt_command="run",
                target="prod",
            )
        for table in touched:
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table,
                dbt_command="test",
                target="prod",
            )

        if not update_metadata:
            return

        for table in touched:
            register_table_materialization_task(
                dataset_id=DATASET_ID,
                table_id=table,
                coverage=COVERAGE[table],
                env="prod",
                bq_project="basedosdados",
            )
        # Last, and only after production succeeded.
        commit_source_update_task(
            dataset_id=DATASET_ID,
            table_id="orden_compra_item",
            source_max_date=max_date,
            env="prod",
            date_format="%Y-%m-%d",
        )

    finally:
        # Covers the early returns as well as any exception. The k8s pool gives each
        # run a fresh pod, but a process or local worker reuses its filesystem, and a
        # run can materialise ~19 months of parquet.
        shutil.rmtree(scratch_root, ignore_errors=True)


# Weekly rather than daily: ChileCompra rewrites a 15-month trailing window of purchase
# orders every day, so a daily run would re-ingest ~19 month-files and fully rebuild an
# 80M-row table each time. Monday 18:23 São Paulo is comfortably after the publisher's
# 12:00-14:00 Chile rebuild window, and the minute is one no other flow uses.
# pyrefly: ignore [missing-attribute]
cl_chilecompra_mercado_publico_flow.deploy_schedules = [
    {"cron": "23 18 * * 1", "timezone": "America/Sao_Paulo"}
]
# pyrefly: ignore [missing-attribute]
cl_chilecompra_mercado_publico_flow.job_variables = {"memory": "16Gi"}
