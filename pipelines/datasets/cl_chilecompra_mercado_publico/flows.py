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

import os
import shutil
import sys
import tempfile
from pathlib import Path

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


def _rss_mb() -> float:
    """Resident set size in MB — the number the container's memory limit counts.

    Read from /proc on Linux (where the workers run) and via resource elsewhere, and
    never allowed to raise: this is diagnostics, and losing the run to a broken probe
    would be worse than losing the number.
    """
    try:
        with open("/proc/self/statm") as fh:
            return int(fh.read().split()[1]) * os.sysconf("SC_PAGE_SIZE") / 1e6
    except (OSError, IndexError, ValueError):
        pass
    try:
        import resource

        raw = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return raw / 1e6 if sys.platform == "darwin" else raw / 1e3
    except Exception:
        return float("nan")


def _arrow_mb() -> float:
    """Bytes pyarrow currently holds. Locally this returns to ~0 after every month."""
    try:
        import pyarrow as pa

        return pa.default_memory_pool().bytes_allocated() / 1e6
    except Exception:
        return float("nan")


def _dir_mb(path: str) -> float:
    """Size of the scratch tree, to separate disk growth from resident memory."""
    try:
        return (
            sum(f.stat().st_size for f in Path(path).rglob("*") if f.is_file())
            / 1e6
        )
    except OSError:
        return float("nan")


def newest_month_per_kind(manifest: list[dict]) -> list[dict]:
    """The latest month the source exposes, one entry per kind.

    Used only by ``force_run`` when nothing is stale, to give the run something real to
    do. One month per kind is the smallest slice that still covers every table.
    """
    newest: dict[str, dict] = {}
    for entry in manifest:
        key = entry["kind"]
        current = newest.get(key)
        if current is None or (entry["year"], entry["month"]) > (
            current["year"],
            current["month"],
        ):
            newest[key] = entry
    return [newest[k] for k in sorted(newest)]


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

        # Recorded for metadata hygiene (it writes the Poll), but deliberately NOT the
        # gate: a retroactive rewrite of a closed month leaves the source's max coverage
        # date unchanged, so gating on it would make the pipeline ignore real revisions.
        #
        # Gated on update_metadata because the task is pinned env="prod" no matter which
        # pool the run is on. Ungated, a validation run on the dev pool with
        # update_metadata=False still writes a Poll into PRODUCTION metadata -- the one
        # prod write a "dev, no metadata" run is supposed to be incapable of making.
        if update_metadata:
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
        if not stale:
            if not force_run:
                logger.info("Não há novas atualizações na fonte original")
                return
            # force_run means "run even though nothing looks stale", and its whole
            # purpose is validating the flow end to end. Proceeding with an empty list
            # ingested nothing, uploaded nothing and ran no dbt, yet still reached the
            # metadata block and committed a source Update -- a green run proving
            # nothing. Fall back to the newest month of each kind so a forced run
            # actually exercises download, clean, upload and dbt.
            stale = newest_month_per_kind(manifest)
            logger.info(
                "force_run with nothing stale: falling back to %s",
                [f"{e['kind']} {e['year']}-{e['month']:02d}" for e in stale],
            )

        ingested = []
        for entry in stale:
            ingested.append(download_and_clean_task(entry, scratch_root))
            # The first dev run was OOMKilled at 16 GiB after four months, and the
            # transform does not explain it: profiled locally over seven months it peaks
            # at 2.5 GB with pyarrow's pool returning to zero every month. Log the curve
            # where it actually fails, so the next failure says whether memory climbs
            # steadily, jumps at one month, or spikes somewhere else entirely.
            logger.info(
                "after %s %d-%02d: RSS %.0f MB, arrow %.0f MB, scratch %.0f MB",
                entry["kind"],
                entry["year"],
                entry["month"],
                _rss_mb(),
                _arrow_mb(),
                _dir_mb(scratch_root),
            )
        logger.info("ingested %d month-files: %s", len(ingested), ingested)

        touched = sorted(
            {t for e in stale for t in constants.TABLES.value[e["kind"]]}
        )
        if not touched:
            # Nothing was built, so there is nothing to record. Falling through would
            # register a materialization and commit a source Update for a run that
            # ingested no rows, advancing the source watermark past data never loaded.
            logger.warning(
                "no table was touched; skipping upload and metadata"
            )
            return
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
