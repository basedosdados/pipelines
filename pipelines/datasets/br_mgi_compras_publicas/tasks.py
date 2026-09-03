"""Prefect task wrappers over the pure harvest/clean functions.

The functions themselves live in `utils.py` and `harvest.py` with no Prefect
imports, so the one-shot backfill under `models/br_mgi_compras_publicas/code/`
and this flow share one implementation and cannot drift.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

from google.cloud import storage
from prefect import get_run_logger, task

from pipelines.datasets.br_mgi_compras_publicas.constants import constants
from pipelines.datasets.br_mgi_compras_publicas.dicionario import (
    build_dicionario,
)
from pipelines.datasets.br_mgi_compras_publicas.harvest import (
    consolidate_table,
    harvest_table,
    plan_jobs,
    year_orgao_pairs,
)
from pipelines.datasets.br_mgi_compras_publicas.utils import TABLE_SPECS
from pipelines.utils.gcs import get_credentials_from_env

DATASET_ID = "br_mgi_compras_publicas"


@task(retries=1, retry_delay_seconds=300)
def refresh_table(
    table: str,
    output_dir: str,
    since: dt.date | None = None,
    max_workers: int | None = None,
) -> str:
    """Harvest one table's recent window and consolidate it to parquet.

    Returns the directory the parquet was written to, for upload_to_gcs.
    """
    logger = get_run_logger()
    root = Path(output_dir)
    spec = TABLE_SPECS[table]

    # A chunk on disk is never re-fetched, which is what makes the one-shot
    # backfill resumable -- and what would make a scheduled refresh a no-op. The
    # window is re-read every run precisely because the source revises rows in
    # place, so last run's chunks have to go or the revisions never arrive.
    chunk_dir = root / "_chunks" / table
    if chunk_dir.is_dir():
        stale = list(chunk_dir.glob("*.parquet"))
        for f in stale:
            f.unlink()
        logger.info(
            "%s: cleared %d chunk(s) from a previous run", table, len(stale)
        )

    year_orgaos = None
    orgaos = None
    if spec.window.value == "year_orgao":
        year_orgaos = year_orgao_pairs(root)
    elif spec.window.value == "orgao":
        # /modulo-contratos/ has no date-only entry point, so the refresh
        # iterates orgaos. Only the 462 known to hold contracts, not all 15,007.
        orgaos = list(constants.CONTRATO_ORGAOS.value)

    stats = harvest_table(
        table,
        root,
        max_workers=max_workers,
        orgaos=orgaos,
        year_orgaos=year_orgaos,
        since=since,
        extraction_date=dt.date.today(),
    )
    logger.info(
        "%s: %s rows, %s jobs, %s failed",
        table,
        f"{stats['rows']:,}",
        stats["ran"],
        stats["failures"],
    )
    # A partial refresh must not reach BigQuery: the upload would overwrite a
    # complete partition with a short one, and every dbt test would still pass.
    if stats["failures"]:
        raise RuntimeError(
            f"{table}: {stats['failures']} job(s) failed; refusing to publish a "
            "partial refresh"
        )

    jobs = plan_jobs(spec, orgaos=orgaos, year_orgaos=year_orgaos, since=since)
    merged = consolidate_table(table, root, jobs=jobs)
    if merged.get("missing"):
        raise RuntimeError(
            f"{table}: {merged['missing']} chunk(s) missing after harvest"
        )
    logger.info("%s: consolidated %s rows", table, f"{merged['rows']:,}")
    return str(root / "output" / table)


@task(retries=1, retry_delay_seconds=120)
def rebuild_dicionario(
    output_dir: str, _after: list[str] | None = None
) -> str:
    """Rebuild the dicionario from every harvested table's chunks.

    Args:
        output_dir: scratch root holding ``_chunks/`` and ``output/``.
        _after: results of the table refreshes, to order this task after them --
            the dictionary is derived from their chunks, so it must run last.

    Returns:
        The directory the parquet was written to.
    """
    logger = get_run_logger()
    root = Path(output_dir)
    rows = build_dicionario(root)
    logger.info("dicionario: %s key/value pairs", f"{rows:,}")
    return str(root / "output" / "dicionario")


@task(retries=2, retry_delay_seconds=30)
def clear_staging_partitions(
    table: str, data_path: str, bucket_name: str
) -> str:
    """Delete exactly the staging partitions this refresh is about to replace.

    An incremental refresh cannot use ``dump_mode="overwrite"``: that drops the
    whole ``staging/<ds>/<table>/`` prefix, so a trailing-window harvest would
    rebuild the table from the window alone and destroy every earlier year.

    Plain ``dump_mode="append"`` is not enough either. ``consolidate_table``
    writes ``data-{i}.parquet`` per partition, so a year that previously needed
    three files and now needs two leaves ``data-2.parquet`` behind holding the
    *old* rows -- silently double counting. Clearing the partition prefix first
    removes those orphans, and append then writes the partition whole.

    Returns ``data_path`` so the caller can order the upload after this task.
    """
    logger = get_run_logger()
    local = Path(data_path)
    partitions = sorted(
        p.name for p in local.iterdir() if p.is_dir() and "=" in p.name
    )
    if not partitions:
        raise RuntimeError(
            f"{table}: {local} holds no hive partitions; refusing to clear "
            "staging, since that would leave the table inconsistent"
        )

    credentials = get_credentials_from_env(
        mode="prod" if bucket_name == "basedosdados" else "staging"
    )
    client = storage.Client(project=bucket_name, credentials=credentials)
    bucket = client.bucket(bucket_name, user_project=bucket_name)

    removed = 0
    for partition in partitions:
        prefix = f"staging/{DATASET_ID}/{table}/{partition}/"
        blobs = list(client.list_blobs(bucket, prefix=prefix))
        if blobs:
            bucket.delete_blobs(blobs)
            removed += len(blobs)
    logger.info(
        "%s: cleared %d stale blob(s) across %d partition(s) in %s: %s",
        table,
        removed,
        len(partitions),
        bucket_name,
        ", ".join(partitions),
    )
    return data_path
