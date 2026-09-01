"""Prefect task wrappers over the pure harvest/clean functions.

The functions themselves live in `utils.py` and `harvest.py` with no Prefect
imports, so the one-shot backfill under `models/br_mgi_compras_publicas/code/`
and this flow share one implementation and cannot drift.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

from prefect import get_run_logger, task

from pipelines.datasets.br_mgi_compras_publicas.harvest import (
    consolidate_table,
    harvest_table,
    plan_jobs,
    year_orgao_pairs,
)
from pipelines.datasets.br_mgi_compras_publicas.utils import TABLE_SPECS


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

    year_orgaos = None
    if spec.window.value == "year_orgao":
        year_orgaos = year_orgao_pairs(root)

    stats = harvest_table(
        table,
        root,
        max_workers=max_workers,
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

    jobs = plan_jobs(spec, year_orgaos=year_orgaos, since=since)
    merged = consolidate_table(table, root, jobs=jobs)
    if merged.get("missing"):
        raise RuntimeError(
            f"{table}: {merged['missing']} chunk(s) missing after harvest"
        )
    logger.info("%s: consolidated %s rows", table, f"{merged['rows']:,}")
    return str(root / "output" / table)
