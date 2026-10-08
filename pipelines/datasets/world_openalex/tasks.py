"""Prefect 3 tasks for world_openalex — thin wrappers over utils and loader."""

import tempfile
from pathlib import Path

from prefect import task

from pipelines.datasets.world_openalex import loader, utils


@task(retries=3, retry_delay_seconds=60)
def get_release_date() -> str:
    """Return the date of the snapshot release currently on S3 (``YYYY-MM-DD``)."""
    return utils.release_date(utils.fetch_manifest())


@task
def load_snapshot_task(
    bucket_name: str, workers: int = 3, fresh: bool = False
) -> dict:
    """Stream the whole snapshot into ``gs://<bucket>/staging/world_openalex/``.

    Resumes from the GCS markers of an interrupted run of the same release and
    code; starts fresh otherwise (a new quarterly release always does).

    Args:
        bucket_name: ``basedosdados-dev`` or ``basedosdados``.
        workers: Source files processed in parallel.
        fresh: Start fresh even when a resumable run exists.

    Returns:
        Release date, rows per table and record-count checks per entity.
    """
    scratch = Path(tempfile.mkdtemp(prefix="world_openalex_"))
    result = loader.load_snapshot(
        bucket_name=bucket_name, scratch=scratch, workers=workers, fresh=fresh
    )
    for table, n in sorted(result["rows"].items()):
        print(f"{table}: {n:,} rows")
    return result
