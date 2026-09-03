"""Prefect 3 tasks for us_state_foreign_assistance — thin wrappers over utils.py."""

import shutil
from pathlib import Path

import basedosdados as bd
from prefect import task

from pipelines.datasets.us_state_foreign_assistance.utils import (
    clean_all,
    download_all,
    source_last_modified,
)


@task(retries=2, retry_delay_seconds=60)
def check_source_release() -> str:
    """Return the release date of the complete file as ``YYYY-MM-DD``.

    ForeignAssistance.gov replaces the whole history on every release and the
    website's "Data last updated" label is compiled into its JavaScript bundle,
    so the S3 ``Last-Modified`` header of ``us_foreign_aid_complete.csv`` is the
    only machine-readable freshness signal. A HEAD request, no download.
    """
    return source_last_modified("transaction").isoformat()


@task(retries=2, retry_delay_seconds=120)
def download_source(work_dir: str) -> str:
    """Stream the two raw CSVs (3.75 GB + 35 MB) into ``<work_dir>/input``."""
    input_dir = Path(work_dir) / "input"
    download_all(input_dir)
    return str(input_dir)


@task
def clean_source(work_dir: str, input_dir: str) -> dict:
    """Build the three tables under ``<work_dir>/output``.

    Returns a mapping of table slug to its output directory plus ``"rows"``
    (rows written per table), for logging and assertions.
    """
    output_dir = Path(work_dir) / "output"
    counts = clean_all(
        Path(input_dir),
        output_dir,
        memory_limit="6GB",
        threads=4,
        db_path=Path(work_dir) / "duckdb.db",
    )
    # The raw CSVs are 3.8 GB and nothing downstream reads them; free the pod's
    # ephemeral disk before the upload.
    shutil.rmtree(input_dir, ignore_errors=True)
    result: dict = {t: str(output_dir / t) for t in counts}
    result["rows"] = counts
    return result


@task
def clear_staging_blobs(
    dataset_id: str, table_id: str, bucket_name: str
) -> None:
    """Delete the table's ``staging/`` blobs so the next upload is a full replace.

    Every ForeignAssistance.gov release restates the full history, so the
    staging files must be replaced, not appended. ``upload_to_gcs`` with
    ``dump_mode="overwrite"`` would do that, but it calls ``tb.delete(mode="all")``,
    which drops the materialised production table — even from a dev-only run.
    Clearing the blobs first and then appending keeps the staging table (and its
    schema) in place and never touches the production table.
    """
    st = bd.Storage(
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name=bucket_name,
        billing_project_id=bucket_name,
    )
    st.delete_table(mode="staging", bucket_name=bucket_name, not_found_ok=True)
    print(f"cleared gs://{bucket_name}/staging/{dataset_id}/{table_id}/")
