"""Prefect 3 tasks for us_treasury_usaspending — thin wrappers over utils.py."""

from pathlib import Path

import basedosdados as bd
from prefect import task

from pipelines.datasets.us_treasury_usaspending.constants import constants
from pipelines.datasets.us_treasury_usaspending.utils import (
    clean_archive,
    download_archive,
    is_archived,
    latest_stamp,
)


@task(retries=3, retry_delay_seconds=120)
def get_archived_fiscal_years(candidates: list[int]) -> list[int]:
    """The subset of ``candidates`` the archive already lists, in order.

    Raises if none is listed, since then there is nothing to refresh and the
    source has most likely changed shape.
    """
    listed = [fy for fy in candidates if is_archived(fy)]
    if not listed:
        raise RuntimeError(f"no archive files listed for any of {candidates}")
    return listed


@task(retries=3, retry_delay_seconds=30)
def clear_staging_fiscal_year(
    bucket_name: str, table_id: str, fiscal_year: int
) -> int:
    """Delete one fiscal year's staging parquet before it is re-uploaded.

    Staging uses ``dump_mode="append"``, and each parquet is named after its
    archive, which carries the build stamp
    (``FY2026_All_Contracts_Full_20260806.parquet``). Without this, next month's
    build would land beside the old file and every row of the year would be
    counted twice.

    Returns:
        The number of objects deleted.
    """
    st = bd.Storage(
        dataset_id=constants.DATASET_ID.value,
        table_id=table_id,
        bucket_name=bucket_name,
        billing_project_id=bucket_name,
    )
    prefix = (
        f"staging/{constants.DATASET_ID.value}/{table_id}/"
        f"{constants.PARTITION_COLUMN.value}={fiscal_year}/"
    )
    blobs = list(st.bucket.list_blobs(prefix=prefix))
    for blob in blobs:
        blob.delete()
    print(f"removed {len(blobs)} object(s) under gs://{bucket_name}/{prefix}")
    return len(blobs)


@task(retries=3, retry_delay_seconds=120)
def get_latest_stamp(fiscal_year: int) -> str:
    """Publication stamp (YYYYMMDD) of the current Award Data Archive build.

    Args:
        fiscal_year: Fiscal year to query; every file in a build carries the
            same stamp, so any year answers for the whole set.

    Returns:
        The stamp as an ISO date string, e.g. ``"2026-08-06"`` — the form the
        metadata poll and commit tasks expect.
    """
    stamp = latest_stamp(fiscal_year)
    return f"{stamp[:4]}-{stamp[4:6]}-{stamp[6:]}"


@task(retries=3, retry_delay_seconds=300)
def refresh_fiscal_year(
    work_dir: str, fiscal_year: int, stamp_iso: str
) -> dict:
    """Download and clean one fiscal year of both award families.

    Retries generously with a long delay: files.usaspending.gov rate-limits and
    truncates large transfers, and the download helper already resumes within a
    single attempt.

    Args:
        work_dir: Directory to work in; archives land in ``<work_dir>/input``
            and parquet in ``<work_dir>/output``.
        fiscal_year: The fiscal year to refresh.
        stamp_iso: Archive stamp as ``YYYY-MM-DD``, from :func:`get_latest_stamp`.

    Returns:
        A mapping of table slug to its partitioned output directory, plus
        ``"row_counts"`` keyed by ``"<table>/<fiscal_year>"``.
    """
    stamp = stamp_iso.replace("-", "")
    input_dir = Path(work_dir) / "input"
    output_dir = Path(work_dir) / "output"
    result: dict = {"row_counts": {}}

    for family, table in constants.AWARD_FAMILIES.value.items():
        zip_path = download_archive(fiscal_year, family, stamp, input_dir)
        counts = clean_archive(
            zip_path, table, output_dir, expected_fiscal_year=fiscal_year
        )
        # Free the archive as soon as it is cleaned: the two families together
        # run to several GB and the pod does not need to hold both.
        zip_path.unlink(missing_ok=True)
        for fy, n in counts.items():
            result["row_counts"][f"{table}/{fy}"] = n
        result[table] = str(output_dir / table)

    return result
