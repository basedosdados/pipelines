"""Prefect 3 tasks for us_treasury_usaspending — thin wrappers over utils.py."""

from pathlib import Path

from prefect import task

from pipelines.datasets.us_treasury_usaspending.constants import constants
from pipelines.datasets.us_treasury_usaspending.utils import (
    clean_archive,
    download_archive,
    latest_stamp,
)


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
