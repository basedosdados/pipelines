"""Prefect 3 tasks for us_bls_oes — thin wrappers over utils.py."""

from pathlib import Path

from prefect import task

from pipelines.datasets.us_bls_oes.constants import constants
from pipelines.datasets.us_bls_oes.utils import (
    assert_dictionary_labels,
    clean_year,
    download_release,
    latest_source_year,
)


@task(retries=2, retry_delay_seconds=60)
def resolve_latest_year() -> int:
    """Read the OEWS tables page and return the newest published reference year.

    Retries: www.bls.gov intermittently rate-limits.

    Returns:
        Four-digit reference year of the newest release.
    """
    year = latest_source_year()
    print(f"newest OEWS release on the tables page: May {year}")
    return year


@task(retries=2, retry_delay_seconds=60)
def download_oes(work_dir: str, year: int) -> str:
    """Download one OEWS release.

    Args:
        work_dir: Directory to download into; files land in ``<work_dir>/input``.
        year: Reference year, from :func:`resolve_latest_year`.

    Returns:
        The input directory path, as a string (Prefect serializes task results).
    """
    input_dir = Path(work_dir) / "input"
    download_release(input_dir, year)
    return str(input_dir)


@task
def clean_oes(work_dir: str, input_dir: str, year: int) -> dict:
    """Clean one release into the `area` and `industry` partitions for that year.

    Only the new year is cleaned. The tables are partitioned by year and the
    staging object path carries the partition, so re-running a year overwrites
    that partition and leaves every earlier one untouched.

    Args:
        work_dir: Directory to write into; tables land under ``<work_dir>/output``.
        input_dir: Directory holding the downloaded zips, from
            :func:`download_oes`.
        year: Reference year.

    Returns:
        Table slug to its partitioned output directory, plus ``"year"`` and
        ``"rows"`` for logging.
    """
    output_dir = Path(work_dir) / "output"
    counts = clean_year(Path(input_dir), output_dir, year)
    assert_dictionary_labels(output_dir, year)
    print(f"May {year}: " + ", ".join(f"{t}={n:,}" for t, n in counts.items()))
    result: dict = {
        table: str(output_dir / table) for table in constants.DATA_TABLES.value
    }
    result["year"] = year
    result["rows"] = counts
    return result
