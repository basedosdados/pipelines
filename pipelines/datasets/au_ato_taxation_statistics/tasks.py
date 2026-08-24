"""Prefect 3 tasks for au_ato_taxation_statistics — wrappers over utils.py."""

from pathlib import Path

from prefect import task

from pipelines.datasets.au_ato_taxation_statistics.utils import (
    clean_all,
    download_all,
    release_year,
)


@task(retries=2, retry_delay_seconds=30)
def download_taxstats(work_dir: str) -> dict:
    """Download every curated workbook for every in-scope release.

    Retries twice: data.gov.au occasionally drops connections partway through
    the ~40 file downloads.

    Args:
        work_dir: Directory to download into; files land in ``<work_dir>/input``.

    Returns:
        A mapping with ``"input_dir"`` and ``"max_year"`` — the start year of
        the newest release, which drives the source-update poll.
    """
    input_dir = Path(work_dir) / "input"
    releases = download_all(input_dir)
    return {
        "input_dir": str(input_dir),
        "max_year": str(release_year(releases[-1])),
    }


@task
def clean_taxstats(work_dir: str, input_dir: str) -> dict:
    """Build every partitioned table from the downloaded workbooks.

    Args:
        work_dir: Directory to write into; tables land under ``<work_dir>/output``.
        input_dir: Directory holding the workbooks, from :func:`download_taxstats`.

    Returns:
        A mapping of table slug to its partitioned output directory.
    """
    output_dir = Path(work_dir) / "output"
    counts = clean_all(Path(input_dir), output_dir)
    for table, rows in sorted(counts.items()):
        print(f"{table}: {rows:,} rows")
    return {table: str(output_dir / table) for table in counts}
