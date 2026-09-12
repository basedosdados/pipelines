"""Prefect 3 tasks for us_nsf_ncses — thin wrappers over utils.py."""

from pathlib import Path

from prefect import task

from pipelines.datasets.us_nsf_ncses.utils import (
    clean_herd,
    clean_sed,
    download_herd,
    download_sed,
    latest_herd_year,
    latest_sed_publication,
)


@task(retries=2, retry_delay_seconds=60)
def check_source() -> dict:
    """Read the newest HERD fiscal year and SED cycle NCSES has published.

    Returns:
        ``herd_year``, ``sed_year`` and ``sed_publication_id``.
    """
    herd_year = latest_herd_year()
    sed_year, publication_id = latest_sed_publication()
    print(
        f"NCSES publishes HERD through FY{herd_year} and SED cycle "
        f"{sed_year} ({publication_id})"
    )
    return {
        "herd_year": herd_year,
        "sed_year": sed_year,
        "sed_publication_id": publication_id,
    }


@task(retries=2, retry_delay_seconds=60)
def download_all(work_dir: str, source: dict) -> str:
    """Download every HERD public use file and the newest SED cycle.

    Both surveys restate their own history on each release — HERD retro-imputes
    prior years and an SED cycle republishes its whole series — so the refresh
    downloads the full set rather than only the new year.

    Args:
        work_dir: Directory to download into; files land in ``<work_dir>/input``.
        source: The result of :func:`check_source`.

    Returns:
        The input directory path, as a string (Prefect serializes task results).
    """
    input_dir = Path(work_dir) / "input"
    download_herd(input_dir, source["herd_year"])
    download_sed(input_dir, source["sed_year"], source["sed_publication_id"])
    return str(input_dir)


@task
def clean_all(work_dir: str, input_dir: str) -> dict:
    """Build all seven tables from the downloaded files.

    Args:
        work_dir: Directory to write into; tables land under ``<work_dir>/output``.
        input_dir: Directory holding the downloads, from :func:`download_all`.

    Returns:
        Table slug -> its partitioned output directory, as strings.
    """
    root = Path(work_dir)
    produced = clean_herd(Path(input_dir), root / "output")
    produced.update(clean_sed(Path(input_dir), root / "output", root / "work"))
    return {table: str(path) for table, path in produced.items()}
