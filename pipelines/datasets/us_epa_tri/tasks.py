"""Prefect 3 tasks for us_epa_tri — thin wrappers over utils.py."""

from pathlib import Path

from prefect import task

from pipelines.datasets.us_epa_tri.utils import (
    clean_all,
    download_facility_fips,
    download_year,
    fetch_page,
    source_processed_date,
    source_years,
)


@task(retries=2, retry_delay_seconds=60)
def check_source_tri() -> dict:
    """Read the poll signal off the Basic Data Files page, no data download.

    Returns ``{"processed_date": "YYYY-MM-DD", "years": [...]}`` — the date EPA
    last regenerated the files and the reporting years on offer. The processed
    date is what the poll compares against ``Table.Update.latest``: EPA bumps
    it both when a new reporting year is published (preliminary, mid-year) and
    when the files are regenerated with revised forms (final, autumn).
    """
    html = fetch_page()
    return {
        "processed_date": source_processed_date(html).isoformat(),
        "years": source_years(html),
    }


@task(retries=1, retry_delay_seconds=300)
def download_tri(work_dir: str, years: list[int]) -> str:
    """Download the national Basic Data File of each year into ``<work_dir>/input``.

    Sequential: Envirofacts streams each ~60 MB file at ~150 KB/s, and every
    file is accepted only when its last row is complete.
    """
    input_dir = Path(work_dir) / "input"
    for y in years:
        download_year(y, input_dir)
    return str(input_dir)


@task(retries=2, retry_delay_seconds=120)
def download_tri_facilities(work_dir: str) -> str:
    """Pull the Envirofacts TRI_FACILITY table (county FIPS per facility)."""
    return str(download_facility_fips(Path(work_dir) / "ref"))


@task
def clean_tri(
    work_dir: str, input_dir: str, facility_fips_path: str, years: list[int]
) -> dict:
    """Clean the downloaded years into partitioned parquet under ``<work_dir>/output``.

    Returns a mapping of table slug to its output directory plus ``counts``,
    ``max_year`` and per-year ``notes``.
    """
    output_dir = Path(work_dir) / "output"
    result = clean_all(
        Path(input_dir), output_dir, Path(facility_fips_path), years=years
    )
    return {
        k: (str(v) if isinstance(v, Path) else v) for k, v in result.items()
    }
