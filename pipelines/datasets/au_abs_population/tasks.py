"""Prefect 3 tasks for au_abs_population — thin wrappers over utils.py."""

from pathlib import Path

from prefect import task

from pipelines.datasets.au_abs_population.utils import clean_all, download_all


@task(retries=2, retry_delay_seconds=30)
def download_population(work_dir: str) -> dict:
    """Download the current release of all three ABS products into ``work_dir``.

    Retries twice: www.abs.gov.au intermittently drops one of the larger
    workbooks, and the download is 45 files across three products.
    """
    input_dir = Path(work_dir) / "input"
    input_dir.mkdir(parents=True, exist_ok=True)
    slugs = download_all(str(input_dir))
    print(f"release slugs: {slugs}")
    return {"input_dir": str(input_dir), "slugs": slugs}


@task
def clean_population(work_dir: str, input_dir: str) -> dict:
    """Build every table under ``<work_dir>/output``.

    Returns each table's partition root plus the two source coverage dates that
    drive the polls: ``max_year_quarter`` for the quarterly 3101.0 series and
    ``max_year`` for the annual 3218.0 regional series.
    """
    output_dir = Path(work_dir) / "output"
    return clean_all(input_dir, str(output_dir))
