"""Prefect 3 tasks for au_abs_prices_inflation — wrappers over the pure transforms."""

from pathlib import Path

from prefect import task

from pipelines.datasets.au_abs_prices_inflation.cpi import (
    clean_all as clean_cpi_all,
)
from pipelines.datasets.au_abs_prices_inflation.cpi import (
    download_all,
)
from pipelines.datasets.au_abs_prices_inflation.releases import clean_all
from pipelines.datasets.au_abs_prices_inflation.timeseries import (
    download_release,
)


@task(retries=2, retry_delay_seconds=30)
def download_cpi(work_dir: str) -> str:
    """Download the current ABS CPI release into ``<work_dir>/input``.

    Retries twice: www.abs.gov.au intermittently drops the larger by-city file.
    """
    input_dir = Path(work_dir) / "input"
    input_dir.mkdir(parents=True, exist_ok=True)
    download_all(str(input_dir))
    return str(input_dir)


@task
def clean_cpi(work_dir: str, input_dir: str) -> dict:
    """Build the quarterly and monthly tables under ``<work_dir>/output``.

    Returns the per-table partition roots plus ``"max_year_month"`` (the latest
    ``"YYYY-MM"`` in the monthly table), which drives the source-update poll.
    """
    output_dir = Path(work_dir) / "output"
    # pyrefly: ignore [unnecessary-type-conversion]
    return clean_cpi_all(str(input_dir), str(output_dir))


@task(retries=2, retry_delay_seconds=30)
def download_price_release(release: str, work_dir: str) -> str:
    """Download one ABS release's workbooks into ``<work_dir>/input/<release>``.

    Retries twice: www.abs.gov.au intermittently drops a larger workbook.
    """
    input_dir = Path(work_dir) / "input"
    input_dir.mkdir(parents=True, exist_ok=True)
    download_release(release, str(input_dir))
    return str(input_dir)


@task
def clean_price_release(release: str, work_dir: str, input_dir: str) -> dict:
    """Build one release's table under ``<work_dir>/output``.

    Returns the partition root plus ``"<release>__max_year_month"``, the latest
    period in the table, which drives the source-update poll.
    """
    output_dir = Path(work_dir) / "output"
    # pyrefly: ignore [unnecessary-type-conversion]
    return clean_all(str(input_dir), str(output_dir), releases=[release])
