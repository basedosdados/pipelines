"""Prefect 3 tasks for us_eia_seds — thin wrappers over utils.py."""

from pathlib import Path

from prefect import task

from pipelines.datasets.us_eia_seds.constants import constants
from pipelines.datasets.us_eia_seds.utils import (
    build_dicionario,
    clean_all,
    download_codes,
    download_complete,
    source_max_date,
)


@task(retries=2, retry_delay_seconds=60)
def probe_source(work_dir: str) -> dict:
    """Download the SEDS complete file and codes, and read its latest year.

    SEDS is one long file that restates the whole 1960-present series on each
    annual release, so the probe simply downloads it (there is no cheaper way to
    learn the latest year than to read the file) and reports the max year as the
    source coverage date.

    Args:
        work_dir: Scratch directory for this flow run.

    Returns:
        ``{"max_date": "YYYY-01-01"}``.
    """
    input_dir = Path(work_dir) / "input"
    download_complete(input_dir)
    download_codes(input_dir)
    max_year = source_max_date(input_dir)
    return {"max_date": f"{max_year}-01-01"}


@task
def clean_corpus(work_dir: str) -> dict:
    """Clean the whole SEDS series to partitioned parquet, then the dicionario.

    Every year partition is rebuilt from the one downloaded file, because SEDS
    restates its entire history on each release; rebuilding makes a double-count
    structurally impossible and keeps the dicionario computed over the whole
    record.

    Args:
        work_dir: Scratch directory for this flow run.

    Returns:
        Table slug -> partitioned output directory (as strings), plus
        ``"row_counts"``.
    """
    input_dir = Path(work_dir) / "input"
    output_dir = Path(work_dir) / "output"
    counts = clean_all(input_dir, output_dir, log=print)
    counts["dicionario"] = build_dicionario(output_dir)
    result: dict = {t: str(output_dir / t) for t in constants.ALL_TABLES.value}
    result["row_counts"] = counts
    return result
