"""Prefect 3 tasks for us_eia_consumption — thin wrappers over utils.py."""

import datetime as _dt
from pathlib import Path

import pandas as pd
from prefect import task

from pipelines.datasets.us_eia_consumption.constants import constants
from pipelines.datasets.us_eia_consumption.utils import (
    build_dicionario,
    build_eia861m,
    clean_all,
    download_eia861m,
    download_year,
)


@task(retries=2, retry_delay_seconds=60)
def probe_source(work_dir: str) -> dict:
    """Find the latest annual report year and the latest 861M month, cheaply.

    Two source clocks:

    * **EIA-861** is annual. The newest report year's ZIP is found by trying the
      current calendar year and a few back until one downloads; that ZIP is kept
      for the clean, and the year becomes the annual coverage date.
    * **EIA-861M** is monthly. The single sales_revenue.xlsx (plus the archived
      historical file) is downloaded and the latest (year, month) it carries is
      the monthly coverage date.

    Args:
        work_dir: Scratch directory for this flow run.

    Returns:
        ``{"years": [...], "max_date": {"eia861": "YYYY-01-01",
        "eia861m": "YYYY-MM-01"}}``.

    Raises:
        RuntimeError: If no recent annual ZIP can be downloaded.
    """
    input_dir = Path(work_dir) / "input"
    this_year = _dt.date.today().year
    max_annual = None
    for candidate in range(this_year + 1, this_year - 4, -1):
        if download_year(candidate, input_dir):
            max_annual = candidate
            break
    if max_annual is None:
        raise RuntimeError("no recent EIA-861 annual ZIP could be downloaded")

    download_eia861m(input_dir)
    m = build_eia861m(input_dir)
    period = (
        pd.to_numeric(m["year"], errors="coerce") * 100
        + pd.to_numeric(m["month"], errors="coerce")
    ).max()
    y, mo = int(period) // 100, int(period) % 100

    years = list(range(constants.FIRST_ANNUAL_YEAR.value, max_annual + 1))
    return {
        "years": years,
        "max_date": {
            "eia861": f"{max_annual}-01-01",
            "eia861m": f"{y}-{mo:02d}-01",
        },
    }


@task(retries=2, retry_delay_seconds=60)
def download_corpus(work_dir: str, probe: dict) -> str:
    """Download every annual report year the probe found (861M is already local)."""
    input_dir = Path(work_dir) / "input"
    got = [y for y in probe["years"] if download_year(y, input_dir)]
    print(f"annual ZIPs available: {len(got)} ({min(got)}-{max(got)})")
    return str(input_dir)


@task
def clean_corpus(work_dir: str, input_dir: str) -> dict:
    """Clean every annual year and the monthly table, then the dicionario."""
    output_dir = Path(work_dir) / "output"
    counts = clean_all(Path(input_dir), output_dir, log=print)
    counts["dicionario"] = build_dicionario(output_dir)
    result: dict = {t: str(output_dir / t) for t in constants.ALL_TABLES.value}
    result["row_counts"] = counts
    return result
