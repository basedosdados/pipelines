"""Prefect task wrappers for us_ffiec_bank_reporting.

Thin by design: every one of these delegates to `utils.py`, which holds the
pure functions the one-shot onboarding scripts also use.
"""

from __future__ import annotations

from prefect import task

from pipelines.datasets.us_ffiec_bank_reporting import utils
from pipelines.datasets.us_ffiec_bank_reporting.common import OUTPUT_DIR


@task(retries=1)
def latest_source_quarter_task() -> tuple[int, int]:
    """Ask the CDR which Call Report period is the newest it publishes."""
    return utils.latest_source_quarter()


@task
def quarter_to_coverage_date_task(period: tuple[int, int]) -> str:
    return utils.quarter_to_coverage_date(period)


@task(retries=1)
def refresh_quarterly_task(last: tuple[int, int], trailing: int) -> str:
    """Download and re-clean the trailing quarters; returns the output root."""
    utils.refresh_quarterly(last=last, trailing=trailing)
    return str(OUTPUT_DIR)


@task(retries=1)
def refresh_cra_task(last_year: int, trailing_years: int) -> str:
    """Download and re-clean the trailing CRA years; returns the output root."""
    utils.refresh_cra(last_year=last_year, trailing_years=trailing_years)
    return str(OUTPUT_DIR)


@task
def table_path_task(output_root: str, table_id: str) -> str:
    """Hive-partitioned directory for one table, as upload_to_gcs wants it."""
    return f"{output_root}/{table_id}"
