"""Prefect tasks for us_fec_campaign_finance — thin wrappers over utils."""

from datetime import date

from prefect import task

from pipelines.datasets.us_fec_campaign_finance.constants import constants
from pipelines.datasets.us_fec_campaign_finance.utils import (
    current_cycle,
    refresh_cycle,
)


@task(retries=2, retry_delay_seconds=120)
def refresh_current_cycle(work_dir: str, cycle: int | None = None) -> dict:
    """Download and clean the current election cycle for every refreshed table.

    Returns ``{table: <dir to hand to upload_to_gcs>}`` plus ``max_date``, the
    latest transaction date across the refreshed tables.
    """
    from pathlib import Path

    target = cycle or current_cycle(date.today())
    print(f"refreshing FEC cycle {target}")
    result = refresh_cycle(
        cycle=target,
        work_dir=Path(work_dir),
        tables=constants.ALL_TABLES.value,
    )
    print(
        f"cycle {target}: refreshed {sorted(k for k in result if k != 'max_date')}"
    )
    print(
        f"cycle {target}: source max transaction_date = {result['max_date']}"
    )
    return result
