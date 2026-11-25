"""Prefect tasks for ``us_osha_enforcement``.

Thin wrappers over :mod:`pipelines.datasets.us_osha_enforcement.utils`, which
holds the download and cleaning logic and imports no Prefect. The one-shot
onboarding bootstrap under ``models/`` calls the same functions, so the two
paths cannot drift.
"""

from __future__ import annotations

from pathlib import Path

from prefect import get_run_logger, task

from pipelines.datasets.us_osha_enforcement.constants import constants
from pipelines.datasets.us_osha_enforcement.utils import (
    clean_all,
    download_all,
    plan_refresh,
)


@task(retries=2, retry_delay_seconds=300)
def download_osha(work_dir: str) -> str:
    """Download every OSHA bulk zip into ``<work_dir>/input``."""
    log = get_run_logger()
    input_dir = Path(work_dir) / "input"
    paths = download_all(input_dir)
    total = sum(p.stat().st_size for p in paths)
    log.info(f"downloaded {len(paths)} files, {total / 1e9:.1f} GB")
    return str(input_dir)


@task
def plan_osha_refresh(
    input_dir: str, trailing_years: int, modified_days: int
) -> dict:
    """Decide the source max date and which partition years to rebuild."""
    log = get_run_logger()
    plan = plan_refresh(Path(input_dir), trailing_years, modified_days)
    log.info(
        f"source max open_date {plan['max_date']}; "
        f"rebuilding {len(plan['years'])} partition years: "
        f"{min(plan['years'])}-{max(plan['years'])}"
    )
    return plan


@task
def clean_osha(
    input_dir: str, work_dir: str, years: list[int]
) -> dict[str, str]:
    """Clean the selected partition years, returning ``{table: output dir}``."""
    log = get_run_logger()
    output_dir = Path(work_dir) / "output"
    counts = clean_all(Path(input_dir), output_dir, years=set(years))
    for slug, rows in counts.items():
        log.info(f"{slug}: {rows:,} rows")
    return {slug: str(output_dir / slug) for slug in constants.TABLES.value}
