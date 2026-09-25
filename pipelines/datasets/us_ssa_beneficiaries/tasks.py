"""Prefect task wrappers over the pure functions in ``utils``."""

from __future__ import annotations

from pathlib import Path

from prefect import task

from pipelines.datasets.us_ssa_beneficiaries import utils as ssa
from pipelines.datasets.us_ssa_beneficiaries.constants import constants


@task(retries=3, retry_delay_seconds=120)
def download_ssa(work_dir: str) -> str:
    """Fetch SSA's flattened time-series files.

    Args:
        work_dir: Scratch directory for this run.

    Returns:
        The directory the JSON files were written to.
    """
    input_dir = Path(work_dir) / "input"
    ssa.download_all(input_dir)
    return str(input_dir)


@task
def clean_ssa(work_dir: str, input_dir: str) -> dict[str, str]:
    """Build every table, gate on reconciliation, and write parquet.

    Args:
        work_dir: Scratch directory for this run.
        input_dir: Directory holding the downloaded JSON.

    Returns:
        A mapping of table slug to its parquet directory, plus ``max_year``:
        the latest year present in the source, as a string.

    Raises:
        ReconciliationError: If any year fails to reconcile against SSA's own
            published state and national totals, so a mis-parsed year is never
            uploaded.
    """
    output_dir = Path(work_dir) / "output"
    tables = ssa.clean_all(input_dir)

    result: dict[str, str] = {}
    max_year = 0
    for table, df in tables.items():
        ssa.write_partitioned(
            df, table, output_dir, constants.ARCHITECTURE_DIR.value
        )
        result[table] = str(output_dir / table)
        if "year" in df.columns:
            max_year = max(max_year, int(df["year"].max()))
    result["max_year"] = str(max_year)
    return result
