"""Prefect 3 tasks for us_dol_oflc — thin wrappers over utils.py."""

from pathlib import Path

from prefect import task

from pipelines.datasets.us_dol_oflc.constants import constants
from pipelines.datasets.us_dol_oflc.utils import (
    clean_fiscal_years,
    download_fiscal_years,
    max_decision_date,
    refresh_fiscal_years,
)


@task
def fiscal_years_to_refresh() -> list[int]:
    """The open fiscal year and the one before it.

    A closed fiscal year is frozen; the previous year stays in scope because its
    final annual file lands after the year has ended.

    Returns:
        The two federal fiscal years to re-materialise, oldest first.
    """
    return refresh_fiscal_years()


@task(retries=3, retry_delay_seconds=60)
def download_program(program: str, years: list[int], work_dir: str) -> str:
    """Download every published workbook for one program and fiscal year range.

    Retries: www.dol.gov is behind Akamai and occasionally drops a large
    transfer part-way through.

    Args:
        program: One of lca, perm, h2a, h2b.
        years: Fiscal years to fetch.
        work_dir: Directory to download into; files land in ``<work_dir>/input``.

    Returns:
        The input directory path, as a string.
    """
    input_dir = Path(work_dir) / "input"
    files = download_fiscal_years(program, years, input_dir)
    if not files:
        raise RuntimeError(
            f"No {program} files found for fiscal years {years} — the source "
            "page layout may have changed"
        )
    return str(input_dir)


@task
def clean_program(
    program: str, years: list[int], work_dir: str, input_dir: str
) -> dict:
    """Re-materialise the given fiscal years of one program.

    Args:
        program: One of lca, perm, h2a, h2b.
        years: Fiscal years to rebuild. Each is rebuilt whole, never appended to.
        work_dir: Directory to write into; tables land under ``<work_dir>/output``.
        input_dir: Directory holding the downloaded workbooks, from
            :func:`download_program`.

    Returns:
        ``{"path": <partitioned output dir>, "max_decision_date": "YYYY-MM-DD",
        "rows": int}``. ``max_decision_date`` is the source's own coverage
        high-water mark, which is what the source poll compares against.
    """
    output_dir = Path(work_dir) / "output"
    report = clean_fiscal_years(program, years, Path(input_dir), output_dir)
    rows = sum(v.get("rows", 0) for v in report.values())
    return {
        "path": str(output_dir / program),
        "max_decision_date": max_decision_date(output_dir, program),
        "rows": rows,
    }


@task
def dataset_id() -> str:
    """The BigQuery dataset id the flow writes to.

    Returns:
        ``"us_dol_oflc"``.
    """
    return constants.DATASET_ID.value
