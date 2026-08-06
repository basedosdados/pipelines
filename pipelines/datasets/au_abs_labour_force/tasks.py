"""Prefect 3 tasks for au_abs_labour_force — thin wrappers over utils.py.

The download splits in two so the source poll can gate the expensive half: the
SDMX extracts (small, and they carry the latest reference month) come first, then
the poll decides whether to fetch the month-stamped Excel spreadsheets (SEM1 is
~38 MB) and rebuild. On a no-op run only the SDMX pull happens.
"""

from pathlib import Path

import pandas as pd
from prefect import task

from pipelines.datasets.au_abs_labour_force.constants import constants
from pipelines.datasets.au_abs_labour_force.utils import (
    clean_all,
    download_excel,
    download_sdmx,
    write_partitioned,
)

# Release-period helpers — pipeline-only (resolve the source-poll key + the
# month-stamped Excel URL), so they live here rather than in the shared transform.
_MONTHS = [
    "jan",
    "feb",
    "mar",
    "apr",
    "may",
    "jun",
    "jul",
    "aug",
    "sep",
    "oct",
    "nov",
    "dec",
]


def latest_period(input_dir: Path) -> str:
    """Latest reference month in the LF SDMX extract, as ``"YYYY-MM"``.

    Zero-padded ``"YYYY-MM"`` sorts lexicographically in calendar order, so the
    max string is the newest month. Drives both the source-update poll and the
    month-stamped Excel URL. Read from ``LF.csv`` — the ABS API and the Excel
    spreadsheets release the same reference month together.
    """
    df = pd.read_csv(input_dir / "LF.csv", dtype=str, na_filter=False)
    tcol = next(
        c for c in df.columns if c.split(":")[0].strip() == "TIME_PERIOD"
    )
    return max(v[:7] for v in df[tcol] if v)


def month_slug(period: str) -> str:
    """Map ``"YYYY-MM"`` to the ABS release slug, e.g. ``"2026-06" -> "jun-2026"``.

    A fixed English abbreviation table (not ``strftime("%b")``) keeps the slug
    locale-independent on the worker.
    """
    y, m = period.split("-")
    return f"{_MONTHS[int(m) - 1]}-{y}"


@task(retries=2, retry_delay_seconds=30)
def download_sdmx_task(work_dir: str) -> str:
    """Download the SDMX full-history extracts (LF, LF_AGES, LF_UNDER).

    Retries twice: the ABS Data API occasionally times out on the full-history
    ``all`` query.

    Args:
        work_dir: Directory to download into; files land in ``<work_dir>/input``.

    Returns:
        The input directory path, as a string (Prefect serializes task results).
    """
    input_dir = Path(work_dir) / "input"
    for flow in constants.SDMX_FLOWS.value:
        download_sdmx(flow, input_dir)
    return str(input_dir)


@task
def latest_month_task(input_dir: str) -> str:
    """Latest reference month in the LF extract, as ``"YYYY-MM"`` — the poll key."""
    return latest_period(Path(input_dir))


@task(retries=2, retry_delay_seconds=30)
def download_excel_task(input_dir: str, source_max_date: str) -> str:
    """Download the release-month Excel spreadsheets (Table 18/19, SEM1).

    Args:
        input_dir: Directory holding the SDMX extracts, from
            :func:`download_sdmx_task`; the Excel files land beside them.
        source_max_date: Latest reference month ``"YYYY-MM"``; maps to the ABS
            month-stamped release path (e.g. ``jun-2026``).

    Returns:
        The input directory path.
    """
    download_excel(month_slug(source_max_date), Path(input_dir))
    return input_dir


@task
def clean_and_write_task(work_dir: str, input_dir: str) -> dict:
    """Build the four tables and write each as all-STRING partitioned parquet.

    Args:
        work_dir: Directory to write into; tables land under ``<work_dir>/output``.
        input_dir: Directory holding the downloaded SDMX + Excel sources.

    Returns:
        A mapping of table slug to its partitioned output directory (as a string).
    """
    output_dir = Path(work_dir) / "output"
    tables = clean_all(Path(input_dir))
    return {
        table: str(write_partitioned(df, table, output_dir))
        for table, df in tables.items()
    }
