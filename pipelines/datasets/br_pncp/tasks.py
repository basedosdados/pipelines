"""Prefect 3 tasks for br_pncp — thin wrappers over utils.py."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

from prefect import task

from pipelines.datasets.br_pncp.constants import constants
from pipelines.datasets.br_pncp.utils import (
    build_dicionario,
    clean_table,
    harvest,
)


@task(retries=2, retry_delay_seconds=120)
def harvest_window(work_dir: str, table: str, lookback_days: int) -> str:
    """Harvest one table's recent updates into ``<work_dir>/input/<table>/``.

    The window runs back ``lookback_days`` from today against the table's
    *update-date* endpoint, so amendments to older records are picked up along
    with new ones. Overlapping windows across runs are harmless: the dbt models
    deduplicate on the PNCP control number.

    Retries twice with a long delay, because the failure mode here is the API's
    per-IP rate limiter rather than anything transient in our own code.

    Returns:
        The input directory path, as a string (Prefect serializes task results).
    """
    input_dir = Path(work_dir) / "input"
    end = date.today()
    start = end - timedelta(days=lookback_days)
    count = harvest(table=table, input_dir=input_dir, start=start, end=end)
    print(
        f"{table}: harvested {count:,} records for {start}..{end}", flush=True
    )
    return str(input_dir)


@task
def clean_window(work_dir: str, input_dir: str, table: str) -> dict:
    """Clean one table's harvested chunks into partitioned staging parquet.

    ``replace=False``: an incremental run produces only the partitions its window
    touched, and must not delete the others from the output tree.

    Returns:
        The cleaning summary, plus ``data_path`` for the upload step.
    """
    output_dir = Path(work_dir) / "output"
    summary = clean_table(Path(input_dir), output_dir, table, replace=False)
    summary["data_path"] = str(output_dir / table)
    print(
        f"{table}: {summary['raw_rows']:,} raw -> {summary['deduped_rows']:,} rows "
        f"across years {summary['years']}",
        flush=True,
    )
    return summary


@task
def build_dicionario_task(work_dir: str) -> dict:
    """Rebuild the dicionario from whatever fact tables this run produced.

    Returns:
        ``data_path`` and the row count, shaped like a cleaning summary so the
        flow can treat it uniformly.
    """
    output_dir = Path(work_dir) / "output"
    rows = build_dicionario(output_dir)
    return {
        "table": "dicionario",
        "deduped_rows": rows,
        "years": [],
        "data_path": str(output_dir / "dicionario"),
    }


@task
def max_publication_date(summaries: list[dict]) -> str:
    """Latest publication year present in this run, as ``YYYY-01-01``.

    PNCP is a continuously-updated register with no release calendar, so there
    is no "new period published" signal comparable to a monthly statistical
    release. The source's coverage therefore advances by publication year, which
    is the granularity the coverage metadata records.
    """
    years = [int(y) for s in summaries for y in (s.get("years") or [])]
    if not years:
        return f"{constants.START_YEAR.value}-01-01"
    return f"{max(years)}-01-01"
