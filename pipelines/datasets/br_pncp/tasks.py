"""Prefect 3 tasks for br_pncp — thin wrappers over utils.py."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

from prefect import task

from pipelines.datasets.br_pncp.constants import constants
from pipelines.datasets.br_pncp.utils import (
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
        f"{table}: {summary['raw_rows']:,} raw -> {summary['written_rows']:,} staging rows "
        f"across years {summary['years']}",
        flush=True,
    )
    return summary


@task
def max_publication_date(summaries: list[dict]) -> str:
    """Latest partition date present in this run, as ``YYYY-MM-DD``.

    PNCP is a continuously-updated register with no release calendar, so there
    is no "new period published" signal comparable to a monthly statistical
    release. What stands in for one is the latest date the run actually saw --
    `data_publicacao` for the procurement tables, `data_inclusao` for
    instrumento_cobranca (`utils.PARTITION_SOURCE`).

    **This must be a day, not a year.** The value is handed to
    `poll_source_for_update_task(..., compare_against="coverage")`, which tests
    ``source_max > Coverage.DateTimeRange`` -- and that range is day-granular,
    because `register_table_materialization_task` reads the real max date out
    of BigQuery. This used to return ``f"{max(years)}-01-01"``, so from the
    first materialization onward the comparison was a January 1st against a
    mid-year date and could not be true again until the next calendar year. The
    prod run of 2026-09-09 harvested 151,488 records, compared 2026-01-01
    against a coverage of 2026-08-28, concluded there was nothing new, and
    exited Completed having ingested none of it. A year-granular value here
    silently converts a daily pipeline into an annual one.

    The max is taken across every fact table rather than contratacao alone,
    matching what the poll is asking: whether the *source* has anything newer
    than what is published. That errs toward materializing, which is the safe
    direction for this comparison -- a needless run costs time, a skipped one
    loses a day of data.
    """
    dates = [
        d for s in summaries if (d := s.get("max_partition_date")) is not None
    ]
    if dates:
        return max(dates)
    # No dated record in this run. Fall back to the year floor, which is older
    # than any registered coverage and therefore reads as "nothing new".
    return f"{constants.START_YEAR.value}-01-01"
