"""Prefect 3 tasks for br_cgu_despesas_publicas — thin wrappers over utils.py."""

from datetime import UTC, datetime
from pathlib import Path

from prefect import task

from pipelines.datasets.br_cgu_despesas_publicas.utils import (
    clean_all,
    download_all,
    month_range,
    probe_latest,
)


@task(retries=2, retry_delay_seconds=300)
def probe_source() -> str:
    """Return when the portal last regenerated the current month, ``YYYY-MM-DD``.

    One HEAD request. Unlike ``orcamento-despesa``, each month here carries its
    own ``Last-Modified``, so this timestamp speaks only for the current month —
    which is the one that moves on every release and therefore the right
    freshness signal for a scheduled run.

    Retries are spaced five minutes apart because the failure worth retrying is
    an AWS WAF rate block, which a fast retry only deepens.
    """
    return probe_latest()


@task(retries=2, retry_delay_seconds=600)
def download_despesas(
    work_dir: str, months_back: int = 6, full_refresh: bool = False
) -> dict:
    """Download the months to refresh into ``<work_dir>/input``.

    Args:
        work_dir: Scratch directory for this run.
        months_back: Size of the trailing window to refresh. The source restates
            closed months, but re-pulling all 150+ of them costs ~25 minutes and
            ~10 GB, so a scheduled run refreshes only the tail.
        full_refresh: Pull every month from 2014-01 instead. Use when the source
            has restated older months and the tail window would miss it.

    Returns:
        ``{"input_dir": str, "months": [[year, month], ...]}``.
    """
    input_dir = Path(work_dir) / "input"
    if full_refresh:
        wanted = month_range()
    else:
        now = datetime.now(UTC)
        start_index = now.year * 12 + (now.month - 1) - max(months_back - 1, 0)
        first = (start_index // 12, start_index % 12 + 1)
        wanted = month_range(first=first)
    months = download_all(input_dir, months=wanted)
    return {"input_dir": str(input_dir), "months": [list(m) for m in months]}


@task
def clean_despesas(work_dir: str, input_dir: str, months: list) -> dict:
    """Clean the downloaded months into hive-partitioned all-STRING parquet.

    Returns:
        ``{"path": str, "rows": {"YYYY-MM": n}, "total": n, "max_period": str}``.
    """
    result = clean_all(
        Path(input_dir),
        Path(work_dir) / "output",
        [tuple(m) for m in months],
    )
    return {**result, "path": str(result["path"])}
