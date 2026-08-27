"""Prefect task wrappers over the pure helpers in utils.py."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from pathlib import Path

from prefect import task

from pipelines.datasets.cl_chilecompra_mercado_publico import utils
from pipelines.datasets.cl_chilecompra_mercado_publico.constants import (
    constants,
)


def _months_until_now(first_year: int, first_month: int):
    now = datetime.now(UTC)
    year, month = first_year, first_month
    while (year, month) <= (now.year, now.month):
        yield year, month
        month += 1
        if month > 12:
            year, month = year + 1, 1


@task(retries=2, retry_delay_seconds=60)
def survey_source_task(kinds: list[str] | None = None) -> list[dict]:
    """HEAD every monthly blob and return what exists, with its Last-Modified.

    About 470 HEAD requests, a couple of minutes. This is the only reliable change
    signal available: the publisher rewrites old months in place, and nothing inside the
    data itself records that a month was revised.
    """
    kinds = kinds or list(constants.CONTAINERS.value)
    found = []
    for kind in kinds:
        for year, month in _months_until_now(
            constants.FIRST_YEAR.value, constants.FIRST_MONTH.value
        ):
            info = utils.head_month(kind, year, month)
            if info:
                found.append(info)
    return found


@task
def select_stale_months_task(
    manifest: list[dict], lookback_days: int, force_all: bool = False
) -> list[dict]:
    """Keep the months the publisher touched within ``lookback_days``."""
    if force_all:
        return manifest
    cutoff = datetime.now(UTC) - timedelta(days=lookback_days)
    stale = []
    for entry in manifest:
        raw = entry.get("last_modified") or ""
        try:
            modified = parsedate_to_datetime(raw)
        except (TypeError, ValueError):
            # Unparseable header: re-ingest rather than silently skip the month.
            stale.append(entry)
            continue
        if modified >= cutoff:
            stale.append(entry)
    return stale


@task(retries=2, retry_delay_seconds=120)
def download_and_clean_task(entry: dict, root: str) -> dict:
    """Download one month, clean it, write parquet, then drop the raw ZIP.

    Returns the row count per table so the flow log records what was actually ingested.
    A run that found nothing new otherwise looks identical to one that loaded data.
    """
    root_path = Path(root)
    input_dir = root_path / "input"
    output_dir = root_path / "output"
    kind, year, month = entry["kind"], entry["year"], entry["month"]

    zip_path = utils.download_month(kind, year, month, input_dir)
    try:
        frames = utils.clean_month(kind, zip_path)
        counts = {}
        for table, df in frames.items():
            utils.write_partitioned(df, table, output_dir)
            counts[table] = len(df)
    finally:
        zip_path.unlink(missing_ok=True)
    return {"kind": kind, "year": year, "month": month, "rows": counts}


@task
def source_max_date_task(manifest: list[dict]) -> str:
    """Latest month present at source, as YYYY-MM-01.

    This is a coverage date, not a wall clock: it is what the publisher has released,
    which is what the raw-data-source Update record stores.
    """
    months = sorted((e["year"], e["month"]) for e in manifest)
    year, month = months[-1]
    return f"{year}-{month:02d}-01"
