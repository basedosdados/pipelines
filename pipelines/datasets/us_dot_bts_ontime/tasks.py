"""Prefect tasks for us_dot_bts_ontime — thin wrappers over :mod:`utils`."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from prefect import task

from pipelines.datasets.us_dot_bts_ontime.utils import (
    build_airport,
    build_dicionario,
    clean_month,
    download_lookups,
    download_month_prezip,
    latest_available_month,
    open_month_zip,
    write_month_parquet,
    write_reference_parquet,
)


@task(retries=2, retry_delay_seconds=60)
def discover_latest_month() -> dict:
    """Find the most recent month BTS has published."""
    now = datetime.now(UTC)
    year, month = latest_available_month((now.year, now.month))
    print(f"latest published month: {year}-{month:02d}")
    return {"year": year, "month": month, "max_date": f"{year}-{month:02d}"}


@task(retries=2, retry_delay_seconds=60)
def download_and_clean_month(work_dir: str, year: int, month: int) -> dict:
    """Fetch one month, clean it, and write its partition plus the reference tables.

    Only the new month is written, so the staging upload appends a single
    partition rather than rewriting the whole history. The reference tables are
    small and are rebuilt every run, since BTS adds airports and carriers to the
    lookups over time.
    """
    work = Path(work_dir)
    raw_dir, out_dir = work / "input", work / "output"

    zip_path = download_month_prezip(
        year, month, raw_dir / f"ontime_{year}_{month:02d}.zip"
    )
    tbl = clean_month(open_month_zip(zip_path))
    write_month_parquet(tbl, out_dir, year, month)
    print(f"{year}-{month:02d}: {tbl.num_rows:,} flights")

    lookups = download_lookups(raw_dir / "lookups")
    print(f"lookups: {len(lookups)} tables")
    airport = build_airport(raw_dir / "lookups")
    dicionario = build_dicionario(raw_dir / "lookups")
    write_reference_parquet(airport, "airport", out_dir)
    write_reference_parquet(dicionario, "dicionario", out_dir)
    print(
        f"airport: {len(airport):,} rows | dicionario: {len(dicionario):,} rows"
    )

    return {
        "flight": str(out_dir / "flight"),
        "airport": str(out_dir / "airport"),
        "dicionario": str(out_dir / "dicionario"),
        "rows": tbl.num_rows,
    }
