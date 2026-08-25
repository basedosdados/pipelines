"""Prefect 3 tasks for br_senado_dados_abertos_administrativos.

Thin wrappers over the shared, Prefect-free ``utils.py``. The source is a REST
API, so there is no separate file download — the extract *is* the "download",
and one task runs the whole ``clean_all`` for the requested window.
"""

import datetime as dt

from prefect import task

from pipelines.datasets.br_senado_dados_abertos_administrativos.utils import (
    clean_all,
)


@task
def extract_and_clean(
    output_dir: str, sub_resources: bool, year: int | None = None
) -> dict:
    """Run the extract for the current window and write partitioned parquet.

    The snapshot tables get a fresh snapshot stamped ``extracted_at`` (today);
    the time-series tables are re-extracted for ``year`` (the current year), so
    ``upload_to_gcs(dump_mode="overwrite")`` + the incremental dbt models replace
    exactly that partition in prod and leave history intact.

    Args:
        output_dir: Directory to write ``<table>/<partitions>/data.parquet`` into.
        sub_resources: When True, also build the contratação children (the weekly
            fan-out). When False, skip them (the daily run).
        year: Current year to bound the time series; ``None`` means full history
            (used only for a from-scratch backfill, not a scheduled run).

    Returns:
        ``{"counts": {table: rows}, "extracted_at": "YYYY-MM-DD",
           "tables": [written table slugs]}``.
    """
    today = dt.date.today()
    extracted_at = today.isoformat()
    counts = clean_all(
        output=output_dir,
        years=[year] if year is not None else None,
        extracted_at=extracted_at,
        today=today,
        sub_resources=sub_resources,
    )
    return {
        "counts": counts,
        "extracted_at": extracted_at,
        "tables": list(counts),
    }
