"""Prefect 3 tasks for br_mf_divida_ativa — thin wrappers over utils.py."""

import datetime
from pathlib import Path
from typing import Literal

from prefect import task

from pipelines.datasets.br_mf_divida_ativa.utils import (
    FIRST_QUARTER,
    FIRST_YEAR,
    all_quarters,
    clean_quarters,
    latest_available_quarter,
    quarter_date_str,
)
from pipelines.utils.metadata.client import MetadataClient


@task
def discover_new_quarters(
    dataset_id: str,
    table_id: str,
    env: Literal["dev", "prod", "staging"] = "prod",
) -> dict:
    """Find which source quarters have not been ingested yet.

    Probes the source for the newest available quarter, then reads the registered
    ``RawDataSource.Update.latest`` (a coverage date, seeded at onboarding and
    advanced by :func:`commit_source_update_task`). Every quarter strictly newer
    than that boundary — up to and including the newest available — is returned,
    so a missed scheduled run catches up on all the quarters it skipped rather
    than silently dropping one. The Update boundary (not a re-scan of the table)
    is what keeps each quarter's partition from being appended to staging twice.

    Args:
        dataset_id: GCP/BigQuery dataset id.
        table_id: probe table (SIDA, present every quarter) that anchors the
            single raw data source the boundary is read from.
        env: backend to read the source update from.

    Returns:
        ``{"quarters": [[year, quarter], ...], "available": [year, quarter] |
        None, "max_date": "YYYY-MM-01" | None}``. ``quarters`` is empty when the
        source has nothing newer than the boundary (or the boundary is unseeded —
        see the note below).
    """
    available = latest_available_quarter()
    if available is None:
        print("Source unreachable — no quarters found.")
        return {"quarters": [], "available": None, "max_date": None}

    client = MetadataClient(env=env)
    last = client.get_raw_source_update_latest(dataset_id, table_id)

    if last is None:
        # Unseeded RawDataSource.Update: refuse to guess a boundary and re-ingest
        # history (that would duplicate staging partitions). Seed the source
        # Update at onboarding to the last loaded quarter; until then this is a
        # no-op. See prefect-pipeline-conventions "Update and Poll records".
        print(
            "RawDataSource.Update.latest is unset — seed it to the last loaded "
            "quarter before the pipeline can detect new data. Skipping."
        )
        new: list[list[int]] = []
    else:
        candidates = all_quarters((FIRST_YEAR, FIRST_QUARTER), available)
        new = [
            [y, q]
            for (y, q) in candidates
            if datetime.date(y, q * 3, 1) > last
        ]

    print(
        f"newest at source={available}, registered boundary={last}, "
        f"new quarters={new}"
    )
    return {
        "quarters": new,
        "available": list(available),
        "max_date": quarter_date_str(*available),
    }


@task(retries=2, retry_delay_seconds=60)
def clean_quarters_task(quarters: list, work_dir: str) -> dict:
    """Download + clean the given quarters for all three tables.

    Retries twice: the PGFN endpoint occasionally drops connections on the larger
    SIDA ZIPs (``download_quarter`` also retries per file).

    Args:
        quarters: list of ``[year, quarter]`` pairs to ingest.
        work_dir: per-run scratch dir; inputs land in ``<work_dir>/input`` and
            partitioned Parquet under ``<work_dir>/output/<table>/``.

    Returns:
        Mapping of table slug to its partitioned output directory (str), or
        ``None`` for a table absent from the requested quarters.
    """
    pairs = [(int(y), int(q)) for y, q in quarters]
    return clean_quarters(pairs, Path(work_dir))
