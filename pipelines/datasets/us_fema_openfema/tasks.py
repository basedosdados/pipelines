"""Prefect tasks for us_fema_openfema.

Thin wrappers over the pure functions in ``utils.py``, which the one-shot
onboarding scripts under ``models/us_fema_openfema/code/`` also import.
"""

from __future__ import annotations

from pathlib import Path

import requests
from prefect import task

from pipelines.datasets.us_fema_openfema.constants import constants
from pipelines.datasets.us_fema_openfema.utils import (
    clean_table,
    download_table,
    write_dicionario,
)


@task(name="us_fema_openfema: check source", retries=2, retry_delay_seconds=60)
def check_source_openfema() -> dict[str, str]:
    """Return `{table: last data refresh date}` from OpenFEMA's own catalog.

    `lastDataSetRefresh` is the timestamp that moves when a set's data is
    republished. `lastRefresh` is a schema/store timestamp and lags it badly —
    DisasterDeclarationsSummaries showed `lastDataSetRefresh` 2026-09-06
    against `lastRefresh` 2025-09-25 — so it must not be used for the poll.

    Also raises if FEMA has scheduled one of the pinned sets for removal, so a
    deprecation is noticed on the next run rather than when the endpoint dies.
    """
    response = requests.get(
        constants.CATALOG_URL.value, timeout=constants.REQUEST_TIMEOUT.value
    )
    response.raise_for_status()
    catalog = {d["name"]: d for d in response.json()["DataSets"]}

    out: dict[str, str] = {}
    deprecated: list[str] = []
    for table, (set_name, version, _) in constants.SOURCES.value.items():
        entry = catalog.get(set_name)
        if entry is None or entry["version"] != version:
            raise RuntimeError(
                f"{table}: OpenFEMA no longer publishes {set_name} v{version}"
            )
        if entry.get("depDate"):
            deprecated.append(
                f"{set_name} v{version} is deprecated from {entry['depDate']}"
                f" -> {entry.get('depNewURL')}"
            )
        out[table] = entry["lastDataSetRefresh"][:10]
    if deprecated:
        raise RuntimeError(
            "pinned OpenFEMA sets are scheduled for removal: "
            + "; ".join(deprecated)
        )
    print(f"source last refreshed: {out}")
    return out


@task(name="us_fema_openfema: download", retries=2, retry_delay_seconds=120)
def download_openfema(work_dir: str, table: str) -> str:
    """Download one set's parquet. Resumes a stalled transfer."""
    path = download_table(table, Path(work_dir) / "input")
    print(f"{table}: downloaded {path.stat().st_size:,} bytes")
    return str(path)


@task(name="us_fema_openfema: clean", retries=1, retry_delay_seconds=60)
def clean_openfema(work_dir: str, table: str, input_path: str) -> str:
    """Clean one set into hive-partitioned all-STRING parquet."""
    output_dir = Path(work_dir) / "output"
    counts = clean_table(table, Path(input_path), output_dir)
    print(
        f"{table}: {sum(counts.values()):,} rows across {len(counts)} "
        f"partitions ({min(counts)}-{max(counts)})"
    )
    return str(output_dir / table)


@task(name="us_fema_openfema: dicionario")
def write_dicionario_task(work_dir: str) -> str:
    """Materialise the committed dictionary CSV as parquet for upload."""
    output_dir = Path(work_dir) / "output"
    rows = write_dicionario(output_dir)
    print(f"dicionario: {rows:,} rows")
    return str(output_dir / "dicionario")
