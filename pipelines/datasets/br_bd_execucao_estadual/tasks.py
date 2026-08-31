"""Tasks for br_bd_execucao_estadual."""

from __future__ import annotations

from pathlib import Path

from prefect import task

from pipelines.datasets.br_bd_execucao_estadual.constants import constants
from pipelines.datasets.br_bd_execucao_estadual.utils import (
    REFRESHERS,
    built_tables,
)


@task(retries=2, retry_delay_seconds=300)
def refresh_state(
    state: str, work_dir: str, year: int, full_refresh: bool
) -> dict[str, str]:
    """Download and clean one state, returning {staging table: parquet dir}.

    Retried twice: three of the four sources are plain HTTP file fetches that fail
    transiently, and São Paulo's WebForms scrape drops sessions. The downloaders skip
    files already on disk, so a retry resumes rather than starting over -- except for
    the open exercise, which the refresher deliberately invalidates first.
    """
    REFRESHERS[state](work_dir, year, full_refresh)
    built = built_tables(work_dir, state)
    if not built:
        raise RuntimeError(
            f"{state}: produced no parquet. Refusing to continue -- an empty result "
            "here would upload nothing and leave the prod staging prefix stale, "
            "which looks like success."
        )
    expected = set(constants.STAGING_BY_STATE.value[state])
    missing = expected - set(built)
    if missing:
        print(
            f"{state}: WARNING {len(missing)} staging table(s) empty: {sorted(missing)}"
        )
    print(f"{state}: {len(built)} staging tables ready")
    return {k: str(v) for k, v in built.items()}


@task
def parquet_row_count(paths: dict[str, str]) -> int:
    """Total rows across a state's parquet, for the run log.

    Cheap: parquet carries its row count in the footer, so nothing is read.
    """
    import pyarrow.parquet as pq

    total = 0
    for directory in paths.values():
        for file in Path(directory).glob("*.parquet"):
            total += pq.read_metadata(file).num_rows
    return total
