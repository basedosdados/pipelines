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
    files already on disk, so a retry within a run resumes rather than starting over.
    Across runs there is nothing to resume from: `work_dir` is a fresh mkdtemp, which
    is why the year scope is an argument to the downloader and not a disk state.
    """
    REFRESHERS[state](work_dir, year, full_refresh)
    built = built_tables(work_dir, state)
    if not built:
        raise RuntimeError(
            f"{state}: produced no parquet. Refusing to continue -- an empty result "
            "here would upload nothing and leave the prod staging prefix stale, "
            "which looks like success."
        )
    # Only a full refresh is expected to produce every mirror. An incremental run
    # rebuilds one exercise, so a source with nothing in the open year legitimately
    # yields no parquet -- Pernambuco's `despesa_legado` covers 2008-2010 and will
    # never appear again. Warning on that daily would train the reader to ignore it.
    if full_refresh:
        expected = set(constants.STAGING_BY_STATE.value[state])
        missing = expected - set(built)
        if missing:
            print(
                f"{state}: WARNING {len(missing)} staging table(s) empty: "
                f"{sorted(missing)}"
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
