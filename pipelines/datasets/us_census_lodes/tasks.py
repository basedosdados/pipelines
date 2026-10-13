"""Prefect 3 tasks for us_census_lodes — thin wrappers over utils.py."""

from pathlib import Path

from prefect import task

from pipelines.datasets.us_census_lodes.constants import STATES
from pipelines.datasets.us_census_lodes.utils import (
    build_dicionario,
    clean_crosswalk,
    clean_state_year,
    latest_source_year,
    load_block_geography,
)


@task(retries=2, retry_delay_seconds=60)
def get_latest_year() -> int:
    """Highest data year LODES8 publishes, read from the directory listing."""
    return latest_source_year()


@task(retries=2, retry_delay_seconds=60)
def build_years(work_dir: str, years: list[int]) -> dict:
    """Download and clean the given data years for every state, plus the crosswalk.

    Returns a mapping of table slug to its output directory. A table absent from
    the mapping had no data at all for these years.

    The crosswalk is rebuilt on every run: LODES restates it wholesale at each
    release, and it is only ~150 MB gzipped.
    """
    input_dir = Path(work_dir) / "input"
    output_dir = Path(work_dir) / "output"
    produced: set[str] = set()

    for state in STATES:
        clean_crosswalk(state, input_dir, output_dir)
        produced.add("geography_crosswalk")
        # County and tract come from the crosswalk, not the block prefix.
        geo = load_block_geography(state, input_dir, output_dir)
        for year in years:
            for table in clean_state_year(
                state, year, input_dir, output_dir, geo=geo
            ):
                produced.add(table)

    # Static reference data, but rebuilt every run so the flow is
    # self-contained rather than depending on a blob the bootstrap left behind.
    build_dicionario(output_dir)
    produced.add("dicionario")

    return {table: str(output_dir / table) for table in sorted(produced)}
