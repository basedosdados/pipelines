"""Prefect 3 tasks for us_dot_fars — thin wrappers over utils.py."""

from pathlib import Path

from prefect import task

from pipelines.datasets.us_dot_fars.constants import constants
from pipelines.datasets.us_dot_fars.utils import (
    build_dicionario,
    clean_year,
    latest_published_year,
    source_max_date,
)


@task(retries=2, retry_delay_seconds=60)
def probe_source() -> dict:
    """Find the newest published FARS year, without downloading anything.

    NHTSA publishes one annual zip per year at a predictable URL, so the newest
    year is found by walking forward with HEAD requests from the first year until
    one 404s. That costs a handful of requests rather than the 1.1 GB the corpus
    would cost, which matters because most scheduled runs are no-ops.
    """
    first = constants.FIRST_YEAR.value
    latest = latest_published_year(first)
    if latest is None:
        raise RuntimeError(
            f"no FARS annual file published at or after {first}"
        )
    return {
        "years": list(range(first, latest + 1)),
        "latest": latest,
        "max_date": source_max_date(latest),
    }


@task(retries=2, retry_delay_seconds=60)
def clean_corpus(work_dir: str, probe: dict) -> dict:
    """Download and clean every year, then rebuild the dicionario.

    Returns:
        Table slug -> partitioned output directory (as strings), plus
        ``"row_counts"``.
    """
    input_dir = Path(work_dir) / "input"
    output_dir = Path(work_dir) / "output"
    counts: dict[str, int] = dict.fromkeys(constants.DATA_TABLES.value, 0)
    for year in probe["years"]:
        for table, n in clean_year(year, input_dir, output_dir).items():
            counts[table] += n
    counts["dicionario"] = build_dicionario(
        probe["years"], input_dir, output_dir
    )

    result: dict = {t: str(output_dir / t) for t in constants.ALL_TABLES.value}
    result["row_counts"] = counts
    return result
