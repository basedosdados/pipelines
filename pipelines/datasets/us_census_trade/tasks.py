"""Prefect task wrappers over the pure functions in ``utils``.

Nothing here contains transform logic: the one-shot bootstrap under
``models/us_census_trade/code/`` imports the same ``utils`` functions, so the
recurring pipeline and the onboarding build share one implementation.
"""

from __future__ import annotations

from pathlib import Path

from prefect import task

from pipelines.datasets.us_census_trade import utils


@task(retries=3, retry_delay_seconds=60)
def download_schedules_task() -> tuple[str, str]:
    """Fetch the Schedule C and Schedule D code lists. No API key required."""
    return utils.download_schedules()


@task(retries=3, retry_delay_seconds=60)
def latest_available_month_task(probe_table: str = "import") -> str:
    """Newest month the API has published, as ``YYYY-MM``."""
    return utils.latest_available_month(probe_table=probe_table)


@task(retries=2, retry_delay_seconds=120)
def harvest_task(
    tables: list[str],
    first_month: str,
    last_month: str,
    output_dir: str,
    schedule_c: str,
) -> dict[str, str]:
    """Download, clean and write every table over a month range."""
    iso2 = utils.load_country_iso2(schedule_c)
    paths = utils.harvest(
        tables=tables,
        first_month=first_month,
        last_month=last_month,
        output_dir=Path(output_dir),
        iso2_by_code=iso2,
    )
    return {table: str(path) for table, path in paths.items()}


@task(retries=2, retry_delay_seconds=60)
def build_dicionario_task(
    schedule_c: str, schedule_d: str, output_dir: str
) -> str:
    """Build the dicionario from the published Census code schedules."""
    df = utils.build_dicionario(schedule_c, schedule_d)
    return str(utils.write_dicionario(df, Path(output_dir)))
