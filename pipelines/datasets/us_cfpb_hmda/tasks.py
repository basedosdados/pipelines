"""Prefect 3 tasks for us_cfpb_hmda - thin wrappers over utils.py."""

from pathlib import Path

from prefect import task

from pipelines.datasets.us_cfpb_hmda.constants import constants
from pipelines.datasets.us_cfpb_hmda.utils import clean_all, latest_source_year


@task
def resolve_years(this_year: int) -> dict:
    """Find the latest published modern year and the full 2018..latest range.

    Args:
        this_year: Current calendar year (passed by the flow).

    Returns:
        {"max_year": int, "years": list[int]} covering FIRST_YEAR..max_year.
    """
    max_year = latest_source_year(this_year)
    years = list(range(constants.FIRST_YEAR.value, max_year + 1))
    return {"max_year": max_year, "years": years}


@task
def build_tables(work_dir: str, years: list[int]) -> dict:
    """Download + clean every modern year into all-STRING partitioned parquet.

    Years are streamed one at a time (raw CSV deleted after each clean), so peak
    disk stays near a single ~4-5 GB file.

    Args:
        work_dir: Scratch dir; input under <work_dir>/input, output under <work_dir>/output.
        years: Modern years to (re)build.

    Returns:
        {"loan_application_register": <partition dir str>, "max_year": "<YYYY>"}.
    """
    base = Path(work_dir)
    return clean_all(base / "output", years, base / "input")
