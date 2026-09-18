"""Prefect 3 tasks for us_noaa_storm_events — thin wrappers over utils.py."""

from pathlib import Path

from prefect import task

from pipelines.datasets.us_noaa_storm_events.utils import (
    clean_all,
    download_file,
    download_years,
    list_source_files,
    source_max_date,
)


@task(retries=2, retry_delay_seconds=60)
def probe_source(work_dir: str) -> dict:
    """Read the source listing and the newest year's coverage, cheaply.

    The poll needs the source's max coverage date, which only the data carries.
    Downloading the whole corpus to find it would cost 363 MB on every scheduled
    run, almost all of them no-ops — so only the newest year's ``details`` file
    (~12 MB) is fetched here. The rest is downloaded after the poll says there is
    something to do.

    Returns:
        ``{"listing": {...}, "years": [...], "max_date": "YYYY-MM-01"}``, where
        ``listing`` maps ``"<family>|<year>"`` to the file name and its ``c``
        creation token. String keys, because Prefect serializes task results and
        tuple keys do not survive the round trip.
    """
    input_dir = Path(work_dir) / "input"
    listing = list_source_files()
    years = sorted({y for (_, y) in listing})
    latest = years[-1]

    download_file(listing[("details", latest)]["file"], input_dir)
    max_date = source_max_date(input_dir, listing, latest)
    if not max_date:
        raise RuntimeError(f"no dated rows in the {latest} details file")

    return {
        "listing": {f"{fam}|{yr}": v for (fam, yr), v in listing.items()},
        "years": years,
        "max_date": max_date,
    }


@task(retries=2, retry_delay_seconds=60)
def download_corpus(work_dir: str, probe: dict) -> str:
    """Download every family for every year. Returns the input directory."""
    input_dir = Path(work_dir) / "input"
    listing = _rehydrate(probe["listing"])
    download_years(probe["years"], input_dir, listing=listing)
    return str(input_dir)


@task
def clean_corpus(work_dir: str, input_dir: str, probe: dict) -> dict:
    """Clean every year to partitioned parquet, then build the dicionario.

    Returns:
        Table slug -> partitioned output directory (as strings), plus
        ``"row_counts"``.
    """
    from pipelines.datasets.us_noaa_storm_events.constants import constants
    from pipelines.datasets.us_noaa_storm_events.utils import build_dicionario

    output_dir = Path(work_dir) / "output"
    listing = _rehydrate(probe["listing"])
    counts = clean_all(Path(input_dir), output_dir, probe["years"], listing)
    counts["dicionario"] = build_dicionario(output_dir)

    result: dict = {t: str(output_dir / t) for t in constants.ALL_TABLES.value}
    result["row_counts"] = counts
    return result


def _rehydrate(flat: dict) -> dict:
    """Turn the string-keyed listing back into ``{(family, year): {...}}``."""
    out = {}
    for key, value in flat.items():
        family, year = key.split("|")
        out[(family, int(year))] = value
    return out
