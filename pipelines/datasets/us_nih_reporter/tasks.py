"""Prefect 3 tasks for us_nih_reporter — thin wrappers over utils.py.

Both flows share these three tasks; what separates them is the family list they
pass to ``probe_source``. The annual flow probes the four year-keyed families,
the links flow probes the two all-fiscal-years ones.
"""

from pathlib import Path

import requests
from prefect import task

from pipelines.datasets.us_nih_reporter.utils import (
    build_dicionario,
    clean_all,
    download_family,
    download_funding_supplement,
    list_source_files,
    source_max_date,
    years_in,
)


@task(retries=2, retry_delay_seconds=120)
def probe_source(families: list[str]) -> dict:
    """Probe the given ExPORTER families for their publication dates, cheaply.

    Costs two requests per file — a redirect-only GET and a HEAD on the document
    service — and downloads nothing, so a scheduled run that finds no change is
    a few hundred kilobytes of headers rather than gigabytes of archives.

    Returns:
        ``{"listing": {"<family>|<year>": {...}}, "max_date": "YYYY-MM-DD"}``.
        String keys, because Prefect serializes task results and tuple keys do
        not survive the round trip.
    """
    listing = list_source_files(families)
    if not listing:
        raise RuntimeError(
            f"no ExPORTER files found for {families} — the source layout changed"
        )
    max_date = source_max_date(listing)
    if not max_date:
        raise RuntimeError("no Last-Modified header on any ExPORTER file")
    return {"listing": listing, "max_date": max_date}


@task(retries=2, retry_delay_seconds=120)
def download_corpus(work_dir: str, probe: dict) -> str:
    """Download every file named in the probe. Returns the input directory."""
    input_dir = Path(work_dir) / "input"
    session = requests.Session()
    listing = probe["listing"]
    families = sorted({k.split("|")[0] for k in listing})
    if "projects" in families:
        # Costs and DUNS are absent from the FY1985-FY1999 project files and
        # published in a separate accessory file keyed on APPLICATION_ID.
        download_funding_supplement(input_dir, session)
    for family in families:
        years = years_in(listing, family)
        download_family(family, years or None, input_dir, session)
    return str(input_dir)


@task
def clean_corpus(work_dir: str, input_dir: str, probe: dict) -> dict:
    """Clean every downloaded year to parquet, and the dicionario when relevant.

    Returns ``{table: path}`` for each table this run produced, plus
    ``row_counts``, so the flow can hand each table's directory straight to
    ``upload_to_gcs``.
    """
    output_dir = Path(work_dir) / "output"
    listing = probe["listing"]
    families = {k.split("|")[0] for k in listing}
    # Each family cleans exactly the years the probe found for it, which are the
    # years download_corpus fetched for it. The four year-keyed families are
    # probed independently and can disagree by a year around a release, so
    # driving two of them from one list would either read a file that was never
    # downloaded or skip one that was.
    counts = clean_all(
        input_dir=Path(input_dir),
        output_dir=output_dir,
        project_years=years_in(listing, "projects"),
        abstract_years=years_in(listing, "abstracts"),
        publication_years=years_in(listing, "publications"),
        link_years=years_in(listing, "linktables"),
        include_all_year_tables="patents" in families,
    )
    # The dicionario's temporal coverage is computed from the project
    # partitions, so it is only rebuilt by the run that rebuilt them.
    if "projects" in families:
        counts["dicionario"] = build_dicionario(output_dir)
    result: dict = {t: str(output_dir / t) for t in counts}
    result["row_counts"] = counts
    return result
