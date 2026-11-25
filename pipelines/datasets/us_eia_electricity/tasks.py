"""Prefect 3 tasks for us_eia_electricity — thin wrappers over utils.py."""

from pathlib import Path

from prefect import task

from pipelines.datasets.us_eia_electricity.constants import constants
from pipelines.datasets.us_eia_electricity.utils import (
    BUILDERS,
    YearReader,
    build_dicionario,
    clean_all,
    download_form,
    list_source_files,
    load_cols,
    source_max_date,
    source_zip,
    write_partition,
)


@task(retries=2, retry_delay_seconds=60)
def probe_source(work_dir: str) -> dict:
    """Read both form listings and the coverage the poll needs, cheaply.

    The two forms move on different clocks and each needs its own coverage date:

    * **EIA-860** is annual, so the newest report year on the landing page is the
      answer and nothing has to be downloaded at all.
    * **EIA-923** is monthly, and a current-year file carries twelve months of
      columns whether or not they are reported — the latest month with data is
      only visible inside the workbook. So the newest year's ZIP alone (13-22 MB)
      is fetched and its page 1 cleaned, which is ~40 s. The other 640 MB is
      downloaded only after the poll says there is work.

    Args:
        work_dir: Scratch directory for this flow run; the probe writes its
            download and its one-year parquet underneath it.

    Returns:
        ``{"years": {form: [...]}, "max_date": {form: "YYYY-MM-DD"}}``.

    Raises:
        RuntimeError: If no EIA-923 report year carries any generation rows.
    """
    input_dir = Path(work_dir) / "input"
    output_dir = Path(work_dir) / "probe"

    listings = {form: list_source_files(form) for form in ("eia860", "eia923")}
    years = {form: sorted(listing) for form, listing in listings.items()}

    latest_860 = years["eia860"][-1]
    max_date = {"eia860": f"{latest_860}-01-01"}

    # Walk back from the newest EIA-923 year until one actually has rows. A ZIP
    # published at the very start of a report year can carry a workbook with the
    # twelve monthly columns and nothing in them; taking its (empty) coverage
    # would hand the poll a nonsense date.
    for candidate in reversed(years["eia923"]):
        download_form(
            "eia923", input_dir, listing=listings["eia923"], years=[candidate]
        )
        with YearReader(
            "eia923", candidate, source_zip(input_dir, "eia923", candidate)
        ) as reader:
            frame = BUILDERS["generation_fuel"](reader)
        rows = write_partition(
            frame,
            load_cols("generation_fuel"),
            output_dir / "generation_fuel",
            candidate,
        )
        if rows:
            max_date["eia923"] = source_max_date("generation_fuel", output_dir)
            break
    else:
        raise RuntimeError(
            "no EIA-923 report year carries any generation rows"
        )

    return {"years": years, "max_date": max_date}


@task(retries=2, retry_delay_seconds=60)
def download_corpus(work_dir: str, probe: dict) -> str:
    """Download every report year of both forms.

    Args:
        work_dir: Scratch directory for this flow run.
        probe: Result of :func:`probe_source`; its ``years`` decides what to
            fetch, so the flow downloads exactly the years the poll saw.

    Returns:
        The input directory, as a string (Prefect serializes task results).
    """
    input_dir = Path(work_dir) / "input"
    for form in ("eia860", "eia923"):
        download_form(form, input_dir, years=probe["years"][form])
    return str(input_dir)


@task
def clean_corpus(work_dir: str, input_dir: str) -> dict:
    """Clean every report year to partitioned parquet, then build the dicionario.

    Every year is rebuilt, not only the newest. EIA republishes a report year
    several times — a monthly file during the year, an early release the
    following spring, then one or more final revisions — and each publication
    **supersedes** the last rather than extending it. Appending the new file to
    the old one would double every month it restates. Rebuilding each year's
    partition from the newest file EIA serves makes that structurally impossible,
    and it also keeps the ``dicionario``, which has no year partition, computed
    over the whole record rather than over whichever years happened to be
    refreshed.

    Args:
        work_dir: Scratch directory for this flow run; the parquet is written
            underneath it.
        input_dir: Directory holding the downloaded source ZIPs.

    Returns:
        Table slug -> partitioned output directory (as strings), plus
        ``"row_counts"`` mapping each table to its row count.
    """
    output_dir = Path(work_dir) / "output"
    counts = clean_all(Path(input_dir), output_dir, log=print)
    counts["dicionario"] = build_dicionario(output_dir)
    result: dict = {t: str(output_dir / t) for t in constants.ALL_TABLES.value}
    result["row_counts"] = counts
    return result
