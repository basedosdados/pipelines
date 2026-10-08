"""Prefect 3 tasks for us_bls_oes — thin wrappers over utils.py, plus the
check_update/extract_and_load entry points the staged pipeline dispatches
through (see the banner above `get_latest_update` below).
"""

from datetime import date
from pathlib import Path

from prefect import task

from pipelines.datasets.us_bls_oes.constants import COVERAGE, constants
from pipelines.datasets.us_bls_oes.utils import (
    assert_dictionary_labels,
    clean_year,
    download_release,
    latest_source_year,
)
from pipelines.utils.stage_dispatch import ExtractAndLoad, SourceInspection


@task(retries=2, retry_delay_seconds=60)
def resolve_latest_year() -> int:
    """Read the OEWS tables page and return the newest published reference year.

    Retries: www.bls.gov intermittently rate-limits.

    Returns:
        Four-digit reference year of the newest release.
    """
    year = latest_source_year()
    print(f"newest OEWS release on the tables page: May {year}")
    return year


@task(retries=2, retry_delay_seconds=60)
def download_oes(work_dir: str, year: int) -> str:
    """Download one OEWS release.

    Args:
        work_dir: Directory to download into; files land in ``<work_dir>/input``.
        year: Reference year, from :func:`resolve_latest_year`.

    Returns:
        The input directory path, as a string (Prefect serializes task results).
    """
    input_dir = Path(work_dir) / "input"
    download_release(input_dir, year)
    return str(input_dir)


@task
def clean_oes(work_dir: str, input_dir: str, year: int) -> dict:
    """Clean one release into the `area` and `industry` partitions for that year.

    Only the new year is cleaned. The tables are partitioned by year and the
    staging object path carries the partition, so re-running a year overwrites
    that partition and leaves every earlier one untouched.

    Args:
        work_dir: Directory to write into; tables land under ``<work_dir>/output``.
        input_dir: Directory holding the downloaded zips, from
            :func:`download_oes`.
        year: Reference year.

    Returns:
        Table slug to its partitioned output directory, plus ``"year"`` and
        ``"rows"`` for logging.
    """
    output_dir = Path(work_dir) / "output"
    counts = clean_year(Path(input_dir), output_dir, year)
    assert_dictionary_labels(output_dir, year)
    print(f"May {year}: " + ", ".join(f"{t}={n:,}" for t, n in counts.items()))
    result: dict = {
        table: str(output_dir / table) for table in constants.DATA_TABLES.value
    }
    result["year"] = year
    result["rows"] = counts
    return result


# ──────────────────────────────────────────────────────────────────────────────
# check_update / extract_and_load
#
# One release zip builds both tables (`area`, `industry`) in a single pass —
# `download_oes` fetches one shared payload and `clean_oes` splits it, so the
# download cannot be decomposed per table. This dataset therefore keeps ONE
# check_update + ONE extract_and_load for the whole dataset (anchored on
# `POLL_TABLE`), same shape as `au_geoscape_gnaf`: `extract_load_data` returns
# one `ExtractAndLoad` per table, and `flows.py` loops over them to upload +
# dispatch `build_and_promote` once per table, using the stage_dispatch
# building blocks directly instead of `CheckThenExtractLoadPipeline` (built for
# the one-table-per-call shape).
# ──────────────────────────────────────────────────────────────────────────────


def get_latest_update() -> SourceInspection:
    """Resolve the newest published OEWS reference year (cheap, no download).

    Returns:
        `SourceInspection` with May of the newest reference year as
        `reference_date` — OEWS always references May of the publication year.
    """
    year = resolve_latest_year()
    return SourceInspection(reference_date=date(year, 5, 1))


def extract_load_data(
    work_dir: str, download_params: dict
) -> dict[str, ExtractAndLoad]:
    """Download + clean the newest release into the `area`/`industry` tables.

    Args:
        work_dir: Scratch directory for this run. Created and removed by the
            caller (`flows.py`, only after every table has been uploaded),
            since the returned `data_path`s point inside it.
        download_params: `{"reference_date": "<year>-05-01"}` —
            `reference_date` is `get_latest_update`'s resolved year,
            round-tripped through `check_update_and_dispatch`
            (`date.isoformat()`).

    Returns:
        One `ExtractAndLoad` per table in `constants.DATA_TABLES`, both
        `dump_mode="append"`/`source_format="parquet"` — a run only appends
        the new year's partition, never rebuilds the panel.
    """
    year = date.fromisoformat(download_params["reference_date"]).year
    input_dir = download_oes(work_dir=work_dir, year=year)
    result = clean_oes(work_dir=work_dir, input_dir=input_dir, year=year)
    return {
        table: ExtractAndLoad(
            coverage=COVERAGE[table].model_dump(),
            data_path=result[table],
            dump_mode="append",
            source_format="parquet",
        )
        for table in constants.DATA_TABLES.value
    }
