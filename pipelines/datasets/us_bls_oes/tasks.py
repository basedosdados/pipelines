"""Prefect 3 tasks for us_bls_oes — thin wrappers over utils.py, plus the
check_update/extract_and_load entry points the staged pipeline dispatches
through (see the banner above `get_latest_update` below).
"""

import tempfile
from collections.abc import Callable
from datetime import date
from pathlib import Path

from prefect import task

from pipelines.datasets.us_bls_oes.constants import (
    DATASET_ID,
    constants,
)
from pipelines.datasets.us_bls_oes.utils import (
    assert_dictionary_labels,
    clean_year,
    download_release,
    latest_source_year,
)
from pipelines.utils.stage_dispatch import (
    ExtractAndLoad,
    SourceInspection,
    pipeline_factory,
)


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
# area / industry — one check_update + one extract_and_load per table (see
# constants.DATA_TABLES), same shape as `br_ibge_ipca`/`br_mf_divida_ativa`.
#
# A single OEWS release zip builds both tables in one pass — `download_oes`
# fetches the shared payload and `clean_oes` splits it into `area`/`industry` —
# so the download genuinely cannot be decomposed per table. Per the "1 flow per
# table" rule, each table's `extract_load_data` still downloads + cleans the
# release independently (redundant work accepted for uniformity) and keeps
# only the partition it is responsible for; the other table's output is
# discarded.
#
# `get_latest_update` is identical for both tables (OEWS publishes one
# reference year for the whole release), so it is shared via
# `lambda _table_id: get_latest_update` in `make_pipeline` below, rather than a
# real per-table factory.
# ──────────────────────────────────────────────────────────────────────────────


def get_latest_update() -> SourceInspection:
    """Resolve the newest published OEWS reference year (cheap, no download).

    Shared by both tables — see the banner above.

    Returns:
        `SourceInspection` with May of the newest reference year as
        `reference_date` — OEWS always references May of the publication year.
    """
    year = resolve_latest_year()
    return SourceInspection(reference_date=date(year, 5, 1))


def make_extract_load_data(table_id: str) -> Callable[[dict], ExtractAndLoad]:
    """Build the extract_and_load callable for one table.

    Args:
        table_id: Table to extract (`"area"` or `"industry"`).

    Returns:
        A callable downloading + cleaning the newest release into its own
        scratch directory and returning only `table_id`'s partition.
    """

    def extract_load_data(download_params: dict) -> ExtractAndLoad:
        """Download + clean the newest release, keeping only `table_id`.

        Args:
            download_params: `{"reference_date": "<year>-05-01"}` —
                `reference_date` is `get_latest_update`'s resolved year,
                round-tripped through `check_update_and_dispatch`
                (`date.isoformat()`).

        Returns:
            `ExtractAndLoad` for `table_id`, `dump_mode="append"`/
            `source_format="parquet"` — a run only appends the new year's
            partition, never rebuilds the panel.
        """
        year = date.fromisoformat(download_params["reference_date"]).year
        work_dir = tempfile.mkdtemp(prefix=f"us_bls_oes_{table_id}_")
        input_dir = download_oes(work_dir=work_dir, year=year)
        result = clean_oes(work_dir=work_dir, input_dir=input_dir, year=year)
        return ExtractAndLoad(
            data_path=result[table_id],
            dump_mode="append",
            source_format="parquet",
        )

    return extract_load_data


make_pipeline = pipeline_factory(
    DATASET_ID,
    # Single check shared by both tables — see the banner above.
    lambda _table_id: get_latest_update,
    make_extract_load_data,
    date_format="%Y",
)
