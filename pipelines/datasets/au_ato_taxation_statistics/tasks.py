"""Prefect 3 tasks for au_ato_taxation_statistics — thin wrappers over
utils.py, plus the check_update/extract_and_load entry points the staged
pipeline dispatches through (see the banner above `make_extract_load_data`
below).
"""

import shutil
import tempfile
from collections.abc import Callable
from datetime import date
from pathlib import Path

from prefect import task

from pipelines.datasets.au_ato_taxation_statistics.constants import constants
from pipelines.datasets.au_ato_taxation_statistics.utils import (
    clean_all,
    download_all,
    release_year,
)
from pipelines.utils.stage_dispatch import (
    ExtractAndLoad,
    SourceInspection,
    pipeline_factory,
)

DATASET_ID = constants.DATASET_ID.value

# Table ids. Kept as plain literals (not Enum members) so this module and
# flows.py can import them directly — one check_update/extract_and_load pair
# per table (see the module banner).
DATA_TABLE_IDS = list(constants.TABLE_SELECTORS.value)
GST_TABLE_ID = "gst_industry"
DICIONARIO_TABLE_ID = "dicionario"


@task(retries=2, retry_delay_seconds=30)
def download_taxstats(work_dir: str) -> dict:
    """Download every curated workbook for every in-scope release.

    Retries twice: data.gov.au occasionally drops connections partway through
    the ~40 file downloads.

    Args:
        work_dir: Directory to download into; files land in ``<work_dir>/input``.

    Returns:
        A mapping with ``"input_dir"`` and ``"max_year"`` — the start year of
        the newest release, which drives the source-update poll.
    """
    input_dir = Path(work_dir) / "input"
    releases = download_all(input_dir)
    return {
        "input_dir": str(input_dir),
        "max_year": release_year(releases[-1]),
    }


@task
def clean_taxstats(work_dir: str, input_dir: str) -> dict:
    """Build every partitioned table from the downloaded workbooks.

    Args:
        work_dir: Directory to write into; tables land under ``<work_dir>/output``.
        input_dir: Directory holding the workbooks, from :func:`download_taxstats`.

    Returns:
        A mapping of table slug to its partitioned output directory.
    """
    output_dir = Path(work_dir) / "output"
    counts = clean_all(Path(input_dir), output_dir)
    for table, rows in sorted(counts.items()):
        print(f"{table}: {rows:,} rows")
    return {table: str(output_dir / table) for table in counts}


def _latest_release_year() -> int:
    """Download the whole collection just to learn the newest release's year.

    data.gov.au exposes no cheaper update signal than the payload itself, so
    this pays the same ~92 MB download as `extract_load_data` — accepted
    duplicate cost (see the module banner) in exchange for the uniform
    check_update/extract_and_load shape. The files are discarded immediately:
    only the parsed `max_year` from `download_taxstats` survives.
    """
    work_dir = tempfile.mkdtemp(prefix="au_ato_taxation_statistics_check_")
    try:
        return download_taxstats(work_dir=work_dir)["max_year"]
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


# ──────────────────────────────────────────────────────────────────────────────
# check_update / extract_and_load — one independent pair per table.
#
# The downloaded workbooks build all 6 tables (5 fact tables + dicionario) in
# one pass — `clean_all` folds every selector's frames together to derive
# `dicionario` (see utils.py). The one-pipeline-per-table_id shape below
# (`pipeline_factory`/`CheckThenExtractLoadPipeline`, the repo-wide rule)
# means each table's extract_and_load redownloads the shared ~92 MB payload
# and reruns the whole clean from scratch, keeping only its own table —
# redundant, but uniformity (1 flow per table, no cross-table coupling) now
# outweighs the saved bandwidth/CPU. `au_ato_abr` took the same trade-off for
# a similar shared-download, multi-table source; its fact tables carry the
# same kind of `custom_dictionary_coverage` dbt test referencing their
# `dicionario` model as this dataset's do, and that test is resolved by dbt's
# own `ref()` against whatever `dicionario` already sits in the warehouse —
# it does not require the tables to be rebuilt in the same run, so splitting
# each table into its own independently-dispatched check_update/
# extract_and_load pair is safe here too.
#
# `get_latest_update` varies only for `gst_industry` (one year ahead of the
# release, see `constants.py`) and for `dicionario` (no Coverage baseline) —
# wired per table via `make_get_latest_update` below.
# ──────────────────────────────────────────────────────────────────────────────


def make_get_latest_update(table_id: str) -> Callable[[], SourceInspection]:
    """Build `table_id`'s check_update: download the collection, pick its year.

    See the module banner for why every table redownloads the same payload.
    """

    def get_latest_update() -> SourceInspection:
        """Resolve the release year relevant to `table_id` (full download).

        Returns:
            `SourceInspection` with the resolved year as `reference_date`
            (always January 1st — this dataset's granularity is the
            financial year, not a calendar date). `gst_industry`'s own data
            year runs one ahead of the release that ships it (the GST
            by-industry table in the 2023-24 release covers 2024-25), so its
            `reference_date` is offset by one year relative to the other
            tables — comparing it straight against the release year would
            never match its actual `Coverage` end year.
            `compare_against="table_update"` for `dicionario`: it is
            `NonHistorical` (see `COVERAGE`), so it has no `Coverage`
            baseline to compare against, same reasoning as `au_ato_abr`'s
            `dicionario`.
        """
        release_start_year = _latest_release_year()
        year = (
            release_start_year + 1
            if table_id == GST_TABLE_ID
            else release_start_year
        )
        reference_date = date(year, 1, 1)
        if table_id == DICIONARIO_TABLE_ID:
            return SourceInspection(
                reference_date=reference_date, compare_against="table_update"
            )
        return SourceInspection(reference_date=reference_date)

    return get_latest_update


def make_extract_load_data(table_id: str) -> Callable[[dict], ExtractAndLoad]:
    """Build `table_id`'s extract_and_load: download + full re-clean, keep only its table.

    See the module banner for why each table redoes the whole download+clean
    instead of sharing one pass across all 6.
    """

    def extract_load_data(download_params: dict) -> ExtractAndLoad:
        """Download the collection and clean it, keeping only `table_id`.

        Args:
            download_params: `{"reference_date": "<resolved release year>"}`
                — unused here: the real per-row `year` is parsed straight out
                of each workbook's sheet title by `clean_all`, not derived
                from the check-time year. Kept for interface symmetry with
                `check_update_and_dispatch`, which always passes
                `reference_date`.

        Returns:
            `ExtractAndLoad` for `table_id`, with `dump_mode="overwrite"`/
            `source_format="parquet"` — the ATO reissues the whole collection
            each year and revises earlier years in place, so every run does a
            full replace rather than appending the newest year.
        """
        # Not cleaned up afterwards (no `shutil.rmtree`/`finally`): the
        # returned `ExtractAndLoad.data_path` points inside `work_dir` and is
        # only read (via `upload_to_gcs`) right after this returns, inside
        # the same flow run — `CheckThenExtractLoadPipeline.run_extract_and_load`
        # uploads immediately, then the flow (and its worker) ends. Same
        # trade-off as `au_ato_abr.tasks.make_extract_load_data`.
        work_dir = tempfile.mkdtemp(
            prefix=f"au_ato_taxation_statistics_{table_id}_"
        )
        input_dir = download_taxstats(work_dir=work_dir)["input_dir"]
        result = clean_taxstats(work_dir=work_dir, input_dir=input_dir)

        return ExtractAndLoad(
            data_path=result[table_id],
            dump_mode="overwrite",
            source_format="parquet",
        )

    return extract_load_data


make_pipeline = pipeline_factory(
    DATASET_ID,
    make_get_latest_update,
    make_extract_load_data,
    date_format="%Y",
)
