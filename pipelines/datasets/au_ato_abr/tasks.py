"""Prefect 3 tasks for au_ato_abr — thin wrappers over utils.py, plus the
check_update/extract_and_load entry points the staged pipeline dispatches
through (see the banner above `get_latest_update` below).
"""

from datetime import date
from pathlib import Path

from prefect import task

from pipelines.datasets.au_ato_abr.constants import COVERAGE, constants
from pipelines.datasets.au_ato_abr.utils import (
    clean_all,
    download_zips,
    source_last_modified,
)
from pipelines.utils.stage_dispatch import ExtractAndLoad, SourceInspection


@task(retries=2, retry_delay_seconds=30)
def check_source_abr() -> str:
    """Return the source's newest publication date ("YYYY-MM-DD") via HTTP HEAD.

    Cheap poll signal — no data download. Compared against ``Table.Update.latest``
    so the flow only fetches the payload when the source has republished.
    """
    return source_last_modified()


@task(retries=2, retry_delay_seconds=60)
def download_abr(work_dir: str) -> str:
    """Download the two ABN Bulk Extract ZIPs into ``<work_dir>/input``.

    Returns:
        The input directory path (Prefect serializes task results).
    """
    input_dir = Path(work_dir) / "input"
    download_zips(input_dir)
    return str(input_dir)


@task
def clean_abr(work_dir: str, input_dir: str) -> dict:
    """Parse the ZIPs into partitioned parquet under ``<work_dir>/output``.

    Returns:
        A mapping of table slug to its partitioned output directory, plus
        ``"max_extraction_date"`` (the snapshot date) and ``"counts"``.
    """
    output_dir = Path(work_dir) / "output"
    result = clean_all(Path(input_dir), output_dir)
    return {
        k: (str(v) if isinstance(v, Path) else v) for k, v in result.items()
    }


# ──────────────────────────────────────────────────────────────────────────────
# check_update / extract_and_load
#
# The two source ZIPs build all 4 tables (entity, other_name, dgr, dicionario)
# in a single streaming `lxml.iterparse` pass — `clean_all` already folds every
# table's rows together while walking each ZIP member once, and the download
# itself is a shared ~1 GB payload (see utils.py). Splitting this into 4
# independent check_update/extract_and_load pairs (the usual
# one-pipeline-per-table_id shape — see `pipeline_factory` in
# stage_dispatch.py) would redownload the ZIPs and rerun the whole streaming
# parse up to 4x for no reason. So this dataset keeps ONE check_update + ONE
# extract_and_load for the whole dataset (anchored on `CORE_TABLE`), and
# `extract_load_data` returns one `ExtractAndLoad` per table — `flows.py`
# loops over them to upload + dispatch `build_and_promote` once per table,
# using the stage_dispatch building blocks directly instead of
# `CheckThenExtractLoadPipeline` (built for the one-table-per-call shape).
# ──────────────────────────────────────────────────────────────────────────────


def get_latest_update() -> SourceInspection:
    """Resolve the source's newest publication date (cheap, no download).

    Returns:
        `SourceInspection` with the resolved publication date as
        `reference_date`. Compared against `Table.Update.latest`
        (`compare_against="table_update"`) rather than the default `Coverage`
        check: the three data tables carry a rolling BD Pro window (see
        `COVERAGE` in `constants.py`), so their free `Coverage.DateTimeRange`
        end date slides forward on every run (`free_end = source_end -
        free_lag`) and isn't a stable "have we already ingested this
        snapshot" baseline the way a plain dated table's `Coverage` is. No
        `extra_download_params`: the two ZIP URLs are fixed constants
        (`constants.ZIP_URLS`), not resolved per run, so there's nothing to
        round-trip through `extract_load_data`.
    """
    source_date = check_source_abr()
    return SourceInspection(
        reference_date=date.fromisoformat(source_date),
        compare_against="table_update",
    )


def extract_load_data(
    work_dir: str, download_params: dict
) -> dict[str, ExtractAndLoad]:
    """Download + clean the weekly snapshot into all 4 tables.

    Args:
        work_dir: Scratch directory for this run, created and cleaned up by
            the caller (`flows.py`) — *not* here, since the returned
            `ExtractAndLoad.data_path`s point inside it and are only read
            (via `upload_to_gcs`) *after* this function returns. Cleaning it
            up in a `finally` inside this function would delete those paths
            before the caller ever uploads them.
        download_params: `{"reference_date": "<source publication date>"}` —
            unused here: unlike au_geoscape_gnaf (which resolves a per-release
            download URL at check time), au_ato_abr's ZIP URLs are static, and
            the real per-row `extraction_date` is parsed straight out of each
            ZIP member's `ExtractTime` by `clean_all` — not derived from the
            check-time publication date. Kept for interface symmetry with
            `check_update_and_dispatch`, which always passes `reference_date`.

    Returns:
        One `ExtractAndLoad` per table in `constants.ALL_TABLES`, all sharing
        `dump_mode="overwrite"`/`source_format="parquet"` — same as the old
        monolithic flow (CNPJ-style stacking: history accumulates in the
        incremental dbt models, not in the staging dump mode).
    """
    input_dir = download_abr(work_dir=work_dir)
    result = clean_abr(work_dir=work_dir, input_dir=input_dir)
    return {
        table: ExtractAndLoad(
            coverage=COVERAGE[table].model_dump(),
            data_path=result[table],
            dump_mode="overwrite",
            source_format="parquet",
        )
        for table in constants.ALL_TABLES.value
    }
