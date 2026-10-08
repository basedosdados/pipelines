"""Prefect 3 tasks for au_geoscape_gnaf — thin wrappers over utils.py, plus the
check_update/extract_and_load entry points the staged pipeline dispatches
through (see the banner above `get_latest_update` below).
"""

import shutil
import tempfile
from datetime import date
from pathlib import Path

from prefect import task

from pipelines.datasets.au_geoscape_gnaf.constants import COVERAGE, constants
from pipelines.datasets.au_geoscape_gnaf.utils import (
    clean_all,
    download_zip,
    resolve_source,
)
from pipelines.utils.stage_dispatch import ExtractAndLoad, SourceInspection


@task(retries=2, retry_delay_seconds=30)
def check_source_gnaf() -> dict:
    """Resolve the current GDA2020 all-states release from the CKAN API.

    Cheap poll signal — no data download. Returns the resolved download URL and
    the derived ``snapshot_date`` (first of the release month), a coverage-style
    date compared against the free ``Coverage`` so the flow only fetches the
    ~1.6 GB payload when a newer quarterly snapshot has been published.

    Returns:
        ``{"url": <download url>, "snapshot_date": "YYYY-MM-01"}``.
    """
    return resolve_source()


@task(retries=2, retry_delay_seconds=60)
def download_gnaf(work_dir: str, url: str) -> str:
    """Download the release zip into ``<work_dir>/input``.

    Args:
        work_dir: Scratch directory for this run.
        url: Download URL resolved by ``check_source_gnaf``.

    Returns:
        The path of the downloaded zip (Prefect serializes task results).
    """
    dest = download_zip(url, Path(work_dir) / "input")
    return str(dest)


@task
def clean_gnaf(work_dir: str, zip_path: str, snapshot_date: str) -> dict:
    """Clean the release into all-STRING partitioned parquet under ``output``.

    Args:
        work_dir: Scratch directory for this run.
        zip_path: Path of the downloaded release zip.
        snapshot_date: Snapshot date ``"YYYY-MM-DD"``.

    Returns:
        A mapping of table slug to its partitioned output directory, plus
        ``"snapshot_date"`` and ``"counts"``.
    """
    output_dir = Path(work_dir) / "output"
    result = clean_all(
        zip_path=Path(zip_path),
        output_dir=output_dir,
        snapshot_date=snapshot_date,
        stringify=True,
    )
    return {
        k: (str(v) if isinstance(v, Path) else v) for k, v in result.items()
    }


# ──────────────────────────────────────────────────────────────────────────────
# check_update / extract_and_load
#
# One release zip builds all 4 tables (address_detail, street_locality,
# locality, dicionario) in a single pass over the per-state PSVs — `clean_all`
# already folds every table's columns together while iterating states once,
# and the download itself is a shared ~1.6 GB payload (see utils.py). Splitting
# this into 4 independent check_update/extract_and_load pairs (the usual
# one-pipeline-per-table_id shape — see `pipeline_factory` in stage_dispatch.py)
# would redownload the zip and rerun the whole per-state clean up to 4x for no
# reason, blowing well past the memory budget `_tune_container_memory`/`_free`
# work to stay under. So this dataset keeps ONE check_update + ONE
# extract_and_load for the whole dataset (anchored on `CORE_TABLE`), and
# `extract_load_data` returns one `ExtractAndLoad` per table —
# `flows.py` loops over them to upload + dispatch `build_and_promote` once per
# table, using the stage_dispatch building blocks directly instead of
# `CheckThenExtractLoadPipeline` (built for the one-table-per-call shape).
# ──────────────────────────────────────────────────────────────────────────────


def get_latest_update() -> SourceInspection:
    """Resolve the current quarterly release from CKAN (cheap, no download).

    Returns:
        `SourceInspection` with the resolved `snapshot_date` as
        `reference_date`, and the resolved download `url` carried through to
        `extract_load_data` via `extra_download_params` — re-querying CKAN in
        extract_and_load could race a brand new release landing in between.
    """
    source = check_source_gnaf()
    return SourceInspection(
        reference_date=date.fromisoformat(source["snapshot_date"]),
        extra_download_params={"url": source["url"]},
    )


def extract_load_data(download_params: dict) -> dict[str, ExtractAndLoad]:
    """Download + clean the quarterly release into all 4 tables.

    Args:
        download_params: `{"reference_date": "<snapshot_date>", "url": ...}`
            — `reference_date` is `get_latest_update`'s resolved
            `snapshot_date` round-tripped through `check_update_and_dispatch`
            (`date.isoformat()`), `url` comes from `extra_download_params`.

    Returns:
        One `ExtractAndLoad` per table in `constants.ALL_TABLES`, all sharing
        `dump_mode="overwrite"`/`source_format="parquet"` — same as the old
        monolithic flow (CNPJ-style stacking: history accumulates in the
        incremental dbt models, not in the staging dump mode).
    """
    work_dir = tempfile.mkdtemp(prefix="au_geoscape_gnaf_")
    try:
        zip_path = download_gnaf(work_dir=work_dir, url=download_params["url"])
        result = clean_gnaf(
            work_dir=work_dir,
            zip_path=zip_path,
            snapshot_date=download_params["reference_date"],
        )
        return {
            table: ExtractAndLoad(
                coverage=COVERAGE[table].model_dump(),
                data_path=result[table],
                dump_mode="overwrite",
                source_format="parquet",
            )
            for table in constants.ALL_TABLES.value
        }
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)
