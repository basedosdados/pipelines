"""Prefect 3 tasks for au_geoscape_gnaf — thin wrappers over utils.py, plus the
make_get_latest_update/make_extract_load_data factories dispatched through
`CheckThenExtractLoadPipeline` (see the banner above the factories below).
"""

import tempfile
from collections.abc import Callable
from datetime import date
from pathlib import Path

from prefect import task

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
# make_get_latest_update / make_extract_load_data
#
# One pipeline per table_id — the repo-wide default shape (see
# `pipeline_factory` in stage_dispatch.py, and `br_ibge_ipca` for the
# reference layout) — even though a single release zip builds all 4 tables
# (address_detail, street_locality, locality, dicionario) in one pass over the
# per-state PSVs (`clean_all`, see the module banner in utils.py). Each of the
# 4 independent `extract_load_data` calls below re-downloads the same ~1.6 GB
# zip and reruns the whole per-state clean, keeping only the one table it's
# responsible for and discarding the other 3 `clean_all` also built along the
# way. That redundancy is accepted in exchange for uniformity — every dataset
# gets exactly one check_update/extract_and_load pair per table_id, and the
# only exception to that rule is when tables genuinely depend on each other
# (e.g. a cross-table dbt test), which isn't the case here. `clean_all` itself
# is untouched: it still folds all 4 tables together while walking each state
# once; `make_extract_load_data` just takes the one slice it needs out of
# what it returns.
#
# `dicionario` has no date column (`NonHistorical`, see constants.py) but is
# rebuilt from the very same release zip as the other 3 tables, so its
# `get_latest_update` resolves the same CKAN snapshot and only differs in
# `compare_against`: it reads `Table.Update.latest` instead of the default
# `Coverage.DateTimeRange` (same reasoning as au_ato_abr's `dicionario`
# handling — a `NonHistorical` table has no coverage baseline to compare a
# snapshot date against).
# ──────────────────────────────────────────────────────────────────────────────


def make_get_latest_update(table_id: str) -> Callable[[], SourceInspection]:
    """Build the check_update callable for one table.

    Args:
        table_id: Table slug (one of `constants.ALL_TABLES`).

    Returns:
        A zero-arg callable that resolves the current quarterly release from
        CKAN (cheap, no download) into a `SourceInspection` — the resolved
        `snapshot_date` as `reference_date`, and the resolved download `url`
        carried through to `extract_load_data` via `extra_download_params`
        (re-querying CKAN in extract_and_load could race a brand new release
        landing in between).
    """

    def get_latest_update() -> SourceInspection:
        source = check_source_gnaf()
        return SourceInspection(
            reference_date=date.fromisoformat(source["snapshot_date"]),
            extra_download_params={"url": source["url"]},
            compare_against=(
                "table_update" if table_id == "dicionario" else "coverage"
            ),
        )

    return get_latest_update


def make_extract_load_data(
    table_id: str,
) -> Callable[[dict], ExtractAndLoad]:
    """Build the extract_and_load callable for one table.

    Args:
        table_id: Table slug (one of `constants.ALL_TABLES`).

    Returns:
        A callable that downloads + cleans the whole release zip again (the
        redundant-by-design re-download described in the banner above) and
        returns only this table's `ExtractAndLoad`.
    """

    def extract_load_data(download_params: dict) -> ExtractAndLoad:
        # Not cleaned up here: each flow run is its own throwaway worker
        # container, same convention as br_mf_divida_ativa's
        # `make_extract_load_data` — the caller (`CheckThenExtractLoadPipeline
        # .run_extract_and_load`) reads `data_path` via `upload_to_gcs` after
        # this function returns, so deleting it first would break the upload.
        work_dir = tempfile.mkdtemp(prefix=f"au_geoscape_gnaf_{table_id}_")
        zip_path = download_gnaf(work_dir=work_dir, url=download_params["url"])
        result = clean_gnaf(
            work_dir=work_dir,
            zip_path=zip_path,
            snapshot_date=download_params["reference_date"],
        )
        return ExtractAndLoad(
            data_path=result[table_id],
            dump_mode="overwrite",
            source_format="parquet",
        )

    return extract_load_data
