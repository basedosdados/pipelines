"""Prefect 3 tasks for us_bls_cpi — thin wrappers over utils.py, plus the
make_get_latest_update/make_extract_load_data factories dispatched through
`CheckThenExtractLoadPipeline` (see the banner above the factories below).
"""

import shutil
import tempfile
from collections.abc import Callable
from datetime import date
from pathlib import Path

from prefect import task

from pipelines.datasets.us_bls_cpi.utils import clean_all, download_flatfiles
from pipelines.utils.stage_dispatch import ExtractAndLoad, SourceInspection


@task(retries=2, retry_delay_seconds=30)
def download_cpi(work_dir: str) -> str:
    """Download the CPI-U and CPI-W flat files from BLS.

    Retries twice: download.bls.gov intermittently rejects or drops connections
    on the larger by-group files.

    Args:
        work_dir: Directory to download into; files land in ``<work_dir>/input``.

    Returns:
        The input directory path, as a string (Prefect serializes task results).
    """
    input_dir = Path(work_dir) / "input"
    download_flatfiles(input_dir)
    return str(input_dir)


@task
def clean_cpi(work_dir: str, input_dir: str) -> dict:
    """Build the four partitioned tables from the downloaded flat files.

    Args:
        work_dir: Directory to write into; tables land under ``<work_dir>/output``.
        input_dir: Directory holding the downloaded flat files, from
            :func:`download_cpi`.

    Returns:
        A mapping of table slug to its partitioned output directory, plus
        ``"max_year_month"`` — the latest ``"YYYY-MM"`` present in the monthly
        table.
    """
    output_dir = Path(work_dir) / "output"
    result = clean_all(Path(input_dir), output_dir)
    return {
        k: (str(v) if isinstance(v, Path) else v) for k, v in result.items()
    }


def _max_partition_year(table_dir: str) -> int | None:
    """Latest ``year=`` partition actually written for a table.

    Reads partition folder names only (``<table_dir>/year=YYYY``) — the
    parquet itself was already built by :func:`clean_cpi`; this just reads off
    the latest year present, without opening any file. Used as the freshness
    signal for ``annual``/``semiannual``, whose own latest period can lag
    behind the monthly table's (BLS only emits the annual average, ``M13``,
    once all 12 months of a year are in — a new month alone doesn't imply a
    new annual or semiannual row).

    Args:
        table_dir: One table's output directory, from :func:`clean_cpi`.

    Returns:
        The highest ``year=`` value present, or ``None`` if the directory has
        no year partitions (e.g. it does not exist).
    """
    base = Path(table_dir)
    if not base.exists():
        return None
    years = [
        int(p.name.split("=", 1)[1])
        for p in base.iterdir()
        if p.is_dir() and p.name.startswith("year=")
    ]
    return max(years) if years else None


# ──────────────────────────────────────────────────────────────────────────────
# make_get_latest_update / make_extract_load_data
#
# One pipeline per table_id — the repo-wide default shape (see
# `pipeline_factory` in stage_dispatch.py, and `br_ibge_ipca` for the
# reference layout) — even though a single download builds all 4 tables
# (`monthly`, `annual`, `semiannual`, `dicionario`) in one pass over the CPI-U
# + CPI-W flat files (`clean_all`, see the module banner in utils.py). Each of
# the 4 independent `extract_load_data` calls below re-downloads the same
# several-hundred-MB flat files and reruns the whole clean, keeping only the
# one table it's responsible for and discarding the other 3 `clean_all` also
# built along the way. That redundancy is accepted in exchange for
# uniformity — every dataset gets exactly one check_update/extract_and_load
# pair per table_id, and the only exception to that rule is when tables
# genuinely depend on each other (e.g. a cross-table dbt test between the
# dataset's own models) — `models/us_bls_cpi/schema.yml` has no `ref()`
# between `us_bls_cpi__monthly`/`__annual`/`__semiannual`/`__dicionario`
# (only to the external `br_bd_diretorios_data_tempo` directory), so that
# exception does not apply here.
#
# BLS exposes no cheap "has this changed" signal — the flat files carry no
# usable Last-Modified, so the only way to know the latest published period is
# to download and parse them. Same as every other staged-pipeline dataset,
# `get_latest_update` downloads and cleans whatever `extract_load_data` would
# need to decide the reference date, and `extract_load_data` downloads and
# cleans it again, independently, when dispatched. Duplicate download cost,
# accepted in exchange for the uniform check_update -> extract_and_load shape
# (the `check_and_download` one-shot variant once considered for this dataset
# was cancelled before being built).
#
# `dicionario` has no date column (`NonHistorical`, see constants.py) and is
# rebuilt from the very same download as the other 3 tables, so its
# `get_latest_update` reuses the monthly table's `(year, month)` reference —
# same reasoning as `us_bea`/`au_geoscape_gnaf`'s `dicionario` handling — and
# only differs in `compare_against`: it reads `Table.Update.latest` instead of
# the default `Coverage.DateTimeRange`, since a `NonHistorical` table has no
# coverage baseline to compare a reference date against.
# ──────────────────────────────────────────────────────────────────────────────


def make_get_latest_update(table_id: str) -> Callable[[], SourceInspection]:
    """Build the check_update callable for one table.

    Args:
        table_id: One of ``constants.ALL_TABLES``.

    Returns:
        A zero-arg callable that downloads + cleans the flat files (discarded
        afterwards) and returns a `SourceInspection` with the latest
        reference date found at the source for this table.

    Raises:
        RuntimeError: If the monthly table comes back empty — an empty
            source, not "no new data": there is nothing to compare.
    """

    def get_latest_update() -> SourceInspection:
        work_dir = tempfile.mkdtemp(prefix=f"us_bls_cpi_{table_id}_check_")
        try:
            input_dir = download_cpi(work_dir=work_dir)
            result = clean_cpi(work_dir=work_dir, input_dir=input_dir)
            max_ym = result["max_year_month"]
            if max_ym is None:
                raise RuntimeError(
                    "us_bls_cpi: no monthly rows found at the source"
                )

            if table_id in ("monthly", "dicionario"):
                year, month = (int(x) for x in max_ym.split("-"))
                return SourceInspection(
                    reference_date=date(year, month, 1),
                    compare_against=(
                        "table_update"
                        if table_id == "dicionario"
                        else "coverage"
                    ),
                )

            # annual / semiannual: own max `year=` partition actually written
            # this run — see `_max_partition_year`.
            year = _max_partition_year(result[table_id])
            if year is None:
                raise RuntimeError(
                    f"us_bls_cpi: no {table_id} rows found at the source"
                )
            return SourceInspection(reference_date=date(year, 1, 1))
        finally:
            shutil.rmtree(work_dir, ignore_errors=True)

    return get_latest_update


def make_extract_load_data(table_id: str) -> Callable[[dict], ExtractAndLoad]:
    """Build the extract_and_load callable for one table.

    Args:
        table_id: One of ``constants.ALL_TABLES``.

    Returns:
        A callable that re-downloads and re-cleans the flat files
        (independently of `get_latest_update`, see the module banner above)
        and returns only this table's `ExtractAndLoad`. BLS flat files carry
        the full history on every release, so every table is a full replace
        (``dump_mode="overwrite"``), never an incremental append.
    """

    def extract_load_data(download_params: dict) -> ExtractAndLoad:
        # Not cleaned up here: each flow run is its own throwaway worker
        # container, same convention as `au_geoscape_gnaf`'s
        # `make_extract_load_data` — the caller
        # (`CheckThenExtractLoadPipeline.run_extract_and_load`) reads
        # `data_path` via `upload_to_gcs` right after this returns.
        work_dir = tempfile.mkdtemp(prefix=f"us_bls_cpi_{table_id}_")
        input_dir = download_cpi(work_dir=work_dir)
        result = clean_cpi(work_dir=work_dir, input_dir=input_dir)

        return ExtractAndLoad(
            data_path=result[table_id],
            dump_mode="overwrite",
            source_format="parquet",
        )

    return extract_load_data
