"""Prefect 3 tasks for mx_sesnsp_incidencia_delictiva — one check_update/
extract_and_load pair per table, thin wrappers over utils.py.

The originally-planned "check_and_download" variant was cancelled — every
table uses the standard check_update -> extract_and_load shape, even though
check_update must itself scrape the rotating SharePoint tokens and download the
table's zip (several hundred MB) to read off the latest published month
(`make_get_latest_update`), and extract_and_load downloads it again,
independently (accepted duplicated cost, same as `br_cgu_sancoes`/
`au_ato_abr`/`au_geoscape_gnaf`).

`scrape_tokens()` resolves all four tables' SharePoint tokens in a single
landing-page scrape (one headless-Chrome/Imperva pass), but each table's
check_update/extract_and_load calls it independently and only uses its own
token — the accepted 1-flow-per-table trade-off, not a shared download step.
"""

import shutil
import tempfile
from collections.abc import Callable
from datetime import date
from pathlib import Path

from pipelines.datasets.mx_sesnsp_incidencia_delictiva.constants import (
    constants,
)
from pipelines.datasets.mx_sesnsp_incidencia_delictiva.utils import (
    clean_table,
    download_table,
    scrape_tokens,
)
from pipelines.utils.stage_dispatch import (
    ExtractAndLoad,
    SourceInspection,
    pipeline_factory,
)

DATASET_ID = constants.DATASET_ID.value


def make_get_latest_update(table_id: str) -> Callable[[], SourceInspection]:
    """Build the check_update callable for one table.

    Args:
        table_id: Table slug (one of `constants.ALL_TABLES`).

    Returns:
        A zero-arg callable that scrapes the current SharePoint tokens,
        downloads `table_id`'s own zip and cleans it just to read off the
        latest published year-month, discards the download, and returns a
        `SourceInspection` with that date (day fixed to 1, since the source
        signal is a competência, not a calendar day).
    """

    def get_latest_update() -> SourceInspection:
        work_dir = tempfile.mkdtemp(prefix=f"mx_sesnsp_check_{table_id}_")
        try:
            input_dir = Path(work_dir) / "input"
            input_dir.mkdir(parents=True, exist_ok=True)
            tokens = scrape_tokens()
            csv_path = download_table(tokens[table_id], table_id, input_dir)
            output_dir = Path(work_dir) / "output"
            _, max_ym = clean_table(csv_path, table_id, output_dir)
        finally:
            shutil.rmtree(work_dir, ignore_errors=True)

        if max_ym is None:
            raise ValueError(
                f"{table_id}: no published rows in the latest SESNSP "
                "release; cannot determine a reference date"
            )

        year, month = max_ym
        return SourceInspection(reference_date=date(year, month, 1))

    return get_latest_update


def make_extract_load_data(table_id: str) -> Callable[[dict], ExtractAndLoad]:
    """Build the extract_and_load callable for one table.

    Args:
        table_id: Table slug (one of `constants.ALL_TABLES`).

    Returns:
        A callable that scrapes the current SharePoint tokens again
        (independently of `get_latest_update` — the accepted duplicated
        cost, see the module banner) and downloads+cleans only `table_id`'s
        own zip. Not cleaned up afterwards (no `shutil.rmtree`/`finally`):
        the returned `data_path` points inside `work_dir` and is only read
        (via `upload_to_gcs`) right after this returns, inside the same flow
        run — same trade-off as `br_cgu_sancoes`/`au_ato_abr`/
        `au_geoscape_gnaf`.
    """

    def extract_load_data(download_params: dict) -> ExtractAndLoad:
        work_dir = tempfile.mkdtemp(prefix=f"mx_sesnsp_{table_id}_")
        input_dir = Path(work_dir) / "input"
        input_dir.mkdir(parents=True, exist_ok=True)
        tokens = scrape_tokens()
        csv_path = download_table(tokens[table_id], table_id, input_dir)
        output_dir = Path(work_dir) / "output"
        clean_table(csv_path, table_id, output_dir)

        # Each SESNSP release ships the full current-year history, so every
        # run is a full replace, not an incremental append.
        return ExtractAndLoad(
            data_path=str(output_dir / table_id),
            dump_mode="overwrite",
            source_format="parquet",
        )

    return extract_load_data


make_pipeline = pipeline_factory(
    DATASET_ID,
    make_get_latest_update,
    make_extract_load_data,
    # The source signal is a competência (year-month), not a calendar day.
    date_format="%Y-%m",
)
