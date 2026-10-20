"""Prefect 3 tasks for au_ato_abr — thin wrappers over utils.py, plus the
check_update/extract_and_load entry points the staged pipeline dispatches
through (see the banner above `make_extract_load_data` below).
"""

import tempfile
from collections.abc import Callable
from datetime import date
from pathlib import Path

from prefect import task

from pipelines.datasets.au_ato_abr.constants import COVERAGE, constants
from pipelines.datasets.au_ato_abr.utils import (
    clean_all,
    download_zips,
    source_last_modified,
)
from pipelines.utils.stage_dispatch import (
    ExtractAndLoad,
    SourceInspection,
    pipeline_factory,
)

DATASET_ID = constants.DATASET_ID.value


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
def clean_abr(work_dir: str, input_dir: str, table_id: str) -> dict:
    """Parse the ZIPs into partitioned parquet under ``<work_dir>/output``, keeping only ``table_id``.

    Returns:
        A mapping with ``table_id``'s output directory, plus
        ``"max_extraction_date"`` (the snapshot date) and ``"counts"``.
    """
    output_dir = Path(work_dir) / "output"
    result = clean_all(Path(input_dir), output_dir, tables=[table_id])
    return {
        k: (str(v) if isinstance(v, Path) else v) for k, v in result.items()
    }


# ──────────────────────────────────────────────────────────────────────────────
# check_update / extract_and_load — one independent pair per table.
#
# The two source ZIPs build all 4 tables (entity, other_name, dgr, dicionario)
# in a single streaming `lxml.iterparse` pass — `clean_all` already folds every
# table's rows together while walking each ZIP member once (see utils.py). The
# one-pipeline-per-table_id shape below (`pipeline_factory`/
# `CheckThenExtractLoadPipeline`, the repo-wide rule) means each table's
# extract_and_load redownloads the shared ~1 GB payload and reruns the whole
# streaming parse from scratch, keeping only its own table — redundant, but
# uniformity (1 flow per table, no cross-table coupling) now outweighs the
# saved bandwidth/CPU. `br_mf_divida_ativa` took the same trade-off for a
# similar shared-download, multi-table source.
#
# `get_latest_update` doesn't vary by table (same two ZIPs, same HEAD poll) —
# wired into `make_pipeline` via `lambda _: get_latest_update` rather than a
# real per-table factory, same pattern as `br_mf_divida_ativa.tasks`.
# ──────────────────────────────────────────────────────────────────────────────


def get_latest_update() -> SourceInspection:
    """Resolve the source's newest publication date (cheap, no download).

    Shared by all 4 tables — see the module banner.

    Returns:
        `SourceInspection` with the resolved publication date as
        `reference_date`. Compared against `Table.Update.latest`
        (`compare_against="table_update"`) rather than the default `Coverage`
        check: the three data tables carry a rolling BD Pro window (see
        `COVERAGE` in `constants.py`), so their free `Coverage.DateTimeRange`
        end date slides forward on every run (`free_end = source_end -
        free_lag`) and isn't a stable "have we already ingested this
        snapshot" baseline the way a plain dated table's `Coverage` is;
        `dicionario` is `NonHistorical` and needs `table_update` for the
        usual reason instead (no `Coverage` baseline at all). No
        `extra_download_params`: the two ZIP URLs are fixed constants
        (`constants.ZIP_URLS`), not resolved per run, so there's nothing to
        round-trip through `extract_load_data`.
    """
    source_date = check_source_abr()
    return SourceInspection(
        reference_date=date.fromisoformat(source_date),
        compare_against="table_update",
    )


def make_extract_load_data(table_id: str) -> Callable[[dict], ExtractAndLoad]:
    """Build `table_id`'s extract_and_load: download + full re-parse, keep only its table.

    See the module banner for why each table redoes the whole download+parse
    instead of sharing one pass across all 4.
    """

    def extract_load_data(download_params: dict) -> ExtractAndLoad:
        """Download + clean the weekly snapshot, keeping only `table_id`.

        Args:
            download_params: `{"reference_date": "<source publication date>"}`
                — unused here: unlike au_geoscape_gnaf (which resolves a
                per-release download URL at check time), au_ato_abr's ZIP
                URLs are static, and the real per-row `extraction_date` is
                parsed straight out of each ZIP member's `ExtractTime` by
                `clean_all` — not derived from the check-time publication
                date. Kept for interface symmetry with
                `check_update_and_dispatch`, which always passes
                `reference_date`.

        Returns:
            `ExtractAndLoad` for `table_id`, with `dump_mode="overwrite"`/
            `source_format="parquet"` — same as before the per-table split
            (CNPJ-style stacking: history accumulates in the incremental dbt
            models, not in the staging dump mode).
        """
        # Not cleaned up afterwards (no `shutil.rmtree`/`finally`): the
        # returned `ExtractAndLoad.data_path` points inside `work_dir` and is
        # only read (via `upload_to_gcs`) right after this returns, inside
        # the same flow run — `CheckThenExtractLoadPipeline.run_extract_and_load`
        # uploads immediately, then the flow (and its worker) ends. Same
        # trade-off as `br_mf_divida_ativa.tasks.make_extract_load_data`.
        work_dir = tempfile.mkdtemp(prefix=f"au_ato_abr_{table_id}_")
        input_dir = download_abr(work_dir=work_dir)
        result = clean_abr(
            work_dir=work_dir, input_dir=input_dir, table_id=table_id
        )

        return ExtractAndLoad(
            coverage=COVERAGE[table_id].model_dump(),
            data_path=result[table_id],
            dump_mode="overwrite",
            source_format="parquet",
        )

    return extract_load_data


make_pipeline = pipeline_factory(
    DATASET_ID,
    # Single check shared by all 4 tables — see the module banner.
    lambda _table_id: get_latest_update,
    make_extract_load_data,
)
