"""Prefect 3 tasks for au_abs_labour_force — thin wrappers over utils.py, plus the
check_update/extract_and_load entry points the staged pipeline dispatches
through (see the module banner in flows.py for why each table is now an
independent check_update/extract_and_load pair).
"""

import tempfile
from collections.abc import Callable
from datetime import date
from pathlib import Path

import pandas as pd

from pipelines.datasets.au_abs_labour_force.constants import (
    constants,
)
from pipelines.datasets.au_abs_labour_force.utils import (
    clean_hours_worked,
    clean_labour_force_status,
    clean_status_in_employment,
    clean_underutilisation,
    download_excel,
    download_sdmx,
    write_partitioned,
)
from pipelines.utils.stage_dispatch import (
    ExtractAndLoad,
    SourceInspection,
    pipeline_factory,
)

DATASET_ID = constants.DATASET_ID.value

# Release-period helpers — pipeline-only (resolve the source-poll key + the
# month-stamped Excel URL), so they live here rather than in the shared transform.
_MONTHS = [
    "jan",
    "feb",
    "mar",
    "apr",
    "may",
    "jun",
    "jul",
    "aug",
    "sep",
    "oct",
    "nov",
    "dec",
]


def latest_period(input_dir: Path) -> str:
    """Latest reference month in an SDMX LF extract, as ``"YYYY-MM"``.

    Zero-padded ``"YYYY-MM"`` sorts lexicographically in calendar order, so the
    max string is the newest month. Read from ``LF.csv`` — the ABS API and the
    Excel spreadsheets release the same reference month together.
    """
    df = pd.read_csv(input_dir / "LF.csv", dtype=str, na_filter=False)
    tcol = next(
        c for c in df.columns if c.split(":")[0].strip() == "TIME_PERIOD"
    )
    return max(v[:7] for v in df[tcol] if v)


def month_slug(period: str) -> str:
    """Map ``"YYYY-MM"`` to the ABS release slug, e.g. ``"2026-06" -> "jun-2026"``.

    A fixed English abbreviation table (not ``strftime("%b")``) keeps the slug
    locale-independent on the worker.
    """
    y, m = period.split("-")
    return f"{_MONTHS[int(m) - 1]}-{y}"


# ──────────────────────────────────────────────────────────────────────────────
# check_update / extract_and_load — one independent pair per table.
#
# Unlike au_ato_abr (a single ~1 GB ZIP pair that builds all 4 tables in one
# streaming pass, so every table's extract_and_load must redownload the whole
# thing), au_abs_labour_force's two source mechanisms already split cleanly by
# table: labour_force_status/underutilisation read only their own SDMX
# dataflows, hours_worked/status_in_employment read only their own Excel
# spreadsheets (see utils.py's module docstring). So each table's
# extract_and_load downloads exactly the source(s) it needs — no table fetches
# data it doesn't use, and there's no shared-download redundancy to trade off.
#
# Both sources publish the same reference month together, so a single
# get_latest_update gates all 4 tables — wired via `lambda _: get_latest_update`,
# same pattern as au_ato_abr.tasks. It reads only the "LF" SDMX dataflow (the
# smallest of the three), since all it needs is the latest TIME_PERIOD, not the
# full LF_AGES/LF_UNDER payload.
# ──────────────────────────────────────────────────────────────────────────────

# Per-table source requirements, keyed exactly like constants.DATA_TABLES.
_SDMX_FLOWS_BY_TABLE = {
    "labour_force_status": ["LF", "LF_AGES"],
    "underutilisation": ["LF_UNDER"],
}
_EXCEL_SLUGS_BY_TABLE = {
    "hours_worked": ["hours_worked"],
    "status_in_employment": ["status_national", "status_states"],
}
_CLEANERS = {
    "labour_force_status": clean_labour_force_status,
    "underutilisation": clean_underutilisation,
    "hours_worked": clean_hours_worked,
    "status_in_employment": clean_status_in_employment,
}


def get_latest_update() -> SourceInspection:
    """Resolve the ABS release's reference month from the small SDMX LF extract.

    Shared by all 4 tables — see the module banner.

    Returns:
        `SourceInspection` with `reference_date` set to the first day of the
        latest reference month (e.g. ``2026-06-01`` for ``"2026-06"``).
    """
    work_dir = Path(tempfile.mkdtemp(prefix="au_abs_labour_force_check_"))
    download_sdmx("LF", work_dir)
    y, m = (int(p) for p in latest_period(work_dir).split("-"))
    return SourceInspection(reference_date=date(y, m, 1))


def make_extract_load_data(table_id: str) -> Callable[[dict], ExtractAndLoad]:
    """Build `table_id`'s extract_and_load: download only its own source(s).

    See the module banner for why this table only fetches what it reads,
    rather than redownloading a shared payload.
    """

    def extract_load_data(download_params: dict) -> ExtractAndLoad:
        """Download `table_id`'s source(s), clean, write partitioned parquet.

        Args:
            download_params: ``{"reference_date": "<first day of the release
                month>"}``. SDMX-sourced tables (labour_force_status,
                underutilisation) ignore it — the ``/all`` query is
                month-agnostic and always returns the full history. Excel-
                sourced tables (hours_worked, status_in_employment) use it to
                resolve the month-stamped release path (e.g. ``"jun-2026"``).

        Returns:
            `ExtractAndLoad` with ``dump_mode="overwrite"``/
            ``source_format="parquet"`` — both sources ship the full history
            on every release, so every run is a full replace.
        """
        work_dir = Path(
            tempfile.mkdtemp(prefix=f"au_abs_labour_force_{table_id}_")
        )
        input_dir = work_dir / "input"

        for flow_id in _SDMX_FLOWS_BY_TABLE.get(table_id, []):
            download_sdmx(flow_id, input_dir)

        if table_id in _EXCEL_SLUGS_BY_TABLE:
            ref = date.fromisoformat(download_params["reference_date"])
            slug = month_slug(f"{ref.year}-{ref.month:02d}")
            wanted = {
                k: constants.EXCEL_FILES.value[k]
                for k in _EXCEL_SLUGS_BY_TABLE[table_id]
            }
            download_excel(slug, input_dir, files=wanted)

        df = _CLEANERS[table_id](input_dir)
        output_dir = write_partitioned(df, table_id, work_dir / "output")

        return ExtractAndLoad(
            data_path=str(output_dir),
            dump_mode="overwrite",
            source_format="parquet",
        )

    return extract_load_data


make_pipeline = pipeline_factory(
    DATASET_ID,
    # Single check shared by all 4 tables — see the module banner.
    lambda _table_id: get_latest_update,
    make_extract_load_data,
    date_format="%Y-%m",
)
