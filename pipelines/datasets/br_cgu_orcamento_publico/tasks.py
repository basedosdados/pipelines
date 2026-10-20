"""Prefect 3 tasks for br_cgu_orcamento_publico — thin wrappers over utils.py.

One table (``orcamento``), one check_update/extract_and_load pair. See the
module banner in ``constants.py`` for the full picture: the real check is a
single HEAD request (``probe_source``) compared against
``Table.Update.latest`` (``compare_against="table_update"``) — the exercise
year itself is not a useful freshness signal, since it moves once a year
while the file contents move continuously.

``extract_load_data`` re-downloads and re-cleans the whole history on every
run and returns ``dump_mode="overwrite"`` — the portal restates closed
exercises, so the table is always fully replaced, never appended to.
"""

import tempfile
from datetime import date
from pathlib import Path

from prefect import task

from pipelines.datasets.br_cgu_orcamento_publico.constants import (
    COVERAGE,
    constants,
)
from pipelines.datasets.br_cgu_orcamento_publico.utils import (
    clean_all,
    download_all,
    probe_latest,
)
from pipelines.utils.stage_dispatch import ExtractAndLoad, SourceInspection

DATASET_ID = constants.DATASET_ID.value
TABLE_ID = constants.TABLE_ID.value


@task(retries=2, retry_delay_seconds=300)
def probe_source() -> str:
    """Return when the portal last regenerated the exercise files, ``YYYY-MM-DD``.

    One HEAD request. The exercise year itself is useless as a freshness signal
    — it moves once a year while the file contents move continuously — so the
    poll compares this publication timestamp against ``Table.Update.latest``.

    Retries are spaced five minutes apart because the failure worth retrying is
    an AWS WAF rate block, which a fast retry only deepens.
    """
    return probe_latest()


@task(retries=2, retry_delay_seconds=600)
def download_orcamento(work_dir: str) -> dict:
    """Download every published exercise ZIP into ``<work_dir>/input``.

    Returns:
        ``{"input_dir": str, "years": [int, ...]}`` — the exercises the portal
        actually published, discovered by walking forward until one is missing.
    """
    input_dir = Path(work_dir) / "input"
    years = download_all(input_dir)
    return {"input_dir": str(input_dir), "years": years}


@task
def clean_orcamento(work_dir: str, input_dir: str, years: list[int]) -> dict:
    """Clean the downloaded exercises into hive-partitioned staging CSV.

    Returns:
        ``{"path": str, "rows": {year: n}, "total": n}`` — ``path`` is the
        table directory handed to ``upload_to_gcs``.
    """
    result = clean_all(Path(input_dir), Path(work_dir) / "output", years)
    return {**result, "path": str(result["path"])}


def get_latest_update() -> SourceInspection:
    """Costs a single HEAD (``probe_source``) to find the publication date.

    Returns:
        ``SourceInspection`` with the ZIP's ``Last-Modified`` date and
        ``compare_against="table_update"`` — a publication timestamp compared
        against a wall clock, not a competência compared against coverage.
    """
    max_modified = probe_source()
    return SourceInspection(
        reference_date=date.fromisoformat(max_modified),
        compare_against="table_update",
    )


def extract_load_data(download_params: dict) -> ExtractAndLoad:
    """Download the whole history again and clean it into one partitioned dir.

    Not cleaned up afterwards (no ``shutil.rmtree``/``finally``) — the
    returned ``data_path`` is only read (via ``upload_to_gcs``) right after
    this returns, inside the same flow run; the source is a handful of
    megabytes, so the leftover scratch directory is a non-issue (same
    trade-off as ``au_ato_abr``/``au_geoscape_gnaf``/``br_cgu_sancoes``).

    Args:
        download_params: received from check_update — unused here, the
            download always fetches the full published history (no filter by
            year or reference date).

    Returns:
        ``ExtractAndLoad`` with the table's coverage, the
        ``ano_exercicio=<year>``-partitioned directory, and
        ``dump_mode="overwrite"`` (the portal restates closed exercises, so
        every run replaces the table instead of appending).
    """
    work_dir = tempfile.mkdtemp(prefix="br_cgu_orcamento_publico_")

    downloaded = download_orcamento(work_dir=work_dir)
    result = clean_orcamento(
        work_dir=work_dir,
        input_dir=downloaded["input_dir"],
        years=downloaded["years"],
    )
    print(
        f"cleaned {len(result['rows'])} exercises, "
        f"{result['total']:,} rows: {result['rows']}"
    )

    return ExtractAndLoad(
        coverage=COVERAGE.model_dump(),
        data_path=result["path"],
        dump_mode="overwrite",
        source_format="csv",
    )
