"""Prefect 3 tasks for br_sfb_sicar — thin wrappers over utils.py.

Download and cleaning are split into two tasks that the flow interleaves per
UF x theme: download one zip, clean it to partitioned parquet, delete the zip.
Processing one state x theme at a time keeps peak disk near a single archive —
the APP theme alone is hundreds of MB per state.
"""

import os

from prefect import task
from SICAR import Sicar

from pipelines.constants import constants
from pipelines.crawler.sfb_sicar.utils import (
    build_table_df,
    filter_to_uf,
    read_theme_zip,
    release_dates_to_iso,
    retry_download_car,
    write_table_partitioned,
)


@task(
    retries=constants.TASK_MAX_RETRIES.value,
    retry_delay_seconds=constants.TASK_RETRY_DELAY.value,
)
def get_release_dates_task() -> dict:
    """Return ``{uf: 'YYYY-MM-DD'}`` of per-UF snapshot (release) dates.

    Reads SICAR's release-dates page and normalizes ``dd/mm/yyyy`` to ISO. This
    is the per-UF snapshot that becomes the ``data`` partition value and, via its
    maximum, the source ``max_date`` for the poll.
    """
    return release_dates_to_iso(Sicar().get_release_dates())


@task(
    retries=constants.TASK_MAX_RETRIES.value,
    retry_delay_seconds=constants.TASK_RETRY_DELAY.value,
)
def download_uf_theme(
    input_dir: str,
    sigla_uf: str,
    polygon: str,
    tries: int,
    max_retries: int,
) -> str:
    """Download one UF x theme zip; return its path (``<UF>_<POLYGON>.zip``).

    Args:
        input_dir: Directory to download into (created if absent).
        sigla_uf: Two-letter UF code.
        polygon: SICAR Polygon enum value (e.g. ``"AREA_IMOVEL"``).
        tries: Captcha attempts per download.
        max_retries: Read-timeout retries wrapping the download.

    Returns:
        The downloaded zip's path.
    """
    os.makedirs(input_dir, exist_ok=True)
    retry_download_car(
        car=Sicar(),
        state=sigla_uf,
        polygon=polygon,
        folder=input_dir,
        tries=tries,
        max_retries=max_retries,
    )
    return os.path.join(input_dir, f"{sigla_uf}_{polygon}.zip")


@task
def clean_uf_theme(
    zip_path: str,
    output_dir: str,
    table: str,
    snapshot_iso: str,
    sigla_uf: str,
) -> int:
    """Clean one UF x theme zip into partitioned parquet, then delete the zip.

    Reads all ``.shp`` parts, drops foreign-UF rows, maps to the architecture's
    all-string columns, and appends the ``data``/``sigla_uf`` partition. The zip
    is removed afterwards to keep peak disk bounded.

    Args:
        zip_path: The downloaded ``<UF>_<POLYGON>.zip``.
        output_dir: Parquet output root (``<output_dir>/<table>/...``).
        table: Output table slug.
        snapshot_iso: Per-UF release date, ``'YYYY-MM-DD'``.
        sigla_uf: UF code.

    Returns:
        The number of rows written for this UF x theme.
    """
    gdf = read_theme_zip(zip_path)
    gdf, dropped = filter_to_uf(gdf, sigla_uf)
    df = build_table_df(table, gdf, snapshot_iso, sigla_uf)
    write_table_partitioned(df, output_dir, table)
    n = len(df)
    print(
        f"{sigla_uf} {table}: rows={n} dropped_foreign_uf={dropped} "
        f"snapshot={snapshot_iso}"
    )
    del gdf, df
    if os.path.exists(zip_path):
        os.remove(zip_path)
    return n
